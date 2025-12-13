// phase4_controller.cpp (Phase 4 Controller - updated handshake-safe version)
// Usage:
//   mapreduce_phase4.exe <inputDir> <tempDir> <outputDir> <stubHost:port>[,<stubHost:port>...] [mappers] [reducers]
// Example:
//   mapreduce_phase4.exe sample_input temp output 127.0.0.1:5001 2 2

#include <unordered_map>
#include <chrono>
#include <thread>

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>

#pragma comment(lib, "Ws2_32.lib")
namespace fs = std::filesystem;

// ------------------- helpers -------------------
static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) out.push_back(item);
    return out;
}

static bool startsWith(const std::string& s, const char* prefix) {
    return s.rfind(prefix, 0) == 0;
}

static std::vector<fs::path> listTextFiles(const fs::path& inputDir) {
    std::vector<fs::path> files;
    if (!fs::exists(inputDir)) return files;
    for (auto& e : fs::directory_iterator(inputDir)) {
        if (!e.is_regular_file()) continue;
        auto p = e.path();
        if (!p.has_extension() || p.extension() == ".txt")
            files.push_back(fs::absolute(p));
    }
    return files;
}

static bool writeManifest(const fs::path& manifestPath, const std::vector<fs::path>& files) {
    std::ofstream out(manifestPath.string(), std::ios::trunc);
    if (!out) return false;
    for (auto& p : files) out << p.string() << "\n";
    return true;
}

// Read until '\n' (TCP-safe for short line messages)
static std::string recvLine(SOCKET s) {
    std::string out;
    char ch = 0;
    while (true) {
        int n = recv(s, &ch, 1, 0);
        if (n <= 0) break;
        if (ch == '\n') break;
        if (ch != '\r') out.push_back(ch);
        if (out.size() > 8192) break;
    }
    return out;
}

static bool tcpSendLine(const std::string& host, int port, const std::string& line, std::string& response) {
    response.clear();

    addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) return false;

    SOCKET s = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (s == INVALID_SOCKET) { freeaddrinfo(res); return false; }

    if (connect(s, res->ai_addr, (int)res->ai_addrlen) == SOCKET_ERROR) {
        closesocket(s);
        freeaddrinfo(res);
        return false;
    }
    freeaddrinfo(res);

    std::string payload = line + "\n";
    send(s, payload.c_str(), (int)payload.size(), 0);

    response = recvLine(s);  // expect "OK" or "ERR"
    closesocket(s);
    return true;
}

static SOCKET startHeartbeatServer(int port) {
    SOCKET listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSock == INVALID_SOCKET) return INVALID_SOCKET;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    int yes = 1;
    setsockopt(listenSock, SOL_SOCKET, SO_REUSEADDR, (const char*)&yes, sizeof(yes));

    if (bind(listenSock, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) return INVALID_SOCKET;
    if (listen(listenSock, SOMAXCONN) == SOCKET_ERROR) return INVALID_SOCKET;
    return listenSock;
}


//Wait for reducer success markers
static bool waitForReducerSuccessFiles(const fs::path& outputDir, int numReducers) {
    // Wait until SUCCESS_r0 ... SUCCESS_r{R-1} exist
    // (No hard timeout here; you can add one if you want)
    while (true) {
        bool all = true;
        for (int r = 0; r < numReducers; ++r) {
            fs::path p = outputDir / ("SUCCESS_r" + std::to_string(r));
            if (!fs::exists(p)) { all = false; break; }
        }
        if (all) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

//Merge reducer outputs into word_counts.txt (aggregate + sort)
static bool mergeReducerOutputs(const fs::path& outputDir, int numReducers) {
    std::unordered_map<std::string, long long> agg;

    for (int r = 0; r < numReducers; ++r) {
        fs::path inPath = outputDir / ("word_counts_r" + std::to_string(r) + ".txt");
        std::ifstream in(inPath.string());
        if (!in) {
            std::cerr << "[controller] Missing reducer output: " << inPath.string() << "\n";
            return false;
        }

        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;

            // expect: word<TAB>count (or word count)
            auto tab = line.find('\t');
            if (tab == std::string::npos) tab = line.find(' ');
            if (tab == std::string::npos) continue;

            std::string w = line.substr(0, tab);
            std::string cstr = line.substr(tab + 1);

            try {
                long long c = std::stoll(cstr);
                if (!w.empty()) agg[w] += c;
            } catch (...) {
                // ignore malformed rows
            }
        }
    }

    std::vector<std::pair<std::string, long long>> rows(agg.begin(), agg.end());
    std::sort(rows.begin(), rows.end(),
              [](auto& a, auto& b){ return a.first < b.first; });

    fs::path outPath = outputDir / "word_counts.txt";
    std::ofstream out(outPath.string(), std::ios::trunc);
    if (!out) return false;

    for (auto& [w, c] : rows) {
        out << w << "\t" << c << "\n";
    }

    std::cout << "[controller] Wrote merged output: " << outPath.string() << "\n";
    return true;
}


// ------------------- main -------------------
int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "Usage:\n"
                  << "  mapreduce_phase4 <inputDir> <tempDir> <outputDir> <stubHost:port>[,...] [mappers] [reducers]\n";
        return 1;
    }

    fs::path inputDir  = fs::absolute(argv[1]);
    fs::path tempDir   = fs::absolute(argv[2]);
    fs::path outputDir = fs::absolute(argv[3]);

    std::string stubsCsv = argv[4];
    int numMappers  = (argc >= 6) ? (std::max)(1, std::stoi(argv[5])) : 2;
    int numReducers = (argc >= 7) ? (std::max)(1, std::stoi(argv[6])) : 2;

    auto stubSpecs = split(stubsCsv, ',');

    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2,2), &wsa) != 0) return 1;

    fs::create_directories(tempDir);
    fs::create_directories(outputDir);

    auto inputs = listTextFiles(inputDir);
    if (inputs.empty()) {
        std::cerr << "[controller] No input files found.\n";
        WSACleanup();
        return 1;
    }

    // Partition files across mappers
    std::vector<std::vector<fs::path>> assigns(numMappers);
    for (size_t i = 0; i < inputs.size(); ++i)
        assigns[i % (size_t)numMappers].push_back(inputs[i]);

    const int controllerPort = 6001;
    SOCKET hbListen = startHeartbeatServer(controllerPort);
    if (hbListen == INVALID_SOCKET) {
        std::cerr << "[controller] Failed to open heartbeat server on port " << controllerPort << ".\n";
        WSACleanup();
        return 1;
    }
    std::cout << "[controller] Heartbeat server listening on port " << controllerPort << "\n";

    auto chooseStub = [&](int idx) -> std::pair<std::string,int> {
        auto sp = split(stubSpecs[idx % (int)stubSpecs.size()], ':');
        std::string host = sp[0];
        int port = (sp.size() > 1) ? std::stoi(sp[1]) : 5001;
        return {host, port};
    };

    // ------------- Tell stubs to SPAWN mappers -------------
    for (int m = 0; m < numMappers; ++m) {
        fs::path manifest = tempDir / ("mapper_input_" + std::to_string(m) + ".txt");
        if (!writeManifest(manifest, assigns[m])) {
            std::cerr << "[controller] Failed to write manifest: " << manifest.string() << "\n";
            closesocket(hbListen);
            WSACleanup();
            return 1;
        }

        auto [host,port] = chooseStub(m);
        std::ostringstream cmd;
        cmd << "SPAWN|MAP|" << m << "|" << numReducers
            << "|" << tempDir.string()
            << "|" << manifest.string()
            << "|" << "127.0.0.1"         // controller host for HELLO (local)
            << "|" << controllerPort;     // controller port for HELLO

        std::string resp;
        if (!tcpSendLine(host, port, cmd.str(), resp)) {
            std::cerr << "[controller] Stub unreachable: " << host << ":" << port << "\n";
            closesocket(hbListen);
            WSACleanup();
            return 1;
        }
        std::cout << "[controller] Stub response: " << resp << "\n";
    }

    // Accept mapper HELLO connections and send BEGIN
    int mapperBegun = 0;
    while (mapperBegun < numMappers) {
        SOCKET s = accept(hbListen, nullptr, nullptr);
        if (s == INVALID_SOCKET) continue;

        std::string msg = recvLine(s);
        if (startsWith(msg, "HELLO|MAP|")) {
            const char* begin = "BEGIN\n";
            send(s, begin, (int)strlen(begin), 0);
            mapperBegun++;
        } else {
            std::cout << "[controller] Ignored msg: '" << msg << "'\n";
        }
        closesocket(s);
    }

    // ------------- Tell stubs to SPAWN reducers -------------
    for (int r = 0; r < numReducers; ++r) {
        auto [host,port] = chooseStub(r);
        std::ostringstream cmd;
        cmd << "SPAWN|REDUCE|" << r << "|" << numMappers
            << "|" << tempDir.string()
            << "|" << outputDir.string()
            << "|" << "127.0.0.1"
            << "|" << controllerPort;

        std::string resp;
        if (!tcpSendLine(host, port, cmd.str(), resp)) {
            std::cerr << "[controller] Stub unreachable for reducer: " << host << ":" << port << "\n";
            closesocket(hbListen);
            WSACleanup();
            return 1;
        }
        std::cout << "[controller] Stub response: " << resp << "\n";
    }

    // Accept reducer HELLO connections and send BEGIN
    int reducerBegun = 0;
    while (reducerBegun < numReducers) {
        SOCKET s = accept(hbListen, nullptr, nullptr);
        if (s == INVALID_SOCKET) continue;

        std::string msg = recvLine(s);
        if (startsWith(msg, "HELLO|REDUCE|")) {
            const char* begin = "BEGIN\n";
            send(s, begin, (int)strlen(begin), 0);
            reducerBegun++;
        } else {
            std::cout << "[controller] Ignored msg: '" << msg << "'\n";
        }
        closesocket(s);
    }
	
	// Wait for reducers to finish (they create SUCCESS_rX)
	waitForReducerSuccessFiles(outputDir, numReducers);

	// Merge reducer outputs into word_counts.txt
	if (!mergeReducerOutputs(outputDir, numReducers)) {
		std::cerr << "[controller] Final merge failed.\n";
		closesocket(hbListen);
		WSACleanup();
		return 1;
}



    // SUCCESS marker (controller responsibility)
    {
        std::ofstream out((outputDir / "SUCCESS").string(), std::ios::trunc | std::ios::binary);
    }

    closesocket(hbListen);
    WSACleanup();
    std::cout << "[controller] Done.\n";
    return 0;
}
