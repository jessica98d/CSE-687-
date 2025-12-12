// phase4_controller.cpp (Phase 4)
// Usage:
//   mapreduce_phase4 <inputDir> <tempDir> <outputDir> <stubHost:port>[,<stubHost:port>...] [mappers] [reducers]
// Example:
//   mapreduce_phase4 sample_input temp output 127.0.0.1:5001 2 2

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
#include <cstring>   // strlen

#pragma comment(lib, "Ws2_32.lib")
namespace fs = std::filesystem;

static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) out.push_back(item);
    return out;
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

    char buf[2048];
    int n = recv(s, buf, (int)sizeof(buf) - 1, 0);
    if (n > 0) { buf[n] = 0; response = buf; }

    closesocket(s);
    return true;
}

// Controller listens for workers to connect for HELLO/HEARTBEAT.
// Minimal implementation: accept, read HELLO, immediately send BEGIN.
static SOCKET startHeartbeatServer(int port) {
    SOCKET listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSock == INVALID_SOCKET) return INVALID_SOCKET;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listenSock, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) return INVALID_SOCKET;
    if (listen(listenSock, SOMAXCONN) == SOCKET_ERROR) return INVALID_SOCKET;
    return listenSock;
}

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
    int numMappers  = (argc >= 6) ? std::max(1, std::stoi(argv[5])) : 2;
    int numReducers = (argc >= 7) ? std::max(1, std::stoi(argv[6])) : 2;

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
        assigns[i % numMappers].push_back(inputs[i]);

    const int controllerPort = 6001;
    SOCKET hbListen = startHeartbeatServer(controllerPort);
    if (hbListen == INVALID_SOCKET) {
        std::cerr << "[controller] Failed to open heartbeat server.\n";
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

    // Tell stubs to SPAWN mappers
    for (int m = 0; m < numMappers; ++m) {
        fs::path manifest = tempDir / ("mapper_input_" + std::to_string(m) + ".txt");
        if (!writeManifest(manifest, assigns[m])) {
            std::cerr << "[controller] Failed to write manifest.\n";
            closesocket(hbListen);
            WSACleanup();
            return 1;
        }

        auto [host,port] = chooseStub(m);
        std::ostringstream cmd;
        cmd << "SPAWN|MAP|" << m << "|" << numReducers
            << "|" << tempDir.string()
            << "|" << manifest.string();

        std::string resp;
        if (!tcpSendLine(host, port, cmd.str(), resp)) {
            std::cerr << "[controller] Stub unreachable: " << host << ":" << port << "\n";
            closesocket(hbListen);
            WSACleanup();
            return 1;
        }
        std::cout << "[controller] Stub response: " << resp;
    }

    // Accept mapper HELLO and send BEGIN
    int mapperBegun = 0;
    while (mapperBegun < numMappers) {
        SOCKET s = accept(hbListen, nullptr, nullptr);
        if (s == INVALID_SOCKET) continue;

        char buf[1024];
        int n = recv(s, buf, (int)sizeof(buf) - 1, 0);
        if (n > 0) {
            buf[n] = 0;
            std::string msg(buf);
            if (msg.rfind("HELLO|MAP|", 0) == 0) {
                const char* begin = "BEGIN\n";
                send(s, begin, (int)strlen(begin), 0);
                mapperBegun++;
            }
        }
        closesocket(s);
    }

    // Tell stubs to SPAWN reducers
    for (int r = 0; r < numReducers; ++r) {
        auto [host,port] = chooseStub(r);
        std::ostringstream cmd;
        cmd << "SPAWN|REDUCE|" << r << "|" << numMappers
            << "|" << tempDir.string()
            << "|" << outputDir.string();

        std::string resp;
        if (!tcpSendLine(host, port, cmd.str(), resp)) {
            std::cerr << "[controller] Stub unreachable for reducer.\n";
            closesocket(hbListen);
            WSACleanup();
            return 1;
        }
        std::cout << "[controller] Stub response: " << resp;
    }

    // Accept reducer HELLO and send BEGIN
    int reducerBegun = 0;
    while (reducerBegun < numReducers) {
        SOCKET s = accept(hbListen, nullptr, nullptr);
        if (s == INVALID_SOCKET) continue;

        char buf[1024];
        int n = recv(s, buf, (int)sizeof(buf) - 1, 0);
        if (n > 0) {
            buf[n] = 0;
            std::string msg(buf);
            if (msg.rfind("HELLO|REDUCE|", 0) == 0) {
                const char* begin = "BEGIN\n";
                send(s, begin, (int)strlen(begin), 0);
                reducerBegun++;
            }
        }
        closesocket(s);
    }

    // Controller writes SUCCESS marker
    {
        std::ofstream out((outputDir / "SUCCESS").string(), std::ios::trunc | std::ios::binary);
    }

    closesocket(hbListen);
    WSACleanup();
    return 0;
}
