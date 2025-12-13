// reducer_worker.cpp (Phase 4)

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")

#include "mr/FileManager.hpp"
#include "mr/Reducer.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <sstream>
#include <cstdlib>   // _putenv_s

static std::string recvLine(SOCKET s) {
    std::string out;
    char ch = 0;
    while (true) {
        int n = recv(s, &ch, 1, 0);
        if (n <= 0) break;
        if (ch == '\n') break;
        out.push_back(ch);
    }
    return out;
}

static bool connectAndHandshakeReduce(const std::string& host, int port, int reducerId) {
    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2,2), &wsa) != 0) return false;

    addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) return false;

    SOCKET s = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (s == INVALID_SOCKET) { freeaddrinfo(res); return false; }

    if (connect(s, res->ai_addr, (int)res->ai_addrlen) == SOCKET_ERROR) {
        closesocket(s); freeaddrinfo(res); return false;
    }
    freeaddrinfo(res);

    // ✅ PRINT #1: connected
    std::cout << "[reducer_worker] connected to controller "
              << host << ":" << port << "\n";

    // HELLO|REDUCE|<id>
    std::string hello = "HELLO|REDUCE|" + std::to_string(reducerId) + "\n";
    send(s, hello.c_str(), (int)hello.size(), 0);

    // wait BEGIN
    std::string resp = recvLine(s);

    // ✅ PRINT #2: what we actually got
    std::cout << "[reducer_worker] received from controller: '" << resp << "'\n";

    closesocket(s);
    WSACleanup();

    if (resp.find("BEGIN") == 0) {
        std::cout << "[reducer_worker] received BEGIN\n";
        return true;
    }
    return false;
}


int main(int argc, char** argv) {
    // reducer_worker.exe <reducerId> <intermDir> <outputDir> <controllerHost> <controllerPort>
    if (argc < 6) {
        std::cerr << "Usage: reducer_worker <reducerId> <intermDir> <outputDir> <controllerHost> <controllerPort>\n";
        return 1;
    }

    int reducerId = std::stoi(argv[1]);
    std::string intermDir = argv[2];
    std::string outputDir = argv[3];
    std::string controllerHost = argv[4];
    int controllerPort = std::stoi(argv[5]);

    if (!connectAndHandshakeReduce(controllerHost, controllerPort, reducerId)) {
        std::cerr << "[reducer_worker] handshake failed\n";
        return 1;
    }

#ifdef _WIN32
    // Makes Reducer write word_counts_rX.txt and SUCCESS_rX (via MR_OUTFILE_SUFFIX)
    const std::string suffix = "_r" + std::to_string(reducerId);
    _putenv_s("MR_OUTFILE_SUFFIX", suffix.c_str());
#endif

    mr::FileManager fm;
    mr::Reducer reducer(fm, outputDir);

    std::unordered_map<std::string, std::vector<int>> grouped;
    auto files = fm.listTextFiles(intermDir);

    const std::string suffixFile = "_r" + std::to_string(reducerId) + ".txt";

    for (const auto& path : files) {
        if (path.size() < suffixFile.size() ||
            path.substr(path.size() - suffixFile.size()) != suffixFile) {
            continue;
        }

        auto lines = fm.readAllLines(path);
        for (const auto& line : lines) {
            if (line.empty()) continue;

            std::istringstream iss(line);
            std::string word;
            int count = 0;
            if (!(iss >> word >> count)) continue;

            grouped[word].push_back(count);
        }
    }

    for (auto& kv : grouped) {
        reducer.reduce(kv.first, kv.second);
    }

    reducer.markSuccess();
    return 0;
}
