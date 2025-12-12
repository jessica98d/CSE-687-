// mapper_worker.cpp (Phase 4)

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")

#include "mr/FileManager.hpp"
#include "mr/Mapper.hpp"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

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

static bool connectAndHandshakeMap(const std::string& host, int port, int mapperId) {
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

    // HELLO|MAP|<id>
    std::string hello = "HELLO|MAP|" + std::to_string(mapperId) + "\n";
    send(s, hello.c_str(), (int)hello.size(), 0);

    // wait BEGIN
    std::string resp = recvLine(s);
    closesocket(s);
    WSACleanup();

    return (resp.find("BEGIN") == 0);
}

static std::vector<std::string> readManifest(const std::string& manifestPath) {
    std::vector<std::string> files;
    std::ifstream in(manifestPath);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) files.push_back(line);
    }
    return files;
}

int main(int argc, char** argv) {
    // mapper_worker.exe <mapperId> <numReducers> <manifestPath> <intermDir> <controllerHost> <controllerPort>
    if (argc < 7) {
        std::cerr << "Usage: mapper_worker <mapperId> <numReducers> <manifestPath> <intermDir> <controllerHost> <controllerPort>\n";
        return 1;
    }

    int mapperId = std::stoi(argv[1]);
    int numReducers = std::stoi(argv[2]);
    std::string manifestPath = argv[3];
    std::string intermDir = argv[4];
    std::string controllerHost = argv[5];
    int controllerPort = std::stoi(argv[6]);

    if (!connectAndHandshakeMap(controllerHost, controllerPort, mapperId)) {
        std::cerr << "[mapper_worker] handshake failed\n";
        return 1;
    }

    mr::FileManager fm;
    const std::size_t flushThreshold = 1000;
    mr::Mapper mapper(fm, intermDir, flushThreshold, mapperId, numReducers);

    auto files = readManifest(manifestPath);
    if (files.empty()) {
        std::cerr << "[mapper_worker] manifest empty: " << manifestPath << "\n";
        mapper.flush();
        return 0;
    }

    for (const auto& path : files) {
        auto lines = fm.readAllLines(path);
        for (const auto& line : lines) {
            mapper.map(path, line);
        }
    }

    mapper.flush();
    return 0;
}
