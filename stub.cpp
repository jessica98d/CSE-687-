// stub.cpp (Phase 4)
// Usage: stub.exe <port> <controllerHost> <controllerPort>
// Example: stub.exe 5001 127.0.0.1 6001
//
// Receives: SPAWN|MAP|m|R|tempDir|manifestPath
//   -> runs: mapper_worker.exe m R manifestPath tempDir controllerHost controllerPort
//
// Receives: SPAWN|REDUCE|r|M|tempDir|outputDir
//   -> runs: reducer_worker.exe r tempDir outputDir controllerHost controllerPort

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>

#pragma comment(lib, "Ws2_32.lib")

static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) out.push_back(item);
    return out;
}

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

static bool spawnProcess(const std::wstring& cmdLine) {
    STARTUPINFOW si{};
    si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};

    // CreateProcessW requires mutable buffer
    std::wstring mutableCmd = cmdLine;

    BOOL ok = CreateProcessW(
        nullptr,
        mutableCmd.data(),
        nullptr, nullptr,
        FALSE,
        CREATE_NO_WINDOW,   // stub stays quiet
        nullptr,
        nullptr,
        &si,
        &pi
    );

    if (!ok) {
        std::wcerr << L"[stub] CreateProcessW failed: " << GetLastError() << L"\n";
        return false;
    }

    CloseHandle(pi.hThread);
    CloseHandle(pi.hProcess);
    return true;
}

static std::wstring widen(const std::string& s) {
    if (s.empty()) return L"";
    int len = MultiByteToWideChar(CP_UTF8, 0, s.c_str(), (int)s.size(), nullptr, 0);
    std::wstring w(len, L'\0');
    MultiByteToWideChar(CP_UTF8, 0, s.c_str(), (int)s.size(), &w[0], len);
    return w;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: stub <port> <controllerHost> <controllerPort>\n";
        return 1;
    }

    int port = std::stoi(argv[1]);
    std::string controllerHost = argv[2];
    int controllerPort = std::stoi(argv[3]);

    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2,2), &wsa) != 0) return 1;

    SOCKET listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSock == INVALID_SOCKET) return 1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listenSock, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        std::cerr << "[stub] bind failed\n";
        return 1;
    }
    if (listen(listenSock, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "[stub] listen failed\n";
        return 1;
    }

    std::cout << "[stub] listening on port " << port
              << " (controller=" << controllerHost << ":" << controllerPort << ")\n";

    while (true) {
        SOCKET s = accept(listenSock, nullptr, nullptr);
        if (s == INVALID_SOCKET) continue;

        std::string line = recvLine(s);
        // expected: SPAWN|...
        auto parts = split(line, '|');

        bool ok = false;
        if (parts.size() >= 2 && parts[0] == "SPAWN") {
            if (parts[1] == "MAP" && parts.size() >= 6) {
                // SPAWN|MAP|m|R|tempDir|manifestPath
                std::string mapperId = parts[2];
                std::string R = parts[3];
                std::string tempDir = parts[4];
                std::string manifest = parts[5];

                // mapper_worker.exe <mapperId> <numReducers> <manifestPath> <intermDir> <controllerHost> <controllerPort>
                std::wstring cmd =
                    L"mapper_worker.exe " + widen(mapperId) + L" " + widen(R) + L" " +
                    widen(manifest) + L" " + widen(tempDir) + L" " +
                    widen(controllerHost) + L" " + widen(std::to_string(controllerPort));

                ok = spawnProcess(cmd);
            }
            else if (parts[1] == "REDUCE" && parts.size() >= 6) {
                // SPAWN|REDUCE|r|M|tempDir|outputDir
                std::string reducerId = parts[2];
                std::string tempDir = parts[4];
                std::string outDir = parts[5];

                // reducer_worker.exe <reducerId> <intermDir> <outputDir> <controllerHost> <controllerPort>
                std::wstring cmd =
                    L"reducer_worker.exe " + widen(reducerId) + L" " +
                    widen(tempDir) + L" " + widen(outDir) + L" " +
                    widen(controllerHost) + L" " + widen(std::to_string(controllerPort));

                ok = spawnProcess(cmd);
            }
        }

        const char* resp = ok ? "OK\n" : "ERR\n";
        send(s, resp, (int)strlen(resp), 0);
        closesocket(s);
    }

    // never reached
    closesocket(listenSock);
    WSACleanup();
    return 0;
}
