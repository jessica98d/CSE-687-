// stub_server.cpp (Phase 4 Stub)
// Listens for SPAWN commands from controller; spawns mapper_worker.exe / reducer_worker.exe
// Build on Windows: link Ws2_32

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <filesystem>

#pragma comment(lib, "Ws2_32.lib")
namespace fs = std::filesystem;

static std::vector<std::string> split(const std::string& s, char delim='|') {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) out.push_back(item);
    return out;
}

static std::wstring s2ws(const std::string& s) {
    int len = MultiByteToWideChar(CP_UTF8, 0, s.c_str(), (int)s.size(), nullptr, 0);
    std::wstring w(len, L'\0');
    MultiByteToWideChar(CP_UTF8, 0, s.c_str(), (int)s.size(), w.data(), len);
    return w;
}

static std::wstring quote(const std::wstring& w) {
    return L"\"" + w + L"\"";
}

static bool spawnProcess(const std::wstring& exe, const std::wstring& args, DWORD& pidOut) {
    std::wstring cmd = quote(exe) + L" " + args;
    std::vector<wchar_t> buf(cmd.begin(), cmd.end());
    buf.push_back(L'\0');

    STARTUPINFOW si{};
    si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};

    if (!CreateProcessW(nullptr, buf.data(), nullptr, nullptr, FALSE, 0, nullptr, nullptr, &si, &pi))
        return false;

    pidOut = pi.dwProcessId;
    CloseHandle(pi.hThread);
    CloseHandle(pi.hProcess);
    return true;
}

int main(int argc, char** argv) {
    // Usage: stub_server.exe <listenPort> <binDir>
    // binDir must contain mapper_worker.exe and reducer_worker.exe
    int port = (argc >= 2) ? std::stoi(argv[1]) : 5001;
    fs::path binDir = (argc >= 3) ? fs::path(argv[2]) : fs::current_path();

    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2,2), &wsa) != 0) return 1;

    SOCKET listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSock == INVALID_SOCKET) return 1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listenSock, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) return 1;
    if (listen(listenSock, SOMAXCONN) == SOCKET_ERROR) return 1;

    std::cout << "[stub] Listening on port " << port
              << " | binDir=" << binDir.string() << "\n";

    for (;;) {
        SOCKET client = accept(listenSock, nullptr, nullptr);
        if (client == INVALID_SOCKET) continue;

        char buf[4096];
        int n = recv(client, buf, sizeof(buf)-1, 0);
        if (n <= 0) { closesocket(client); continue; }
        buf[n] = '\0';
        std::string line(buf);
        // trim CRLF
        while (!line.empty() && (line.back()=='\n' || line.back()=='\r')) line.pop_back();

        auto parts = split(line);
        if (parts.size() < 2 || parts[0] != "SPAWN") {
            std::string resp = "ERR|BadCommand\n";
            send(client, resp.c_str(), (int)resp.size(), 0);
            closesocket(client);
            continue;
        }

        std::string kind = parts[1]; // MAP or REDUCE
        fs::path exePath = (kind == "MAP")
            ? (binDir / "mapper_worker.exe")
            : (binDir / "reducer_worker.exe");

        if (!fs::exists(exePath)) {
            std::string resp = "ERR|WorkerExeMissing\n";
            send(client, resp.c_str(), (int)resp.size(), 0);
            closesocket(client);
            continue;
        }

        // Rebuild worker args based on message:
        // SPAWN|MAP|mapperId|numReducers|tempDir|manifest
        // SPAWN|REDUCE|reducerId|numMappers|tempDir|outputDir
        std::wstring wargs;
        {
            std::wstringstream wss;
            for (size_t i = 2; i < parts.size(); ++i) {
                std::wstring w = s2ws(parts[i]);
                // quote path-like args (heuristic)
                if (parts[i].find(':') != std::string::npos || parts[i].find('\\') != std::string::npos || parts[i].find('/') != std::string::npos)
                    wss << quote(w);
                else
                    wss << w;
                if (i + 1 < parts.size()) wss << L" ";
            }
            wargs = wss.str();
        }

        DWORD pid = 0;
        bool ok = spawnProcess(exePath.wstring(), wargs, pid);
        std::string resp = ok ? ("OK|" + std::to_string(pid) + "\n") : "ERR|CreateProcessFailed\n";
        send(client, resp.c_str(), (int)resp.size(), 0);
        closesocket(client);
    }

    closesocket(listenSock);
    WSACleanup();
    return 0;
}
