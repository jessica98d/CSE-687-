#include <string>
#include <vector>
#include <iomanip>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include "mr/FileManager.hpp"
#include "mr/Workflow.hpp"
#include <windows.h>
#include <sstream>
#include <iomanip>


static HFONT monoFont = (HFONT)GetStockObject(ANSI_FIXED_FONT);

using namespace std;
namespace fs = std::filesystem;


static const char* kInputDir  = "./sample_input";
static const char* kTempDir   = "./temp";
static const char* kOutputDir = "./output";


static HWND  gEdit  = nullptr;
static HFONT gFont  = nullptr;


static vector<pair<string,int>> readWordCountsFromOutput(const string& outDir)
{
    unordered_map<string,int> agg;

    if (!fs::exists(outDir)) return {};

    for (const auto& entry : fs::directory_iterator(outDir)) {
        if (!entry.is_regular_file()) continue;

        // Accept .txt and files without extension (some reducers do that)
        const auto& p = entry.path();
        if (p.has_extension() && p.extension() != ".txt") continue;

        ifstream in(p);
        if (!in) continue;

        string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;

            // Expect "word<TAB>count"
            auto tab = line.find('\t');
            if (tab == string::npos) {
                // Accept "word count" too (fallback)
                tab = line.find(' ');
            }
            if (tab == string::npos) continue;

            string w = line.substr(0, tab);
            string cstr = line.substr(tab + 1);

            // trim spaces
            auto ltrim = [](string& s){
                size_t i=0; while (i<s.size() && isspace((unsigned char)s[i])) ++i;
                s.erase(0,i);
            };
            auto rtrim = [](string& s){
                size_t i=s.size(); while (i>0 && isspace((unsigned char)s[i-1])) --i;
                s.erase(i);
            };
            ltrim(w); rtrim(w); ltrim(cstr); rtrim(cstr);

            try {
                int c = stoi(cstr);
                if (!w.empty()) agg[w] += c;
            } catch (...) {
                // ignore malformed rows
            }
        }
    }

    vector<pair<string,int>> rows(agg.begin(), agg.end());
    sort(rows.begin(), rows.end(),
         [](const auto& a, const auto& b){ return a.first < b.first; });
    return rows;
}

static void setEditText(const std::string& s) {
    SetWindowTextA(gEdit, s.c_str());
}


static void runAndDisplay()
{
    mr::FileManager fm;
    mr::Workflow wf(fm, kInputDir, kTempDir, kOutputDir);
    wf.run();

    auto rows = readWordCountsFromOutput(kOutputDir);

    std::ostringstream oss;
    if (rows.empty()) {
        oss << "No words found (check input files and output folder).\r\n";
        setEditText(oss.str());
        return;
    }

    // Header
    oss << "+----------------------+-------+\r\n";
    oss << "| Word                 | Count |\r\n";
    oss << "+----------------------+-------+\r\n";

    // Body
    int total = 0;
    for (const auto& [w, c] : rows) {
        oss << "| " << std::left << std::setw(20) << w
            << " | " << std::right << std::setw(5) << c << " |\r\n";
        total += c;
    }

    // Footer
    oss << "+----------------------+-------+\r\n";
    oss << "| TOTAL WORD COUNT:    | "
        << std::right << std::setw(5) << total << " |\r\n";
    oss << "+----------------------+-------+\r\n";

    setEditText(oss.str());
}




// ---------------- Win32 UI ----------------
LRESULT CALLBACK WndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    switch (msg) {
    case WM_CREATE: {
        CreateWindowW(L"BUTTON", L"Run MapReduce",
                      WS_TABSTOP | WS_VISIBLE | WS_CHILD | BS_DEFPUSHBUTTON,
                      10, 10, 160, 32, hWnd, (HMENU)1, nullptr, nullptr);

        gEdit = CreateWindowW(L"EDIT", L"",
                              WS_CHILD | WS_VISIBLE | WS_VSCROLL |
                              ES_LEFT | ES_MULTILINE | ES_AUTOVSCROLL | ES_READONLY,
                              10, 50, 940, 630, hWnd, nullptr, nullptr, nullptr);

        // Monospace font (Consolas)
        gFont = CreateFontW(
            18, 0, 0, 0, FW_NORMAL, FALSE, FALSE, FALSE,
            DEFAULT_CHARSET, OUT_DEFAULT_PRECIS, CLIP_DEFAULT_PRECIS, DEFAULT_QUALITY,
            FIXED_PITCH | FF_MODERN, L"Consolas");
        SendMessageW(gEdit, WM_SETFONT, (WPARAM)gFont, TRUE);
        break;
    }
    case WM_COMMAND:
        if (LOWORD(wParam) == 1) {
            runAndDisplay();
        }
        break;
    case WM_DESTROY:
        PostQuitMessage(0);
        break;
    default:
        return DefWindowProc(hWnd, msg, wParam, lParam);
    }
    return 0;
}

int WINAPI wWinMain(HINSTANCE hInst, HINSTANCE, PWSTR, int nShowCmd)
{
    const wchar_t CLASS_NAME[] = L"MRGUIWin32";

    WNDCLASS wc{};
    wc.lpfnWndProc   = WndProc;
    wc.hInstance     = hInst;
    wc.lpszClassName = CLASS_NAME;

    RegisterClass(&wc);

    HWND hWnd = CreateWindowEx(
        0, CLASS_NAME, L"MapReduce Phase 1 – Word Count (GUI)",
        WS_OVERLAPPEDWINDOW,
        CW_USEDEFAULT, CW_USEDEFAULT, 980, 750,
        nullptr, nullptr, hInst, nullptr);

    ShowWindow(hWnd, nShowCmd);

    MSG msg{};
    while (GetMessage(&msg, nullptr, 0, 0)) {
        TranslateMessage(&msg);
        DispatchMessage(&msg);
    }
    return 0;
}
