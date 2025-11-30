#define NOMINMAX    
#include <windows.h>
#include <commdlg.h>
#include <shlobj.h>

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iomanip>    
#include <utility>
#include <algorithm>


#include "MapReduceController.h"
#include "Logger.h"

#pragma comment(lib, "User32.lib")
#pragma comment(lib, "Comdlg32.lib")
#pragma comment(lib, "Shell32.lib")

// Control IDs
constexpr int IDC_BUTTON_SELFILE   = 101;
constexpr int IDC_BUTTON_SELFOLDER = 102;
constexpr int IDC_BUTTON_RUN       = 103;
constexpr int IDC_BUTTON_CLEAR     = 104;
constexpr int IDC_EDIT_OUTPUT      = 105;

// Globals
Logger g_logger;
std::string g_selectedPath;
HFONT g_hMonoFont = nullptr;

// ----------------------------------------------------------------------
// Small helpers
// ----------------------------------------------------------------------

void SetControlText(HWND hWnd, int controlId, const std::string& text) {
    HWND hCtrl = GetDlgItem(hWnd, controlId);
    if (hCtrl != nullptr) {
        SetWindowTextA(hCtrl, text.c_str());
    }
}

std::string GetControlText(HWND hWnd, int controlId) {
    HWND hCtrl = GetDlgItem(hWnd, controlId);
    if (hCtrl == nullptr) {
        return {};
    }
    int len = GetWindowTextLengthA(hCtrl);
    if (len <= 0) {
        return {};
    }
    std::string buffer;
    buffer.resize(static_cast<std::size_t>(len));
    GetWindowTextA(hCtrl, &buffer[0], len + 1);
    return buffer;
}

// Trim spaces and quotes from both ends of a path string.
std::string TrimPath(const std::string& raw) {
    std::size_t start = 0;
    std::size_t end = raw.size();

    while (start < end &&
           (raw[start] == ' ' || raw[start] == '\t' || raw[start] == '"')) {
        ++start;
    }
    while (end > start &&
           (raw[end - 1] == ' ' || raw[end - 1] == '\t' || raw[end - 1] == '"')) {
        --end;
    }
    return raw.substr(start, end - start);
}

// ----------------------------------------------------------------------
// File / folder browse helpers
// ----------------------------------------------------------------------

std::string BrowseForFile(HWND owner) {
    char szFile[MAX_PATH] = {};
    OPENFILENAMEA ofn{};
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = owner;
    ofn.lpstrFile = szFile;
    ofn.nMaxFile = MAX_PATH;
    ofn.lpstrFilter = "Text Files\0*.txt\0All Files\0*.*\0";
    ofn.nFilterIndex = 1;
    ofn.Flags = OFN_PATHMUSTEXIST | OFN_FILEMUSTEXIST | OFN_EXPLORER;

    if (GetOpenFileNameA(&ofn)) {
        return std::string(szFile);
    }
    return {};
}

std::string BrowseForFolder(HWND owner) {
    BROWSEINFOA bi{};
    bi.hwndOwner = owner;
    bi.lpszTitle = "Select input folder";
    bi.ulFlags = BIF_RETURNONLYFSDIRS | BIF_NEWDIALOGSTYLE;

    LPITEMIDLIST pidl = SHBrowseForFolderA(&bi);
    if (pidl == nullptr) {
        return {};
    }

    char path[MAX_PATH] = {};
    if (SHGetPathFromIDListA(pidl, path)) {
        CoTaskMemFree(pidl);
        return std::string(path);
    }

    CoTaskMemFree(pidl);
    return {};
}

// ----------------------------------------------------------------------
// Build ASCII table from CSV (word, count) file
// ----------------------------------------------------------------------
std::string BuildAsciiTableFromCsv(const std::string& csvPath) {
    std::ifstream in(csvPath);
    if (!in.is_open()) {
        return std::string("No output file found at: ") + csvPath + "\r\n";
    }

    std::vector<std::pair<std::string, std::size_t>> rows;
    std::string line;
    std::size_t totalCount = 0;

    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::size_t commaPos = line.find(',');
        if (commaPos == std::string::npos) {
            continue;
        }

        std::string word = line.substr(0, commaPos);
        std::string countStr = line.substr(commaPos + 1);

        std::size_t count = 0;
        std::istringstream iss(countStr);
        iss >> count;

        rows.emplace_back(word, count);
        totalCount += count;
    }

    if (rows.empty()) {
        return "No data found in output CSV.\r\n";
    }

    // --- determine column widths without std::max/std::setw ---
    std::size_t wordWidth = 4;   // "Word"
    std::size_t countWidth = 5;  // "Count"

    for (const auto& p : rows) {
        if (p.first.size() > wordWidth) {
            wordWidth = p.first.size();
        }
        std::string countStr = std::to_string(p.second);
        if (countStr.size() > countWidth) {
            countWidth = countStr.size();
        }
    }

    auto repeatChar = [](char c, std::size_t n) {
        return std::string(n, c);
    };

    auto padRight = [](const std::string& s, std::size_t width) {
        if (s.size() >= width) {
            return s;
        }
        return s + std::string(width - s.size(), ' ');
    };

    std::ostringstream out;

    // Top border
    out << "+" << repeatChar('-', wordWidth + 2)
        << "+" << repeatChar('-', countWidth + 2)
        << "+\r\n";

    // Header row
    out << "| " << padRight("Word", wordWidth)
        << " | " << padRight("Count", countWidth)
        << " |\r\n";

    // Header separator
    out << "+" << repeatChar('-', wordWidth + 2)
        << "+" << repeatChar('-', countWidth + 2)
        << "+\r\n";

    // Data rows
    for (const auto& p : rows) {
        std::string countStr = std::to_string(p.second);
        out << "| " << padRight(p.first, wordWidth)
            << " | " << padRight(countStr, countWidth)
            << " |\r\n";
    }

    // Separator before total
    out << "+" << repeatChar('-', wordWidth + 2)
        << "+" << repeatChar('-', countWidth + 2)
        << "+\r\n";

    // Total row
    std::string totalLabel = "TOTAL WORD COUNT:";
    if (totalLabel.size() > wordWidth) {
        totalLabel.resize(wordWidth);
    }
    std::string totalStr = std::to_string(totalCount);

    out << "| " << padRight(totalLabel, wordWidth)
        << " | " << padRight(totalStr, countWidth)
        << " |\r\n";

    // Bottom border
    out << "+" << repeatChar('-', wordWidth + 2)
        << "+" << repeatChar('-', countWidth + 2)
        << "+\r\n";

    return out.str();
}


// Update the output edit control with text
void ShowOutputText(HWND hwnd, const std::string& text) {
    SetControlText(hwnd, IDC_EDIT_OUTPUT, text);
}

// ----------------------------------------------------------------------
// Window procedure
// ----------------------------------------------------------------------

LRESULT CALLBACK WndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam) {
    switch (msg) {
    case WM_CREATE:
    {
        // Buttons row
        CreateWindowExA(
            0, "BUTTON", "Select File",
            WS_CHILD | WS_VISIBLE | BS_PUSHBUTTON,
            10, 10, 120, 28,
            hwnd, reinterpret_cast<HMENU>(static_cast<INT_PTR>(IDC_BUTTON_SELFILE)),
            nullptr, nullptr);

        CreateWindowExA(
            0, "BUTTON", "Select Folder",
            WS_CHILD | WS_VISIBLE | BS_PUSHBUTTON,
            140, 10, 120, 28,
            hwnd, reinterpret_cast<HMENU>(static_cast<INT_PTR>(IDC_BUTTON_SELFOLDER)),
            nullptr, nullptr);

        CreateWindowExA(
            0, "BUTTON", "Run MapReduce",
            WS_CHILD | WS_VISIBLE | BS_PUSHBUTTON,
            270, 10, 140, 28,
            hwnd, reinterpret_cast<HMENU>(static_cast<INT_PTR>(IDC_BUTTON_RUN)),
            nullptr, nullptr);

        CreateWindowExA(
            0, "BUTTON", "Clear",
            WS_CHILD | WS_VISIBLE | BS_PUSHBUTTON,
            420, 10, 100, 28,
            hwnd, reinterpret_cast<HMENU>(static_cast<INT_PTR>(IDC_BUTTON_CLEAR)),
            nullptr, nullptr);

        // Output edit (ASCII table display)
        HWND hEditOutput = CreateWindowExA(
            WS_EX_CLIENTEDGE, "EDIT", "",
            WS_CHILD | WS_VISIBLE | ES_MULTILINE | ES_AUTOVSCROLL |
            ES_AUTOHSCROLL | ES_READONLY | WS_VSCROLL,
            10, 50, 660, 360,
            hwnd, reinterpret_cast<HMENU>(static_cast<INT_PTR>(IDC_EDIT_OUTPUT)),
            nullptr, nullptr);

        // Monospaced font for nice ASCII table
        g_hMonoFont = CreateFontA(
            -16, 0, 0, 0, FW_NORMAL, FALSE, FALSE, FALSE,
            ANSI_CHARSET, OUT_DEFAULT_PRECIS, CLIP_DEFAULT_PRECIS, DEFAULT_QUALITY,
            FIXED_PITCH | FF_MODERN, "Consolas");

        if (g_hMonoFont != nullptr && hEditOutput != nullptr) {
            SendMessageA(hEditOutput, WM_SETFONT, reinterpret_cast<WPARAM>(g_hMonoFont), TRUE);
        }

        // Initial message
        ShowOutputText(hwnd,
            "MapReduce Phase 3 - Word Count (GUI)\r\n"
            "Click 'Select File' or 'Select Folder', then 'Run MapReduce'.\r\n");
    }
    break;

    case WM_COMMAND:
    {
        int id = LOWORD(wParam);
        if (id == IDC_BUTTON_SELFILE) {
            std::string path = BrowseForFile(hwnd);
            path = TrimPath(path);
            if (!path.empty()) {
                g_selectedPath = path;
                std::string msg = "Selected file:\r\n" + path + "\r\n";
                ShowOutputText(hwnd, msg);
            }
        } else if (id == IDC_BUTTON_SELFOLDER) {
            std::string path = BrowseForFolder(hwnd);
            path = TrimPath(path);
            if (!path.empty()) {
                g_selectedPath = path;
                std::string msg = "Selected folder:\r\n" + path + "\r\n";
                ShowOutputText(hwnd, msg);
            }
        } else if (id == IDC_BUTTON_CLEAR) {
            g_selectedPath.clear();
            g_logger.clear();
            ShowOutputText(hwnd, "");
        } else if (id == IDC_BUTTON_RUN) {
            if (g_selectedPath.empty()) {
                MessageBoxA(hwnd, "Please select a file or folder first.",
                            "Input Required", MB_OK | MB_ICONWARNING);
                break;
            }

            g_logger.clear();
            ShowOutputText(hwnd, "Running MapReduce...\r\n");

            std::string outputFile = "output/word_counts.csv";
            MapReduceController controller(g_selectedPath, outputFile, 3u);
            bool ok = controller.run(g_logger);

            if (!ok) {
                std::string text = "MapReduce completed, but no .txt files were found.\r\n";
                ShowOutputText(hwnd, text);
            } else {
                // Build ASCII table from CSV and display it.
                std::string table = BuildAsciiTableFromCsv(outputFile);
                ShowOutputText(hwnd, table);
            }
        }
    }
    break;

    case WM_CLOSE:
        DestroyWindow(hwnd);
        break;

    case WM_DESTROY:
        if (g_hMonoFont != nullptr) {
            DeleteObject(g_hMonoFont);
            g_hMonoFont = nullptr;
        }
        PostQuitMessage(0);
        break;

    default:
        return DefWindowProcA(hwnd, msg, wParam, lParam);
    }

    return 0;
}

// ----------------------------------------------------------------------
// WinMain
// ----------------------------------------------------------------------

int WINAPI WinMain(HINSTANCE hInstance, HINSTANCE, LPSTR, int nCmdShow) {
    const char* className = "MRP2Phase3GuiWindowClass";

    WNDCLASSEXA wc{};
    wc.cbSize        = sizeof(WNDCLASSEXA);
    wc.style         = CS_HREDRAW | CS_VREDRAW;
    wc.lpfnWndProc   = WndProc;
    wc.cbClsExtra    = 0;
    wc.cbWndExtra    = 0;
    wc.hInstance     = hInstance;
    wc.hIcon         = LoadIcon(nullptr, IDI_APPLICATION);
    wc.hCursor       = LoadCursor(nullptr, IDC_ARROW);
    wc.hbrBackground = reinterpret_cast<HBRUSH>(COLOR_WINDOW + 1);
    wc.lpszMenuName  = nullptr;
    wc.lpszClassName = className;
    wc.hIconSm       = LoadIcon(nullptr, IDI_APPLICATION);

    if (!RegisterClassExA(&wc)) {
        MessageBoxA(nullptr, "Failed to register window class.", "Error", MB_OK | MB_ICONERROR);
        return 0;
    }

    HWND hwnd = CreateWindowExA(
        0,
        className,
        "MapReduce Phase 3 - Word Count (GUI)",
        WS_OVERLAPPEDWINDOW,
        CW_USEDEFAULT, CW_USEDEFAULT,
        700, 480,
        nullptr, nullptr, hInstance, nullptr);

    if (!hwnd) {
        MessageBoxA(nullptr, "Failed to create main window.", "Error", MB_OK | MB_ICONERROR);
        return 0;
    }

    ShowWindow(hwnd, nCmdShow);
    UpdateWindow(hwnd);

    MSG msg;
    while (GetMessageA(&msg, nullptr, 0, 0) > 0) {
        TranslateMessage(&msg);
        DispatchMessageA(&msg);
    }

    return static_cast<int>(msg.wParam);
}


