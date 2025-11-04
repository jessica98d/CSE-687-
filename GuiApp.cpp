// GuiApp.cpp — Win32 GUI for CSE-687 repo (no DLLs required), C++17
#include <windows.h>
#include <commdlg.h>
#include <shlobj.h>
#include <string>
#include <fstream>
#include <filesystem>

#include "include/MapReduceInterfaces.h"
#include "include/Workflow.h"
#include "include/FileManager.h"
#include "include/Logger.h"

enum {
    IDC_EDIT_INPUT = 101, IDC_EDIT_OUTPUT, IDC_BTN_RUN,
    IDC_RB_TEXT, IDC_RB_FILE, IDC_RB_FOLDER, IDC_BTN_PICKFILE, IDC_BTN_PICKFOLDER
};

static HFONT gMono = nullptr;
static const std::string kDefaultInputDir = "./sample_input";
static mr::Paths g_paths{ kDefaultInputDir, ".", "./out/output", "./out/temp" };
static std::string g_pick_file, g_pick_folder;
static int g_mode = 0;

static void set_text(HWND h, const std::string& s){ std::wstring ws(s.begin(), s.end()); SetWindowTextW(h, ws.c_str()); }
static std::string get_text(HWND h){ int L=GetWindowTextLengthW(h); std::wstring ws(L, L'\0'); GetWindowTextW(h, ws.data(), L+1); return std::string(ws.begin(), ws.end()); }
static std::string slurp(const std::string& p){ std::ifstream in(p, std::ios::binary); return std::string((std::istreambuf_iterator<char>(in)),{}); }
static void set_text_crlf(HWND h, const std::string& s){ std::string o; o.reserve(s.size()*11/10); for(char c: s) o += (c=='\n')? "\r\n" : std::string(1,c); std::wstring ws(o.begin(), o.end()); SetWindowTextW(h, ws.c_str()); }

static std::string pick_file(HWND h){
    OPENFILENAMEA ofn{ sizeof(ofn) }; char sz[1024]={0};
    ofn.hwndOwner=h; ofn.lpstrFilter="Text Files\0*.txt\0All Files\0*.*\0"; ofn.lpstrFile=sz; ofn.nMaxFile=1024;
    ofn.Flags=OFN_FILEMUSTEXIST|OFN_PATHMUSTEXIST;
    return GetOpenFileNameA(&ofn)? sz : std::string();
}
static std::string pick_folder(HWND h){
    BROWSEINFOW bi{}; bi.hwndOwner=h; bi.ulFlags=BIF_RETURNONLYFSDIRS|BIF_NEWDIALOGSTYLE;
    PIDLIST_ABSOLUTE pid=SHBrowseForFolderW(&bi); wchar_t buf[MAX_PATH];
    if(pid && SHGetPathFromIDListW(pid, buf)){ std::wstring ws(buf); return std::string(ws.begin(), ws.end()); }
    return {};
}
static size_t count_txt(const std::string& dir){
    namespace fs=std::filesystem; size_t n=0; std::error_code ec;
    for(auto& e: fs::directory_iterator(dir, ec)) if(e.is_regular_file() && e.path().extension()==".txt") ++n;
    return n;
}

static void run_mapreduce(HWND hIn, HWND hOut, HWND hStatus){
    set_text(hStatus, "Running...");
    set_text_crlf(hOut, "");

    FileManager::ensure_dir(g_paths.temp_dir);
    FileManager::ensure_dir(g_paths.output_dir);
    FileManager::clear_dir(g_paths.temp_dir);
    FileManager::clear_dir(g_paths.output_dir);

    if(g_mode==0){ // Type Text
        FileManager::ensure_dir(kDefaultInputDir);
        FileManager::clear_dir(kDefaultInputDir);
        std::ofstream(FileManager::join(kDefaultInputDir,"user_input.txt")) << get_text(hIn);
        g_paths.input_dir = kDefaultInputDir;
    }else if(g_mode==1){ // Pick File
        if(g_pick_file.empty()){ set_text(hStatus,"No file selected."); return; }
        FileManager::ensure_dir(kDefaultInputDir);
        FileManager::clear_dir(kDefaultInputDir);
        std::error_code ec;
        auto dest = FileManager::join(kDefaultInputDir, std::filesystem::path(g_pick_file).filename().string());
        std::filesystem::copy_file(g_pick_file, dest, std::filesystem::copy_options::overwrite_existing, ec);
        if(ec){ set_text(hStatus, "Copy failed: "+ec.message()); return; }
        g_paths.input_dir = kDefaultInputDir;
    }else{ // Pick Folder
        if(g_pick_folder.empty()){ set_text(hStatus,"No folder selected."); return; }
        if(count_txt(g_pick_folder)==0){ set_text(hStatus,"Folder has no .txt files."); return; }
        g_paths.input_dir = g_pick_folder;
    }

    Logger log(FileManager::join(g_paths.output_dir,"mapreduce.log"));
    Workflow wf(g_paths, log);
    bool ok = wf.run();

    set_text(hStatus, ok ? "SUCCESS. Results rendered in the GUI." : "FAILED. See mapreduce.log.");
    set_text_crlf(hOut, slurp(wf.output_file_path()));
}

static LRESULT CALLBACK WndProc(HWND h, UINT m, WPARAM w, LPARAM l){
    static HWND hIn,hOut,hRun,hStatus,rbT,rbF,rbD,btnPickFile,btnPickFolder;
    switch(m){
    case WM_CREATE:{
        rbT=CreateWindowW(L"BUTTON",L"Type Text",WS_CHILD|WS_VISIBLE|BS_AUTORADIOBUTTON,10,10,110,20,h,(HMENU)IDC_RB_TEXT,0,0);
        rbF=CreateWindowW(L"BUTTON",L"Pick File",WS_CHILD|WS_VISIBLE|BS_AUTORADIOBUTTON,130,10,110,20,h,(HMENU)IDC_RB_FILE,0,0);
        rbD=CreateWindowW(L"BUTTON",L"Pick Folder",WS_CHILD|WS_VISIBLE|BS_AUTORADIOBUTTON,250,10,120,20,h,(HMENU)IDC_RB_FOLDER,0,0);
        SendMessage(rbT,BM_SETCHECK,BST_CHECKED,0);

        hIn=CreateWindowW(L"EDIT",L"",WS_CHILD|WS_VISIBLE|WS_BORDER|ES_LEFT|ES_MULTILINE|ES_AUTOVSCROLL|WS_VSCROLL,10,40,610,200,h,(HMENU)IDC_EDIT_INPUT,0,0);
        btnPickFile=CreateWindowW(L"BUTTON",L"Pick File...",WS_CHILD|WS_VISIBLE,640,40,140,28,h,(HMENU)IDC_BTN_PICKFILE,0,0);
        btnPickFolder=CreateWindowW(L"BUTTON",L"Pick Folder...",WS_CHILD|WS_VISIBLE,640,78,140,28,h,(HMENU)IDC_BTN_PICKFOLDER,0,0);
        hRun=CreateWindowW(L"BUTTON",L"Run MapReduce",WS_CHILD|WS_VISIBLE,10,248,160,30,h,(HMENU)IDC_BTN_RUN,0,0);
        hStatus=CreateWindowW(L"STATIC",L" ",WS_CHILD|WS_VISIBLE,180,250,600,26,h,0,0,0);
        hOut=CreateWindowW(L"EDIT",L"",WS_CHILD|WS_VISIBLE|WS_BORDER|ES_LEFT|ES_MULTILINE|ES_READONLY|WS_VSCROLL|WS_HSCROLL|ES_AUTOVSCROLL|ES_AUTOHSCROLL,10,290,770,260,h,(HMENU)IDC_EDIT_OUTPUT,0,0);
        gMono = CreateFontW(-18,0,0,0,FW_NORMAL,FALSE,FALSE,FALSE,ANSI_CHARSET,OUT_DEFAULT_PRECIS,CLIP_DEFAULT_PRECIS,DEFAULT_QUALITY,FF_DONTCARE,L"Consolas");
        SendMessageW(hIn,  WM_SETFONT, (WPARAM)gMono, TRUE);
        SendMessageW(hOut, WM_SETFONT, (WPARAM)gMono, TRUE);
        SendMessageW(hOut, EM_SETLIMITTEXT, (WPARAM)-1, 0);
        break; }
    case WM_COMMAND:{
        int id=LOWORD(w);
        if(id==IDC_RB_TEXT) g_mode=0;
        else if(id==IDC_RB_FILE) g_mode=1;
        else if(id==IDC_RB_FOLDER) g_mode=2;
        else if(id==IDC_BTN_PICKFILE){ auto f=pick_file(h); if(!f.empty()){ g_pick_file=f; g_mode=1; SendMessage(rbF,BM_SETCHECK,BST_CHECKED,0); set_text(hStatus,"Picked file: "+f);} }
        else if(id==IDC_BTN_PICKFOLDER){ auto d=pick_folder(h); if(!d.empty()){ g_pick_folder=d; g_mode=2; SendMessage(rbD,BM_SETCHECK,BST_CHECKED,0); set_text(hStatus,"Picked folder: "+d);} }
        else if(id==IDC_BTN_RUN){ run_mapreduce(hIn,hOut,hStatus); }
        break; }
    case WM_DESTROY:
        if(gMono){ DeleteObject(gMono); gMono=nullptr; }
        PostQuitMessage(0); break;
    default: return DefWindowProc(h,m,w,l);
    } return 0;
}

int APIENTRY wWinMain(HINSTANCE hi,HINSTANCE,LPWSTR,int nCmd){
    WNDCLASSW wc{}; wc.lpszClassName=L"MRGUI"; wc.hInstance=hi; wc.lpfnWndProc=WndProc; wc.hbrBackground=(HBRUSH)(COLOR_WINDOW+1);
    RegisterClassW(&wc);
    HWND w=CreateWindowW(L"MRGUI",L"MapReduce Phase 2 – Word Count (GUI)",WS_OVERLAPPEDWINDOW, CW_USEDEFAULT,CW_USEDEFAULT,840,640,0,0,hi,0);
    ShowWindow(w,nCmd);
    MSG msg; while(GetMessage(&msg,0,0,0)){ TranslateMessage(&msg); DispatchMessage(&msg); }
    return (int)msg.wParam;
}
