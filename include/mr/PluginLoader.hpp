#pragma once
#include "Interfaces.hpp"
#include <windows.h>
#include <string>
#include <stdexcept>

namespace mr {

struct PluginHandles {
    HMODULE mapDLL = nullptr;
    HMODULE reduceDLL = nullptr;

    CreateMapperFn  createMapper = nullptr;
    DestroyMapperFn destroyMapper= nullptr;
    CreateReducerFn createReducer= nullptr;
    DestroyReducerFn destroyReducer= nullptr;
};

inline HMODULE loadDll(const std::string& path) {
    HMODULE h = ::LoadLibraryA(path.c_str());
    if (!h) throw std::runtime_error("Failed to load DLL: " + path);
    return h;
}

template <typename Fn>
Fn loadSym(HMODULE h, const char* name) {
    auto p = reinterpret_cast<Fn>(::GetProcAddress(h, name));
    if (!p) throw std::runtime_error(std::string("Missing symbol: ") + name);
    return p;
}

inline PluginHandles loadPlugins(const std::string& dllDir) {
    PluginHandles ph;

    // Expected names, or you can accept via CLI
    const std::string mapDllPath = dllDir + "/Map.dll";
    const std::string redDllPath = dllDir + "/Reduce.dll";

    ph.mapDLL    = loadDll(mapDllPath);
    ph.reduceDLL = loadDll(redDllPath);

    ph.createMapper   = loadSym<CreateMapperFn>(ph.mapDLL,  kCreateMapperSym);
    ph.destroyMapper  = loadSym<DestroyMapperFn>(ph.mapDLL, kDestroyMapperSym);
    ph.createReducer  = loadSym<CreateReducerFn>(ph.reduceDLL,  kCreateReducerSym);
    ph.destroyReducer = loadSym<DestroyReducerFn>(ph.reduceDLL, kDestroyReducerSym);

    return ph;
}

inline void freePlugins(PluginHandles& ph) {
    if (ph.mapDLL)    ::FreeLibrary(ph.mapDLL), ph.mapDLL = nullptr;
    if (ph.reduceDLL) ::FreeLibrary(ph.reduceDLL), ph.reduceDLL = nullptr;
}

} // namespace mr
