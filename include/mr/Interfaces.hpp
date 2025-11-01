#pragma once
#include <string>
#include <vector>
#include <utility>
#include <cstdint>

namespace mr {

using Word  = std::string;
using Count = int;

// Context lets DLLs emit output without touching raw filesystem:
struct IMapContext {
    virtual ~IMapContext() = default;
    // write ("word", 1) to intermediate store
    virtual void emit(const Word& w, Count c) = 0;
};

struct IReduceContext {
    virtual ~IReduceContext() = default;
    // write final ("word", total) to output store
    virtual void emit(const Word& w, Count total) = 0;
};

// Mapper / Reducer interfaces (polymorphism)
struct IMapper {
    virtual ~IMapper() = default;
    // fileName provided for parity; DLL may ignore it
    virtual void map(const std::string& fileName, const std::string& line, IMapContext& ctx) = 0;
    virtual void flush(IMapContext& ctx) = 0; // finalize buffered output
};

struct IReducer {
    virtual ~IReducer() = default;
    // counts is the grouped list e.g. [1,1,1,...]
    virtual void reduce(const Word& word, const std::vector<Count>& counts, IReduceContext& ctx) = 0;
};

// --------- C factories expected from DLLs ---------
// extern "C" to avoid C++ name mangling.
using CreateMapperFn = IMapper*  (__stdcall*)();
using DestroyMapperFn= void      (__stdcall*)(IMapper*);
using CreateReducerFn= IReducer* (__stdcall*)();
using DestroyReducerFn=void      (__stdcall*)(IReducer*);

static constexpr const char* kCreateMapperSym  = "CreateMapper";
static constexpr const char* kDestroyMapperSym = "DestroyMapper";
static constexpr const char* kCreateReducerSym = "CreateReducer";
static constexpr const char* kDestroyReducerSym= "DestroyReducer";

} // namespace mr
