#include "mr/Interfaces.hpp"
#include <numeric>

namespace {
struct SimpleReducer : mr::IReducer {
  void reduce(const mr::Word& w, const std::vector<mr::Count>& counts, mr::IReduceContext& ctx) override {
    int total = std::accumulate(counts.begin(), counts.end(), 0);
    ctx.emit(w, total);
  }
};
}

extern "C" __declspec(dllexport) mr::IReducer* __stdcall CreateReducer()  { return new SimpleReducer(); }
extern "C" __declspec(dllexport) void         __stdcall DestroyReducer(mr::IReducer* p) { delete p; }
