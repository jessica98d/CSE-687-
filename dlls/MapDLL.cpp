#include "mr/Interfaces.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>

namespace {
struct SimpleMapper : mr::IMapper {
  void map(const std::string&, const std::string& line, mr::IMapContext& ctx) override {
    std::string cleaned = line;
    for (char& c : cleaned) {
      unsigned char uc = static_cast<unsigned char>(c);
      c = std::isalpha(uc) ? static_cast<char>(std::tolower(uc)) : ' ';
    }
    std::istringstream iss(cleaned);
    std::string tok;
    while (iss >> tok) ctx.emit(tok, 1);
  }
  void flush(mr::IMapContext&) override {}
};
}

extern "C" __declspec(dllexport) mr::IMapper*  __stdcall CreateMapper()  { return new SimpleMapper(); }
extern "C" __declspec(dllexport) void          __stdcall DestroyMapper(mr::IMapper* p) { delete p; }
