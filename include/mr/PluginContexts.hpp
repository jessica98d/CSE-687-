#pragma once
#include "Interfaces.hpp"
#include "FileManager.hpp"
#include <string>

namespace mr {

class MapContextAdapter : public IMapContext {
public:
    MapContextAdapter(FileManager& fm, std::string tempDir)
        : fm_(fm), tempDir_(std::move(tempDir)), tmpPath_(tempDir_ + "/intermediate.txt") {
        fm_.ensureDir(tempDir_);
    }
    void emit(const Word& w, Count c) override {
        fm_.appendLine(tmpPath_, w + "\t" + std::to_string(c));
    }
private:
    FileManager& fm_;
    std::string  tempDir_;
    std::string  tmpPath_;
};

class ReduceContextAdapter : public IReduceContext {
public:
    ReduceContextAdapter(FileManager& fm, std::string outputFile)
        : fm_(fm), outFile_(std::move(outputFile)) {
        fm_.writeAll(outFile_, ""); // truncate once before first write
    }
    void emit(const Word& w, Count total) override {
        fm_.appendLine(outFile_, w + "\t" + std::to_string(total));
    }
private:
    FileManager& fm_;
    std::string  outFile_;
};

} // namespace mr
