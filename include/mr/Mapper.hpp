#pragma once 
#include "FileManager.hpp"
#include <string>
#include <vector>
#include <utility>

namespace mr {

class Mapper {
public:
    // Existing Phase 2 constructor – keep it for backward compatibility
    Mapper(FileManager& fm, const std::string& tempDir, std::size_t flushThreshold);

    // New Phase 3 constructor (mapper-aware)
    Mapper(FileManager& fm,
           const std::string& tempDir,
           std::size_t flushThreshold,
           int mapperId,
           int numReducers);

    void map(const std::string& fileName, const std::string& line);
    void flush();
    void exportKV(); // per spec: export intermediate key-value pairs

private:
    FileManager& fileManager_;
    std::string tempDir_;
    std::vector<std::pair<std::string, int>> buffer_;
    std::size_t flushThreshold_;

    // New fields:
    int mapperId_     = 0;
    int numReducers_  = 1;
};

} // namespace mr
