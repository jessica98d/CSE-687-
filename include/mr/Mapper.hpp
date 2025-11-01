#pragma once
#include "FileManager.hpp"
#include <string>
#include <vector>
#include <utility>

namespace mr {

class Mapper {
public:
    Mapper(FileManager& fm, const std::string& tempDir, std::size_t flushThreshold);
    void map(const std::string& fileName, const std::string& line);
    void flush();
    void exportKV(); // per spec: export intermediate key-value pairs

private:
    FileManager& fileManager_;
    std::string tempDir_;
    std::vector<std::pair<std::string, int>> buffer_;
    std::size_t flushThreshold_;
};

} // namespace mr
