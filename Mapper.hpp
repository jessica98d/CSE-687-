#ifndef MAPPER_HPP
#define MAPPER_HPP

#include "Types.hpp"
#include "FileManager.hpp"

#include <cstddef>
#include <string>

namespace mr {

class Mapper {
public:
    explicit Mapper(FileManager& fm, const std::string& tempDir, std::size_t flushThreshold = 4096);
    void map(const std::string& fileName, const std::string& line);
    void flush();

private:
    FileManager& fileManager_;
    std::string tempDir_;
    KVBuffer buffer_;
    std::size_t flushThreshold_;
    void flushInternal();
};

} // namespace mr

#endif // MAPPER_HPP
