#ifndef FILE_MANAGER_HPP
#define FILE_MANAGER_HPP

#include <string>
#include <vector>

namespace mr {

class FileManager {
public:
    FileManager() = default;

    std::vector<std::string> listFiles(const std::string& directory) const;
    std::vector<std::string> readAllLines(const std::string& filePath) const;
    void appendLine(const std::string& filePath, const std::string& line) const;
    void writeAll(const std::string& filePath, const std::string& content) const;
    void ensureDir(const std::string& directory) const;
    bool exists(const std::string& path) const;
};

} // namespace mr

#endif // FILE_MANAGER_HPP
