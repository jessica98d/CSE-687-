#include "FileManager.hpp"

#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace fs = std::filesystem;

namespace mr {

std::vector<std::string> FileManager::listFiles(const std::string& directory) const {
    std::vector<std::string> result;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.is_regular_file()) {
            result.push_back(entry.path().string());
        }
    }
    return result;
}

std::vector<std::string> FileManager::readAllLines(const std::string& filePath) const {
    std::vector<std::string> lines;
    std::ifstream in(filePath, std::ios::binary);
    if (!in) return lines;
    std::string line;
    while (std::getline(in, line)) {
        // Strip CR if present (Windows line endings)
        if (!line.empty() && line.back() == '\r') line.pop_back();
        lines.push_back(line);
    }
    return lines;
}

void FileManager::appendLine(const std::string& filePath, const std::string& line) const {
    std::ofstream out(filePath, std::ios::binary | std::ios::app);
    if (!out) throw std::runtime_error("Failed to open file for append: " + filePath);
    out << line << "\n";
}

void FileManager::writeAll(const std::string& filePath, const std::string& content) const {
    std::ofstream out(filePath, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Failed to open file for write: " + filePath);
    out << content;
}

void FileManager::ensureDir(const std::string& directory) const {
    std::filesystem::create_directories(directory);
}

bool FileManager::exists(const std::string& path) const {
    return fs::exists(path);
}

} // namespace mr
