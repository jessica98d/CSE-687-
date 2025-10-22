#include "FileManager.hpp"
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace mr {

void FileManager::ensureDir(const std::string& dir) {
    fs::create_directories(dir);
}

bool FileManager::exists(const std::string& path) {
    return fs::exists(path);
}

void FileManager::writeAll(const std::string& path, const std::string& data) {
    ensureDir(fs::path(path).parent_path().string());
    std::ofstream out(path, std::ios::trunc);
    out << data;
}

void FileManager::appendLine(const std::string& path, const std::string& line) {
    ensureDir(fs::path(path).parent_path().string());
    std::ofstream out(path, std::ios::app);
    out << line << "\n";
}

std::vector<std::string> FileManager::readAllLines(const std::string& path) {
    std::vector<std::string> lines;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line))
        lines.push_back(line);
    return lines;
}

std::vector<std::string> FileManager::listFiles(const std::string& dir) {
    std::vector<std::string> files;
    if (!exists(dir)) return files;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.is_regular_file())
            files.push_back(entry.path().string());
    }
    return files;
}

bool FileManager::writeEmptyFile(const std::string& path) {
    ensureDir(fs::path(path).parent_path().string());
    std::ofstream out(path, std::ios::trunc | std::ios::binary);
    return static_cast<bool>(out);
}

std::vector<std::string> FileManager::listTextFiles(const std::string& dir) {
    std::vector<std::string> out;
    if (!exists(dir)) return out;
    for (const auto& e : fs::directory_iterator(dir)) {
        if (!e.is_regular_file()) continue;
        const auto& p = e.path();
        if (!p.has_extension() || p.extension() == ".txt")
            out.push_back(p.string());
    }
    return out;
}

} // namespace mr
