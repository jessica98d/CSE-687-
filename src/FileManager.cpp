#include "FileManager.h"

#include <fstream>
#include <iostream>
#include <algorithm>

FileManager::FileManager(const std::string& rootDirectoryIn)
    : rootDirectory(rootDirectoryIn) {
}

std::vector<std::filesystem::path> FileManager::listTextFiles() const {
    std::vector<std::filesystem::path> results;

    std::filesystem::path rootPath(rootDirectory);

    // If the path is a single existing file, just return it.
    if (std::filesystem::exists(rootPath) && std::filesystem::is_regular_file(rootPath)) {
        results.push_back(rootPath);
        return results;
    }

    // Otherwise, it must be a directory.
    if (!std::filesystem::exists(rootPath) || !std::filesystem::is_directory(rootPath)) {
        std::cerr << "Input path does not exist or is not a directory: "
                  << rootDirectory << "\n";
        return results;
    }

    for (const auto& entry : std::filesystem::directory_iterator(rootPath)) {
        if (entry.is_regular_file()) {
            auto path = entry.path();
            if (path.extension() == ".txt") {
                results.push_back(path);
            }
        }
    }

    std::sort(results.begin(), results.end());
    return results;
}

std::vector<std::string> FileManager::readAllLines(const std::filesystem::path& filePath) const {
    std::vector<std::string> lines;
    std::ifstream in(filePath);
    if (!in.is_open()) {
        std::cerr << "Failed to open file for reading: " << filePath << "\n";
        return lines;
    }

    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }

    return lines;
}

void FileManager::ensureDirectory(const std::filesystem::path& dir) const {
    if (dir.empty()) {
        return;
    }
    if (!std::filesystem::exists(dir)) {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        if (ec) {
            std::cerr << "Failed to create directory: " << dir
                      << " error: " << ec.message() << "\n";
        }
    }
}

void FileManager::writeWordCounts(
    const std::string& outputFile,
    const std::vector<std::pair<std::string, std::size_t>>& wordCounts
) const {
    std::ofstream out(outputFile);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file for writing: "
                  << outputFile << "\n";
        return;
    }

    for (const auto& pair : wordCounts) {
        out << pair.first << "," << pair.second << "\n";
    }

    std::cout << "Wrote " << wordCounts.size()
              << " word counts to " << outputFile << "\n";
}

const std::string& FileManager::getRootDirectory() const {
    return rootDirectory;
}
