#ifndef FILEMANAGER_H
#define FILEMANAGER_H

#include <string>
#include <vector>
#include <filesystem>
#include <utility>

class FileManager {
public:
    explicit FileManager(const std::string& rootDirectory);

    // If rootDirectory is a directory, returns all .txt files inside.
    // If it is a single file, returns a vector containing just that file.
    std::vector<std::filesystem::path> listTextFiles() const;

    // Read all lines from a text file.
    std::vector<std::string> readAllLines(const std::filesystem::path& filePath) const;

    // Make sure a directory (and its parents) exist.
    void ensureDirectory(const std::filesystem::path& dir) const;

    // Write (word, count) pairs as CSV to an output file.
    void writeWordCounts(
        const std::string& outputFile,
        const std::vector<std::pair<std::string, std::size_t>>& wordCounts
    ) const;

    const std::string& getRootDirectory() const;

private:
    std::string rootDirectory;
};

#endif // FILEMANAGER_H
