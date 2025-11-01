#pragma once
#include <string>
#include <vector>

namespace mr {

// ------------------------------------------------------------------
// FileManager: encapsulates all file and directory operations.
// ------------------------------------------------------------------
class FileManager {
public:
    // Ensuring that the directory exists; create it in case it's missing.
    void ensureDir(const std::string& dir);

    // Checking for the existence of a file or directory
    bool exists(const std::string& path);

    // entire string to a file (truncates existing contents).
    void writeAll(const std::string& path, const std::string& data);

    // Appending a single line to a file and create directories if needed
    void appendLine(const std::string& path, const std::string& line);

    // Reading all lines from text file into a vector
    std::vector<std::string> readAllLines(const std::string& path);

    // Listings all the files in a given or determined directory
    std::vector<std::string> listFiles(const std::string& dir);

    // Creating an empty file to be used for the SUCCESS MARKER
    bool writeEmptyFile(const std::string& path);

    // Listing only text files (*.txt or no extension) in a directory.
    std::vector<std::string> listTextFiles(const std::string& dir);
};

} // namespace mr
