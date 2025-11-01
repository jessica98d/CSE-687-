#include "mr/Mapper.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>

namespace mr {

Mapper::Mapper(FileManager& fm, const std::string& tempDir, std::size_t flushThreshold)
    : fileManager_(fm), tempDir_(tempDir), flushThreshold_(flushThreshold) {}

void Mapper::map(const std::string&, const std::string& line) {
    std::string cleaned = line;
    for (char& c : cleaned) {
        unsigned char uc = static_cast<unsigned char>(c);
        c = std::isalpha(uc) ? static_cast<char>(std::tolower(uc)) : ' ';
    }

    std::istringstream iss(cleaned);
    std::string token;
    while (iss >> token) {
        buffer_.push_back({token, 1});
        if (buffer_.size() >= flushThreshold_) exportKV();
    }
}

void Mapper::flush() { exportKV(); }

void Mapper::exportKV() {
    if (buffer_.empty()) return;
    fileManager_.ensureDir(tempDir_);
    const std::string tmpPath = tempDir_ + "/intermediate.txt";
    for (const auto& kv : buffer_)
        fileManager_.appendLine(tmpPath, kv.first + "\t" + std::to_string(kv.second));
    buffer_.clear();
}

} // namespace mr
