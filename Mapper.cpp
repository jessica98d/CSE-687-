#include "Mapper.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>

namespace mr {

Mapper::Mapper(FileManager& fm, const std::string& tempDir, std::size_t flushThreshold)
    : fileManager_(fm), tempDir_(tempDir), buffer_(), flushThreshold_(flushThreshold) {}

void Mapper::map(const std::string& /*fileName*/, const std::string& line) {
    // normalize: lowercase; non-letters -> space
    std::string cleaned = line;
    for (char& c : cleaned) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (std::isalpha(uc)) {
            c = static_cast<char>(std::tolower(uc));
        } else {
            c = ' ';
        }
    }

    std::istringstream iss(cleaned);
    std::string token;
    while (iss >> token) {
        if (!token.empty()) {
            buffer_.push_back({ token, 1 });
            if (buffer_.size() >= flushThreshold_) flushInternal();
        }
    }
}

void Mapper::flush() {
    flushInternal();
}

void Mapper::flushInternal() {
    if (buffer_.empty()) return;
    fileManager_.ensureDir(tempDir_);
    const std::string tmpPath = tempDir_ + "/intermediate.txt";
    for (const auto& kv : buffer_) {
        fileManager_.appendLine(tmpPath, kv.first + "\t" + std::to_string(kv.second));
    }
    buffer_.clear();
}

} // namespace mr
