#include "mr/Mapper.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>
#include <functional>  // for std::hash

namespace mr {

Mapper::Mapper(FileManager& fm,
               const std::string& tempDir,
               std::size_t flushThreshold)
    : Mapper(fm, tempDir, flushThreshold, /*mapperId*/ 0, /*numReducers*/ 1) {}

// New Phase 3 constructor
Mapper::Mapper(FileManager& fm,
               const std::string& tempDir,
               std::size_t flushThreshold,
               int mapperId,
               int numReducers)
    : fileManager_(fm),
      tempDir_(tempDir),
      flushThreshold_(flushThreshold),
      mapperId_(mapperId),
      numReducers_(numReducers > 0 ? numReducers : 1) {}

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

    std::hash<std::string> hasher;

    for (const auto& kv : buffer_) {
        const std::string& word = kv.first;
        int count = kv.second;

        int bucket = static_cast<int>(hasher(word) % numReducers_);

        // File name: tempDir_/m<mapperId>_r<bucket>.txt
        std::string path = tempDir_ +
                           "/m" + std::to_string(mapperId_) +
                           "_r" + std::to_string(bucket) + ".txt";

        fileManager_.appendLine(path, word + "\t" + std::to_string(count));
    }

    buffer_.clear();
}

} // namespace mr
