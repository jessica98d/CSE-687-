#include "mr/Mapper.hpp"

#include <cctype>
#include <sstream>
#include <functional>   // std::hash
#include <utility>      // std::move (optional)

namespace mr {

Mapper::Mapper(FileManager& fm,
               const std::string& tempDir,
               std::size_t flushThreshold)
    : Mapper(fm, tempDir, flushThreshold, /*mapperId*/ 0, /*numReducers*/ 1) {}

// Phase 4 / multi-process constructor
Mapper::Mapper(FileManager& fm,
               const std::string& tempDir,
               std::size_t flushThreshold,
               int mapperId,
               int numReducers)
    : fileManager_(fm),
      tempDir_(tempDir),
      flushThreshold_(flushThreshold ? flushThreshold : 1), // avoid 0 threshold
      mapperId_(mapperId),
      numReducers_(numReducers > 0 ? numReducers : 1) {}

void Mapper::map(const std::string& /*fileName*/, const std::string& line) {
    // normalize: lowercase letters; everything else -> space
    std::string cleaned = line;
    for (char& c : cleaned) {
        unsigned char uc = static_cast<unsigned char>(c);
        c = std::isalpha(uc) ? static_cast<char>(std::tolower(uc)) : ' ';
    }

    std::istringstream iss(cleaned);
    std::string token;

    while (iss >> token) {
        buffer_.push_back({ token, 1 });

        if (buffer_.size() >= flushThreshold_) {
            exportKV();
        }
    }
}

void Mapper::flush() {
    exportKV();
}

void Mapper::exportKV() {
    if (buffer_.empty()) return;

    fileManager_.ensureDir(tempDir_);

    const std::hash<std::string> hasher;

    for (const auto& kv : buffer_) {
        const std::string& word = kv.first;
        const int count = kv.second;

        // SAFE: hash returns size_t, so modulus should be size_t too
        const std::size_t bucketSz =
            hasher(word) % static_cast<std::size_t>(numReducers_);
        const int bucket = static_cast<int>(bucketSz);

        // File name: tempDir_/m<mapperId>_r<bucket>.txt
        const std::string path =
            tempDir_ + "/m" + std::to_string(mapperId_) +
            "_r" + std::to_string(bucket) + ".txt";

        fileManager_.appendLine(path, word + "\t" + std::to_string(count));
    }

    buffer_.clear();
}

} // namespace mr
