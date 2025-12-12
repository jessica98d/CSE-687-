#include "mr/Reducer.hpp"
#include <numeric>

namespace mr {

Reducer::Reducer(FileManager& fm, const std::string& outputDir)
    : fileManager_(fm), outputDir_(outputDir) {
    fileManager_.ensureDir(outputDir_);
    outFilePath_ = outputDir_ + "/word_counts.txt";
    fileManager_.writeAll(outFilePath_, ""); // truncate previous output
}

void Reducer::reduce(const Word& word, const std::vector<Count>& counts) {
    int total = std::accumulate(counts.begin(), counts.end(), 0);
    exportResult(word, total);
}

void Reducer::exportResult(const Word& word, int total) {
    fileManager_.appendLine(outFilePath_, word + "\t" + std::to_string(total));
}// Reducer.cpp
#include "mr/Reducer.hpp"
#include <numeric>
#include <cstdlib>   // std::getenv

namespace mr {

static std::string envOrEmpty(const char* key) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : std::string();
}

Reducer::Reducer(FileManager& fm, const std::string& outputDir)
    : fileManager_(fm), outputDir_(outputDir) {

    fileManager_.ensureDir(outputDir_);

    // If reducer_worker sets this, we write to a reducer-specific output file
    // Example: MR_OUTFILE_SUFFIX = "_r0"  -> word_counts_r0.txt
    const std::string suffix = envOrEmpty("MR_OUTFILE_SUFFIX");

    outFilePath_ = outputDir_ + "/word_counts" + suffix + ".txt";

    // Truncate only our own target output file (safe for multi-reducer)
    fileManager_.writeAll(outFilePath_, "");
}

void Reducer::reduce(const Word& word, const std::vector<Count>& counts) {
    const int total = std::accumulate(counts.begin(), counts.end(), 0);
    exportResult(word, total);
}

void Reducer::exportResult(const Word& word, int total) {
    fileManager_.appendLine(outFilePath_, word + "\t" + std::to_string(total));
}

void Reducer::markSuccess() {
    // Create per-reducer success marker if suffix is present; otherwise default.
    const std::string suffix = envOrEmpty("MR_OUTFILE_SUFFIX");
    const std::string successPath =
        outputDir_ + (suffix.empty() ? "/_SUCCESS.txt" : ("/_SUCCESS" + suffix + ".txt"));

    fileManager_.appendLine(successPath, "ok");
}

} // namespace mr
