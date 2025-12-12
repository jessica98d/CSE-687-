// Reducer.cpp
#include "mr/Reducer.hpp"
#include <numeric>
#include <cstdlib>
#include <string>

namespace mr {

static std::string envOrEmpty(const char* key) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : std::string();
}

Reducer::Reducer(FileManager& fm, const std::string& outputDir)
    : fileManager_(fm), outputDir_(outputDir) {

    fileManager_.ensureDir(outputDir_);

    const std::string suffix = envOrEmpty("MR_OUTFILE_SUFFIX");
    outFilePath_ = outputDir_ + "/word_counts" + suffix + ".txt";

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
    const std::string suffix = envOrEmpty("MR_OUTFILE_SUFFIX");
    const std::string successPath =
        outputDir_ + (suffix.empty() ? "/SUCCESS" : ("/SUCCESS" + suffix));

    fileManager_.writeEmptyFile(successPath);
}

} // namespace mr
