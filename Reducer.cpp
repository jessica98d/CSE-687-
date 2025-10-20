#include "Reducer.hpp"
#include <numeric>

namespace mr {

Reducer::Reducer(FileManager& fm, const std::string& outputDir)
    : fileManager_(fm), outputDir_(outputDir) {
    fileManager_.ensureDir(outputDir_);
    outFilePath_ = outputDir_ + "/word_counts.txt";
    // truncate previous output
    fileManager_.writeAll(outFilePath_, "");
}

void Reducer::reduce(const Word& word, const std::vector<Count>& counts) {
    int total = std::accumulate(counts.begin(), counts.end(), 0);
    fileManager_.appendLine(outFilePath_, word + "\t" + std::to_string(total));
}

void Reducer::markSuccess() {
    fileManager_.appendLine(outputDir_ + "/_SUCCESS.txt", "ok");
}

} // namespace mr
