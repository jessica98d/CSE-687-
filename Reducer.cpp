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
}

void Reducer::markSuccess() {
    fileManager_.writeEmptyFile(outputDir_ + "/SUCCESS"); // empty file 
}

} // namespace mr
