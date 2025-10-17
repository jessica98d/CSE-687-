#include "Reducer.hpp"
#include <numeric>

namespace mr {

Reducer::Reducer(FileManager& fm, std::string_view outputDir)
    : fileManager_(fm), outputDir_(outputDir),
      successFilePath_(std::string(outputDir_) + "/_SUCCESS.txt") 
{
    fileManager_.ensureDir(outputDir_);
    outFilePath_ = std::string(outputDir_) + "/word_counts.txt";
    // Truncate previous output
    fileManager_.writeAll(outFilePath_, "");
}

void Reducer::reduce(std::string_view word, const std::vector<Count>& counts) {
    const auto total = std::accumulate(counts.begin(), counts.end(), 0);
    fileManager_.appendLine(outFilePath_, std::string(word) + "\t" + std::to_string(total));
}

void Reducer::markSuccess() {
    fileManager_.appendLine(successFilePath_, "ok");
}

} // namespace mr
