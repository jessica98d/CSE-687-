#include "Workflow.hpp"
#include <sstream>

namespace mr {

Workflow::Workflow(FileManager& fm,
                   const std::string& inputDir,
                   const std::string& tempDir,
                   const std::string& outputDir)
    : fileManager_(fm),
      inputDir_(inputDir),
      tempDir_(tempDir),
      outputDir_(outputDir) {
    fileManager_.ensureDir(tempDir_);
    fileManager_.ensureDir(outputDir_);
}

void Workflow::run() {
    doMapPhase();
    Grouped grouped = doSortAndGroup();
    doReducePhase(grouped);
}

void Workflow::doMapPhase() {
    // clear previous intermediate
    const std::string tmpFile = tempDir_ + "/intermediate.txt";
    fileManager_.writeAll(tmpFile, "");

    Mapper mapper(fileManager_, tempDir_, 2048);
    const auto files = fileManager_.listFiles(inputDir_);
    for (const auto& path : files) {
        const auto lines = fileManager_.readAllLines(path);
        for (const auto& line : lines) {
            mapper.map(path, line);
        }
    }
    mapper.flush();
}

Grouped Workflow::doSortAndGroup() {
    Grouped grouped;
    const std::string tmpFile = tempDir_ + "/intermediate.txt";
    if (!fileManager_.exists(tmpFile)) {
        return grouped;
    }
    const auto lines = fileManager_.readAllLines(tmpFile);
    for (const auto& line : lines) {
        std::size_t tabPos = line.find('\t');
        if (tabPos == std::string::npos) continue;
        std::string word = line.substr(0, tabPos);
        int value = 0;
        try {
            value = std::stoi(line.substr(tabPos + 1));
        } catch (...) { value = 0; }
        if (!word.empty() && value != 0) {
            grouped[word].push_back(value);
        }
    }
    return grouped;
}

void Workflow::doReducePhase(const Grouped& grouped) {
    Reducer reducer(fileManager_, outputDir_);
    for (const auto& kv : grouped) {
        reducer.reduce(kv.first, kv.second);
    }
    reducer.markSuccess();
}

std::vector<std::pair<std::string, int>> Workflow::runAndGetCounts() {
    doMapPhase();
    Grouped grouped = doSortAndGroup();

    std::map<std::string, int> totals;
    for (const auto& kv : grouped) {
        int sum = 0;
        for (int v : kv.second) sum += v;
        totals[kv.first] = sum;
    }

    Reducer reducer(fileManager_, outputDir_);
    for (const auto& p : totals) {
        std::vector<int> one{ p.second };
        reducer.reduce(p.first, one);
    }
    reducer.markSuccess();

    std::vector<std::pair<std::string, int>> out;
    out.reserve(totals.size());
    for (const auto& p : totals) out.emplace_back(p.first, p.second);
    return out;
}

} // namespace mr
