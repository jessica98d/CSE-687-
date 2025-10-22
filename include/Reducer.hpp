#pragma once
#include "FileManager.hpp"
#include <string>
#include <vector>
#include <utility>

namespace mr {

using Word = std::string;
using Count = int;

class Reducer {
public:
    Reducer(FileManager& fm, const std::string& outputDir);
    void reduce(const Word& word, const std::vector<Count>& counts); // compute only
    void exportResult(const Word& word, int total);                  // file IO
    void markSuccess();

private:
    FileManager& fileManager_;
    std::string outputDir_;
    std::string outFilePath_;
};

} // namespace mr
