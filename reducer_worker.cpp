// reducer_worker.cpp

#include "mr/FileManager.hpp"
#include "mr/Reducer.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <sstream>
#include <cstdlib>   // _putenv_s on Windows

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: reducer_worker <reducerId> <intermDir> <outputDir>\n";
        return 1;
    }

    int reducerId         = std::stoi(argv[1]);
    std::string intermDir = argv[2];
    std::string outputDir = argv[3];

    std::cout << "[reducer_worker] reducerId=" << reducerId
              << " intermDir=" << intermDir
              << " outputDir=" << outputDir << "\n";

#ifdef _WIN32
    // Makes Reducer write to: word_counts_rX.txt instead of word_counts.txt
    const std::string suffix = "_r" + std::to_string(reducerId);
    _putenv_s("MR_OUTFILE_SUFFIX", suffix.c_str());
#endif

    mr::FileManager fm;
    mr::Reducer reducer(fm, outputDir);

    std::unordered_map<std::string, std::vector<int>> grouped;

    std::vector<std::string> intermFiles = fm.listTextFiles(intermDir);
    const std::string suffixFile = "_r" + std::to_string(reducerId) + ".txt";

    for (const auto& path : intermFiles) {
        // Only pick files intended for this reducer
        if (path.size() < suffixFile.size() ||
            path.substr(path.size() - suffixFile.size()) != suffixFile) {
            continue;
        }

        auto lines = fm.readAllLines(path);
        for (const auto& line : lines) {
            if (line.empty()) continue;

            std::istringstream iss(line);
            std::string word;
            int count = 0;

            // Accept tab or space separated
            if (!(iss >> word >> count)) continue;

            grouped[word].push_back(count);
        }
    }

    for (auto& kv : grouped) {
        reducer.reduce(kv.first, kv.second);
    }

    reducer.markSuccess();
    return 0;
}
