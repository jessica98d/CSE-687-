// reducer_worker.cpp

#include "mr/FileManager.hpp"
#include "mr/Reducer.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <sstream>

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: reducer_worker <reducerId> <intermDir> <outputDir>\n";
        return 1;
    }

    int reducerId      = std::stoi(argv[1]);
    std::string intermDir = argv[2];
    std::string outputDir = argv[3];

    std::cout << "[reducer_worker] reducerId=" << reducerId
              << " intermDir=" << intermDir
              << " outputDir=" << outputDir << "\n";

    mr::FileManager fm;
    mr::Reducer reducer(fm, outputDir);

    std::unordered_map<std::string, std::vector<int>> grouped;

    std::vector<std::string> intermFiles = fm.listTextFiles(intermDir);
    std::string suffix = "_r" + std::to_string(reducerId) + ".txt";

    for (const auto& path : intermFiles) {
        // Only pick files intended for this reducer
        if (path.size() < suffix.size() ||
            path.substr(path.size() - suffix.size()) != suffix) {
            continue;
        }

        auto lines = fm.readAllLines(path);
        for (const auto& line : lines) {
            if (line.empty()) continue;

            std::istringstream iss(line);
            std::string word;
            int count = 0;

            // "word\t1" is fine here: >> treats any whitespace (space, tab, newline)
            if (!(iss >> word >> count)) {
                continue;
            }

            grouped[word].push_back(count);
        }
    }

    for (auto& kv : grouped) {
        const std::string& word = kv.first;
        std::vector<int>& counts = kv.second;

        int total = 0;
        for (int c : counts) total += c;

        reducer.reduce(word, counts);
        reducer.exportResult(word, total);
    }

    reducer.markSuccess();
    return 0;
}
