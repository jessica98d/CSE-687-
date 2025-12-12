// mapper_worker.cpp

#include "mr/FileManager.hpp"
#include "mr/Mapper.hpp"

#include <iostream>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "Usage: mapper_worker <mapperId> <numReducers> <inputDir> <intermDir>\n";
        return 1;
    }

    int mapperId            = std::stoi(argv[1]);
    int numReducers         = std::stoi(argv[2]);
    std::string inputDir    = argv[3];
    std::string intermDir   = argv[4];

    std::cout << "[mapper_worker] mapperId=" << mapperId
              << " numReducers=" << numReducers
              << " inputDir=" << inputDir
              << " intermDir=" << intermDir << "\n";

    try {
        mr::FileManager fm;

        std::size_t flushThreshold = 1000;

        // Mapper is assumed to implement partitioned intermediate outputs:
        // e.g., intermDir/mapper_<id>_r<rid>.txt or similar
        mr::Mapper mapper(fm, intermDir, flushThreshold, mapperId, numReducers);

        std::vector<std::string> inputFiles = fm.listTextFiles(inputDir);
        std::size_t linesProcessed = 0;

        for (const auto& path : inputFiles) {
            auto lines = fm.readAllLines(path);
            for (const auto& line : lines) {
                mapper.map(path, line);
                ++linesProcessed;
            }
        }

        mapper.flush();

        std::cout << "[mapper_worker] processed files=" << inputFiles.size()
                  << " lines=" << linesProcessed << "\n";
        return 0;
    }
    catch (const std::exception& ex) {
        std::cerr << "[mapper_worker] FATAL: " << ex.what() << "\n";
        return 2;
    }
}
