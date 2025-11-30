#include "MapReduceController.h"

#include "FileManager.h"
#include "Mapper.h"
#include "Reducer.h"
#include "Logger.h"

#include <filesystem>
#include <thread>
#include <mutex>
#include <vector>
#include <utility>
#include <iostream>

MapReduceController::MapReduceController(const std::string& inputPath,
                                         const std::string& outputFile,
                                         unsigned int workerCount)
    : inputPath_(inputPath),
      outputFile_(outputFile),
      workerCount_(workerCount < 3 ? 3u : workerCount) { // ensure at least 3
}

bool MapReduceController::run(Logger& logger) {
    logger.log("Starting MapReduce workflow...");

    FileManager fileManager(inputPath_);
    std::vector<std::filesystem::path> files = fileManager.listTextFiles();

    if (files.empty()) {
        logger.log("No .txt files found. Nothing to do.");
        return false;
    }

    logger.log("Discovered " + std::to_string(files.size()) + " file(s).");

    Mapper mapper;
    std::vector<std::pair<std::string, int>> allPairs;
    std::mutex pairsMutex;

    std::size_t index = 0;
    std::mutex indexMutex;

    auto worker = [&]() {
        while (true) {
            std::filesystem::path filePath;
            {
                std::lock_guard<std::mutex> lock(indexMutex);
                if (index >= files.size()) {
                    break;
                }
                filePath = files[index];
                ++index;
            }

            logger.log("Worker processing file: " + filePath.string());

            std::vector<std::string> lines = fileManager.readAllLines(filePath);
            std::vector<std::pair<std::string, int>> localPairs;

            for (const auto& line : lines) {
                auto mapped = mapper.mapLine(line);
                localPairs.insert(localPairs.end(), mapped.begin(), mapped.end());
            }

            {
                std::lock_guard<std::mutex> lock(pairsMutex);
                allPairs.insert(allPairs.end(), localPairs.begin(), localPairs.end());
            }

            logger.log("Finished file: " + filePath.string());
        }
    };

    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < workerCount_; ++i) {
        workers.emplace_back(worker);
    }

    for (auto& t : workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    logger.log("Mapping complete. Reducing results...");

    Reducer reducer;
    auto reduced = reducer.reduce(allPairs);

    std::filesystem::path outPath(outputFile_);
    fileManager.ensureDirectory(outPath.parent_path());

    fileManager.writeWordCounts(outputFile_, reduced);

    logger.log("MapReduce workflow complete. Output written to: " + outputFile_);

    return true;
}
