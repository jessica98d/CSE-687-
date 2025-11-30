#ifndef MAPREDUCECONTROLLER_H
#define MAPREDUCECONTROLLER_H

#include <string>

class Logger;

class MapReduceController {
public:
    MapReduceController(const std::string& inputPath,
                        const std::string& outputFile,
                        unsigned int workerCount = 3);

    // Run the full workflow: discover files, map in parallel, reduce, write CSV.
    // Returns true on success.
    bool run(Logger& logger);

private:
    std::string inputPath_;
    std::string outputFile_;
    unsigned int workerCount_;
};

#endif // MAPREDUCECONTROLLER_H
