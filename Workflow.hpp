#ifndef WORKFLOW_HPP
#define WORKFLOW_HPP

#include "Types.hpp"
#include "FileManager.hpp"
#include "Mapper.hpp"
#include "Reducer.hpp"

#include <string>
#include <vector>

namespace mr {

class Workflow {
public:
    Workflow(FileManager& fm,
             const std::string& inputDir,
             const std::string& tempDir,
             const std::string& outputDir);

    void run();
    std::vector<std::pair<std::string, int>> runAndGetCounts();

private:
    FileManager& fileManager_;
    std::string inputDir_;
    std::string tempDir_;
    std::string outputDir_;

    void doMapPhase();
    Grouped doSortAndGroup();
    void doReducePhase(const Grouped& grouped);
};

} // namespace mr

#endif // WORKFLOW_HPP
