#pragma once
#include <string>
#include <vector>
#include "mr/Types.hpp"      // <-- ADD this so Grouped is visible

namespace mr {

class FileManager;

class Workflow {
public:
  // Expose the alias so Workflow::Grouped compiles without other edits
  using Grouped = mr::Grouped;    // <-- ADD this one line

  Workflow(FileManager& fm,
           const std::string& inputDir,
           const std::string& tempDir,
           const std::string& outputDir);

  void run();
  std::vector<std::pair<std::string,int>> runAndGetCounts();

  bool runWithPlugins(const std::string& dllDir);

private:
  void    doMapPhase();
  Grouped doSortAndGroup();                    // keep signature as-is
  void    doReducePhase(const Grouped& g);     // keep signature as-is

  FileManager&   fileManager_;
  std::string    inputDir_, tempDir_, outputDir_;
};
} // namespace mr
