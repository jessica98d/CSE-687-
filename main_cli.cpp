#include "mr/Workflow.hpp"
#include "mr/FileManager.hpp"
#include <iostream>

int main(int argc, char** argv) {
    std::string inputDir  = (argc > 1 ? argv[1] : "sample_input");
    std::string tempDir   = (argc > 2 ? argv[2] : "temp");
    std::string outputDir = (argc > 3 ? argv[3] : "output");

    mr::FileManager fm;
    mr::Workflow wf(fm, inputDir, tempDir, outputDir);
    wf.run();

    std::cout << "MapReduce completed. Results in " << outputDir << std::endl;
    return 0;
}
