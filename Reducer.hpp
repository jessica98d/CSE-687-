#ifndef REDUCER_HPP
#define REDUCER_HPP

#include "Types.hpp"
#include "FileManager.hpp"
#include <string>
#include <vector>

namespace mr {

class Reducer {
public:
    explicit Reducer(FileManager& fm, const std::string& outputDir);

    // Reduce a single key's list of counts to a total and write it
    void reduce(const Word& word, const std::vector<Count>& counts);

    // Write success marker and close out
    void markSuccess();

private:
    FileManager& fileManager_;
    std::string outputDir_;
    std::string outFilePath_;
};

} // namespace mr

#endif // REDUCER_HPP
