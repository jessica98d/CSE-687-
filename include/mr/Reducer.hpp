#ifndef REDUCER_H
#define REDUCER_H

#include <string>
#include <vector>
#include <utility>
#include <map>

class Reducer {
public:
    Reducer() = default;

    // Aggregates all (word, 1) pairs into (word, totalCount).
    std::vector<std::pair<std::string, std::size_t>> reduce(
        const std::vector<std::pair<std::string, int>>& mappedPairs) const;
};

#endif // REDUCER_H
