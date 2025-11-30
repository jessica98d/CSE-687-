#include "Reducer.h"

std::vector<std::pair<std::string, std::size_t>> Reducer::reduce(
    const std::vector<std::pair<std::string, int>>& mappedPairs) const {

    std::map<std::string, std::size_t> accumulator;

    for (const auto& pair : mappedPairs) {
        const std::string& word = pair.first;
        int value = pair.second;
        if (value != 0) {
            accumulator[word] += static_cast<std::size_t>(value);
        }
    }

    std::vector<std::pair<std::string, std::size_t>> result;
    result.reserve(accumulator.size());

    for (const auto& entry : accumulator) {
        result.emplace_back(entry.first, entry.second);
    }

    return result;
}
