#include "Mapper.h"

#include <cctype>

bool Mapper::isWordCharacter(char c) {
    return static_cast<bool>(std::isalnum(static_cast<unsigned char>(c)));
}

std::vector<std::pair<std::string, int>> Mapper::mapLine(const std::string& line) const {
    std::vector<std::pair<std::string, int>> pairs;
    std::string current;

    for (char c : line) {
        if (isWordCharacter(c)) {
            current.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        } else {
            if (!current.empty()) {
                pairs.emplace_back(current, 1);
                current.clear();
            }
        }
    }

    if (!current.empty()) {
        pairs.emplace_back(current, 1);
    }

    return pairs;
}

