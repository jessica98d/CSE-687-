#ifndef MAPPER_H
#define MAPPER_H

#include <string>
#include <vector>
#include <utility>

class Mapper {
public:
    Mapper() = default;

    // Breaks a line into normalized (lowercase, alnum-only) word tokens.
    std::vector<std::pair<std::string, int>> mapLine(const std::string& line) const;

private:
    static bool isWordCharacter(char c);
};

#endif // MAPPER_H
