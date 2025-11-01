#ifndef TYPES_HPP
#define TYPES_HPP

#include <string>
#include <vector>
#include <utility>
#include <map>

namespace mr {

using Word   = std::string;
using Count  = int;
using KVPair = std::pair<Word, Count>;
using KVBuffer = std::vector<KVPair>;
using Grouped = std::map<Word, std::vector<Count>>;

} // namespace mr

#endif // TYPES_HPP
