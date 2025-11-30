#include "Logger.h"

void Logger::log(const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    lines_.push_back(message);
}

std::string Logger::getAll() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string result;
    for (const auto& line : lines_) {
        result += line;
        result += "\r\n";
    }
    return result;
}

void Logger::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lines_.clear();
}
