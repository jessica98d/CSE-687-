#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <vector>
#include <mutex>

class Logger {
public:
    void log(const std::string& message);

    std::string getAll() const;

    void clear();

private:
    mutable std::mutex mutex_;
    std::vector<std::string> lines_;
};

#endif // LOGGER_H

