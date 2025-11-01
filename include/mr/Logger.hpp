#pragma once
#include <string>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <sstream>
#include "FileManager.hpp"  

namespace mr {

class Logger {
public:
    enum class Level { Info, Warn, Error };

    Logger(FileManager& fm, std::string logPath, Level min=Level::Info)
        : fm_(fm), path_(std::move(logPath)), minLevel_(min) {
        // Ensure directory exists; FileManager handles parent creation
        fm_.appendLine(path_, banner("LOGGER STARTED"));
    }

    void info (const std::string& m) { write(Level::Info,  "INFO ", m); }
    void warn (const std::string& m) { write(Level::Warn,  "WARN ", m); }
    void error(const std::string& m) { write(Level::Error, "ERROR", m); }

    void setLevel(Level lvl) { minLevel_ = lvl; }

private:
    static std::string nowIso() {
        using namespace std::chrono;
        const auto t = system_clock::to_time_t(system_clock::now());
        std::tm tm{};
#ifdef _WIN32
        localtime_s(&tm, &t);
#else
        localtime_r(&t, &tm);
#endif
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    static std::string banner(const char* msg) {
        std::ostringstream oss;
        oss << "========== " << msg << " ==========";
        return oss.str();
    }

    void write(Level lvl, const char* tag, const std::string& m) {
        if (lvl < minLevel_) return;
        std::lock_guard<std::mutex> lk(mu_);
        fm_.appendLine(path_, nowIso() + " [" + tag + "] " + m);
    }

    FileManager&   fm_;
    std::string    path_;
    std::mutex     mu_;
    Level          minLevel_;
};

} // namespace mr
