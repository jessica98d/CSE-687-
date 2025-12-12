// phase3_main.cpp

#include "mr/FileManager.hpp"
#include "mr/Types.hpp"
#include "mr/Interfaces.hpp"

#include <iostream>
#include <string>
#include <vector>

#ifdef _WIN32
  #include <process.h>   // _spawnvp, _cwait
  #include <cstdlib>
  using ProcessId = intptr_t;
#else
  #include <sys/wait.h>
  #include <unistd.h>
  using ProcessId = pid_t;
#endif

// -------------------------
// Small helper wrappers
// -------------------------
struct MapperProcess {
    ProcessId pid = -1;
    int id;
    int numReducers;
    std::string inputDir;
    std::string intermDir;

    MapperProcess(int id_, int R,
                  std::string inDir,
                  std::string imDir)
        : id(id_), numReducers(R),
          inputDir(std::move(inDir)),
          intermDir(std::move(imDir)) {}

    void run() {
#ifdef _WIN32
        const char* exe = "mapper_worker";

        std::string idStr = std::to_string(id);
        std::string RStr  = std::to_string(numReducers);

        const char* args[] = {
            exe,
            idStr.c_str(),
            RStr.c_str(),
            inputDir.c_str(),
            intermDir.c_str(),
            nullptr
        };

        pid = _spawnvp(_P_NOWAIT, exe, const_cast<char* const*>(args));
        if (pid == -1) {
            std::perror("spawn failed for mapper_worker");
        }
#else
        pid = fork();
        if (pid < 0) {
            std::perror("fork() failed for mapper");
            return;
        }

        if (pid == 0) {
            const char* exe = "./mapper_worker";

            std::string idStr  = std::to_string(id);
            std::string RStr   = std::to_string(numReducers);

            execlp(
                exe, exe,
                idStr.c_str(),
                RStr.c_str(),
                inputDir.c_str(),
                intermDir.c_str(),
                nullptr
            );

            std::perror("execlp() failed for mapper");
            _exit(1);
        }
#endif
    }

    int wait() const {
        if (pid <= 0) return -1;
        int status = 0;

#ifdef _WIN32
        _cwait(&status, pid, 0);
#else
        waitpid(pid, &status, 0);
#endif
        return status;
    }
};

struct ReducerProcess {
    ProcessId pid = -1;
    int id;
    std::string intermDir;
    std::string outDir;

    ReducerProcess(int id_,
                   std::string imDir,
                   std::string oDir)
        : id(id_),
          intermDir(std::move(imDir)),
          outDir(std::move(oDir)) {}

    void run() {
#ifdef _WIN32
        const char* exe = "reducer_worker";

        std::string idStr = std::to_string(id);

        const char* args[] = {
            exe,
            idStr.c_str(),
            intermDir.c_str(),
            outDir.c_str(),
            nullptr
        };

        pid = _spawnvp(_P_NOWAIT, exe, const_cast<char* const*>(args));
        if (pid == -1) {
            std::perror("spawn failed for reducer_worker");
        }
#else
        pid = fork();
        if (pid < 0) {
            std::perror("fork() failed for reducer");
            return;
        }

        if (pid == 0) {
            const char* exe = "./reducer_worker";

            std::string idStr = std::to_string(id);

            execlp(
                exe, exe,
                idStr.c_str(),
                intermDir.c_str(),
                outDir.c_str(),
                nullptr
            );

            std::perror("execlp() failed for reducer");
            _exit(1);
        }
#endif
    }

    int wait() const {
        if (pid <= 0) return -1;
        int status = 0;

#ifdef _WIN32
        _cwait(&status, pid, 0);
#else
        waitpid(pid, &status, 0);
#endif
        return status;
    }
};

// -------------------------
// CLI parsing helpers
// -------------------------

struct Config {
    int numMappers  = 3;
    int numReducers = 2;
    std::string inputDir  = "input";
    std::string intermDir = "intermediate";
    std::string outputDir = "output";
};

Config parseArgs(int argc, char** argv) {
    Config cfg;
    // mapreduce_phase3 [numMappers] [numReducers] [inputDir] [intermDir] [outputDir]
    if (argc > 1) cfg.numMappers  = std::stoi(argv[1]);
    if (argc > 2) cfg.numReducers = std::stoi(argv[2]);
    if (argc > 3) cfg.inputDir    = argv[3];
    if (argc > 4) cfg.intermDir   = argv[4];
    if (argc > 5) cfg.outputDir   = argv[5];

    return cfg;
}

int main(int argc, char** argv) {
    auto cfg = parseArgs(argc, argv);

    std::cout << "Phase 3 controller starting...\n";
    std::cout << "Mappers: " << cfg.numMappers
              << ", Reducers: " << cfg.numReducers << "\n";
    std::cout << "Input: " << cfg.inputDir
              << ", Intermediate: " << cfg.intermDir
              << ", Output: " << cfg.outputDir << "\n";

    // 1. Start all mapper processes
    std::vector<MapperProcess> mappers;
    mappers.reserve(cfg.numMappers);

    for (int i = 0; i < cfg.numMappers; ++i) {
        mappers.emplace_back(i, cfg.numReducers, cfg.inputDir, cfg.intermDir);
        mappers.back().run();
    }

    // 2. Wait for all mappers
    for (const auto& m : mappers) {
        int status = m.wait();
        if (status != 0) {
            std::cerr << "Mapper " << m.id
                      << " exited with status " << status << "\n";
        }
    }

    std::cout << "All mapper processes completed.\n";

    // 3. Start all reducer processes
    std::vector<ReducerProcess> reducers;
    reducers.reserve(cfg.numReducers);

    for (int r = 0; r < cfg.numReducers; ++r) {
        reducers.emplace_back(r, cfg.intermDir, cfg.outputDir);
        reducers.back().run();
    }

    // 4. Wait for all reducers
    for (const auto& r : reducers) {
        int status = r.wait();
        if (status != 0) {
            std::cerr << "Reducer " << r.id
                      << " exited with status " << status << "\n";
        }
    }

    std::cout << "All reducer processes completed.\n";
    std::cout << "Phase 3 MapReduce job finished.\n";
    return 0;
}
