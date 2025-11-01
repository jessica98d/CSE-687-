#include "mr/Workflow.hpp"
#include "mr/Mapper.hpp"
#include "mr/Reducer.hpp"
#include "mr/FileManager.hpp"
#include "mr/Types.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <cstddef>

// ---------- Phase-2 (DLL plugins) headers are optional ----------
// Enable by defining MR_PHASE2_AVAILABLE via CMake if/when you need it.
//   target_compile_definitions(mapreduce_cli PRIVATE MR_PHASE2_AVAILABLE)
//   target_compile_definitions(mapreduce_gui PRIVATE MR_PHASE2_AVAILABLE)
#ifdef MR_PHASE2_AVAILABLE
  #include "mr/Interfaces.hpp"      // mr::IMapper, mr::IReducer, factory symbols
  #include "mr/PluginLoader.hpp"    // loadPlugins / freePlugins
  #include "mr/PluginContexts.hpp"  // MapContextAdapter / ReduceContextAdapter
#endif

namespace mr {

// ------------------------- ctor -------------------------
Workflow::Workflow(FileManager& fm,
                   const std::string& inputDir,
                   const std::string& tempDir,
                   const std::string& outputDir)
    : fileManager_(fm),
      inputDir_(inputDir),
      tempDir_(tempDir),
      outputDir_(outputDir) {
    fileManager_.ensureDir(tempDir_);
    fileManager_.ensureDir(outputDir_);
}

// -------------------- Phase-1 entrypoint -----------------
void Workflow::run() {
    doMapPhase();
    Grouped grouped = doSortAndGroup();
    doReducePhase(grouped);
}

// ------------- Phase-1: Map (to temp/intermediate.txt) -------------
void Workflow::doMapPhase() {
    const std::string tmpFile = tempDir_ + "/intermediate.txt";
    // Clear previous intermediate output
    fileManager_.writeAll(tmpFile, "");

    // Tuneable flush threshold
    Mapper mapper(fileManager_, tempDir_, /*flushThreshold=*/2048);

    const auto files = fileManager_.listFiles(inputDir_);
    for (const auto& path : files) {
        const auto lines = fileManager_.readAllLines(path);
        for (const auto& line : lines) {
            mapper.map(path, line);
        }
    }
    mapper.flush();
}

// -------- Phase-1: Sort & Group (word -> [1,1,...]) --------
Workflow::Grouped Workflow::doSortAndGroup() {
    Grouped grouped;
    const std::string tmpFile = tempDir_ + "/intermediate.txt";
    if (!fileManager_.exists(tmpFile)) {
        return grouped;
    }

    const auto lines = fileManager_.readAllLines(tmpFile);
    for (const auto& line : lines) {
        std::size_t tabPos = line.find('\t');
        if (tabPos == std::string::npos) {
            // also accept single-space separated fallback
            const std::size_t sp = line.find(' ');
            if (sp == std::string::npos) continue;

            const std::string word = line.substr(0, sp);
            int value = 0;
            try { value = std::stoi(line.substr(sp + 1)); } catch (...) { value = 0; }
            if (!word.empty() && value != 0) {
                grouped[word].push_back(value);
            }
            continue;
        }

        const std::string word = line.substr(0, tabPos);
        int value = 0;
        try { value = std::stoi(line.substr(tabPos + 1)); } catch (...) { value = 0; }
        if (!word.empty() && value != 0) {
            grouped[word].push_back(value);
        }
    }
    return grouped;
}

// --------------------- Phase-1: Reduce ---------------------
void Workflow::doReducePhase(const Grouped& grouped) {
    Reducer reducer(fileManager_, outputDir_);
    for (const auto& kv : grouped) {
        reducer.reduce(kv.first, kv.second);
    }
    reducer.markSuccess();
}

// ------------- Convenience: run and return counts ----------
std::vector<std::pair<std::string, int>> Workflow::runAndGetCounts() {
    doMapPhase();
    Grouped grouped = doSortAndGroup();

    std::map<std::string, int> totals;
    for (const auto& kv : grouped) {
        int sum = 0;
        for (int v : kv.second) sum += v;
        totals[kv.first] = sum;
    }

    // Write results like normal reduce, so files are consistent
    Reducer reducer(fileManager_, outputDir_);
    for (const auto& p : totals) {
        std::vector<int> one{ p.second };
        reducer.reduce(p.first, one);
    }
    reducer.markSuccess();

    std::vector<std::pair<std::string, int>> out;
    out.reserve(totals.size());
    for (const auto& p : totals) out.emplace_back(p.first, p.second);
    return out;
}

// ======================= Phase-2 path =======================
// Dynamically load Map/Reduce from DLLs and run with contexts.
// Keeps Phase-1 intact; you only use this when asked explicitly.
bool Workflow::runWithPlugins(const std::string& dllDir)
{
#ifndef MR_PHASE2_AVAILABLE
    (void)dllDir;
    throw std::runtime_error("Phase-2 plugins are not enabled in this build.");
#else
    // ----- Load user-specified Map/Reduce plugins -----
    PluginHandles ph = loadPlugins(dllDir);

    // Create mapper/reducer instances via factories with RAII cleanup
    std::unique_ptr<IMapper, void(*)(IMapper*)> mapper(
        ph.createMapper(), ph.destroyMapper);
    std::unique_ptr<IReducer, void(*)(IReducer*)> reducer(
        ph.createReducer(), ph.destroyReducer);

    // ----- MAP via plugin -----
    const std::string tmpFile = tempDir_ + "/intermediate.txt";
    fileManager_.writeAll(tmpFile, ""); // clear any previous intermediate

    MapContextAdapter mapCtx(fileManager_, tempDir_);

    const auto files = fileManager_.listFiles(inputDir_);
    for (const auto& path : files) {
        const auto lines = fileManager_.readAllLines(path);
        for (const auto& line : lines) {
            mapper->map(path, line, mapCtx);
        }
    }
    mapper->flush(mapCtx);

    // ----- SORT & GROUP (same as Phase-1) -----
    Grouped grouped = doSortAndGroup();

    // ----- REDUCE via plugin -----
    const std::string outFile = outputDir_ + "/word_counts.txt";
    ReduceContextAdapter reduceCtx(fileManager_, outFile);

    for (auto& kv : grouped) {
        reducer->reduce(kv.first, kv.second, reduceCtx);
    }

    // Reuse Phase-1 success marker for consistency
    Reducer builtin(fileManager_, outputDir_);
    builtin.markSuccess();

    // ----- Unload DLLs -----
    freePlugins(ph);
    return true;
#endif
}

} // namespace mr
