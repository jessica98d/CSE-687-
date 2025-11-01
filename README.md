# 🧩 MapReduce Word Count — C++ (GUI + CLI)

## 📘 Overview
This project implements a simplified **MapReduce framework** in modern C++17, designed to process text files and count word occurrences across multiple inputs.

It demonstrates:
- Object-oriented modular design (FileManager, Mapper, Reducer, Workflow)
- Clean separation of computation and I/O
- Both **Win32 GUI** and **Command-line (CLI)** executables
- Compliance with all specification requirements

---

## ⚙️ Components Summary

| Component | Description |
|------------|--------------|
| **FileManager** | Handles all file system I/O — reading, writing, appending, directory creation. All file access flows through this class. |
| **Mapper** | Reads each line, tokenizes words, normalizes text (lowercase, remove punctuation), and buffers (“exports”) key-value pairs to disk periodically. |
| **Sorting & Grouping** | Performed by the `Workflow` class — converts raw intermediary tuples into grouped lists for reduction. |
| **Reducer** | Sums the values for each unique word. The `reduce()` method performs computation only; `exportResult()` writes to the output directory. Creates an empty `SUCCESS` file upon completion. |
| **Workflow** | Orchestrates the entire pipeline: Map → Sort/Group → Reduce. |
| **Executive (CLI/GUI)** | Initiates the workflow either via command-line arguments or a Win32 graphical user interface. |

---

## 🖥️ Build Instructions (Visual Studio 2022 / CMake)

### Prerequisites
- Visual Studio 2022 (with “Desktop Development with C++” workload)
- CMake ≥ 3.20

### Steps
1. Open Visual Studio → **File → Open → Folder...**
2. Select the root project folder (contains `CMakeLists.txt`).
3. Wait for CMake to configure automatically.
4. Choose configuration: `x64-Debug` or `x64-Release`.
5. Build: **Ctrl + Shift + B**

### Build Outputs
After build:
```
out/build/x64-Debug/bin/
├── mapreduce_gui.exe
├── mapreduce_cli.exe
├── sample_input/
├── temp/
└── output/
```

---

## 🧭 Run Instructions

### GUI Mode
- Run **mapreduce_gui.exe** (or `Ctrl + F5` in Visual Studio).  
- Click **“Run MapReduce”** to start.  
- The output text box will display formatted word counts.

### Command Line Mode
You can also use the CLI version:
```bash
mapreduce_cli.exe [inputDir] [tempDir] [outputDir]
```
Example:
```bash
mapreduce_cli.exe sample_input temp output
```
Output appears in the specified `output/` directory:
```
word_counts.txt
SUCCESS
```

---

## 🗂️ Sample Input and Output

### Input (sample_input/a.txt)
```
Hello world
This is CSE MapReduce Phase
Map Reduce Map Reduce
MapReduce Word Count Counts Words
```

### Intermediate File (temp/intermediate.txt)
```
hello   1
world   1
this    1
is      1
...
```

### Final Output (output/word_counts.txt)
```
count   1
counts  1
cse     1
hello   2
is      1
map     2
mapreduce 2
...
```

### Success Indicator
```
output/SUCCESS   # empty file
```

---

## 🧩 Design Highlights

- ✅ **Abstraction:** File system encapsulated by `FileManager`.
- ✅ **Buffered I/O:** Mapper exports data periodically based on buffer size.
- ✅ **Separation of Concerns:** Reducer does no direct file I/O.
- ✅ **Cross-Executable Reuse:** GUI and CLI share identical business logic.
- ✅ **Standards Compliance:** Follows the six requirements of the course specification.

---

## 🧪 Testing & Debugging Tips
- To debug filesystem issues, confirm `sample_input`, `temp`, and `output` directories exist alongside the executable.
- Run with `Ctrl + F5` to keep the GUI window open.
- Logs or additional `std::cout` statements can be added to `Workflow::run()` or `Mapper::flush()` for inspection.

---

## 📦 Packaging (Optional)
To generate a distributable build:
```bash
cmake --install out/build/x64-Debug --prefix out/install
```
You’ll get a self-contained package at:
```
out/install/bin/
```

---

## 👩🏽‍💻 Authors
**Jessica, Michael and Taylor**

---

_Last updated: 2025-11-01 13:23:54_
