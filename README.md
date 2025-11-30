# 📘 CSE-687 Project — Phase 3: Multi-Process MapReduce

This project implements a **multi-process MapReduce system** in C++ using the architecture defined in CSE-687 (Object-Oriented Design).  
Phase 3 extends the Phase 2 single-process design by adding a **controller** and multiple **worker processes** for mappers and reducers.

## 🚀 Features

### ✔ Multi-Process Architecture
- A **controller** (`mapreduce_phase3.exe`) spawns:
  - *N* mapper processes
  - *R* reducer processes  
- Mappers and reducers are run as separate executables:
  - `mapper_worker.exe`
  - `reducer_worker.exe`

### ✔ Dynamic worker creation
Workers receive command-line arguments:
- Mapper: `mapperId`, `numReducers`, `inputDir`, `intermediateDir`
- Reducer: `reducerId`, `intermediateDir`, `outputDir`

### ✔ Partitioned intermediate output
Each mapper generates files of the form:

```
m<mapperId>_r<reducerId>.txt
```

### ✔ Reducer aggregation
Reducers read all matching partition files and compute final counts.

### ✔ Final output
Reducers produce final results in the `output/` directory.  
Example:

```
output/
  word_counts.txt
  SUCCESS.txt
```

## 📂 Directory Structure

```
bin/
  mapreduce_phase3.exe
  mapper_worker.exe
  reducer_worker.exe
  sample_input/
  intermediate/
  output/
```

## 🛠️ Build Instructions (CMake + Visual Studio)

Build using Visual Studio 2022:
1. Open the CMake project folder.
2. Configure and build using **x64-Debug** or **x64-Release**.
3. Executables appear under:

```
out/build/<config>/bin/
```

## ▶️ Running the Program

From inside the **bin** directory:

```bash
mapreduce_phase3.exe <numMappers> <numReducers> <inputDir> <intermediateDir> <outputDir>
```

### Example:

```bash
mapreduce_phase3.exe 3 2 sample_input intermediate output
```

## 📝 Input Requirements

- Input files should be `.txt` (lowercase).
- Place them inside the folder provided as `<inputDir>`.

## 📜 Intermediate File Format

Tab-separated key-value pairs:

```
word    1
word    1
hello   1
```

## 📦 Output Files

```
word_counts.txt   # aggregated results
SUCCESS.txt       # completion flag
```

## 👤 Author
**Jessica, Michael and Taylor**

