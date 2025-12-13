# MapReduce Framework – Phase 4 (Distributed Execution)

## Overview

This project implements a multi-phase MapReduce framework in C++, culminating in **Phase 4**, which supports:

- Distributed execution using a **controller + stub** architecture
- Multiple mapper and reducer worker processes
- TCP-based coordination using **HELLO / BEGIN** handshakes
- Deterministic partitioning and reduction
- A final **merged output file** (`word_counts.txt`)
- No hard-coded paths; all configuration via command-line arguments

This Phase 4 implementation builds directly on Phases 1–3 and satisfies all stated Phase 4 requirements.

---

## Architecture (Phase 4)

```
+----------------------+
| phase4_controller    |
| (central coordinator)|
+----------+-----------+
           |
     TCP SPAWN / BEGIN
           |
+----------+-----------+
|      phase4_stub     |
|  (process spawner)   |
+----------+-----------+
           |
   CreateProcess()
           |
+----------+-----------+
| mapper_worker /      |
| reducer_worker       |
+----------------------+
```

---

## Build Output

All executables are generated into:

```
out/build/x64-Debug/bin/
```

### Executables

| Executable | Description |
|----------|-------------|
| mapreduce_gui.exe | Phase 1 GUI |
| mapreduce_cli.exe | Phase 1–2 CLI |
| mapper_worker.exe | Mapper worker (Phase 3–4) |
| reducer_worker.exe | Reducer worker (Phase 3–4) |
| mapreduce_phase4.exe | Phase 4 controller |
| phase4_stub.exe | Phase 4 stub (distributed spawner) |

---

## Runtime Directory Layout

```
bin/
├── sample_input/        # Input text files
├── temp/                # Intermediate mapper output (mX_rY.txt)
├── output/
│   ├── word_counts_r0.txt
│   ├── word_counts_r1.txt
│   ├── word_counts.txt   # FINAL MERGED OUTPUT
│   ├── SUCCESS_r0
│   ├── SUCCESS_r1
│   └── SUCCESS
```

---

## Build Instructions (Windows)

### Clean Build (Recommended)

```powershell
cd out/build/x64-Debug
rmdir /s /q *
cmake ../..
cmake --build . --config Debug
```

---

## Running Phase 4

### Terminal 1 – Start Stub

```powershell
cd bin
phase4_stub.exe 5001 127.0.0.1 6001
```

---

### Terminal 2 – Run Controller

```powershell
cd bin
mapreduce_phase4.exe sample_input temp output 127.0.0.1:5001 2 2
```

---

## Execution Flow

1. Controller partitions input files across mappers
2. Controller instructs stubs to spawn mapper workers
3. Mapper workers wait for BEGIN, then emit partitioned intermediate files
4. Controller instructs stubs to spawn reducer workers
5. Reducers process only their assigned partitions
6. Reducers write `word_counts_rX.txt` and `SUCCESS_rX`
7. Controller merges reducer outputs into `word_counts.txt`
8. Controller writes global `SUCCESS` marker

---

## Phase 4 Compliance

✔ Distributed controller/stub design  
✔ TCP-based worker coordination  
✔ Multiple mapper and reducer processes  
✔ No shared-memory or hard-coded paths  
✔ Correct partitioning and reduction  
✔ Final merged output  
✔ SUCCESS markers  

---

## Author
Jessica, Michael Taylor
