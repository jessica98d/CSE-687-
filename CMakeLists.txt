cmake_minimum_required(VERSION 3.20)
project(CSE_687_MapReduce LANGUAGES CXX)

# ----------------------------------------------------------
# Compiler settings
# ----------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (MSVC)
    add_definitions(-DUNICODE -D_UNICODE)
endif()

# ----------------------------------------------------------
# Include path for shared headers (include/mr/* and friends)
# ----------------------------------------------------------
include_directories(${CMAKE_SOURCE_DIR}/include)

# ----------------------------------------------------------
# Runtime output folder for all executables / dlls
# ----------------------------------------------------------
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# ----------------------------------------------------------
# Shared sources (core MapReduce implementation)
# ----------------------------------------------------------
set(SHARED_SOURCES
    FileManager.cpp
    Mapper.cpp
    Reducer.cpp
    Workflow.cpp
)

# ----------------------------------------------------------
# GUI target (Phase 1)
# ----------------------------------------------------------
add_executable(mapreduce_gui WIN32
    GuiApp.cpp
    ${SHARED_SOURCES}
)

target_include_directories(mapreduce_gui PRIVATE ${CMAKE_SOURCE_DIR}/include)

if (WIN32)
    target_link_libraries(mapreduce_gui PRIVATE user32 gdi32 comdlg32 shell32)
endif()

set_target_properties(mapreduce_gui PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# ----------------------------------------------------------
# CLI target (Phase 1 + Phase 2 runner)
# ----------------------------------------------------------
add_executable(mapreduce_cli
    main_cli.cpp
    ${SHARED_SOURCES}
)

target_include_directories(mapreduce_cli PRIVATE ${CMAKE_SOURCE_DIR}/include)

set_target_properties(mapreduce_cli PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Common MSVC warnings/options
if (MSVC)
    target_compile_options(mapreduce_gui PRIVATE /W3 /MP /permissive-)
    target_compile_options(mapreduce_cli  PRIVATE /W3 /MP /permissive-)
endif()

# ----------------------------------------------------------
# Phase 2: Map/Reduce plugins (DLLs)
# ----------------------------------------------------------
add_library(Map SHARED dlls/MapDLL.cpp)
add_library(Reduce SHARED dlls/ReduceDLL.cpp)

target_include_directories(Map    PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(Reduce PRIVATE ${CMAKE_SOURCE_DIR}/include)

set_target_properties(Map PROPERTIES
    OUTPUT_NAME "Map"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)
set_target_properties(Reduce PROPERTIES
    OUTPUT_NAME "Reduce"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# ----------------------------------------------------------
# Convenience: create runtime folders beside the EXEs
# ----------------------------------------------------------
add_custom_command(TARGET mapreduce_gui POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_gui>/sample_input"
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_gui>/temp"
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_gui>/output"
)

add_custom_command(TARGET mapreduce_cli POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_cli>/sample_input"
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_cli>/temp"
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_cli>/output"
)

# ----------------------------------------------------------
# Copy Phase-2 DLLs next to executables that may load them
# ----------------------------------------------------------
function(copy_phase2_dlls target_name)
    add_custom_command(TARGET ${target_name} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different "$<TARGET_FILE:Map>"    "$<TARGET_FILE_DIR:${target_name}>/"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different "$<TARGET_FILE:Reduce>" "$<TARGET_FILE_DIR:${target_name}>/"
    )
endfunction()

copy_phase2_dlls(mapreduce_cli)
copy_phase2_dlls(mapreduce_gui)

# ==========================================================
# Phase 4: Networked controller + stub + workers
# ==========================================================

# Phase 4 Controller (Winsock) — ONLY builds phase4_controller.cpp
add_executable(mapreduce_phase4
    phase4_controller.cpp
)

target_include_directories(mapreduce_phase4 PRIVATE ${CMAKE_SOURCE_DIR}/include)

set_target_properties(mapreduce_phase4 PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if (MSVC)
    target_compile_options(mapreduce_phase4 PRIVATE /W3 /MP /permissive-)
endif()

# Mapper worker (uses core MapReduce classes)
add_executable(mapper_worker
    mapper_worker.cpp
    ${SHARED_SOURCES}
)

target_include_directories(mapper_worker PRIVATE ${CMAKE_SOURCE_DIR}/include)

set_target_properties(mapper_worker PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if (MSVC)
    target_compile_options(mapper_worker PRIVATE /W3 /MP /permissive-)
endif()

# Reducer worker (uses core MapReduce classes)
add_executable(reducer_worker
    reducer_worker.cpp
    ${SHARED_SOURCES}
)

# newly added
# ----------------------------------------------------------
# Silence MSVC getenv() deprecation warnings (Phase 4)
# ----------------------------------------------------------
if (MSVC)
    target_compile_definitions(mapreduce_gui PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(mapreduce_cli PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(mapper_worker  PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(reducer_worker PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()


target_include_directories(reducer_worker PRIVATE ${CMAKE_SOURCE_DIR}/include)

set_target_properties(reducer_worker PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if (MSVC)
    target_compile_options(reducer_worker PRIVATE /W3 /MP /permissive-)
endif()

# Phase 4 Stub (Winsock) — builds stub.cpp
add_executable(phase4_stub
    stub.cpp
)

target_include_directories(phase4_stub PRIVATE ${CMAKE_SOURCE_DIR}/include)

set_target_properties(phase4_stub PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if (MSVC)
    target_compile_options(phase4_stub PRIVATE /W3 /MP /permissive-)
endif()

# Link winsock on Windows for controller + stub
if (WIN32)
    target_link_libraries(mapreduce_phase4 PRIVATE ws2_32)
    target_link_libraries(phase4_stub     PRIVATE ws2_32)
endif()

# Convenience folders for phase4 controller runtime
add_custom_command(TARGET mapreduce_phase4 POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_phase4>/sample_input"
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_phase4>/temp"
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:mapreduce_phase4>/output"
)
