cmake_minimum_required(VERSION 3.20)
project(mapreduce_gui LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Force Unicode WinAPI
if (MSVC)
  add_definitions(-DUNICODE -D_UNICODE)
endif()

include_directories(${CMAKE_SOURCE_DIR}/include)

add_executable(mapreduce_gui WIN32
  src/GuiApp.cpp
  src/FileManager.cpp
  src/Mapper.cpp
  src/Reducer.cpp
  src/Workflow.cpp
)


# For MSVC: enable multithreaded compile and warnings
if (MSVC)
  target_compile_options(mapreduce_gui PRIVATE /W3 /permissive-)
endif()
