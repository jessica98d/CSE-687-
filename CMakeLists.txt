cmake_minimum_required(VERSION 3.22)

project(MRP2_Phase3_GUI LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

foreach(dir RUNTIME LIBRARY ARCHIVE)
  set(CMAKE_${dir}_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endforeach()

add_executable(MRP2_Phase3_GUI WIN32
    src/GuiApp.cpp
    src/FileManager.cpp
    src/Mapper.cpp
    src/Reducer.cpp
    src/MapReduceController.cpp
    src/Logger.cpp
)

target_include_directories(MRP2_Phase3_GUI PRIVATE src)
