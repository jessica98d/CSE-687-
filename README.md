Click the green 'Code' button, then select download code
  Opening the Project in Visual Studio 2022
    1. Launch Visual Studio 2022.
    2. From the start screen, choose 'Open a local folder'.
    3. Browse to the folder that contains the CMakeLists.txt file and click 'Select Folder'.
    4. Visual Studio will detect and load the CMake project automatically.
    5. Wait for the message 'CMake generation finished' to appear in the Output window.
  Configuring the Build Settings
    1.In the Visual Studio toolbar, find the configuration dropdown menus.
    2. Set 'Configuration' to Release (or Debug for development).
    3. Set 'Platform' to x64.
    4. If the options are unavailable, go to 'Manage Configurations' and enable 'x64-Release'.
    5. Visual Studio will regenerate the CMake build system for the chosen configuration.
  Building the Project
    1. In Visual Studio, open the menu Build → Build All (or press Ctrl + Shift + B).
    2. Wait until the build completes successfully with the message:
       '========== Build: 1 succeeded, 0 failed =========='
    3. The compiled executable (mapreduce_gui.exe) will appear under:
       build\Release\mapreduce_gui.exe
 Running the Program
    1. Open 'Developer Command Prompt for VS 2022'.
    2. Navigate to the project directory:
    3. Run the following commands line-by-line:
      rd /s /q build 2>nul
     cmake -S . -B build
     cmake --build build --config Release
   .\build\Release\mapreduce_gui.exe
  Verification
    1. After running successfully, check the output folder for result files.
    2. The GUI window should display a neatly formatted Word/Count table.
    3. The total word count should appear at the bottom of the di

