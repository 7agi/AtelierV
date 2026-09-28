@echo off
setlocal
title AtelierV - Creative Commission & Escrow Hub

echo =====================================================================
echo  AtelierV: VGen-Style Single-Binary Local Hub
echo =====================================================================
echo.

:: 1. Look for atelierv.exe in current folder or build target
set "EXE_PATH="

if exist "%~dp0atelierv.exe" (
    set "EXE_PATH=%~dp0atelierv.exe"
) else if exist "%~dp0target\release\atelierv.exe" (
    set "EXE_PATH=%~dp0target\release\atelierv.exe"
) else if exist "%~dp0target\x86_64-pc-windows-msvc\release\atelierv.exe" (
    set "EXE_PATH=%~dp0target\x86_64-pc-windows-msvc\release\atelierv.exe"
)

if defined EXE_PATH (
    echo [*] Found AtelierV executable: "!EXE_PATH!"
    echo [*] Starting local engine and opening browser at http://localhost:7860...
    echo.
    "%EXE_PATH%"
    goto :end
)

:: 2. If executable not found, check if local cargo exists
where cargo >nul 2>nul
if %errorlevel% equ 0 (
    echo [*] atelierv.exe not found, but Cargo was detected. Building locally...
    cargo run --release
    goto :end
)

:: 3. Executable not found and no local Rust installed -> guide to GitHub Actions build
echo [!] atelierv.exe was not found in this folder.
echo.
echo Because you don't have Rust or disk space locally, download the precompiled binary:
echo   1. Push this code to your GitHub repository.
echo   2. Go to your repo's "Actions" tab.
echo   3. Click the latest run of "Build & Release AtelierV Executable".
echo   4. Download the "atelierv-windows-x64" zip from the Artifacts section.
echo   5. Unzip and place `atelierv.exe` right next to this run.bat file!
echo.
echo Once placed here, run this script again to launch immediately without installing anything.
echo.

:end
pause
