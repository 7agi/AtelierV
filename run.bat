@echo off
setlocal
title AtelierV - Creative Hub & Digital Marketplace

echo =====================================================================
echo  AtelierV: User & Artist Portal (Port 7860)
echo =====================================================================
echo.

set "EXE_PATH="

if exist "%~dp0atelierv.exe" (
    set "EXE_PATH=%~dp0atelierv.exe"
) else if exist "%~dp0target\release\atelierv.exe" (
    set "EXE_PATH=%~dp0target\release\atelierv.exe"
) else if exist "%~dp0target\x86_64-pc-windows-msvc\release\atelierv.exe" (
    set "EXE_PATH=%~dp0target\x86_64-pc-windows-msvc\release\atelierv.exe"
)

if defined EXE_PATH (
    echo [*] Found User Portal executable: "!EXE_PATH!"
    echo [*] Launching User & Artist portal on http://localhost:7860...
    echo.
    "%EXE_PATH%"
    goto :end
)

where cargo >nul 2>nul
if %errorlevel% equ 0 (
    echo [*] Building locally with Cargo...
    cargo run --release --bin atelierv
    goto :end
)

echo [!] atelierv.exe was not found in this folder.
echo.
echo Download the precompiled binary from your GitHub Actions run:
echo   1. Push this code to GitHub.
echo   2. Go to Actions -> 'Build & Release AtelierV Executables'.
echo   3. Download the 'atelierv-windows-x64' artifact.
echo   4. Place atelierv.exe right here next to run.bat!
echo.

:end
pause
