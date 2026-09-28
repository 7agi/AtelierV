@echo off
setlocal enabledelayedexpansion
title AtelierV - Admin Command Center (Port 7861)

echo =====================================================================
echo  AtelierV: Dedicated Admin Command Center (Port 7861)
echo =====================================================================
echo.

set "ADMIN_EXE="

if exist "%~dp0atelierv-admin.exe" (
    set "ADMIN_EXE=%~dp0atelierv-admin.exe"
) else if exist "%~dp0target\release\atelierv-admin.exe" (
    set "ADMIN_EXE=%~dp0target\release\atelierv-admin.exe"
) else if exist "%~dp0target\x86_64-pc-windows-msvc\release\atelierv-admin.exe" (
    set "ADMIN_EXE=%~dp0target\x86_64-pc-windows-msvc\release\atelierv-admin.exe"
)

if defined ADMIN_EXE (
    echo [*] Found Admin Portal executable: "!ADMIN_EXE!"
    echo [*] Launching Admin Command Center on http://localhost:7861...
    echo.
    "%ADMIN_EXE%"
    goto :end
)

where cargo >nul 2>nul
if %errorlevel% equ 0 (
    echo [*] Building admin binary locally with Cargo...
    cargo run --release --bin atelierv-admin
    goto :end
)

echo [!] atelierv-admin.exe was not found in this folder.
echo.
echo Download the precompiled admin binary from your GitHub Actions run:
echo   1. Push this code to GitHub.
echo   2. Go to Actions -> 'Build & Release AtelierV Executables'.
echo   3. Download the 'atelierv-admin-windows-x64' artifact.
echo   4. Place atelierv-admin.exe right here next to run-admin.bat!
echo.

:end
pause
