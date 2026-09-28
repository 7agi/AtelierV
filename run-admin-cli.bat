@echo off
setlocal
title AtelierV - Native Admin Console CLI

echo =====================================================================
echo  AtelierV: Native Terminal Admin CLI (Non-Web)
echo =====================================================================
echo.

set "CLI_EXE="

if exist "%~dp0atelierv-admin-cli.exe" (
    set "CLI_EXE=%~dp0atelierv-admin-cli.exe"
) else if exist "%~dp0target\release\atelierv-admin-cli.exe" (
    set "CLI_EXE=%~dp0target\release\atelierv-admin-cli.exe"
) else if exist "%~dp0target\x86_64-pc-windows-msvc\release\atelierv-admin-cli.exe" (
    set "CLI_EXE=%~dp0target\x86_64-pc-windows-msvc\release\atelierv-admin-cli.exe"
)

if defined CLI_EXE (
    "%CLI_EXE%"
    goto :end
)

where cargo >nul 2>nul
if %errorlevel% equ 0 (
    cargo run --release --bin atelierv-admin-cli
    goto :end
)

echo [!] atelierv-admin-cli.exe not found in this folder.
echo     Download the 'atelierv-suite-windows-x64' artifact from your GitHub Actions run!
echo.

:end
pause
