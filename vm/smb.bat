@echo off
setlocal enabledelayedexpansion

:: Initialize variables
set "SMB_USERNAME="
set "SMB_PASSWORD="
set "SMB_SHARE_NAME="
set "SHARED_FOLDER="
set "arg_count=0"

:: Loop through all arguments
for %%a in (%*) do (
    set /a arg_count+=1
    if !arg_count! == 1 set "SMB_USERNAME=%%~a"
    if !arg_count! == 2 set "SMB_PASSWORD=%%~a"
    if !arg_count! == 3 set "SMB_SHARE_NAME=%%~a"
    if !arg_count! gtr 3 (
        if defined SHARED_FOLDER (
            set "SHARED_FOLDER=!SHARED_FOLDER! %%~a"
        ) else (
            set "SHARED_FOLDER=%%~a"
        )
    )
)

:: Use defaults if parameters not provided
if not defined SMB_USERNAME set "SMB_USERNAME=qemuguest"
if not defined SMB_PASSWORD set "SMB_PASSWORD=1234"
if not defined SMB_SHARE_NAME set "SMB_SHARE_NAME=qemu_share"
if not defined SHARED_FOLDER set "SHARED_FOLDER=%~dp0shared"

echo Setting up SMB share with parameters:
echo Username: %SMB_USERNAME%
echo Share Name: %SMB_SHARE_NAME%
echo Folder: "%SHARED_FOLDER%"

:: Create the user with specified username and password
net user "%SMB_USERNAME%" "%SMB_PASSWORD%" /add >nul 2>&1

:: Create the folder if it doesn't exist
if not exist "%SHARED_FOLDER%" (
    mkdir "%SHARED_FOLDER%"
)

:: Share the folder with specified name
net share "%SMB_SHARE_NAME%"="%SHARED_FOLDER%" /GRANT:Everyone,FULL

:: Set NTFS permissions to allow the user and Everyone full access
icacls "%SHARED_FOLDER%" /grant Everyone:(OI)(CI)F /T
icacls "%SHARED_FOLDER%" /grant "%SMB_USERNAME%":(OI)(CI)F /T

echo Done. Folder "%SHARED_FOLDER%" is shared as "%SMB_SHARE_NAME%"
echo SMB user "%SMB_USERNAME%" created with specified password.
endlocal
