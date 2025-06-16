@echo off
setlocal

:: Parameters with defaults
set SMB_USERNAME=%1
set SMB_PASSWORD=%2
set SMB_SHARE_NAME=%3
set SHARED_FOLDER=%~4


:: Use defaults if parameters not provided
if "%SMB_USERNAME%"=="" set SMB_USERNAME=qemuguest
if "%SMB_PASSWORD%"=="" set SMB_PASSWORD=1234
if "%SHARED_FOLDER%"=="" set SHARED_FOLDER=D:\CMP 4th Year\First Term\GP\RunSurge-Master\vm\shared
if "%SMB_SHARE_NAME%"=="" set SMB_SHARE_NAME=qemu_share


echo Setting up SMB share with parameters:
echo Username: %SMB_USERNAME%
echo Share Name: %SMB_SHARE_NAME%
echo Folder: %SHARED_FOLDER%

:: Create the user with specified username and password
net user %SMB_USERNAME% %SMB_PASSWORD% /add

:: Create the folder if it doesn't exist
if not exist "%SHARED_FOLDER%" (
    mkdir "%SHARED_FOLDER%"
)

:: Share the folder with specified name
net share %SMB_SHARE_NAME%="%SHARED_FOLDER%" /GRANT:Everyone,FULL

:: Set NTFS permissions to allow the user and Everyone full access
icacls "%SHARED_FOLDER%" /grant Everyone:(OI)(CI)F /T
icacls "%SHARED_FOLDER%" /grant %SMB_USERNAME%:(OI)(CI)F /T

echo Done. Folder "%SHARED_FOLDER%" is shared as "%SMB_SHARE_NAME%"
echo SMB user "%SMB_USERNAME%" created with specified password
pause
