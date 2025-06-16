@echo off
setlocal

set "MEMORY=%~1"
set "CPUS=%~2"
set "SSH_PORT=%~3"
set "DISK_IMAGE=%~4"
set "QEMU_PATH=%~5"

if "%DISK_IMAGE%"=="" set "DISK_IMAGE=D:\CMP 4th Year\First Term\GP\RunSurge-Master\vm\mydisk.qcow2"
if "%MEMORY%"=="" set "MEMORY=2048"
if "%CPUS%"=="" set "CPUS=2"
if "%SSH_PORT%"=="" set "SSH_PORT=2222"
if "%QEMU_PATH%"=="" set "QEMU_PATH=C:\Program Files\qemu\qemu-system-x86_64.exe"

echo Starting VM with parameters:
echo Disk Image: "%DISK_IMAGE%"
echo Memory: %MEMORY%MB
echo CPUs: %CPUS%
echo SSH Port: %SSH_PORT%
echo QEMU Path: "%QEMU_PATH%"

"%QEMU_PATH%" ^
  -m %MEMORY% ^
  -smp %CPUS% ^
  -cpu qemu64 ^
  -accel tcg ^
  -net nic ^
  -net user,hostfwd=tcp::%SSH_PORT%-:22 ^
  -hda "%DISK_IMAGE%" ^
  -display none ^
  -nographic
