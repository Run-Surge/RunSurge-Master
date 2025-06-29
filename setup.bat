@echo off
set "VBOX_MANAGE=C:\Program Files\Oracle\VirtualBox\VBoxManage.exe"
set "DISK_PATH=C:\Users\manga\Downloads\AlpineVM.vdi"
@REM set "CD_PATH=alpine.iso"

"%VBOX_MANAGE%" --version

:: Create the VM (only run once)
"%VBOX_MANAGE%" createvm --name RunSurge --register

:: Allocate resources (only run once)
"%VBOX_MANAGE%" modifyvm RunSurge --memory 1024 --cpus 2 --ostype Linux_64

:: Attach NAT networking (for internet access)
"%VBOX_MANAGE%" modifyvm RunSurge --nic1 nat

:: Create a virtual hard disk (only run once)
@REM "%VBOX_MANAGE%" createhd --filename "%DISK_PATH%" --size 10240
@REM "%VBOX_MANAGE%" clonehd "%DISK_PATH%" "AlpineVM-new.vdi" --format VDI


:: Add the SATA controller (MUST run before attaching storage)
"%VBOX_MANAGE%" storagectl RunSurge --name "SATA Controller" --add sata --controller IntelAhci

:: Attach the virtual hard disk
"%VBOX_MANAGE%" storageattach RunSurge --storagectl "SATA Controller" --port 0 --device 0 --type hdd --medium "%DISK_PATH%"

@REM :: Attach the Alpine ISO
@REM "%VBOX_MANAGE%" storageattach RunSurge --storagectl "SATA Controller" --port 1 --device 0 --type dvddrive --medium "%CD_PATH%"



:: Forward the SSH Port
"%VBOX_MANAGE%" modifyvm RunSurge --natpf1 "ssh,tcp,,2222,,22"

:: Forward SMB/CIFS ports for file sharing
::"%VBOX_MANAGE%" modifyvm RunSurge --natpf1 "smb-445,tcp,,445,,445"
::"%VBOX_MANAGE%" modifyvm RunSurge --natpf1 "smb-139,tcp,,139,,139"

:: Add VirtualBox shared folder (replace C:\path\to\your\shared\folder with actual path)
@REM "%VBOX_MANAGE%" sharedfolder add RunSurge --name "shared" --hostpath "C:\path\to\your\shared\folder" --automount
