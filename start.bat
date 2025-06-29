@echo off
set "VBOX_MANAGE=C:\Program Files\Oracle\VirtualBox\VBoxManage.exe"
"%VBOX_MANAGE%" startvm RunSurge --type headless
pause