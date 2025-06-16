#!/bin/bash
qemu-system-x86_64.exe -m 2048 -smp 2 -net user,hostfwd=tcp::2222-:22 -accel tcg -cpu qemu64  -net nic -hda mydisk.qcow2