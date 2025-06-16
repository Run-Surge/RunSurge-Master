import paramiko
import subprocess
import sys
import time
import os
import logging
import threading
import platform
from typing import Optional, Tuple, Dict, Any
import socket
import psutil

class VMTaskExecutor:    
    def __init__(self, 
                 disk_image: str = "D:\\CMP 4th Year\\First Term\\GP\\RunSurge-Master\\vm\\mydisk.qcow2",
                 ssh_username: str = "root", 
                 ssh_password: str = "1234",
                 ssh_port: int = 2222,
                 memory: int = 2048,
                 cpus: int = 2,
                 vm_startup_timeout: int = 90,
                 task_timeout: int = 120,
                 shared_folder_host: str = "",
                 shared_folder_guest: str = "/mnt/win",
                 smb_username: str = "qemuguest",
                 smb_password: str = "1234",
                 smb_share_name: str = "qemu_share",
                 log_level: str = "INFO"):
        """
        Initialize the VM Task Executor.
        
        Args:
            disk_image: Path to the QEMU disk image
            ssh_username: SSH username for VM connection
            ssh_password: SSH password for VM connection
            ssh_port: SSH port for VM connection
            memory: VM memory in MB
            cpus: Number of CPU cores for VM
            vm_startup_timeout: Timeout for VM startup in seconds
            task_timeout: Timeout for task execution in seconds
            shared_folder_host: Local directory path to share with VM
            shared_folder_guest: Mount point in VM for shared folder
            smb_username: SMB username for Windows file sharing
            smb_password: SMB password for Windows file sharing
            smb_share_name: SMB share name
            log_level: Logging level
        """
        self.disk_image = disk_image
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_port = ssh_port
        self.memory = memory
        self.cpus = cpus
        self.vm_startup_timeout = vm_startup_timeout
        self.task_timeout = task_timeout
        self.shared_folder_host = shared_folder_host
        self.shared_folder_guest = shared_folder_guest
        self.smb_username = smb_username
        self.smb_password = smb_password
        self.smb_share_name = smb_share_name
        
        # OS detection for cross-platform compatibility
        self.is_windows = platform.system() == "Windows"
            
        self.vm_process: Optional[subprocess.Popen] = None
        self.vm_running = False
        
        self.ssh_client: Optional[paramiko.SSHClient] = None
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Suppress paramiko's verbose logging
        logging.getLogger("paramiko").setLevel(logging.CRITICAL)
        logging.getLogger("paramiko.transport").setLevel(logging.CRITICAL)
        logging.getLogger("paramiko.transport.sftp").setLevel(logging.WARNING)
    
    def _get_qemu_executable(self) -> str:
        """Get the appropriate QEMU executable for the current OS."""
        import shutil
        
        if self.is_windows:
            # Common QEMU installation paths on Windows
            possible_paths = [
                "C:\\Program Files\\qemu\\qemu-system-x86_64.exe",
            ]
            
            # Check each path in order
            for path in possible_paths:
                if os.path.exists(path) or (path == "qemu-system-x86_64.exe" and shutil.which(path)):
                    self.logger.info(f"Found QEMU at: {path}")
                    return path
        else:
            # Linux/Unix systems
            if shutil.which("qemu-system-x86_64"):
                return "qemu-system-x86_64"
            else:
                raise FileNotFoundError(
                    "QEMU not found. Please install QEMU using your package manager:\n"
                    "  - Ubuntu/Debian: sudo apt install qemu-system-x86\n"
                    "  - CentOS/RHEL: sudo yum install qemu-kvm\n"
                    "  - Arch: sudo pacman -S qemu"
                )
    
    def _load_config_from_env(self, vm_config_file: str = "vm_config.env", 
                             ssh_config_file: str = "ssh_config.env"):
        """Load configuration from environment files."""
        try:
            if os.path.exists(vm_config_file):
                with open(vm_config_file, 'r') as f:
                    for line in f:
                        if line.strip() and not line.startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == "DEFAULT_DISK_IMAGE":
                                self.disk_image = value
                            elif key == "AVAILABLE_PORTS":
                                ports = [int(p.strip()) for p in value.split(',')]
                                self.ssh_port = ports[0]
            
            if os.path.exists(ssh_config_file):
                with open(ssh_config_file, 'r') as f:
                    for line in f:
                        if line.strip() and not line.startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == "SSH_USERNAME":
                                self.ssh_username = value
                            elif key == "SSH_PASSWORD":
                                self.ssh_password = value
                            elif key == "SHARED_FOLDER_HOST":
                                self.shared_folder_host = value
                            elif key == "SHARED_FOLDER_GUEST":
                                self.shared_folder_guest = value
                            elif key == "TASK_TIMEOUT":
                                self.task_timeout = int(value)
                            elif key == "VM_STARTUP_TIMEOUT":
                                self.vm_startup_timeout = int(value)
                                
        except Exception as e:
            self.logger.warning(f"Failed to load config from files: {e}")
    
    def _check_admin_privileges(self) -> bool:
        """Check if the current process has administrator privileges."""
        try:
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin()
        except Exception as e:
            self.logger.warning(f"Could not check administrator privileges: {e}")
            return False

    def _request_admin_privileges(self) -> bool:
        """Request administrator privileges from the user."""
        try:
            import ctypes
            
            self.logger.info("Administrator privileges are required for SMB share setup.")
            
            # Prompt user for elevation
            response = input("Do you want to request administrator privileges? (y/n): ").strip().lower()
            if response not in ['y', 'yes']:
                self.logger.info("Administrator privileges declined by user.")
                return False
            
            self.logger.info("Requesting administrator privileges...")
            
            # Get the current script path
            if getattr(sys, 'frozen', False):
                # If running as a compiled executable
                script_path = sys.executable
            else:
                # If running as a Python script
                script_path = sys.argv[0]
            
            # Prepare arguments to pass to the elevated process
            args = ' '.join(sys.argv[1:])
            
            # Request elevation using ShellExecuteW
            result = ctypes.windll.shell32.ShellExecuteW(
                None, 
                "runas",  # Verb to request elevation
                sys.executable,  # Python executable
                f'"{script_path}" {args}',  # Script and arguments
                None,  # Working directory
                1  # Show window
            )
            
            if result > 32:  # Success
                self.logger.info("Administrator privileges requested. Please approve the UAC prompt.")
                self.logger.info("The application will restart with elevated privileges.")
                # Exit current process as the elevated one will take over
                sys.exit(0)
            else:
                self.logger.error("Failed to request administrator privileges.")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to request elevation: {e}")
            return False

    def _setup_smb_with_batch(self) -> bool:
        """
        Set up SMB share using smb.bat script.
        Requests admin privileges before running the batch file.
        """
        try:
            if not self.is_windows:
                self.logger.warning("SMB share setup is only supported on Windows")
                return False
                
            if not self.shared_folder_host or not os.path.exists(self.shared_folder_host):
                self.logger.error(f"Shared folder path does not exist: {self.shared_folder_host}")
                return False
            
            # Check if we have admin privileges
            if not self._check_admin_privileges():
                self.logger.warning("Administrator privileges required for SMB share setup!")
                if not self._request_admin_privileges():
                    self.logger.error("Cannot proceed without administrator privileges.")
                    self.logger.info("Alternative: Run this script as Administrator")
                    return False
                # If we reach here, the script should have restarted with admin privileges
                return False

            
         
            self.logger.info("Setting up SMB share using smb.bat...")
            smb_script = os.path.join(os.path.dirname(__file__), "smb.bat")
            print(f"SMB Script: {smb_script}")
            if not os.path.exists(smb_script):
                self.logger.error("smb.bat not found in the current directory")
                return False
            print(f"SMB Username: {self.smb_username}")
            print(f"SMB Password: {self.smb_password}")
            print(f"Shared Folder: {self.shared_folder_host}")
            print(f"SMB Share Name: {self.smb_share_name}")
            
            # Run smb.bat without parameters - it will use its defaults
            cmd = f'"{smb_script}" "{self.smb_username}" "{self.smb_password}" "{self.smb_share_name}" "{self.shared_folder_host}"'
            
            print(f"Debug - SMB command: {cmd}")
            
            try:
                # Run the batch file without parameters
                result = subprocess.run(cmd, shell=True)
                if result.returncode == 0:
                    self.logger.info("SMB share setup completed successfully")
                    return True
                else:
                    self.logger.error(f"SMB setup failed with return code {result.returncode}")
                    return False
                    
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to execute smb.bat: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to setup SMB share: {e}")
            return False

    def _launch_vm_with_batch(self) -> bool:
        """
        Launch VM using startup.bat script with parameters.
        """
        try:
            self.logger.info("Launching VM using startup.bat...")
            
            # Prepare command for startup.bat with parameters
            startup_script = os.path.join(os.path.dirname(__file__), "startup.bat")
            if not os.path.exists(startup_script):
                self.logger.error("startup.bat not found in the current directory")
                return False
            
            # Run startup.bat without parameters - it will use its defaults
            cmd = f'"{startup_script}" "{self.memory}" "{self.cpus}" "{self.ssh_port}" "{self.disk_image}" "{self._get_qemu_executable()}"'          
            try:
                # Run the batch file in background with visible output
                self.vm_process = subprocess.Popen(
                    cmd,
                    shell=True,
                    text=True,
                )
                self.logger.info("VM process started via startup.bat")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to execute startup.bat: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to launch VM with batch file: {e}")
            return False
            
    
    def launch_vm(self) -> bool:

        if self.vm_running:
            self.logger.warning("VM is already running")
            return True
        
        try:
            start_time = time.time()
            self.logger.info("Starting VM launch process...")
            
            # Setup SMB share on Windows if shared folder is configured
            if self.shared_folder_host and os.path.exists(self.shared_folder_host):
                if self.is_windows:
                    if self._setup_smb_with_batch():
                        self.logger.info(f"SMB share configured: {self.shared_folder_host} -> {self.shared_folder_guest}")
                    else:
                        self.logger.warning("Failed to setup SMB share")
                        self.shared_folder_host = ""
                else:
                    self.logger.warning("SMB sharing is only supported on Windows host")
                    self.shared_folder_host = ""
            elif self.shared_folder_host:
                self.logger.warning(f"Shared folder path does not exist: {self.shared_folder_host}")
                self.shared_folder_host = ""
            
            # Launch VM using batch file on Windows, or traditional method on Linux
            if self.is_windows:
                if not self._launch_vm_with_batch():
                    self.logger.error("Failed to launch VM using startup.bat")
                    return False
            else:
                # Traditional QEMU launch for Linux/Unix
                qemu_cmd = [
                    self._get_qemu_executable(),
                    "-m", str(self.memory),
                    "-smp", str(self.cpus),
                    "-net", f"user,hostfwd=tcp::{self.ssh_port}-:22",
                    "-accel", "tcg",
                    "-cpu", "qemu64",
                    "-net", "nic",
                    "-hda", self.disk_image,
                    "-display", "none",
                    "-nographic"
                ]
                
                self.vm_process = subprocess.Popen(
                    qemu_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE
                )
            
            self.logger.info("VM process started, waiting for SSH connection...")
            if self._wait_for_ssh_connection():
                self.vm_running = True
                if self.shared_folder_host:
                    self._setup_shared_folder()
                
                elapsed_time = time.time() - start_time
                self.logger.info(f"VM launched successfully in {elapsed_time:.2f} seconds")
                return True
            else:
                elapsed_time = time.time() - start_time
                self.logger.error(f"VM failed to start or SSH connection failed after {elapsed_time:.2f} seconds")
                self.stop_vm()
                return False
                
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.logger.error(f"Failed to launch VM after {elapsed_time:.2f} seconds: {e}")
            return False
    
    def _wait_for_ssh_connection(self) -> bool:
        """Wait for SSH connection to become available."""
        start_time = time.time()
        while time.time() - start_time < self.vm_startup_timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(("localhost", self.ssh_port))
                sock.close()
                
                if result == 0:
                    if self._establish_ssh_connection():
                        return True
                        
            except Exception:
                pass
            
            time.sleep(2)
        
        return False
    
    def _establish_ssh_connection(self) -> bool:
        try:
            if self.ssh_client:
                self.ssh_client.close()
            
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                "localhost", 
                port=self.ssh_port, 
                username=self.ssh_username, 
                password=self.ssh_password,
                timeout=10
            )
            return True
            
        except Exception as e:
            self.logger.debug(f"SSH connection attempt failed: {e}")
            return False
    
    def _setup_shared_folder(self) -> bool:
        try:
            self.logger.info("Setting up shared folder in VM...")
            
            # Create mount point directory
            mkdir_cmd = f"mkdir -p {self.shared_folder_guest}"
            output, error = self.execute_command(mkdir_cmd)
            if error:
                self.logger.warning(f"mkdir warning: {error}")            
            check_mount_cmd = f"mount | grep {self.shared_folder_guest}"
            output, error = self.execute_command(check_mount_cmd)
            if output and self.shared_folder_guest in output:
                self.logger.info("Shared folder already mounted")
                return True
            
            mount_cmd = f"mount -t cifs //10.0.2.2/{self.smb_share_name} {self.shared_folder_guest} -o user={self.smb_username},password={self.smb_password},vers=2.0"
            output, error = self.execute_command(mount_cmd)
            if error and "already mounted" not in error.lower():
                self.logger.error(f"Failed to mount SMB share: {error}")
                return False
            verify_cmd = f"ls -la {self.shared_folder_guest}"
            output, error = self.execute_command(verify_cmd)
            if error:
                self.logger.error(f"Failed to verify shared folder mount: {error}")
                return False
            
            self.logger.info(f"SMB shared folder mounted successfully at {self.shared_folder_guest}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup shared folder: {e}")
            return False
    
    def stop_vm(self) -> bool:
        try:
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
            
            if self.vm_process:
                self.vm_process.terminate()
                try:
                    self.vm_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.vm_process.kill()
                    self.vm_process.wait()
                
                self.vm_process = None
            
            self.vm_running = False
            self.logger.info("VM stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop VM: {e}")
            return False
    
    def get_vm_status(self) -> Dict[str, Any]:
        """
        Get the current status of the virtual machine.
        
        Returns:
            dict: VM status information
        """
        status = {
            "vm_running": self.vm_running,
            "vm_process_alive": False,
            "ssh_connected": False,
            "ssh_port": self.ssh_port,
            "memory": self.memory,
            "cpus": self.cpus,
            "disk_image": self.disk_image,
            "shared_folder_host": self.shared_folder_host,
            "shared_folder_guest": self.shared_folder_guest,
            "shared_folder_mounted": False
        }
        
        if self.vm_process:
            status["vm_process_alive"] = self.vm_process.poll() is None
        
        if self.ssh_client:
            try:
                self.ssh_client.exec_command("echo test", timeout=5)
                status["ssh_connected"] = True
                
                if self.shared_folder_host and self.shared_folder_guest:
                    try:
                        stdin, stdout, stderr = self.ssh_client.exec_command(f"mount | grep {self.shared_folder_guest}", timeout=5)
                        mount_output = stdout.read().decode('utf-8', errors='ignore')
                        status["shared_folder_mounted"] = bool(mount_output.strip())
                    except Exception:
                        status["shared_folder_mounted"] = False
                        
            except Exception:
                status["ssh_connected"] = False
        
        return status
    
    def execute_command(self, command: str, timeout: Optional[int] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Execute a command on the virtual machine via SSH.
        
        Args:
            command: Command to execute
            timeout: Command timeout in seconds
            
        Returns:
            tuple: (stdout, stderr) or (None, error_message)
        """
        if not self.vm_running or not self.ssh_client:
            return None, "VM is not running or SSH not connected"
        
        try:
            timeout = timeout or self.task_timeout
            ## async command
            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=timeout)
            ## sync command
            output = stdout.read().decode('utf-8', errors='ignore')
            error = stderr.read().decode('utf-8', errors='ignore')
            
            return output, error if error else None
            
        except Exception as e:
            self.logger.error(f"Failed to execute command '{command}': {e}")
            return None, str(e)
    

    
    def execute_script(self, script_path: str, script_args: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Execute a Python script on the virtual machine using python3.
        Script must be accessible from within the VM (e.g., in shared folder).
        
        Args:
            script_path: Path to the Python script (VM path, e.g., /mnt/win/script.py)
            script_args: Arguments to pass to the script
            
        Returns:
            tuple: (stdout, stderr) or (None, error_message)
        """
        try:
            start_time = time.time()
            script_name = os.path.basename(script_path)
            
            command = f"python3 {script_path}"
            if script_args:
                command += f" {script_args}"
            
            self.logger.info(f"Executing Python script: {script_name}")
            output, error = self.execute_command(command)
            
            elapsed_time = time.time() - start_time
            if output is not None:
                self.logger.info(f"Script '{script_name}' executed successfully in {elapsed_time:.2f} seconds")
            else:
                self.logger.error(f"Script '{script_name}' failed after {elapsed_time:.2f} seconds")
            
            return output, error
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.logger.error(f"Failed to execute script after {elapsed_time:.2f} seconds: {e}")
            return None, str(e)
    
    def get_shared_folder_path(self, filename: str = "") -> str:
        """
        Get the full path to a file in the shared folder (VM side).
        
        Args:
            filename: Optional filename to append to the shared folder path
            
        Returns:
            str: Full path in the VM's shared folder
        """
        if filename:
            return os.path.join(self.shared_folder_guest, filename).replace('\\', '/')
        return self.shared_folder_guest
    
    def execute_script_from_shared(self, script_filename: str, input_filename: str = "data.txt", output_filename: str = "result.txt") -> Tuple[Optional[str], Optional[str]]:
        if not self.shared_folder_host:
            return None, "No shared folder configured"
        
        input_path = self.get_shared_folder_path(input_filename)
        output_path = self.get_shared_folder_path(output_filename)        
        script_path = self.get_shared_folder_path(script_filename)
        script_args = f'"{input_path}" "{output_path}"'
        check_cmd = f"test -f {script_path}"
        _, error = self.execute_command(check_cmd)
        if error:
            return None, f"Script not found in shared folder: {script_filename}"
        
        return self.execute_script(script_path, script_args)
    
    def __enter__(self):
        """Context manager entry."""
        self._load_config_from_env()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_vm()

if __name__ == "__main__":
    ## Testing Example
    ## shared_dir is one for the host, put the files on it task.py, data.txt
    ## run the script (vm.py)
    ## see the result in the result.txt
    shared_dir = "D:\\CMP 4th Year\\First Term\\GP\\RunSurge-Master\\vm\\shared"  
    
    vm_executor = VMTaskExecutor(shared_folder_host=shared_dir)
    if vm_executor.launch_vm():
        status = vm_executor.get_vm_status()
        vm_executor.logger.info(f"VM Status - Running: {status['vm_running']}, SSH Connected: {status['ssh_connected']}")
        
        if status.get('shared_folder_mounted'):
            vm_executor.logger.info(f"Shared folder mounted: {status['shared_folder_host']} -> {status['shared_folder_guest']}")
            script_output, script_error = vm_executor.execute_script_from_shared("task.py", "data.txt", "result.txt")
            if script_output:
                print("Script Console Output:")
                print("="*50)
                print(script_output)
                print("="*50)
            elif script_error:
                if "not found" in script_error:
                    vm_executor.logger.warning("No task.py found in shared folder. Please place your script in the shared directory.")
                    vm_executor.logger.info(f"Shared folder location: {shared_dir}")
                else:
                    vm_executor.logger.error(f"Script Error: {script_error}")
            
            result_file_path = os.path.join(shared_dir, "result.txt")
            if os.path.exists(result_file_path):
                vm_executor.logger.info("Result file found in shared folder")
                try:
                    with open(result_file_path, 'r') as f:
                        result_content = f.read()
                    print("\n" + "="*50)
                    print("RESULT FILE CONTENTS:")
                    print("="*50)
                    print(result_content)
                    print("="*50)
                except Exception as e:
                    vm_executor.logger.error(f"Failed to read result file: {e}")
            else:
                vm_executor.logger.info("No result file found in shared folder")
                
        else:
            vm_executor.logger.error("Shared folder not mounted. Cannot proceed without shared folder.")
            vm_executor.logger.info("Please ensure:")
            vm_executor.logger.info(f"1. Shared folder path exists: {shared_dir}")
            vm_executor.logger.info("2. VM has proper CIFS/SMB support")
            vm_executor.logger.info("3. VM has necessary mount permissions")
            
    vm_executor.stop_vm()
