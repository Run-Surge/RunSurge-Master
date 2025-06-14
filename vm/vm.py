import paramiko
import subprocess
import time
import os
import logging
import threading
from typing import Optional, Tuple, Dict, Any
import socket
import psutil

class VMTaskExecutor:    
    def __init__(self, 
                 disk_image: str = "mydisk.qcow2",
                 ssh_username: str = "root", 
                 ssh_password: str = "1234",
                 ssh_port: int = 2222,
                 memory: int = 2048,
                 cpus: int = 2,
                 vm_startup_timeout: int = 90,
                 task_timeout: int = 120,
                 remote_temp_dir: str = "",
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
            remote_temp_dir: Temporary directory on VM for file transfers
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
        self.remote_temp_dir = remote_temp_dir
            
        self.vm_process: Optional[subprocess.Popen] = None
        self.vm_running = False
        
        self.ssh_client: Optional[paramiko.SSHClient] = None
        
        # Configure logging with timestamps
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
                            elif key == "REMOTE_TEMP_DIR":
                                self.remote_temp_dir = value
                            elif key == "TASK_TIMEOUT":
                                self.task_timeout = int(value)
                            elif key == "VM_STARTUP_TIMEOUT":
                                self.vm_startup_timeout = int(value)
                                
        except Exception as e:
            self.logger.warning(f"Failed to load config from files: {e}")
    
    def launch_vm(self) -> bool:
        """
        Launch the QEMU virtual machine with specified configurations.
            
        Returns:
            bool: True if VM launched successfully, False otherwise
        """
        if self.vm_running:
            self.logger.warning("VM is already running")
            return True
        
        try:
            start_time = time.time()
            self.logger.info("Starting VM launch process...")
            
            qemu_cmd = [
                "qemu-system-x86_64",
                "-m", str(self.memory),
                "-smp", str(self.cpus),
                "-net", f"user,hostfwd=tcp::{self.ssh_port}-:22",
                "-accel", "tcg",
                "-cpu", "qemu64",
                "-net", "nic",
                "-hda", self.disk_image,
                "-display", "none",
                "-daemonize"
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
            "disk_image": self.disk_image
        }
        
        if self.vm_process:
            status["vm_process_alive"] = self.vm_process.poll() is None
        
        if self.ssh_client:
            try:
                self.ssh_client.exec_command("echo test", timeout=5)
                status["ssh_connected"] = True
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
    
    def transfer_file_to_vm(self, local_path: str, remote_path: Optional[str] = None) -> bool:
        """
        Returns:
            bool: True if transfer successful, False otherwise
        """
        if not self.vm_running or not self.ssh_client:
            self.logger.error("VM is not running or SSH not connected")
            return False
        
        if not os.path.exists(local_path):
            self.logger.error(f"Local file does not exist: {local_path}")
            return False
        
        try:
            print("Manga",self.remote_temp_dir)
            if remote_path is None:
                filename = os.path.basename(local_path)
                remote_path = os.path.join(self.remote_temp_dir, filename)
            
            sftp = self.ssh_client.open_sftp()
            sftp.put(local_path, remote_path)
            sftp.close()
            
            self.logger.info(f"File transferred successfully: {local_path} -> {remote_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to transfer file: {e}")
            return False
    
    def transfer_file_from_vm(self, remote_path: str, local_path: str) -> bool:
        """            
        Returns:
            bool: True if transfer successful, False otherwise
        """
        if not self.vm_running or not self.ssh_client:
            self.logger.error("VM is not running or SSH not connected")
            return False
        
        try:
            sftp = self.ssh_client.open_sftp()
            sftp.get(remote_path, local_path)
            sftp.close()
            
            self.logger.info(f"File transferred successfully: {remote_path} -> {local_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to transfer file from VM: {e}")
            return False
    
    def execute_script(self, script_path: str, script_args: Optional[str] = None, 
                      transfer_script: bool = True) -> Tuple[Optional[str], Optional[str]]:
        """
        Execute a Python script on the virtual machine using python3.
        
        Args:
            script_path: Path to the Python script (local if transfer_script=True, remote otherwise)
            script_args: Arguments to pass to the script
            transfer_script: Whether to transfer the script to VM first
            
        Returns:
            tuple: (stdout, stderr) or (None, error_message)
        """
        try:
            start_time = time.time()
            script_name = os.path.basename(script_path)
            
            if transfer_script:
                self.logger.info(f"Transferring script: {script_name}")
                if not self.transfer_file_to_vm(script_path):
                    return None, "Failed to transfer script to VM"
                
                remote_script_path = os.path.join(self.remote_temp_dir, script_name)
            else:
                remote_script_path = script_path
            
            command = f"python3 {remote_script_path}"
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
    
    def __enter__(self):
        """Context manager entry."""
        self._load_config_from_env()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_vm()


if __name__ == "__main__":
    vm_executor = VMTaskExecutor()
    if vm_executor.launch_vm():
            status = vm_executor.get_vm_status()
            vm_executor.logger.info(f"VM Status - Running: {status['vm_running']}, SSH Connected: {status['ssh_connected']}")
            
            # Check if required files exist
            required_files = ["data.txt", "task.py"]
            missing_files = [f for f in required_files if not os.path.exists(f)]
            
            if missing_files:
                vm_executor.logger.error(f"Missing required files: {missing_files}")
            else:
                # Transfer data file first
                vm_executor.logger.info("Transferring data file...")
                if vm_executor.transfer_file_to_vm("data.txt"):
                    vm_executor.logger.info("Data file transferred successfully")
                    
                    # Execute the task script (which will also be transferred)
                    script_output, script_error = vm_executor.execute_script("task.py")
                    if script_output:
                        print("Script Console Output:")
                        print("Here 111111111111")
                        print(script_output)
                    if script_error:
                        vm_executor.logger.error(f"Script Error: {script_error}")
                    
                    # Transfer result file back from VM
                    vm_executor.logger.info("Transferring result file back from VM...")
                    remote_result_path = "result.txt"
                    local_result_path = "result_from_vm.txt"
                    
                    if vm_executor.transfer_file_from_vm(remote_result_path, local_result_path):
                        vm_executor.logger.info(f"Result file transferred successfully to: {local_result_path}")
                        
                        # Display the result file contents
                        try:
                            with open(local_result_path, 'r') as f:
                                result_content = f.read()
                            print("\n" + "="*50)
                            print("RESULT FILE CONTENTS:")
                            print("="*50)
                            print(result_content)
                            print("="*50)
                        except Exception as e:
                            vm_executor.logger.error(f"Failed to read result file: {e}")
                    else:
                        vm_executor.logger.error("Failed to transfer result file from VM")
                        
                else:
                    vm_executor.logger.error("Failed to transfer data file")
    vm_executor.stop_vm()
