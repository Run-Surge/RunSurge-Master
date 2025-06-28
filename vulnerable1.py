# system_calls_script.py

import subprocess
import sys
import time



def run_command_with_subprocess_run(command_list):
    """
    Executes a command using subprocess.run().
    
    This is the recommended modern approach. By default, it avoids
    using the shell, which prevents command injection.
    """
    print(f"\n--- Running command with subprocess.run(): '{' '.join(command_list)}' ---")
    try:
        # Recommended usage: without shell=True
        # This is safe against injection as arguments are passed directly.
        result = subprocess.run(command_list, capture_output=True, text=True, check=True)
        print("Command stdout:")
        print(result.stdout)
        print("Command stderr:")
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}:")
        print(f"Stderr: {e.stderr}")
    except FileNotFoundError:
        print(f"Error: The command '{command_list[0]}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def run_command_with_subprocess_shell_true(command_string):
    """
    Executes a command string using subprocess.run(..., shell=True).
    
    This is equivalent to os.system() and is vulnerable to command injection
    if the input is not sanitized. Bandit will flag this.
    """
    print(f"\n--- Running command with subprocess.run(shell=True): '{command_string}' ---")
    try:
        # WARNING: This is vulnerable to command injection!
        result = subprocess.run(command_string, shell=True, capture_output=True, text=True, check=True)
        print("Command stdout:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}:")
        print(f"Stderr: {e.stderr}")

def create_and_run_child_process():
    """
    Demonstrates creating a child process using os.fork() (Unix-like systems only).
    
    This is a low-level system call for process management.
    """
    # This part of the script will only work on Unix-like systems (Linux, macOS).
    # On Windows, os.fork() does not exist.
    if sys.platform == 'win32':
        print("\n--- os.fork() is not available on Windows. Skipping this example. ---")
        return

    print("\n--- Creating a child process with os.fork() ---")
    pid = os.fork()

    if pid > 0:
        # This is the parent process
        print(f"Parent process (PID: {os.getpid()}) created child with PID: {pid}")
        # Wait for the child process to finish
        child_pid, status = os.waitpid(pid, 0)
        print(f"Parent: Child process {child_pid} finished with status {status}.")
    else:
        # This is the child process
        print(f"Child process (PID: {os.getpid()}) is running.")
        print("Child process is sleeping for 3 seconds...")
        time.sleep(3)
        print("Child process is exiting.")
        # Exit the child process
        sys.exit(0)

if __name__ == "__main__":
    # Example 1: Using os.system (vulnerable)
    # Bandit will likely flag this as B605 or B607.

    # Example 2: Using subprocess.run with a list of arguments (safe)
    run_command_with_subprocess_run(["ls", "-l", "/tmp"]) # Safe equivalent

    # Example 3: Using subprocess.run with shell=True (vulnerable)
    # Bandit will flag this as B605.
    run_command_with_subprocess_shell_true("ls -l /tmp")
    
    # Example 4: A command injection vulnerability example
    # Let's say a user provides input.
    user_input = "test.txt; echo 'malicious command executed!'"
    print(f"\n--- Demonstrating command injection with user input: '{user_input}' ---")
    # The command `cat test.txt` and `echo 'malicious command executed!'` will both run.
    
    # Example 5: Using os.fork()
    create_and_run_child_process()