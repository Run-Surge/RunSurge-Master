import os
import subprocess
def read_and_process_data(filename="data.txt", output_file="result.txt"):
    """Read data from file, process it, and write results to output file."""
    
    # Get the directory where this script is located (shared folder)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Make sure we're working with full paths in the shared folder
    if not os.path.isabs(filename):
        filename = os.path.join(script_dir, filename)
    if not os.path.isabs(output_file):
        output_file = os.path.join(script_dir, output_file)
    
    # Debug information
    print(f"Script directory: {script_dir}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Input file path: {filename}")
    print(f"Output file path: {output_file}")
    
    try:
        # Check if input file exists
        if not os.path.exists(filename):
            error_msg = f"Error: {filename} not found!"
            print(error_msg)
            with open(output_file, 'w') as f:
                f.write(error_msg + "\n")
            return
        
        # Open output file for writing
        with open(output_file, 'w') as output:
            # Write header
            output.write(f"Data Processing Results\n")
            output.write("=" * 50 + "\n")
            output.write(f"Input file: {filename}\n")
            output.write(f"Processing timestamp: {os.popen('date').read().strip()}\n")
            output.write("=" * 50 + "\n\n")
            
            output.write(f"Reading data from: {filename}\n")
            output.write("-" * 40 + "\n")
            
            with open(filename, 'r') as file:
                lines = file.readlines()
            
            output.write(f"Total lines read: {len(lines)}\n")
            output.write("-" * 40 + "\n\n")
            
            # Process each line
            for i, line in enumerate(lines, 1):
                line = line.strip()
                line_info = f"Line {i}: {line}\n"
                output.write(line_info)
                
                # Count words in each line
                word_count = len(line.split())
                word_info = f"  -> Word count: {word_count}\n"
                output.write(word_info)
                
                # Check if line contains numbers
                has_numbers = any(char.isdigit() for char in line)
                number_info = f"  -> Contains numbers: {has_numbers}\n"
                output.write(number_info)
                
                output.write("\n")
            
            # Summary statistics
            total_words = sum(len(line.strip().split()) for line in lines)
            total_chars = sum(len(line.strip()) for line in lines)
            
            summary = [
                "=" * 40,
                "SUMMARY:",
                f"Total lines: {len(lines)}",
                f"Total words: {total_words}",
                f"Total characters: {total_chars}",
                "=" * 40
            ]
            
            for line in summary:
                output.write(line + "\n")
        
        print(f"\nResults written to: {output_file}")
        print("Processing completed successfully!")
        
    except Exception as e:
        error_msg = f"Error processing file: {e}"
        print(error_msg)
        with open(output_file, 'w') as f:
            f.write(f"ERROR: {error_msg}\n")

if __name__ == "__main__":
    import sys
    
    # Default to shared folder paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_input = os.path.join(script_dir, "data.txt")
    default_output = os.path.join(script_dir, "result.txt")
    
    # Parse command line arguments
    if len(sys.argv) >= 2:
        input_file = sys.argv[1]
    else:
        input_file = default_input
        
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    else:
        output_file = default_output
    
    print(f"Using input file: {input_file}")
    print(f"Using output file: {output_file}")
    
    read_and_process_data(input_file, output_file)
    print("Finally 1111111111111")
    ## remove the input file
"""     os.remove(input_file)
 """"""     print(os.getcwd())
    result = subprocess.run("cd .. && ls -la", shell=True, capture_output=True, text=True)
    print("Contents of parent directory:")
    print(result.stdout)
    if result.stderr:
        print("Error:", result.stderr) """