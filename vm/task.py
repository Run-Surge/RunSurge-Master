import os

def read_and_process_data(filename="data.txt", output_file="result.txt"):
    """Read data from file, process it, and write results to output file."""
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
            
            print(f"Reading data from: {filename}")
            output.write(f"Reading data from: {filename}\n")
            output.write("-" * 40 + "\n")
            
            with open(filename, 'r') as file:
                lines = file.readlines()
            
            print(f"Total lines read: {len(lines)}")
            output.write(f"Total lines read: {len(lines)}\n")
            output.write("-" * 40 + "\n\n")
            
            # Process each line
            for i, line in enumerate(lines, 1):
                line = line.strip()
                line_info = f"Line {i}: {line}\n"
                print(f"Line {i}: {line}")
                output.write(line_info)
                
                # Count words in each line
                word_count = len(line.split())
                word_info = f"  -> Word count: {word_count}\n"
                print(f"  -> Word count: {word_count}")
                output.write(word_info)
                
                # Check if line contains numbers
                has_numbers = any(char.isdigit() for char in line)
                number_info = f"  -> Contains numbers: {has_numbers}\n"
                print(f"  -> Contains numbers: {has_numbers}")
                output.write(number_info)
                
                print()
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
                print(line)
                output.write(line + "\n")
        
        print(f"\nResults written to: {output_file}")
        print("Processing completed successfully!")
        
    except Exception as e:
        error_msg = f"Error processing file: {e}"
        print(error_msg)
        with open(output_file, 'w') as f:
            f.write(f"ERROR: {error_msg}\n")

if __name__ == "__main__":
    read_and_process_data()