import os
import json

def extract_int_from_last_line(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            last_line = lines[-4].strip()
            value = last_line.split(':')[1].strip()
            return int(value)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return 0

def main(directory):
    total_sum = 0
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            total_sum += extract_int_from_last_line(file_path)
    print("Total sum of integers from last line in all documents:", total_sum)

if __name__ == "__main__":
    directory = "/home/ubuntu/terminal/Backend/Operations/logs/le"  # Replace this with the directory containing your files
    main(directory)
