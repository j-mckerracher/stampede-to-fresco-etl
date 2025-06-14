import os
import shutil
import re


def organize_parquet_files(source_directory):
    """
    Iterates over Parquet files in a directory and moves them
    into sub-folders named YYYY-MM based on their filename.
    Handles filenames starting with 'sorted_' or 'sorted-'.

    Args:
        source_directory (str): The path to the directory containing
                                 the Parquet files.
    """
    print(f"Starting to process files in: {source_directory}")

    # Regex to find files with the pattern 'sorted[_-]YYYY-MM-DD.parquet'
    # It captures the YYYY-MM part.
    # [_-] means: match either an underscore OR a hyphen.
    pattern = re.compile(r"sorted[_-](\d{4}-\d{2})-\d{2}\.parquet")

    try:
        # List all entries in the source directory
        for filename in os.listdir(source_directory):
            source_path = os.path.join(source_directory, filename)

            # Check if it's a file
            if os.path.isfile(source_path):
                match = pattern.match(filename)

                if match:
                    # Extract the YYYY-MM part
                    year_month = match.group(1)

                    # Create the destination folder path
                    destination_folder = os.path.join(source_directory, year_month)

                    # Create the destination folder if it doesn't exist
                    os.makedirs(destination_folder, exist_ok=True)

                    # Create the full destination path for the file
                    destination_path = os.path.join(destination_folder, filename)

                    # Move the file
                    print(f"Moving '{filename}' to '{destination_folder}'...")
                    shutil.move(source_path, destination_path)
                else:
                    print(f"Skipping '{filename}': Does not match expected pattern.")
            else:
                # Check if it's a directory (and likely one we created)
                if not os.path.isdir(source_path):
                    print(f"Skipping '{filename}': Is not a file.")

        print("File organization complete. ✅")

    except FileNotFoundError:
        print(f"Error: The directory '{source_directory}' was not found. ❌")
    except Exception as e:
        print(f"An error occurred: {e} ❌")


# --- Main Execution ---
if __name__ == "__main__":
    # Define the target directory
    directory_to_organize = r"C:\Users\jmckerra\Documents\Stampede\step-1-complete-sorted"

    # Run the organization function
    organize_parquet_files(directory_to_organize)