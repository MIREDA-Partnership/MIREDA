import csv
import hashlib
import os
import pandas as pd

def hash_value(value, salt: str) -> str:
    """Return a salted SHA-256 hash of the given value."""
    if pd.isna(value) or value is None or str(value).strip() == "" or str(value).lower() == "nan":
        return value  # Leave empty or NaN values unchanged
    combined = f"{salt}{value}"
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()

def process_files(input_dir: str, output_dir: str, chunk_size: int, col_mapping: dict, columns_to_pseudonymise: list, reference_filename: str, salt: str):
    """Pseudonymise specified columns in all CSV files from input_dir and write to output_dir."""
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nStarting pseudonymisation...\nInput: {input_dir}\nOutput: {output_dir}\nChunk size: {chunk_size}")
    print(f"Columns to pseudonymise: {columns_to_pseudonymise}")
    print(f"Column Mapping: {col_mapping}")
    print(f"Target Reference File: {reference_filename}")

    # Derive mapping output filename from the reference file name
    base_name, _ = os.path.splitext(reference_filename)
    mapping_filename = f"{base_name}_mapping.csv"

    for filename in os.listdir(input_dir):
        if filename.lower().endswith(".csv"):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, filename)

            is_target_file = filename.upper() == reference_filename.upper()

            print(f"\nProcessing file: {input_file_path}")
            try:
                # Read and process in chunks
                chunk_iter = pd.read_csv(input_file_path, chunksize=chunk_size, low_memory=False)
                first_chunk = True

                for chunk in chunk_iter:
                    # Check if all mandatory mapping columns exist in this chunk if it's the target file
                    mandatory_cols = [v for k, v in col_mapping.items() if "DAD" not in k]
                    can_map = is_target_file and all(col in chunk.columns for col in mandatory_cols)

                    # 1. If this is the target file, capture original unhashed values for mapping BEFORE modifying chunk
                    mapping_out_df = pd.DataFrame()
                    if can_map:
                        for new_col, orig_col in col_mapping.items():
                            if orig_col and orig_col in chunk.columns:
                                # Keep original unhashed values
                                mapping_out_df[orig_col] = chunk[orig_col].copy()
                                # Generate hashed values for the mapping companion column
                                mapping_out_df[new_col] = chunk[orig_col].apply(lambda x: hash_value(x, salt))

                    # 2. Pseudonymise specified columns across all files (modifies chunk in place)
                    for col in columns_to_pseudonymise:
                        if col in chunk.columns:
                            chunk[col] = chunk[col].apply(lambda x: hash_value(x, salt))

                    # 3. Write main chunk to output file
                    mode = 'w' if first_chunk else 'a'
                    header = first_chunk
                    chunk.to_csv(output_file_path, mode=mode, index=False, header=header)

                    # 4. Write mapping chunk if applicable
                    if can_map:
                        mapping_file_path = os.path.join(output_dir, mapping_filename)
                        mapping_out_df.to_csv(mapping_file_path, mode=mode, index=False, header=header)

                    first_chunk = False

                print(f"Successfully pseudonymised: {filename}")
                if is_target_file and can_map:
                    print(f"Successfully generated mapping file: {mapping_filename}")

            except Exception as e:
                print(f"Error processing {filename}: {e}")

    print("\nScript finished.")

if __name__ == "__main__":
    # Prompt user for inputs
    input_dir = os.path.normpath(input("Enter INPUT_DIR path for files awaiting pseudonymisation: ").strip())
    output_dir = os.path.normpath(input("Enter OUTPUT_DIR path to save pseudonymised files: ").strip())
    
    chunk_input = input("Enter CHUNK_SIZE (press Enter to accept default: 5000): ").strip()
    chunk_size = int(chunk_input) if chunk_input else 5000

    salt_input = input("Enter SALT string for hashing (press Enter to accept default: beb2839bd78): ").strip()
    salt = salt_input if salt_input else "beb2839bd78"

    print("\n--- Column Name Configuration ---")
    baby_col = input("Enter original column name for Baby ID (mandatory): ").strip()
    mom_col = input("Enter original column name for Mother ID (mandatory): ").strip()
    preg_col = input("Enter original column name for Pregnancy ID (mandatory): ").strip()
    dad_col = input("Enter original column name for Dad ID (optional, press Enter to skip): ").strip()

    reference_filename = input("Enter the reference CSV file name for ID mapping (e.g., PREGREFS1.CSV): ").strip()

    # Validate mandatory inputs
    if not baby_col or not mom_col or not preg_col:
        raise ValueError("Error: Baby ID, Mother ID, and Pregnancy ID are mandatory columns.")
    if not reference_filename:
        raise ValueError("Error: Reference filename must be provided.")
    if not salt:
        raise ValueError("Error: A SALT string must be provided for hashing.")

    # Build dynamic mapping and list of columns to pseudonymise
    col_mapping = {
        f"{baby_col.upper()}_HASH": baby_col,
        f"{mom_col.upper()}_HASH": mom_col,
        f"{preg_col.upper()}_HASH": preg_col
    }
    
    columns_to_pseudonymise = [baby_col, mom_col, preg_col]

    if dad_col:
        columns_to_pseudonymise.append(dad_col)
        col_mapping[f"{dad_col.upper()}_HASH"] = dad_col

    process_files(input_dir, output_dir, chunk_size, col_mapping, columns_to_pseudonymise, reference_filename, salt)