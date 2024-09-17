import polars as pl
import os
import argparse
import logging
from tqdm import tqdm
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Print Polars version
logging.info(f"Polars version: {pl.__version__}")

def is_blank_column(df, column):
    try:
        return df[column].is_null().all() or (df[column].cast(pl.Utf8) == "").all()
    except Exception as e:
        logging.warning(f"Unable to check if column '{column}' is blank. Assuming it's not blank. Error: {str(e)}")
        return False

def is_blank_row(df):
    return df.select(
        pl.all().cast(pl.Utf8).is_null().all().over(pl.all()) |
        (pl.all().cast(pl.Utf8) == "").all().over(pl.all())
    ).to_series()

def determine_common_type(types):
    type_count = Counter(types)
    if pl.Date in type_count or pl.Datetime in type_count:
        return pl.Datetime
    elif pl.Float64 in type_count:
        return pl.Float64
    elif pl.Int64 in type_count:
        return pl.Int64
    else:
        return pl.Utf8

def safe_cast(s, dtype):
    try:
        return s.cast(dtype)
    except:
        return s.cast(pl.Utf8)

def harmonize_dataframes(df_list):
    all_columns = set()
    column_types = {}
    
    for df in df_list:
        all_columns.update(df.columns)
        for col in df.columns:
            if col not in column_types:
                column_types[col] = []
            column_types[col].append(df[col].dtype)
    
    all_columns = sorted(list(all_columns))
    common_types = {col: determine_common_type(types) for col, types in column_types.items()}
    
    harmonized_df_list = []
    for df in tqdm(df_list, desc="Harmonizing DataFrames"):
        new_columns = []
        for col in all_columns:
            if col not in df.columns:
                new_columns.append(pl.lit(None).cast(common_types.get(col, pl.Utf8)).alias(col))
            else:
                new_columns.append(safe_cast(df[col], common_types.get(col, pl.Utf8)).alias(col))
        df = df.with_columns(new_columns).select(all_columns)
        harmonized_df_list.append(df)
    
    return harmonized_df_list, all_columns, common_types

def merge_csv_files(input_dir, output_file):
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        logging.error(f"No CSV files found in {input_dir}")
        return
    
    logging.info(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    df_list = []
    for file in tqdm(csv_files, desc="Reading CSV files"):
        try:
            df = pl.read_csv(os.path.join(input_dir, file),
                             truncate_ragged_lines=True,
                             ignore_errors=True,
                             has_header=True,
                             try_parse_dates=True,
                             infer_schema_length=500)  # Use all rows for type inference
            non_blank_columns = [col for col in df.columns if not is_blank_column(df, col)]
            df = df.select(non_blank_columns)
            df_list.append(df)
            logging.debug(f"Successfully read {file}. Shape: {df.shape}, Non-blank columns: {', '.join(non_blank_columns)}")
        except Exception as e:
            logging.error(f"Error reading {file}: {str(e)}")
            continue

    if not df_list:
        logging.error("No valid CSV files could be read.")
        return

    harmonized_df_list, all_columns, common_types = harmonize_dataframes(df_list)
    logging.info(f"Total unique non-blank columns found across all files: {len(all_columns)}")
    logging.debug(f"Columns and their common types: {common_types}")

    logging.info("Concatenating all dataframes...")
    merged_df = pl.concat(harmonized_df_list)
    logging.info(f"Total rows after concatenation: {len(merged_df)}")

    logging.info("Removing blank rows...")
    initial_row_count = len(merged_df)
    merged_df = merged_df.filter(~is_blank_row(merged_df))
    blank_rows_removed = initial_row_count - len(merged_df)
    logging.info(f"Removed {blank_rows_removed} blank rows")
    logging.info(f"Rows after removing blank rows: {len(merged_df)}")

    logging.info("Removing duplicate rows...")
    initial_row_count = len(merged_df)
    merged_df = merged_df.unique()
    duplicate_rows_removed = initial_row_count - len(merged_df)
    logging.info(f"Removed {duplicate_rows_removed} duplicate rows")
    logging.info(f"Rows after deduplication: {len(merged_df)}")

    # Sort by the first date column if available, otherwise by the first column
    date_columns = [col for col, dtype in common_types.items() if dtype in [pl.Date, pl.Datetime]]
    sort_column = date_columns[0] if date_columns else merged_df.columns[0]
    logging.info(f"Sorting dataframe by column: {sort_column}")
    merged_df = merged_df.sort(sort_column)

    logging.info(f"Writing merged CSV to {output_file}")
    merged_df.write_csv(output_file)
    logging.info("Merge complete")
    logging.info(f"Final non-blank columns in merged file: {', '.join(merged_df.columns)}")
    logging.debug(f"Final column types: {merged_df.dtypes}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge multiple CSV files into one")
    parser.add_argument("input_dir", help="Directory containing CSV files to merge")
    parser.add_argument("output_file", help="Path to save the merged CSV file")
    parser.add_argument("--log-level", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help="Set the logging level")
    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level)

    merge_csv_files(args.input_dir, args.output_file)
