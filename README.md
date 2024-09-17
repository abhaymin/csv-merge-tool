# CSV Merge Tool

This tool is designed to merge multiple CSV files into a single, cleaned, and consolidated CSV file. It handles various data inconsistencies, removes blank rows and columns, and ensures a uniform output.

## Features

- Merges multiple CSV files into one
- Removes blank rows and columns
- Harmonizes data types across different input files
- Removes duplicate rows
- Sorts the output based on date columns (if available)
- Provides detailed logging of the merge process

## Requirements

- Python 3.6+
- Polars library (version compatibility may vary, see troubleshooting section)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/abhaymin/csv-merge-tool.git
   cd csv-merge-tool
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the script from the command line:

```
python csvmerge.py input_directory output_file.csv
```

- `input_directory`: The directory containing the CSV files to merge
- `output_file.csv`: The name of the output file where the merged data will be saved

Optional arguments:
- `--log-level`: Set the logging level (choices: DEBUG, INFO, WARNING, ERROR, CRITICAL; default: INFO)

Example:
```
python csvmerge.py ./smapledata ./output/merged_output.csv --log-level DEBUG
```

## Process

1. The script scans the input directory for CSV files.
2. Each CSV file is read and processed:
   - Blank columns are removed
   - Data types are inferred
3. All dataframes are harmonized to have the same columns and data types.
4. The harmonized dataframes are concatenated.
5. Blank rows are removed from the merged dataframe.
6. Duplicate rows are removed.
7. The data is sorted by the first date column (if available) or the first column.
8. The final merged and cleaned data is written to the output CSV file.

## Sample Files

The repository includes two sample CSV files (`sample1.csv` and `sample2.csv`) that you can use to test the tool. These files demonstrate the tool's ability to handle different column structures and data types.

## Troubleshooting

- If you encounter issues related to Polars functions or methods, check your Polars version and update it to the latest version:
  ```
  pip install --upgrade polars
  ```
- The script logs the Polars version at the start of execution. If you encounter any issues, please include this version information when seeking help.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the [MIT License](LICENSE).