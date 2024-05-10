# Data-Platform-Code-Challenge

This project consisted of using Python to create a library that can read, process, and transform large-scaled CSV files and upload them to Parquet tables. The library name is "csv_to_parquet.csv_to_parquet".

Each CSV file has a .txt file associated with it that specifies the schema of the CSV and how the schema of the respective Parquet table should be. The schema portrays each of the column names as well their data types, if they can have null values, and if they need standardization applied to them (this is only for Date columns).

The "src" folder has the CSV files, schema .txt files, and the Python source code used for the project. The "output_files" folder has the outputted Parquet tables from the source code.

### src folder
  - The "main.py" Python file shows how the "csv_to_parquet.csv_to_parquet" library can be used
  - The "csv_to_parquet.py" Python file has the code that reads, processes, and transforms CSV files and uploads them to Parquet tables
  - The "unit_tests.py" Python file has unit tests to test out the features of the "csv_to_parquet.csv_to_parquet" library
  - The "LoanStats_securev1_2018Q4.csv" file is the default csv file. It has extra text at the top and bottom as well as empty rows
  - The "LoanStats_securev1_2018Q4 - No Header.csv" file has the same data as the "LoanStats_securev1_2018Q4.csv" file except it does not have any headers
  - The "LoanStats_securev1_2018Q4 - Nullability.csv" has the same data as the above CSV files, except there are null values in some columns that should not have null values based on the schema
  - The "LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt" file is a schema .txt file (this has one extra column that is not in the above CSV files)
  - The "LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt" file is a schema .txt file (the columns match with the CSV column)
  - The "LoanStats_securev1_2018Q4 - Developer Supplied Schema - Empty Schema.txt" file is a schema .txt file that is empty
  - The "LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing Columns in Schema.txt" file is a schema .txt file that has columns which are not in the above CSV files
  - The "LoanStats_securev1_2018Q4 - Developer Supplied Schema - One Column.txt" file is a schema .txt file that has only one column present
  - The "LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt" file is a schema .txt file that has three columns present
