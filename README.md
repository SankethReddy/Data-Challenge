# Data-Platform-Code-Challenge

This project consisted of using Python to create a library that can read, process, and transform large-scaled CSV files and upload them to Parquet tables. 

Each CSV file has a .txt file associated with it that specifies the schema of the CSV and how the schema of the respective Parquet table should be. The schema portrays each of the column names as well their data types, if they can have null values, and if they need standardization applied to them (this is only for Date columns).

The "src" folder has the CSV files, schema .txt files, and the Python source code used for the project. The "output_files" folder has the outputted Parquet tables from the source code.
