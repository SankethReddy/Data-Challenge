# -*- coding: utf-8 -*-
## main.py file will be used to show how the csv_to_parquet.csv_to_parquet library can be used
from csv_to_parquet import csv_to_parquet ## The library I created is csv_to_parquet.csv_to_parquet

if __name__ == '__main__':
    ## Hard-coding the file names to be processed for testing purposes
    files_list = [['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt'], ## Creates a Parquet table and returns a DataFrame but gives a warning that there are more columns in the schema .txt file than in the actual .csv data
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt'], ## Creates a Parquet table and returns a DataFrame
                  ['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt'], ## Does not create a Parquet table and returns None since there's no header provided (infers column names from the schema .txt file provided) and the number of columns in the .csv file does not equal the number of columns in the schema .txt file provided
                  ['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt'], ## Creates a Parquet table and returns a DataFrame
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Empty Schema.txt'], ## Does not create a Parquet table and returns None since the schema .txt file does not have any columns
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing Columns in Schema.txt'], ## Creates a Parquet table and returns a DataFrame but gives a warning that there are more columns in the actual .csv data than in the schema .txt file and those columns not in the schema .txt file will not be in the Parquet table
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - One Column.txt'], ## Creates a Parquet table and returns a DataFrame but gives a warning that there are more columns in the actual .csv data than in the schema .txt file and those columns not in the schema .txt file will not be in the Parquet table
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt'], ## Creates a Parquet table and returns a DataFrame but gives a warning that there are more columns in the actual .csv data than in the schema .txt file and those columns not in the schema .txt file will not be in the Parquet table
                  ['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt'], ## Does not create a Parquet table and returns None since there's no header provided (infers column names from the schema .txt file provided) and the number of columns in the .csv file does not equal the number of columns in the schema .txt file provided
                  ['LoanStats_securev1_2018Q4 - Nullability.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt'] ## Does not create a Parquet table since there is a null value in a column that should not have null values
                  ]
    ## The below can be used for user's inputting the names of the .csv files and schema .txt files
# =============================================================================
#     files_list = []
#     number_files = int(input("How many csv files will be processed: "))
#     for i in range(number_files):
#         csv_file = input("Enter csv file number " + str(i+1) + ": ")
#         schema_file = input("Enter the schema .txt file for csv file number " + str(i+1) + ": ")
#         files_list.append([csv_file, schema_file])
# =============================================================================
    ## the csv_to_parquet library takes in a multidimensional list where each index is another list where the 0th index is the name of the .csv file and the 1st index is a .txt file that contains the respective .csv file's developer-supplied file schema
    lst = csv_to_parquet(files_list) 