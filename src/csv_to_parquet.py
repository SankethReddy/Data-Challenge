# -*- coding: utf-8 -*-
## csv_to_parquet.py file has the code that reads, processes, and transforms CSV files and uploads them to Parquet tables
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
import os
import csv
import re
from dateutil.parser import parse
import calendar

## csv_to_parquet reads in a multidimensional list and returns a list where each index is either a DataFrame object or None
def csv_to_parquet(files_list): ## files_list is a list where each index contains another list where the 0th index is a .csv file and the 1st index is a .txt file for the csv's schema
    final_lst = []
    if len(files_list) > 5000: ## The csv_to_parquet library read up to 5000 files
        output_message("WARNING: The csv_to_parquet library can only read a maximum of 5000 files at once. Only the first 5000 files will be read.")
        files_list = files_list[:5000]
    for file in files_list:
        file_size_bytes = os.path.getsize(file[0])
        file_size_gb = file_size_bytes / (1024*1024*1024)
        if file_size_gb > 10: ## The csv_to_parquet library can only read files up to 10 GB
            output_message(f"ERROR: The csv_to_parquet library can only read files up to 10 GB. The {file[0]} is {file_size_gb:.2f} GB and will not be processed.")
            final_lst.append(None) ## final_lst appends None which means it will not process a file that is greater than 10 GB
            continue
        else:
            schema_dictionary = parse_schema_txt_file(file[1]) ## schema_dictionary is a dictionary object that represents the schema
            output_message("Processing the '" + file[0] + "' file.............................................................")
            if len(schema_dictionary) == 0: ## Library cannot process a file if the schema .txt file is empty
                output_message("ERROR: The '" + file[1] + "' schema .txt file is empty. The '" + file[0] + "' file cannot be processed.")
                final_lst.append(None)
                continue
            raw_df = csv_reader(file[0], schema_dictionary) ## Getting the raw data of the csv file provided given the .txt schema file
            if raw_df is None: ## Library cannot process the file
                final_lst.append(None)
                continue
            final_lst.append(to_parquet(file[0], raw_df, schema_dictionary)) ## Standardizing the column values and outputting df to the .parquet file; in actual production, the .parquet file will be outputted to a folder/location specified by the user
    return final_lst

## csv_reader reads the csv file and the schema .txt file and returns a DataFrame for the raw data of the .csv file        
def csv_reader(file, schema): ## file is a single .csv file and schema is a dictionary for the .csv schema
    columns, schema, original_file_content = get_column_names_new_schema_original_file_content(file, schema) ## columns represents the column names; schema is a dictionary that represents the file's schema; original_file_content is a nested list for the raw data
    if columns is None and schema is None and original_file_content is None: ## This happens if the .csv file does not have a header and the number of columns in the .csv file does not equal the number of columns from the schema .txt file provided; the csv file cannot be processed
        return None
    raw_data_content = get_raw_data(original_file_content, columns) ## getting the raw DataFrame from the raw nested list and the columns
    raw_data_content = check_if_columns_in_csv_not_in_schema(raw_data_content, file, schema) ## Checking if columns in the DataFrame are not in the schema .txt file and removing these columns
    return raw_data_content

## to_parquet takes in the .csv file name, the raw DataFrame, and the schema dictionary and standardizes the columns, ensures the data types are aligned with the schema, ensures that the nullability rules are aligned with the schema, exports the transformed DataFrame to a .parquet file, and returns the transformed DataFrame
def to_parquet(file, df, schema):
    for i in df.columns:
        df[i] = [get_column_values(file, v, i, schema[i]) for v in df[i]] ## Getting the values for each of the columns
    
    for i in df.columns:
        for j in df[i]:
            if 'Invalid Conversion' in str(j):
                output_message("ERROR: The '" + i + "' column has a value of '" + str(j).split('-')[0] + "' which cannot be converted to " + schema[i][0] + ". The '" + file + "' file cannot be processed.")
                return None
    
    for i in df.columns:
        if schema[i][0] == 'Integer':
            df[i] = pd.to_numeric(df[i], errors='coerce').astype('Int64')
        elif schema[i][0] == 'String':
            df[i] = pd.Series(df[i], dtype="string")
        elif schema[i][0] == 'Date':
            df[i] = pd.to_datetime(df[i], format = '%Y-%m-%d')
    null_columns = df.columns[df.isna().any()].tolist()
    for i in null_columns:
        if schema[i][1] == 'false':
            output_message("ERROR: The '" + i + "' column in the '" + file + "' file has null values when the schema .txt file says it cannot have null values. The '" + file + "' file cannot be processed.")
            return None
    if not os.path.exists("../output_files"):
        os.makedirs("../output_files")
    #parquet_file = file.split('.csv')[0] + " - " + str(datetime.now().hour) + '_' + str(datetime.now().minute) + '_' + str(datetime.now().second) + ".parquet" ## For Testing Purposes
    parquet_file = file.split('.csv')[0] + ".parquet"
    df.to_parquet("../output_files/" + parquet_file)
    output_message("The '" + file + "' file has been successfully processed and the '" + parquet_file + "' file has been created.............................................................")
    return df

## parse_schema_txt_file takes in the schema .txt file and returns a dictionary to represent the schema
def parse_schema_txt_file(txt):
    schema_file = open(txt)
    schema_file_lst = [line for line in schema_file.readlines() if line.strip()] ## reading .txt schema file like this to account for blank lines in the file
    schema_dictionary = {} ## The keys will be the column names; the values for non-Date columns will be a list that specify the Data Type and Nullability; the values for Date columns will be a list that specify Data Type, Nullability, and whether standardizaion needs to apply to the column (if it's already in ISO, no standardization is required)
    for i in schema_file_lst[1:]:
        if len(i.split(' ')) == 3:
            schema_dictionary[i.split(' ')[0].strip()] = []
            schema_dictionary[i.split(' ')[0].strip()].append(i.split(' ')[1].strip())
            schema_dictionary[i.split(' ')[0].strip()].append(i.split(' ')[2].strip())
        else: ## For columns with a Date DataType, the user can specify whether or not they want to apply standardizations to it (if it's already in ISO format)
            schema_dictionary[i.split(' ')[0].strip()] = []
            schema_dictionary[i.split(' ')[0].strip()].append(i.split(' ')[1].strip())
            schema_dictionary[i.split(' ')[0].strip()].append(i.split(' ')[2].strip())
            schema_dictionary[i.split(' ')[0].strip()].append(i.split(' ')[3].strip())
    schema_file.close()
    return schema_dictionary

## get_column_names_new_schema_original_file_content takes in the .csv file and the schema dictionary and returns the Column Names, New Schema, and Original File Content
def get_column_names_new_schema_original_file_content(file, schema):
    spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate() ## Incorporating PySpark to read in .csv files to account for massive data inputs
    spark_file = spark.read.csv(file) 
    spark_file_rdd = spark_file.rdd ## Converting Spark file to RDD because there could be lines with empty data or lines that are not relevant to the data
    nested_lst = []
    for i in spark_file_rdd.collect(): ## Converting  RDD to a list object and reading in data like this
        i = list(i)
        new_i = ['' if v is None else v for v in i]
        nested_lst.append(new_i)  
        
    columns = []
    for i in nested_lst:
        if i.count('') == 0: ## If every column has a value in it, the first row where this takes place will be the headers (column names)
            columns = i
            break
             
    columns_schema_match = 0 ## checking if the number of columns in the .csv file matches the number of columns in the schema .txt file provided
    for i in columns:
        if i in schema:
            columns_schema_match += 1
    if columns_schema_match < len(schema)/2: ## If csv file provided does not have a header
        columns = list(schema.keys())
        output_message("WARNING: The '" + file + "' file did not have a header provided. The columns of the data for the '" + file + "' file will be inferred from the schema .txt file of the '" + file + "' file provided.")
        if len(nested_lst[0]) != len(list(schema.keys())): ## Number of columns in the .csv file does not match the number of columns in the schema .txt file; csv file cannot be processed
            output_message("ERROR: Since the '" + file + "' file did not have a header provided and since the number of columns from the schema .txt file provided does not match the number of columns in the csv data provided, the '" + file + "' file cannot be processed.")
            return None, None, None
        else:
            return columns, schema, nested_lst
    ## Checking if there are more columns in the schema provided than in the csv file provided        
    schema_columns_not_in_data = []
    for i in schema:
        if i not in columns:
            schema_columns_not_in_data.append(i)
    for i in schema_columns_not_in_data:
        output_message("WARNING: The '" + i + "' column in the schema .txt file of the '" + file + "' file is not in the csv data provided. The '" + i + "' column will not be in the '" + file + "' parquet file.")
        del schema[i] ## removing those columns from the schema
    return columns, schema, nested_lst

## get_raw_data takes in the nested list represented by the raw data and the column names and returns a DataFrame for the raw data
def get_raw_data(lst, columns):
    raw_lst = []
    for i in lst:
        if i.count('') >= len(lst[0])-1: ## Remove rows that are empty or only have one column filled
            continue
        if i == columns:
            continue
        raw_lst.append(i)
    raw_data = pd.DataFrame(raw_lst, columns=columns)
    return raw_data

## check_if_columns_in_csv_not_in_schema takes in the raw DataFrame, the csv file name, and the schema dictionary and removes the columns from the DataFrame if they are not provided in the schema and returns this new DataFrame
def check_if_columns_in_csv_not_in_schema(df, file, schema):
    columns_to_drop = []
    for i in df.columns:
        if i not in schema:
            columns_to_drop.append(i)
            output_message("WARNING: The '" + i + "' column in the '" + file + "' file is not in the schema .txt file provided. The '" + i + "' column will not be in the '" + file + "' parquet file.")
    if len(columns_to_drop) == 0:
        return df
    df = df.drop(columns = columns_to_drop)
    return df

## get_column_values takes in the .csv file name, the row value, the name of the column, and list from the schema dictionary that represents the column
def get_column_values(file, value, col, schema_lst): ## Get Column Values
    if value == '':
        return None
    
    if schema_lst[0] == 'Double' or schema_lst[0] == 'Integer': ## Checking if a numerical column has a non-numerical value
        if re.match(r'^[-+]?[0-9]*\.?[0-9]+$', value) is None:
            return str(value) + '-Invalid Conversion' ## If there is a non-numerical value
    
    if schema_lst[0] == 'Double':
        return float(value)
    elif schema_lst[0] == 'Integer':
        if '.' in value: ## If an Integer value is numerical but has a decimal in it - convert decimal to int
            output_message("WARNING: The '" + col + "' column in the '" + file + "' file has a value of " + str(value) + " and the schema .txt file says the '" + col + "' column is an Integer. It will be converted to " + str(int(float(value))) + '.')
            return int(float(value))
        else:
            return int(value)
    elif schema_lst[0] == 'String':
        if value.lower() == 'n/a':
            return None
        return value
    elif schema_lst[0] == 'Date' and schema_lst[2] == 'false': ## if schema says a Date column and doesn't need standardization, then return value as is after it is parsed
        try:
            return parse(value).date() ## parse value as date and return
        except ValueError:
            return str(value) + 'Invalid Conversion' ## if it cannot be parsed as a date
    elif schema_lst[0] == 'Date' and schema_lst[2] == 'true': ## if schema says a Date column and DOES need standardization, apply the below rules based on how the data is formatted in the column
        if len(value.split('-')) > 1:
            if len(value.split('-')) == 3: ## if the splitted string forms a list with 3 values in it
                try:
                    return parse(value).date() ## parse value as date and return
                except ValueError:
                    return str(value) + '-Invalid Conversion' ## if it cannot be parsed as a date
            elif len(value.split('-')) == 2: ## if the splitted string forms a list with 2 values in it
                if value.split('-')[0] in calendar.month_abbr: ## if the 0th index has a month abbreviation in it
                    month = list(calendar.month_abbr).index(value.split('-')[0])
                    if int(value.split('-')[1]) <= int(str(datetime.today().year)[-2:]): ## if the year is two digits and less than or equal to the last two digits of the current year
                        year = int(value.split('-')[1]) + 2000
                    else: ## if the year is two digits and greater than the last two digits of the current year
                        year = int(value.split('-')[1]) + 1900
                elif value.split('-')[1] in calendar.month_abbr: ## if the 1st index has a month abbreviation in it
                    month = list(calendar.month_abbr).index(value.split('-')[1])
                    if int(value.split('-')[0]) <= int(str(datetime.today().year)[-2:]):
                        year = int(value.split('-')[0]) + 2000
                    else:
                        year = int(value.split('-')[0]) + 1900
                try:
                    return parse(str(year) + '-' + str(month) + '-' + '01').date() ## parse standardized value as date and return
                except ValueError:
                    return str(value) + '-Invalid Conversion' ## if standardized value cannot be parsed as date
        elif len(value.split('/')) == 3: ## if the splitted string forms a list with 3 values in it
            try:
                return parse(value).date() ## parse value as date and return
            except ValueError:
                return str(value) + '-Invalid Conversion' ## if it cannot be parsed as a date
        else: ## if there is no "-" or "/"
            year = int(re.findall(r'\d+', value)[0]) ## finiding the digits as this could indicate the year, month, or the entire date itself
            if len(str(year)) == 2: ## if the number of digits is 2
                if year <= int(str(datetime.today().year)[-2:]): ## if the year is two digits and less than or equal to the last two digits of the current year
                    year += 2000
                else: ## if the year is two digits and greater than the last two digits of the current year
                    year += 1900
            elif len(str(year)) == 4: ## if the number of digits is 4, this will indicate the year
                year = year
            elif len(str(year)) == 8: ## if the number of digits is 8, this will indicate the entire date
                try:
                    return parse(str(year)).date() ## parse value as date and return
                except ValueError:
                    return str(value) + '-Invalid Conversion' ## if it cannot be parsed as a date
            else:
                try:
                    return parse(str(year)[:4] + '-' + str(year)[4:6] + '-' + '01').date() ## parse standardized value as date and return
                except ValueError:
                    return str(value) + '-Invalid Conversion' ## if standardized value cannot be parsed as a date
            month = "".join(re.split("[^a-zA-Z]*", value)).strip() ## getting the alphabetic values which indicates the month
            flag = False
            for j in calendar.month_abbr: ## checking if alphabetic values is a month abbreviation
                if month.lower() == j.lower():
                    month = list(calendar.month_abbr).index(j) ## month number
                    flag = True
                    break
            if flag:
                try:
                    return parse(str(year) + '-' + str(month) + '-01').date() ## parse standardized value as date and return
                except ValueError:
                    return str(value) + '-Invalid Conversion' ## if standardized value cannot be parsed as a date
            for j in calendar.month_name: ## checking if alphabetic values is a month name
                if month.lower() == j.lower():
                    month = list(calendar.month_name).index(j) ## month number
                    break
            try:
                return parse(str(year) + '-' + str(month) + '-01').date() ## parse standardized value as date and return
            except ValueError:
                return str(value) + '-Invalid Conversion' ## if standardized value cannot be parsed as a date

## output_message gets called when a message needs to be displayed; For this project, messages will be outputted to console. In actual production, messages can be sent out via email, slack, etc.
def output_message(message): 
    print(message)
    print()