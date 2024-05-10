# -*- coding: utf-8 -*-
## unit_tests.py file has unit tests to test out the features of the "csv_to_parquet.csv_to_parquet" library
import unittest
from csv_to_parquet import csv_to_parquet, parse_schema_txt_file
import pandas as pd
from pandas.api.types import is_int64_dtype, is_float_dtype, is_string_dtype, is_datetime64_dtype
import numpy as np
from datetime import datetime
import os

"""
Test these use-cases:
    files_list = [['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt'],
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt'],
                  ['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt'],
                  ['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt'],
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Empty Schema.txt'],
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing Columns in Schema.txt'],
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - One Column.txt'],
                  ['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt'],
                  ['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt'],
                  ['LoanStats_securev1_2018Q4 - Nullability.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']
                  ]
"""
class Test_csv_to_parquet(unittest.TestCase):
    
    def test_success_dataframe_returned_schema_has_more_columns(self): ## Test to see that a DataFrame is returned even if the schema .txt file has more columns than the csv data
        print("In the test_success_dataframe_returned_schema_has_more_columns test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']])
        self.assertTrue(isinstance(result[0], pd.DataFrame))
        
    def test_success_parquet_created_schema_has_more_columns(self): ## Test to see that a Parquet table is created even if the schema .txt file has more columns than the csv data
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4.parquet")
        print("In the test_success_parquet_created_schema_has_more_columns test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertTrue(flag)
        
    def test_success_dataframe_returned_schema_has_same_columns(self): ## Test to see that a DataFrame is returned when the schema .txt file has the same number of columns as the csv data
        print("In the test_success_dataframe_returned_schema_has_same_columns test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt']])
        self.assertTrue(isinstance(result[0], pd.DataFrame))
        
    def test_success_parquet_created_schema_has_same_columns(self): ## Test to see that a Parquet table is created when the schema .txt file has the same number of columns as the csv data
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4.parquet")
        print("In the test_success_parquet_created_schema_has_same_columns test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertTrue(flag)
        
    def test_error_none_returned_no_header_in_csv_schema_has_more_columns(self): ## Test to see that None is returned when the csv file has no headers and the schema .txt file has more columns than the csv data
        print("In the test_error_none_returned_no_header_in_csv_schema_has_more_columns test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']])
        self.assertEqual(result[0], None)
        
    def test_error_parquet_not_created_no_header_in_csv_schema_has_more_columns(self): ## Test to see that a Parquet table is not created when the csv file has no headers and the schema .txt file has more columns than the csv data
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4 - No Header.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4 - No Header.parquet")
        print("In the test_error_parquet_not_created_no_header_in_csv_schema_has_more_columns test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4 - No Header.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertFalse(flag)
        
    def test_success_dataframe_returned_no_header_in_csv_schema_has_same_columns(self): ## Test to see that a DataFrame is returned when the csv file has no header but the same number of columns as the schema .txt file
        print("In the test_success_dataframe_returned_no_header_in_csv_schema_has_same_columns test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt']])
        self.assertTrue(isinstance(result[0], pd.DataFrame))
        
    def test_success_parquet_created_no_header_in_csv_schema_has_same_columns(self): ## Test to see that a Parquet table is created when the csv file has no header but the same number of columns as the schema .txt file
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4 - No Header.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4 - No Header.parquet")
        print("In the test_success_parquet_created_no_header_in_csv_schema_has_same_columns test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing open_il_6m Column in Schema - Matching with CSV Data.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4 - No Header.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertTrue(flag)    
    
    def test_error_none_returned_empty_schema(self): ## Test to see that None is returned when the schema .txt file is empty
        print("In the test_error_none_returned_empty_schema test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Empty Schema.txt']])
        self.assertEqual(result[0], None)
        
    def test_error_parquet_not_created_empty_schema(self): ## Test to see that a Parquet table is not created when the schema .txt file is empty
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4.parquet")
        print("In the test_error_parquet_not_created_empty_schema test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Empty Schema.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertFalse(flag)
        
    def test_success_dataframe_returned_schema_has_less_columns(self): ## Test to see that a DataFrame is returned when the schema .txt file has less columns than the csv data but the csv data has headers
        print("In the test_success_dataframe_returned_schema_has_less_columns test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing Columns in Schema.txt']])
        self.assertTrue(isinstance(result[0], pd.DataFrame))
    
    def test_success_parquet_created_schema_has_less_columns(self): ## Test to see that a Parquet table is created when the schema .txt file has less columns than the csv data but the csv data has headers
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4.parquet")
        print("In the test_success_parquet_created_schema_has_less_columns test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Missing Columns in Schema.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertTrue(flag)    

    def test_success_dataframe_returned_schema_has_one_column(self): ## Test to see that a DataFrame is returned when the schema .txt file has only one column
        print("In the test_success_dataframe_returned_schema_has_one_column test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - One Column.txt']])
        self.assertTrue(isinstance(result[0], pd.DataFrame))
    
    def test_success_parquet_created_schema_has_one_column(self): ## Test to see that a Parquet table is created when the schema .txt file has only one column
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4.parquet")
        print("In the test_success_parquet_created_schema_has_one_column test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - One Column.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertTrue(flag)
        
    def test_success_dataframe_returned_schema_has_three_columns(self): ## Test to see that a DataFrame is returned when the schema .txt file has 3 columns
        print("In the test_success_dataframe_returned_schema_has_three_columns test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt']])
        self.assertTrue(isinstance(result[0], pd.DataFrame))
    
    def test_success_parquet_created_schema_has_three_columns(self): ## Test to see that a Parquet table is created when the schema .txt file has 3 columns
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4.parquet")
        print("In the test_success_parquet_created_schema_has_three_columns test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertTrue(flag)        
        
    def test_error_none_returned_no_header_in_csv_schema_has_less_columns(self): ## Test to see that None is returned when the csv file has no headers and the schema .txt file has less columns than the csv file
        print("In the test_error_none_returned_no_header_in_csv_schema_has_less_columns test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt']])
        self.assertEqual(result[0], None)
        
    def test_error_parquet_not_created_no_header_in_csv_schema_has_less_columns(self): ## Test to see that a Parquet table is not created when the csv file has no headers and the schema .txt file has less columns than the csv file
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4 - No Header.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4 - No Header.parquet")
        print("In the test_error_parquet_not_created_no_header_in_csv_schema_has_less_columns test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4 - No Header.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema - Three Columns.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4 - No Header.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertFalse(flag)        
    
    def test_error_none_returned_schema_mismatch_nullability(self): ## Test to see that None is returned when there is a mismatch with the csv data and the schema .txt file based on nullability
        print("In the test_error_none_returned_schema_mismatch_nullability test case")
        result = csv_to_parquet([['LoanStats_securev1_2018Q4 - Nullability.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']])
        self.assertEqual(result[0], None)
        
    def test_error_parquet_not_created_schema_mismatch_nullability(self): ## Test to see that a Parquet table is not created when there is a mismatch with the csv data and the schema .txt file based on nullability
        if os.path.isfile("../output_files/LoanStats_securev1_2018Q4 - Nullability.parquet"):
            os.remove("../output_files/LoanStats_securev1_2018Q4 - Nullability.parquet")
        print("In the test_error_parquet_not_created_schema_mismatch_nullability test case")
        csv_to_parquet([['LoanStats_securev1_2018Q4 - Nullability.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']])
        flag = False
        if 'LoanStats_securev1_2018Q4 - Nullability.parquet' in os.listdir("../output_files"):
            flag = True
        self.assertFalse(flag)  
        
    def test_success_dataframe_columns_match_with_schema(self): ## Test to see that the data types of the DataFrame are aligned with the schema .txt file
        print("In the test_success_dataframe_columns_match_with_schema test case")
        schema_dictionary = parse_schema_txt_file('LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt')
        result = csv_to_parquet([['LoanStats_securev1_2018Q4.csv', 'LoanStats_securev1_2018Q4 - Developer Supplied Schema.txt']])[0]
        for i in result.columns:
            if schema_dictionary[i] == 'Integer':
                self.assertTrue(is_int64_dtype(result[i]))
            elif schema_dictionary[i] == 'Double':
                self.assertTrue(is_float_dtype(result[i]))
            elif schema_dictionary[i] == 'String':
                self.assertTrue(is_string_dtype(result[i]))
            elif schema_dictionary[i] == 'Date':
                self.assertTrue(is_datetime64_dtype(result[i]))
            
        
if __name__ == '__main__':
    test = Test_csv_to_parquet()
    test.test_success_dataframe_returned_schema_has_more_columns()
    test.test_success_parquet_created_schema_has_more_columns()
    test.test_success_dataframe_returned_schema_has_same_columns()
    test.test_success_parquet_created_schema_has_same_columns()
    test.test_error_none_returned_no_header_in_csv_schema_has_more_columns()
    test.test_error_parquet_not_created_no_header_in_csv_schema_has_more_columns()
    test.test_success_dataframe_returned_no_header_in_csv_schema_has_same_columns()
    test.test_success_parquet_created_no_header_in_csv_schema_has_same_columns()
    test.test_error_none_returned_empty_schema()
    test.test_error_parquet_not_created_empty_schema()
    test.test_success_dataframe_returned_schema_has_less_columns()
    test.test_success_parquet_created_schema_has_less_columns()
    test.test_success_dataframe_returned_schema_has_one_column()
    test.test_success_parquet_created_schema_has_one_column()
    test.test_success_dataframe_returned_schema_has_three_columns()
    test.test_success_parquet_created_schema_has_three_columns()
    test.test_error_none_returned_no_header_in_csv_schema_has_less_columns()
    test.test_error_parquet_not_created_no_header_in_csv_schema_has_less_columns()
    test.test_error_none_returned_schema_mismatch_nullability()
    test.test_error_parquet_not_created_schema_mismatch_nullability()
    test.test_success_dataframe_columns_match_with_schema()