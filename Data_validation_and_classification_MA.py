"""
import glob, os: Navigating the file system and matching file patterns
import shutil:Copying, moving, and deleting files
import pandas as pd: Manipulating and analyzing data
import numpy as np: provides support for arrays, mathematical functions, and random number generation
import re: Matching and parsing text patterns

Usage in the Script:
These imported modules and libraries will be used to:
- Access and extract data from ZIP files.
- Find and organize data files based on specific patterns.
- Manipulate file locations and contents.
- Analyze and manipulate data tables containing resistance measurements.
- Validate the format and content of data using regular expressions.
"""
import zipfile
import glob
import os
import shutil
import pandas as pd
import numpy as np
import re
import unittest
import openpyxl


# creating objects including All the properties and functions needed to handle and process a data file.


class Data_File:
    # list of keywords used in the data file to identify the columns.
    keywords = ["R1", "R2", "R3", "R", "Resistance"]

    """
    #tolerance level for error used for validating data.
    #any measurement within 10% of a reference value is considered acceptable or valid. 
    """
    relative_error_percent = 10

    # The minimum percentage of rows required for the data file to be considered valid.
    min_required_rows_percent = 80
    # range of valid temperatures from -50 to 300 degrees
    temperature_range = range(-50, 300)

    # Constructor Method(__init__) assign values with specific attributes.it takes path to the data file.
    def __init__(self, file_path):
        # Stores the file path of the data file.
        self.file_path = file_path

        """
        os.path.splitext split the file path into two parts:
        1)The file path without the extension[0]
          [0] extracts the first element of the tuple (the file path without the extension).
        2)The file extension[1].
          [1] extracts the second element of the tuple (the file extension).

        These steps help in separating the main part of the file path from its extension,
        which can be useful for various file handling operations.
        """

        # name of the file without its extension.
        self.file_name = os.path.splitext(file_path)[0]
        # The extension of the file (e.g., .csv)
        self.file_extension = os.path.splitext(file_path)[1]
        # The base name of the file (name without directory path and extension)
        self.base_name = os.path.basename(self.file_name)
        # The directory path where the file is located.
        self.dir_name = os.path.dirname(os.path.realpath(self.file_path))
        # The base name of the directory where the file is located.
        self.base_name_dir = os.path.basename(self.dir_name)

        """
        specify the encoding to be used for reading files in different contexts:
        Encoding used when reading the file directly.

        ISO-8859-1:
        It is used for reading files containing text in Western European languages and do not require the representation
        of characters outside this range.
        """
        self.encoding_read_file = "ISO-8859-1"

        """
        unicode_escape:
        Encoding used when reading the file into a pandas DataFrame.
        unicode_escape is used to process text that contains Unicode escape sequences.
        It is useful for reading files where characters are represented using escape sequences.
        This line sets the encoding for reading CSV files into a pandas DataFrame to unicode_escape.
        unicode_escape is used for reading files where text may include Unicode escape sequences,
        which need to be converted to their respective characters.
        """
        self.encoding_pd_read_csv = 'unicode_escape'

        """
        utf8:
        Encoding used when opening the file for other operations (e.g., reading text content).
        UTF-8:Unicode Transformation Format - 8-bit) is a variable-width character encoding representing every character
        in the Unicode character set.
        opening files to utf8. ensuring that the file can be read correctly if it contains a wide range of characters
        from different languages. utf8 is used for reading files with a range of characters from different languages.
        """
        self.encoding_open_file = 'utf8'

    """
    find_type_and_keyword Function aims to:
    * Identify keywords and the data types of each word(column) in a file.
    * Generate a dictionary with:
    - The count of each keyword found in the file.(e.g., "R1", "R2", "R3", "R", "Resistance")
    - The count of lines that have a similar number of columns that can be converted to floating-point numbers (referred
    to as "float able") means that the function identifies lines in the file where the columns contain numerical values
    that can be converted to floating-point numbers. 

    The result will be stored in a dictionary where:
    The key is the keyword.
    The value is the number of times the keyword appears in the line.
    """

    def find_type_and_keyword(self):
        count_valid_Data_File = 0  # count_valid_data_file is initialized but not used in the provided code.

        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"The file does not exist: {self.file_path}")

        count_l = 0  # tracking the number of line
        float_list = []  # Stores the counts of float able columns in each line.
        same_col_dict = {}  # Dictionary to store the final results.
        count_keyword = 0  # Counts the occurrences of keywords in the file

        # Reading Excel file using pandas
        if self.file_extension in ["xlsx", "xls"]:
            df = pd.read_excel(self.file_path)
            """
            #this loop iterates through each line in the file,
            initializing counters for floatable columns and non-floatable columns for each line.
            """

            for col in df.columns:
                count_l += 1  # increment line counter
                count_fl = 0  # counter for floatable columns in a line
                count_str = 0  # counter for non-floatable columns in a line.

                # Check for keywords in column names
                for word in Data_File.keywords:
                    if word in col:
                        count_keyword += 1
                        # print(line)
                        # break

                # Check each cell in the column
                for c in df[col]:
                    try:
                        float(c)
                        count_fl += 1
                    except (ValueError, TypeError):
                        count_str += 1
                if count_fl > 1:
                    float_list.append(count_fl)
                    if len(float_list) > 100:
                        break

        # Reading CSV and TXT files
        else:
            with open(self.file_path, 'r', encoding=self.encoding_read_file) as f:
                for line in f:
                    count_fl = 0  # counter for floatable columns in a line
                    count_str = 0  # counter for non-floatable columns in a line
                    count_l += 1  # increment line counter

                    # counting Keyword
                    for word in Data_File.keywords:
                        if word in line:
                            count_keyword += 1
                    """
                    Splitting Columns:
                    The line is split into columns based on whether the file is a CSV file (using commas) or another
                    type (using whitespace).             
                    """
                    # Splitting Columns
                    if self.file_extension == "csv":
                        col = line.split(",")
                    else:
                        col = line.split()

                    """
                    Determining Floatable Columns:
                    For each column, the method tries to convert it to a float. If successful, it increments count_fl.
                    If not, it increments count_str.
                    If a line has more than one floatable column, count_fl is added to float_list.
                    If float_list exceeds 100 entries, the loop breaks, to limit the processing for performance reasons.            
                    """
                    # Determining Floatable Columns
                    if len(col) > 0:
                        for c in col:
                            c = c.replace(',', '.')
                            try:
                                float(c)
                                count_fl += 1
                            except ValueError:
                                count_str += 1
                        if count_fl > 1:
                            float_list.append(count_fl)
                            if len(float_list) > 100:
                                break

            """
            #Building the Results Dictionary:
            #total count of keywords found.
            After processing all lines in the file, the function stores the total count of keywords found
            (count_keyword) in the dictionary same_col_dict under the key "keyword".        
            """
        same_col_dict["keyword"] = count_keyword
        # same_list is useful for not counting a value in float list more than 1 time.
        same_list = []

        """
        For each unique count in float_list, if it appears more than five times and hasn't been processed yet,
        it is added to same_col_dict with the count of its occurrences.
        """
        # Loop through each unique count of floatable columns in float_list.
        for i in float_list:
            # keys= the number of columns with float type,
            # value = how many times this kind of column are repeated.

            # Check if the count appears more than 5 times and hasn't been processed yet.
            if float_list.count(i) > 5 and i not in same_list:
                val_fl = i  # Assign the count of floatable columns to val_fl.
                count_val_float = float_list.count(i)  # Get the number of times this count appears in float_list.
                same_list.append(i)  # Mark this count as processed.
                same_col_dict[val_fl] = count_val_float  # Add this count and its frequency to the dictionary.
        return same_col_dict  # Return the final dictionary.

    """
    This function generates appropriate column names for a text file that contains a table of measurements,
    particularly when the file lacks keywords that can help determine these column names directly.          
    """

    def create_column_name(self):

        # an empty list to store the column names that will be created.
        created_column_name = []
        # get a dictionary of keyword counts and counts of floatable columns.
        same_dict = self.find_type_and_keyword()
        # Checks if no keywords were found in the file.
        if same_dict["keyword"] == 0:
            # Determine Column Names Based on Floatable Columns
            # If the file has 5 floatable columns, they represent coordinates and three resistance measurements.
            column_name = []
            if 5 in same_dict.keys():
                column_name = ['x', 'y', 'R1', 'R2', 'R3']
            # If the file has 7 floatable columns, it assumes they include two unknown columns.
            elif 7 in same_dict.keys():
                column_name = ['x', 'y', 'R1', 'R2', 'R3', 'unknown_1', 'unknown_2']
            elif 6 in same_dict.keys():
                column_name = ['x', 'y', 'R1', 'R2', 'R3', 'unknown_1']
            elif 4 in same_dict.keys():
                column_name = ['x', 'R1', 'R2', 'R3']
            # If none of these cases match, it sets column_name to an empty list.
            # else:
            # column_name = ['x','y','R','I','V','unknown_1','unknown_2']
            # column_name = []

            """
            Create DataFrame and Populate Columns
            If valid column names are determined, it creates a DataFrame df with these columns.
            - Reads the file line by line, splits each line into columns based on the file type (CSV or other).
            - Populates the DataFrame with these columns.
            """
            if len(column_name) > 0:
                df = pd.DataFrame(columns=column_name)
                count_l = 0
                with open(self.file_path, encoding=self.encoding_open_file) as f:
                    for line in f:
                        if self.file_extension == ".csv":
                            col = line.split(",")
                        else:
                            col = line.split()

                        if len(col) == len(column_name):  # Ensure the column count matches the expected column names
                            df.loc[count_l] = col
                            count_l += 1

                        # if len(col) > 1:
                        # count_l += 1
                        # df.loc[count_l] = col

                """
                Replaces commas with dots in the DataFrame to ensure that numeric values are correctly formatted for
                conversion to floats.
                """
                # df.columns is list of all column names in the DataFrame df
                # col_n is a variable representing the current column name in each iteration of the loop
                # iterates over each column name in the DataFrame.
                for col_n in df.columns:
                    # df[col_n] accesses the column in the DataFrame df with the name col_n.
                    # lambda x: x.replace(',', '.') is an anonymous function (lambda function) that
                    # takes one argument x (an element of the column) and replaces any commas , with dots.
                    # x is expected to be a string.
                    df[col_n] = df[col_n].apply(lambda x: x.replace(',', '.') if isinstance(x, str) else x)

                """
                Convert Data to Numeric and Validate Columns:
                The data of created dataframe in the above way are string so we need to make the float if possible for
                further evaluations!
                """
                try:
                    # Convert Data to Numeric and Validate Columns
                    df = df.apply(pd.to_numeric, errors='coerce')  # Coerce errors to NaN
                    # df.apply applies a function along an axis of the DataFrame.
                    # pd.to_numeric is a pandas function that attempts to convert values to numeric types.
                    # df = df.apply(pd.to_numeric)
                    # if the column 'unknown_1' exists in the list column_name.
                    if 'unknown_1' in column_name:
                        """
                        # df['unknown_1'] accesses the 'unknown_1' column in the DataFrame.
                        # .apply(lambda x: x in data_file.temperature_range) applies a lambda function to each element x
                        in the 'unknown_1' column.
                        # x in data_file.temperature_range checks if x is within a predefined temperature range
                        (e.g., -50 to 300 degrees).
                        # all_in_range_T is a Series of boolean values indicating whether each element in 'unknown_1' is
                        within the temperature range.
                        # all_in_range_T = df['unknown_1'].apply(lambda x: x in Data_File.temperature_range) checks if
                        all values in the Series all_in_range_T are True.  
                        """
                        all_in_range_T = df['unknown_1'].apply(lambda x: x in Data_File.temperature_range)
                        if all_in_range_T.all():
                            # finds the index of 'unknown_1' in the column_name list, accesses this index in the list.
                            # assigns the new name 'T_set' to this position in the list, effectively renaming
                            # 'unknown_1' to 'T_set'.
                            column_name[column_name.index('unknown_1')] = 'T_set'

                    if 'unknown_2' in column_name:
                        all_in_range_T = df['unknown_2'].apply(lambda x: x in Data_File.temperature_range)
                        if all_in_range_T.all():
                            column_name[column_name.index('unknown_2')] = 'T_set'

                    """
                    Check If First Column is Temperature
                    As it was seen in some csv files maybe the first column is temperature not coordinate.
                    Checks if the first column (x) contains temperature values. If so, reassigns the first few columns.
                    """
                    all_x_in_range_T = df['x'].apply(lambda x: x in Data_File.temperature_range)
                    if all_x_in_range_T.all():
                        column_name[0] = 'T_set'
                        column_name[1] = 'R1'
                        column_name[2] = 'R2'
                        column_name[3] = 'R3'
                        if len(column_name) > 4:
                            column_name[4] = 'unknown'

                    """
                    * Checking the columns attributed to 'R1', 'R2' and 'R3' by the following conditions:
                    1) relative error percentage (difference of 'R1', 'R2' or 'R3' with their average/their average)*100
                       be less than relative_error_percent defined as a constant in data_file class.
                    2) At least min_required_rows_percent of the rows of the columns attributed to 'R1', 'R2' or 'R3'
                       follows the 1st condition

                    * Validate Resistance Columns:
                    Calculates the average resistance (average_R) and differences for R1, R2, and R3 from this average.
                    Computes the relative error percentages for these differences.
                    Counts the total number of rows and the number of rows where the relative error is within the
                    acceptable range.
                    If the percentage of acceptable rows exceeds the minimum required percentage,
                    assigns created_column_name to column_name.
                    """

                    # Validate Resistance Columns
                    # average resistance (average_R) and the differences for R1, R2, and R3 from this average.
                    df['average_R'] = abs(df['R1'] + df['R2'] + df['R3']) / 3
                    df['difference_1'] = abs(df['R1'] - df['average_R'])
                    df['difference_2'] = abs(df['R2'] - df['average_R'])
                    df['difference_3'] = abs(df['R3'] - df['average_R'])

                    # relative error percentages for these differences.
                    df['relative_Error_1'] = (df['difference_1'] / df['average_R']) * 100
                    df['relative_Error_2'] = (df['difference_2'] / df['average_R']) * 100
                    df['relative_Error_3'] = (df['difference_3'] / df['average_R']) * 100

                    """
                    It aims to validate columns 'R1', 'R2', and 'R3' by checking if their values are within
                    an acceptable range of relative error.
                    """
                    # count_total is total number of elements across the three relative error columns.
                    # len(df['relative_Error_1']) gives the number of rows in the 'relative_Error_1' column.
                    # len(df['relative_Error_2']) gives the number of rows in the 'relative_Error_2' column.
                    # len(df['relative_Error_3']) gives the number of rows in the 'relative_Error_3' column.
                    count_total = len(df['relative_Error_1']) + len(df['relative_Error_2']) + len(
                        df['relative_Error_3'])

                    """
                    #count_accept calculates the number of rows where the relative error is within the acceptable range.
                    df[df['relative_Error_1'] < data_file.relative_error_percent] filters the DataFrame df
                    to include only rows where 'relative_Error_1' is less than the acceptable percentage.
                    
                    len(df[df['relative_Error_1'] < data_file.relative_error_percent]) gives the number of rows that
                    meet this condition.
                    """
                    count_accept = (len(df[df['relative_Error_1'] < Data_File.relative_error_percent]) +
                                    len(df[df['relative_Error_2'] < Data_File.relative_error_percent]) +
                                    len(df[df['relative_Error_3'] < Data_File.relative_error_percent]))

                    # calculates the percentage of rows with acceptable relative errors.
                    # count_accept / count_total gives the fraction of rows that have acceptable relative errors.
                    accept_per_total = (count_accept / count_total) * 100

                    """
                    #checks if the percentage of acceptable rows (accept_per_total) is greater than a predefined minimum
                     required percentage
                    If this condition is met, it means the columns 'R1', 'R2', and 'R3' have a sufficient number of
                    valid rows based on the relative error criteria.
                    """
                    if accept_per_total > Data_File.min_required_rows_percent:
                        # assigns the validated column names (column_name) to created_column_name.
                        created_column_name = column_name
                except ValueError as e:
                    print(f"ValueError occurred: {e}")
                    created_column_name = []
            # If no valid column names were determined, created_column_name remains an empty list.
            else:
                created_column_name = []
        # Returns the created_column_name, which either contains the generated column names or
        # is empty if no valid columns were determined.
        return created_column_name

    """
    file_validation method:
    validates the structure and content of the file to ensure it meets the expected criteria.
    """

    def file_validation(self):
        # Identify Column Structure and Keywords.
        # same_col_dict Calls find_type_and_keyword to get a dictionary with counts of keywords and floatable columns.
        same_col_dict = self.find_type_and_keyword()

        """
        Check if Measurement Table Exists.
        If same_col_dict has fewer than two items, it implies that no valid measurement table was found in the file.
        """
        if len(same_col_dict) < 2:
            return {"Code": 500,
                    "Message": "This file is not valid:Table of measurement was not found",
                    "Warning": None}

        # This variable stores the result of self.create_column_name(), which attempts to generate column names.
        created_column_name = self.create_column_name()
        # column_name: An empty list initialized to store the final column names.
        column_name = []

        """
        Validate Column Names When No Keywords Found and Column Names Not Created
        If the dictionary has more than one item, no keywords were found, and column names couldn't be created:
        Returns an error code 400 with a message asking the user to correctly name the columns related to resistance.
        """
        if len(same_col_dict) > 1 and same_col_dict["keyword"] == 0 and len(created_column_name) > 0:
            column_name = created_column_name
        elif len(same_col_dict) > 1 and same_col_dict["keyword"] > 0:
            column_name = self.find_column_name()
        col_len = len(column_name)
        print(column_name)

        if col_len == 0:
            return {"Code": 400,
                    "Message": "Column names have not been defined correctly, name the column related to"
                               "resistance measurement R,in case of more than once resistance measurement"
                               "please name the related columns R1, R2, ...",
                    "Warning": None}
        elif col_len > 0:
            # coord_key = ['x', 'y', ]
            if 'MA' not in column_name and 'x' not in column_name:
                return {"Code": 400,
                        "Message": " Coordinate was not found, there should be a column with name of MA or two columns"
                                   "with names of x and y to find coordinate",
                        "Warning": None}

        df_MA, df = self.find_min_max_resistance_in_MA()

        # If 'MA' is a column in df and keywords were found (same_col_dict["keyword"] > 0),
        # the file is considered valid, returning a status code 0.
        if 'MA' in df.columns and same_col_dict["keyword"] > 0:
            return {"Code": 0,
                    "Message": None,
                    "Warning": None}

        # This condition checks if both 'x' and 'y' are columns in the DataFrame df.
        # If this condition is true, it means the file contains coordinate information.
        elif 'x' in df.columns and 'y' in df.columns:

            # df[['x', 'y']]: Selects the 'x' and 'y' columns from df.
            # Creates a copy of this selection to ensure modifications are made to this copy, not the original DataFrame
            df_cords = df[['x', 'y']].copy()
            # Sorts df_cords first by the 'y' column and then by the 'x' column in ascending order.
            df_cords = df_cords.sort_values(['y', 'x'], ascending=[True, True])
            # drop_duplicates(keep='first'): Removes duplicate rows from df_cords, keeping only the first occurrence of
            # each unique coordinate pair.
            # This step ensures that each unique coordinate (x, y) pair is represented only once in the DataFrame.
            df_cords = df_cords.drop_duplicates(keep='first')
            # len(df_cords): Calculates the number of unique coordinate pairs by getting the length of df_cords.
            # df_len: Stores the count of unique coordinate pairs.
            df_len = len(df_cords)

            if len(same_col_dict) > 1 and same_col_dict["keyword"] == 0 and col_len > 0 and df_len != 342:
                return {"Code": 300,
                        "Message": "The measurement areas are more or less than 342.File can be valid,"
                                   "if the coordinates are defined completely or the MA corresponding each coordinate"
                                   "be specified in a separate column",
                        "Warning": "Columns names are inferred from the file structure"}

            if len(same_col_dict) > 1 and same_col_dict["keyword"] == 0 and col_len > 0 and df_len == 342:
                return {"Code": 0,
                        "Message": None,
                        "Warning": "Columns names are inferred from the file structure"}

            if len(same_col_dict) > 1 and same_col_dict["keyword"] > 0 and df_len != 342:
                return {"Code": 300,
                        "Message": "The measurement areas are more or less than 342.File can be valid, if the"
                                   "coordinates be defined completely or the MA corresponding each coordinate be"
                                   "specified in a separate column",
                        "Warning": None}

            if len(same_col_dict) > 1 and same_col_dict["keyword"] > 0 and df_len == 342:
                return {"Code": 0,
                        "Message": None,
                        "Warning": None}

    """
    The file_temperature function categorizes a file based on whether it pertains to room temperature ("RT"),
    time-dependent measurements ("MA"), or is otherwise unknown.
    The file will be categorized based on the existence of
    "RT": room temperature, "MA": time dependant and unknown when nor RT neither MA can be find in file name or
    its folder.
    """

    def file_temperature(self):
        # Initializes T_list with four elements, all set to 0.
        # This list will store the file path and flags for different temperature categories. why 4 items??
        T_list = [0, 0, 0, 0]
        # Sets the first element to the file path, indicating that the first entry will always be the file path.
        T_list[0] = self.file_path
        # Open the File and Split Name and Directory Elements
        with open(self.file_path, 'r', encoding=self.encoding_read_file) as f:
            # Splits the base name of the file (self.base_name) by underscores into name_elements.
            name_elements = self.base_name.split('_')
            # Splits the base name of the directory (self.base_name_dir) by underscores into dir_elements.
            dir_elements = self.base_name_dir.split('_')

            """
            Check for Keywords and Update T_list:
            Checks if "RT" is present in the name_elements or dir_elements lists.
            """
            # If found, sets the second element of T_list to 1, indicating room temperature.
            if "RT" in name_elements or "RT" in dir_elements:
                T_list[1] = 1
            # If "RT" is not found, checks if "MA" is present in the name_elements list.
            # If found, sets the third element of T_list to 1, indicating time-dependent measurements.
            elif "MA" in name_elements:
                T_list[2] = 1
            # If neither "RT" nor "MA" are found, sets the fourth element of T_list to 1,
            # indicating an unknown temperature category.
            else:
                T_list[3] = 1

        """
        Create List of Labels
        # Creates list_el, a list of labels corresponding to the elements in T_list.
        List_el shows that each element of T list is related to what.
        1 means yes.
        0 means No. 
        """
        list_el = ["File_Path", "RT", "Temperature_dependent", "Unknown_T"]
        return list_el, T_list

    """
    The find_skip-rows function identifies rows to skip when generating a dataframe from a file,
    ensuring the dataframe contains consistent and valid data.
    if the number of columns in the rows of file are not similar or the type of column in one row 
    is string while the other rows corresponds to measurement and are float.  
    """

    def find_skiprows(self):
        # empty list to store row indices that should be skipped.
        skip_list = []
        # counter for the line numbers
        count_l = 0
        # counter for the occurrences of keywords
        count_w = 0
        col_name = []
        with open(self.file_path, 'r', encoding=self.encoding_read_file) as f:
            for line in f:
                # Splits the line by commas into a list of columns (col).
                col = line.strip().split(",")

                """
                #Clean Up Column Values.
                #Iterates through each column value (n) and its index (i).
                Uses a regular expression to find word-like elements in the column value.
                Updates the column value with the cleaned version.
                """
                for i, n in enumerate(col):
                    # Checks if the column value is not empty or a newline
                    if n != '' and n != '\n':
                        # Uses a regular expression to find word-like elements in the column value.
                        n = re.findall(r"[\w']+", n)[0]
                        # Updates the column value with the cleaned version.
                        col[i] = n

                """
                #Check for Keywords:
                Iterates through predefined keywords (data_file.keywords).
                If a keyword is found in the columns, increments the keyword counter (count_w)
                """
                for word in Data_File.keywords:
                    if word in col:
                        count_w += 1
                        # print(word)
                        # Assigns the column names (col_name) to the current columns.
                        col_name = col
                """
                #update Skip List:
                If no keywords are found in the current row and the line number is not already in the skip list,
                appends the line number to the skip list.
                Increments the line counter (count_l).
                """
                if count_w == 0 and count_l not in skip_list:
                    skip_list.append(count_l)
                count_l += 1
            """
            #Final Adjustments to Skip List:
            If there are entries in the skip list, appends an additional row index to skip (one more than the last entry).
            If the skip list is empty, appends 0 to skip the first row.
            """
            if len(skip_list) >= 1:
                skip_list.append(skip_list[-1] + 1)
            else:
                skip_list.append(0)

            """
            Handle Cases with Repeated Keywords or No Keywords:
            If keywords are found in four or more rows, appends two more row indices to skip.
            If no keywords are found, clears the skip list and column names.
            """
            # This is the case when the keyword is repeated in more than one row
            if count_w >= 4:
                # Appends two more row indices to skip.
                skip_list.extend([skip_list[-1] + 1, skip_list[-1] + 2])

            # This is the case when there is no keyword in the files such as txt files
            if count_w == 0:
                skip_list = []
                col_name = []
        # Returns the list of row indices to skip (skip_list) and the column names (col_name).
        return skip_list, col_name

    """
    The find_column_name function defines the column names for the dataframe based on the keywords found in the file and
    makes adjustments if the columns don't match expected measurements.
    The column name is defined based on the the column name in the row which contain keywords. 
    that row will be taken and if the columns number is different from the columns numbers of measurement
    the name of "unknown" will be associated to them.
    If the name is empty but counted as column the name of "header" will be associated to that column. 
    """

    def find_column_name(self):
        """
        #Identify Keywords and Rows to Skip:
        same_dict Calls find_type_and_keyword to get a dictionary with counts of keywords and floatable columns.
        skip_list, col_name Calls find_skiprows to get the list of rows to skip and the initial column names.
        """
        same_dict = self.find_type_and_keyword()
        skip_list, col_name = self.find_skiprows()

        """
        Calculate Difference in Column Count:
        Calculates the difference between the number of floatable columns (from same_dict) and the length of col_name.
        #difference between the number of expected floatable columns and the number of columns identified in the initial
         row with keywords. 
        [1]: This accesses the second key in the list.
        Typically, the first key might be a count of keywords, and the second key would represent the number of
        floatable columns.
        The purpose of this calculation is to determine if the number of columns identified from the initial row
        with keywords matches the expected number of floatable columns. 
        
        """
        diff = list(same_dict.keys())[1] - len(col_name)

        """
        Adjust Column Names if Necessary:
        If there's no difference (diff == 0), uses col_name as column_name.
        If there's a difference, appends "unknown" columns to col_name to match the expected number of columns and then
        assigns it to column_name.
        """
        if diff == 0:
            column_name = col_name
        else:
            for i in range(diff):
                col_name.append(f"unknown_{i}")
            column_name = col_name

        """
        Handle Empty Column Names and Clean Up:
        Iterates through column_name.
        If any column name is empty, assigns a name "header" with its index.
        """
        for j, n in enumerate(column_name):
            if n == '':
                column_name[j] = f'header{j}'

            # n = re.findall(r"[\w']+", n)[0]
            # column_name[j] = n

            """
            Additional Clean Up and Renaming:
            Removes newline characters from column names.
            Normalizes column names: replaces "X" with "x", "Y" with "y", and "Resistance" with "R".
            """
            # checks if the newline character ("\n") is present in the string n, which represents a column name.
            if "\n" in n:
                # The .strip() method in Python is used to remove leading and trailing characters from a string.
                # If the newline character is found ("\n" in n evaluates to True), it removes leading and
                # trailing whitespace characters from the column name using the .strip("\n") method.
                column_name[j] = column_name[j].strip("\n")
            if "X" in n:
                column_name[j] = "x"
            if "Y" in n:
                column_name[j] = "y"
            if "Resistance" in n:
                column_name[j] = "R"
        # Returns the cleaned and adjusted list of column names.
        return column_name

    """
     The file_division method categorizes a file into one of five predefined types based on the presence and
     names of columns:
     "type_R1_R2_R3",
     "type_R1_R2_R3_temperature_dependant"
     "type_R_temperature_dependant",
     "type_R",
     "new_type" 
     """

    def file_division(self):
        """
        Identify Keywords and Column Names:
        same_dict Calls find_type_and_keyword to get a dictionary with counts of keywords and floatable columns.
        """
        same_dict = self.find_type_and_keyword()
        # created_column_name Calls create_column_name to generate column names if they can't be identified from
        # the keywords.
        created_column_name = self.create_column_name()

        """
        Determine Column Names Based on Keywords:
        If same_dict has more than one key (indicating multiple floatable columns) and
        no keywords were found (same_dict["keyword"] == 0), but created_column_name is not empty,
        assigns created_column_name to column_name.
        """
        if len(same_dict) > 1 and same_dict["keyword"] == 0 and len(created_column_name) > 0:
            column_name = created_column_name

        # If same_dict has more than one key and keywords were found (same_dict["keyword"] > 0),
        # calls find_column_name to identify column names based on the keywords and assigns it to column_name.
        elif len(same_dict) > 1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()

        # If neither condition is met (indicating the file is not valid for the intended categorization),
        # returns a message indicating the file is not valid.
        else:
            raise ValueError('File is not valid')
        """
        It determines the type of measurements in a file based on its column names and categorizes it into one of
        several predefined types. 
        #Split the Base Name:
        Splits the base_name (the file name without extension) using underscores (_) as the delimiter and
        stores the resulting list of elements in name_elements.
        """
        name_elements = self.base_name.split('_')

        """
        Initialize Measurement Type List:
        Initializes a list measurement_type with six zeros.
        """
        measurement_type = [0, 0, 0, 0, 0, 0]

        # Sets the first element of measurement_type to the file path.
        measurement_type[0] = self.file_path

        """
        Define Measurement Type Headers:
        Defines a list measurement_type_header with the column headers corresponding to different measurement types.
        """
        measurement_type_header = ["file_path", "type_R1_R2_R3_temperature_dependant", "type_R1_R2_R3",
                                   "type_R_temperature_dependant", "type_R", "new_type"]

        """
        Define Sets for Measurement Types:
        Defines sets s1, s2, s3, and s4 that represent different combinations of column names corresponding to
        different measurement types.
        """
        s1 = {"R1", "R2", "R3", "T_set"}
        s2 = {"R1", "R2", "R3"}
        s3 = {"R", "T_set"}
        s4 = {"R"}

        print("Column Names: ", column_name)

        """
         Check for Measurement Type and Update the List:
         Checks if the intersection of the set of column_name with each predefined set matches exactly with that set.
         """
        """
        If list(set) and intersection method is used the order of elements in the list will be changed and == condition
        will not work!
        """
        # If the columns match s1, sets the second element of measurement_type to 1.
        if s1 == set(column_name):
            measurement_type[1] = 1
        # If the columns match s2, sets the third element of measurement_type to 1
        elif s2 == set(column_name):
            measurement_type[2] = 1
        # If the columns match s3, sets the fourth element of measurement_type to 1
        elif s3 == set(column_name):
            measurement_type[3] = 1
        elif s4 == set(column_name):
            measurement_type[4] = 1
        # If none of the sets match, sets the sixth element of measurement_type to 1.
        else:
            measurement_type[5] = 1

        """
        Create and Return DataFrame:
        Creates a DataFrame df_measurement_type with columns defined by measurement_type_header
        """
        df_measurement_type = pd.DataFrame(columns=measurement_type_header)

        # Adds the measurement_type list as the first row of the DataFrame.
        df_measurement_type.loc[0] = measurement_type
        # Returns the DataFrame df_measurement_type
        return df_measurement_type

    """
    finding max and min of resistance in each measurement Area:
    This function aims to find the maximum and minimum resistance values in each measurement area (MA).
    In this function two dataframe will be generated:
            df: a clean data frame from the initial row file that has the created column name or the found column name
            by previoues functions  
            df_MA: the df that is grouped by the coordinates of x, y. 
            The average and median of measured resistances will also added to the columns of initial row file will be
            added as new column to both dataframes.
            in addition to mean and median , df_MA also shows R_min and R_max for database.
    """

    def find_min_max_resistance_in_MA(self):
        # Get Measurement Type and Column Names
        # Retrieves the measurement type using the file_division method.
        df_measurement_type = self.file_division()
        # Gets the list of rows to skip and the column names using find_skiprows.
        skip_list, col_name = self.find_skiprows()
        # Identifies the types and keywords using find_type_and_keyword.
        same_dict = self.find_type_and_keyword()
        # Creates column names if they are not present using create_column_name.
        created_column_name = self.create_column_name()

        """
        Determine the Column Names:
        Sets the column names based on the dictionary same_dict.
        """
        # If there are columns without keywords but with created names, use the created names
        if len(same_dict) > 1 and same_dict["keyword"] == 0 and len(created_column_name) > 0:
            column_name = created_column_name
        # If there are columns with keywords, find the column names.
        elif len(same_dict) > 1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()
        # If neither condition is met, return that the file is not valid.
        else:
            raise ValueError('file is not valid')

        # Initialize Data Analysis Dictionary and Read File
        # Initializes data_ana as an empty dictionary

        data_ana = {}

        """
        Reads the file into a DataFrame df based on its extension.
        For CSV files, it uses pd.read_csv with specified column names and skip rows.
        For other file types, it manually processes each line to form the DataFrame.
        Converts all string values to numeric if possible, handling non-floatable columns.
        """
        if self.file_extension == ".csv":
            df = pd.read_csv(self.file_path, names=column_name, skiprows=skip_list, encoding=self.encoding_pd_read_csv)
        else:
            df = pd.DataFrame(columns=column_name)
            count_l = 0
            with open(self.file_path, encoding=self.encoding_open_file) as f:
                for line in f:
                    col = line.split()
                    if len(col) > 1:
                        count_l += 1
                        df.loc[count_l] = col
            for col_n in df.columns:
                df[col_n] = df[col_n].apply(lambda x: x.replace(',', '.'))
            # Additional cleaning: replace commas with periods and attempt to convert columns to numeric.
            try:
                df = df.apply(pd.to_numeric, errors='coerce')
                df.dropna(inplace=True)
            except ValueError:
                raise ValueError(f'{self.file_path}: contains non floatable columns')

        # If 'MA' is found in column_name, the DataFrame df is sorted based on the values in the 'MA' column.
        if 'MA' in column_name:
            df = df.sort_values(['MA'], ascending=[True])
        # If both 'x' and 'y' are found in column_name, the DataFrame df is sorted first by the 'y' column and
        # then by the 'x' column.
        elif 'x' in column_name and 'y' in column_name:
            df = df.sort_values(['y', 'x'], ascending=[True, True])

        # Process Different Measurement Types
        # For type_R1_R2_R3
        if df_measurement_type["type_R1_R2_R3"].iloc[0] == 1:
            # finding negative measurements! Why should we?  I ignored this part not to lose any of 342 coordinates.
            # as this is one of the measure of validity.
            # df[ df['R1'] < 0 ]= None
            # df[ df['R2'] < 0 ]= None
            # df[ df['R3'] < 0 ]= None
            # finding mean, min, max of the same condition
            # Here both the mean and median of 3 R measurements are found but only median is used in
            # further data processing.
            # calculates the average and median of R1, R2, and R3.

            # This line computes the mean (average) of the columns 'R1', 'R2', and 'R3'
            # along each row (axis=1) and assigns the result to a new column named "R_ave" in the DataFrame df.
            df["R_ave"] = df[['R1', 'R2', 'R3']].mean(axis=1)
            # The median is the middle value in a sorted list of numbers.
            df["R"] = df[['R1', 'R2', 'R3']].median(axis=1)

            # If the mean can give better accuracy, then 'R_ave' should be used instead of R.
            # If the column names include 'x' and 'y', groups by these coordinates and
            # calculates median, mean, min, and max resistance values.
            # Checks if the column name list (column_name) contains the string 'MA'.
            # If true, it indicates that there is a column named 'MA' in the DataFrame df.
            if 'MA' in column_name:
                # Groups the DataFrame df by the values in the column 'MA' and aggregates the column 'R' using
                # median, mean, min, and max functions.
                df_MA = df.groupby(["MA"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['MA'],
                                                                                                    ascending=[True])
                return df_MA, df
                # Sorts the resulting DataFrame df_MA by the values in the column 'MA' in ascending order.
                # Returns the sorted DataFrame df_MA and the original DataFrame df.
            # Checks if both 'x' and 'y' are present in the column_name list.
            elif 'x' in column_name and 'y' in column_name:
                df_MA = df.groupby(["x", "y"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['y', 'x'],
                                                                                                        ascending=[True,
                                                                                                                   True])
                return df_MA, df
            # If coordinates are missing, returns an error message.
            else:
                raise ValueError('The column related to coordinate was not found')

        elif df_measurement_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1:
            # finding negative measurements!
            # df[ df['R1'] < 0 ]= None
            # df[ df['R2'] < 0 ]= None
            # df[ df['R3'] < 0 ]= None
            # finding mean, min, max of the same condition
            df["R_ave"] = df[['R1', 'R2', 'R3']].mean(axis=1)
            # df[R] describes the median of R1,R2 and R3
            df["R"] = df[['R1', 'R2', 'R3']].median(axis=1)
            """
                   For type_R1_R2_R3_temperature_dependant:
                   Similar to the previous block but for temperature-dependent measurements.
                   """
            # iloc[0] selects the first row of the specified column.This is a comparison operation to check if the value
            # retrieved using iloc[0] is equal to 1.
            # checks whether value in the first row of the column "type_R1_R2_R3_temperature_dependant" is equal to 1.

            if 'MA' in column_name:
                df_MA = df.groupby(["MA"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['MA'],
                                                                                                    ascending=[True])
                return df_MA, df

            elif 'x' and 'y' in column_name:
                df_MA = df.groupby(["x", "y"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['y', 'x'],
                                                                                                        ascending=[True,
                                                                                                                   True])

                return df_MA, df
            else:
                raise ValueError('The column related to coordinate was not found')

        elif df_measurement_type["type_R_temperature_dependant"].iloc[0] == 1:
            if 'MA' in column_name:
                df_MA = df.groupby(["MA"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['MA'],
                                                                                                    ascending=[True])
                return df_MA, df
            elif 'x' and 'y' in column_name:
                df_MA = df.groupby(["x", "y"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['y', 'x'],
                                                                                                        ascending=[True,
                                                                                                                   True])

                return df_MA, df
            else:
                raise ValueError('The column related to coordinate was not found')

        elif df_measurement_type["type_R"].iloc[0] == 1:
            # finding negative measurements! 
            # df[ df['R'] < 0 ]= None
            if 'MA' in column_name:
                df_MA = df.groupby(["MA"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['MA'],
                                                                                                    ascending=[True])

                return df_MA, df
            elif 'x' and 'y' in column_name:
                df_MA = df.groupby(["x", "y"]).agg({'R': ['median', 'mean', 'min', 'max']}).sort_values(['y', 'x'],
                                                                                                        ascending=[True,
                                                                                                                   True])

                return df_MA, df
            else:
                raise ValueError('The column related to coordinate was not found')

        elif df_measurement_type["new_type"].iloc[0] == 1:
            raise ValueError("The type of file is unknown")

    """
    The find_coordinate method processes a DataFrame to identify unique (x, y) coordinate pairs,
    ensuring they meet specific criteria, and then assigns a unique identifier to each coordinate pair. 
    """

    """
    The method calls self.find_min_max_resistance_in_MA() to retrieve two DataFrames:
    - df_MA: contains summary statistics (e.g., min, max, mean, median) of resistance values grouped by
    measurement area (MA).
    - df: Contains detailed data including (x, y) coordinates.
    """

    def find_coordinate(self):
        df_MA, df = self.find_min_max_resistance_in_MA()
        # Check if required columns 'x' and 'y' exist in the DataFrame df
        if 'x' not in df.columns or 'y' not in df.columns:
            raise ValueError('Columns x and y are required in the DataFrame')
        # Extracts columns 'x' and 'y' from df into a new DataFrame df_cords and make a copy of that.
        df_cords = df[['x', 'y']].copy()
        # Sorts df_cords by 'y' and then by 'x' in ascending order.
        df_cords = df_cords.sort_values(['y', 'x'], ascending=[True, True])
        # Removes duplicate coordinate pairs, keeping the first occurrence.
        df_cords = df_cords.drop_duplicates(keep='first')

        # Checks if the length of df_cords is exactly 342.
        if len(df_cords) == 342:
            # If true, assigns a unique identifier p to each coordinate pair, ranging from 1 to 342.
            df_cords = df_cords.assign(p=list(range(1, 343)))
        else:
            """
            #### In this Error as the MA is expected in column name, we should find a strategy for that!
            ### a solution is : if 'MA' in df.columns , However we never had MA in columns! 
            #### Right now only files containing all MA are accepted! so we may lose some files
            ### we didn't delete negative resistance in last function, can this make a trouble? 
            """
            raise ValueError('Please define measurement area in a separate column')

        # Prepare the final DataFrame with columns 'p', 'x', 'y', and 'cords' as tuple of (x, y)
        df_cords = df_cords[['p', 'x', 'y']]
        df_cords['cords'] = df_cords[['x', 'y']].apply(tuple, axis=1)
        # df_cords DataFrame containing unique (x, y) coordinates along with their unique identifiers and tuple
        # representation.
        return df_cords

    """
    The table_of_df method in this code processes a DataFrame which is generated by find_min_max_resistance_in_MA()
    will be converted into a list of dictionaries,where each dictionary represents a row of the DataFrame.
    then the value of each cell of a row will be associated to its corresponding column name in a dictionary. 
    This list is then returned in a JSON-like structure. 
    """

    def table_of_df(self):
        # Calls self.find_min_max_resistance_in_MA() to retrieve two DataFrames: df_MA and df.
        df_MA, df = self.find_min_max_resistance_in_MA()
        # Initializes empty lists
        row_list = []
        data_table = []
        # Extracts column names from df into the list cols.
        cols = list(df.columns)
        # If the columns 'R_ave' and 'R' are present in cols, renames 'R' to 'R_median'.
        # This is done to distinguish between average and median resistance values.
        if 'R_ave' in cols and 'R' in cols:
            pos = cols.index('R')
            cols[pos] = 'R_median'
        # Iterates over each row of the DataFrame df.
        for i in range(len(df)):
            # Initializes empty dictionary
            df_row_dict = {}
            # For each row, iterates over each column to extract cell values.
            for j, c in enumerate(cols):
                cell = (df.iloc[i])[j]

                """
                # any method to take a cell value from a dataframe will return the value as numpy.float64 or numpy.int64 
                # numpy values cannot be used in dictionary in fastAPI , so I used item() method to just have
                normal int and float :<<<
                """
                # Checks if the cell value is of type numpy.float64 or numpy.int64 (common types in DataFrames).
                if isinstance(cell, (np.float64, np.int64)):
                    # Converts these types to standard Python float or int using the item() method,
                    cell_val = cell.item()
                # If the cell value is not a numpy type, assigns it directly to cell_val.
                else:
                    cell_val = cell

                # print(type(cell_val))
                # The below method for taking cell value didnt work for txt files but it is easier than iloc!
                # cell_val = df.at[i, c].item()

                # Assigns the cell value to a key in df_row_dict, where the key is the column name.
                df_row_dict[c] = cell_val
            # After processing all columns in a row, appends the row dictionary df_row_dict to data_table.
            data_table.append(df_row_dict)
            # Resets df_row_dict to an empty dictionary for the next row.
            df_row_dict = {}

        # Returns the data_table list in a dictionary with the key "DataTable".
        # This structure mimics a JSON format, making it suitable for API responses or other uses where JSON is required
        return {
            "DataTable": data_table
        }

    """
    The info_R_in_MA_for_database method processes the measurement data and prepares them for database insertion.
    The function focuses on resistance (R) values, both at room temperature and temperature-dependent, and organizes
    them by measurement area.
    """

    def info_R_in_MA_for_database(self):
        # Get Measurement Type and Min/Max Resistance Data
        global dict_cords
        same_dict = self.find_type_and_keyword()
        # Depending on the values in same_dict, it determines the appropriate column names.

        created_column_name = self.create_column_name()
        if len(same_dict) > 1 and same_dict["keyword"] == 0 and len(created_column_name) > 0:
            column_name = created_column_name
        elif len(same_dict) > 1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()
        # If neither condition is met, it raises an error indicating an invalid file.
        else:
            raise ValueError('file is not valid')

            # Divides the file to identify its type and reads the data, returning two DataFrames:
            # df_min_max_in_MA and df
        df_measurement_type = self.file_division()
        df_min_max_in_MA, df = self.find_min_max_resistance_in_MA()

        # Checks if 'MA' is in the column names. If so, it creates a dictionary mapping MA values to themselves.
        if 'MA' in column_name:
            df_cords = df[['MA']]
            dict_cords = dict(zip(df_cords.MA, df_cords.MA))

        # If 'x' and 'y' are in the column names (but 'MA' is not), it calls self.find_coordinate() to
        # get the coordinates
        # and creates a dictionary mapping coordinate tuples to a unique identifier p.
        elif 'x' in column_name and 'y' in column_name and 'MA' not in column_name:
            df_cords = self.find_coordinate()
            dict_cords = dict(zip(df_cords['cords'], df_cords['p']))

        # Initializes counters and lists to store the properties for measurement areas and overall measurements.
        count = 0
        properties_MA_RT = []
        properties_overall_RT = []
        properties_MA_R_T = []
        properties_overall_MA_R_T = []

        # Checks if the measurement type is either type_R1_R2_R3 or type_R.
        if (
                df_measurement_type["type_R1_R2_R3"].iloc[0] == 1 or
                df_measurement_type["type_R"].iloc[0] == 1
        ):
            # Calculates overall minimum and maximum resistance values.
            value_min_overall = df.agg({'R': ['min']}).iloc[0].R
            value_max_overall = df.agg({'R': ['max']}).iloc[0].R
            # Iterates through df_min_max_in_MA to extract mean, min, and max resistance values for
            # each measurement area.
            for i in df_min_max_in_MA.index:
                count += 1
                value_mean = df_min_max_in_MA.loc[i][("R", "mean")]
                value_min = df_min_max_in_MA.loc[i][("R", "min")]
                value_max = df_min_max_in_MA.loc[i][("R", "max")]

                # Creates dictionaries for measurement area properties and overall properties.
                value_MA = dict_cords.get(i)

                # returns a dictionary containing the properties for room temperature measurements.
                # Append Properties for Each Measurement Area (MA)
                properties_MA_RT.append({
                    "Predicate": {
                        "Properties": [
                            {
                                # "Type": 2: Indicates the type of property.
                                "Type": 2,  # The value type is tuple how should type change?
                                # "Name": "Measurement Area": Names the property as "Measurement Area".
                                "Name": "Measurement Area",
                                "Value": value_MA
                            }
                        ]
                    },
                    "DeletePreviousProperties": False,
                    "Properties": [
                        {
                            "PropertyId": 0,
                            "Type": 1,
                            "Name": "R",
                            "Value": value_mean,
                            # None: No epsilon value.
                            "ValueEpsilon": None,
                            # 10: Sorting code for ordering
                            "SortCode": 10,
                            "Row": None,
                            "Comment": f"Resistance (Room Temperature) for Measurement Area {value_MA}"
                        }
                    ]
                })

                # last section - overall
            properties_overall_RT.append(
                {
                    "PropertyId": 0,
                    "Type": 1,
                    "Name": "R",
                    "Value": value_min_overall,
                    "ValueEpsilon": None,
                    "SortCode": 10,
                    "Row": 1,
                    "Comment": f"Minimal Resistance (Room Temperature) of Materials Library (of all 342 MAs)"
                })
            properties_overall_RT.append({
                "PropertyId": 0,
                "Type": 1,
                "Name": "R",
                "Value": value_max_overall,
                "ValueEpsilon": None,
                "SortCode": 10,
                "Row": 2,
                "Comment": f"Maximal Resistance (Room Temperature) of Materials Library (of all 342 MAs)"
            })

            return {
                "CompositionsForSampleUpdate": properties_MA_RT,
                "DeletePreviousProperties": True,
                "Properties": properties_overall_RT
            }

        if (
                df_measurement_type["type_R_temperature_dependant"].iloc[0] == 1 or
                df_measurement_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1
        ):
            value_min_overall = df.agg({'R': ['min']}).iloc[0].R
            value_max_overall = df.agg({'R': ['max']}).iloc[0].R
            T_value_min_overall = df[df['R'] == value_min_overall].iloc[0].T_set
            T_value_max_overall = df[df['R'] == value_max_overall].iloc[0].T_set
            T_min_overall = df.agg({'T_set': ['min']}).iloc[0].T_set
            T_max_overall = df.agg({'T_set': ['max']}).iloc[0].T_set
            for i in df_min_max_in_MA.index:
                count += 1
                value_mean = df_min_max_in_MA.loc[i][("R", "mean")]
                value_min = df_min_max_in_MA.loc[i][("R", "min")]
                value_max = df_min_max_in_MA.loc[i][("R", "max")]

                T_min = df[df['R'] == value_min].iloc[0].T_set
                T_max = df[df['R'] == value_max].iloc[0].T_set
                value_MA = dict_cords.get(i)

                properties_MA_R_T.append({
                    "Predicate": {
                        "Properties": [
                            {
                                "Type": 2,
                                "Name": "Measurement Area",
                                "Value": value_MA
                            }
                        ]
                    },
                    "DeletePreviousProperties": False,
                    "Properties": [
                        {
                            "PropertyId": 0,
                            "Type": 1,
                            "Name": "R",
                            "Value": value_mean,
                            "ValueEpsilon": None,
                            "SortCode": 10,
                            "Row": None,
                            "Comment": f"Resistance (temperature dependant) for Measurement Area {value_MA} at T={T_min}"
                        }
                    ]
                })

                # last section - overall
            properties_overall_MA_R_T.append(
                {
                    "PropertyId": 0,
                    "Type": 1,
                    "Name": "R",
                    "Value": value_min_overall,
                    "ValueEpsilon": None,
                    "SortCode": 10,
                    "Row": 1,
                    "Comment": f"Minimal Resistance of Materials Library (of all 342 MAs) at T= {T_value_min_overall}"
                })
            properties_overall_MA_R_T.append({
                "PropertyId": 0,
                "Type": 1,
                "Name": "R",
                "Value": value_max_overall,
                "ValueEpsilon": None,
                "SortCode": 10,
                "Row": 2,
                "Comment": f"Maximal Resistance of Materials Library (of all 342 MAs) at T= {T_value_max_overall}"
            })
            # Strangly fastAPI doesnot accept T_min_overall directly like value_max_overall in resistance and
            # raise ValueError!!!
            properties_overall_MA_R_T.append({
                "PropertyId": 0,
                "Type": 1,
                "Name": "T",
                "Value": int(T_min_overall),
                "ValueEpsilon": None,
                "SortCode": 10,
                "Row": 1,
                "Comment": "Maximal Temperature of Materials Library (of all 342 MAs)"
            })
            properties_overall_MA_R_T.append({
                "PropertyId": 0,
                "Type": 1,
                "Name": "T",
                "Value": int(T_max_overall),
                "ValueEpsilon": None,
                "SortCode": 10,
                "Row": 2,
                "Comment": "Maximal Temperature of Materials Library (of all 342 MAs)"
            })

            return {
                "CompositionsForSampleUpdate": properties_MA_R_T,
                "DeletePreviousProperties": True,
                "Properties": properties_overall_MA_R_T
            }

    """
    Finding max and min of resistance in each temperature:
    The find_min_max_resistance method processes a dataset to find the minimum, maximum, median, and
    mean resistance values.
    This is done based on the type of measurements and temperature dependencies. 
    """

    def find_min_max_resistance(self):
        df_measurement_type = self.file_division()
        skip_list, col_name = self.find_skiprows()
        same_dict = self.find_type_and_keyword()
        created_column_name = self.create_column_name()
        if len(same_dict) > 1 and same_dict["keyword"] == 0 and len(created_column_name) > 0:
            column_name = created_column_name
        elif len(same_dict) > 1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()
        else:
            return 'file is not valid'

        if self.file_extension == ".csv":
            df = pd.read_csv(self.file_path, names=column_name, skiprows=skip_list, encoding=self.encoding_pd_read_csv)
        else:
            df = pd.DataFrame(columns=column_name)
            count_l = 0
            # For text files, it reads each line.
            # splits it into columns, replaces commas with dots for numeric conversion, and converts the data
            # to numeric types.
            with open(self.file_path, encoding=self.encoding_open_file) as f:
                for line in f:
                    col = line.split()
                    if len(col) > 1:
                        count_l += 1
                        df.loc[count_l] = col

            for col_n in df.columns:
                df[col_n] = df[col_n].apply(lambda x: x.replace(',', '.'))

            try:
                df = df.apply(pd.to_numeric, errors='coerce')
            except ValueError:
                print(self.file_path, ': contains non floatable columns')

        if df.empty:
            return 'Data frame is empty or could not be loaded properly.'

        if df_measurement_type["type_R1_R2_R3"].iloc[0] == 1:
            # finding negative measurements!
            # Sets negative resistance values (R1, R2, R3) to None.
            # df[df['R1'] < 0] = None
            # df[df['R2'] < 0] = None
            # df[df['R3'] < 0] = None
            # finding mean, min, max of the same condition
            # Computes the average (R_ave) and median (R) resistance values.
            df[['R1', 'R2', 'R3']] = df[['R1', 'R2', 'R3']].apply(lambda x: x.where(x >= 0))
            df['R_ave'] = df[['R1', 'R2', 'R3']].mean(axis=1)
            df["R"] = df[['R1', 'R2', 'R3']].median(axis=1)
            # Aggregates the median, mean, minimum, and maximum resistance values.
            df_R_detail = df.agg({'R': ['median', 'mean', 'min', 'max']})

            return df_R_detail

        elif df_measurement_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1:
            # finding negative measurements! 
            df[df['R1'] < 0] = None
            df[df['R2'] < 0] = None
            df[df['R3'] < 0] = None
            # finding mean, min, max of the same condition
            df["R_ave"] = df[['R1', 'R2', 'R3']].mean(axis=1)
            # df[R] describes the median of R1,R2 and R3
            df["R"] = df[['R1', 'R2', 'R3']].median(axis=1)
            # Similar to the above, but the results are grouped by temperature (T_set)
            df_R_detail = df.groupby(["T_set"]).agg({'R': ['median', 'mean', 'min', 'max']})

            return df_R_detail

        # Handles temperature-dependent measurements where resistance is measured directly (R).
        elif df_measurement_type["type_R_temperature_dependant"].iloc[0] == 1:
            # finding negative measurements! 
            df[df['R'] < 0] = None
            df_R_detail = df.groupby(["T_set"]).agg({'R': ['median', 'mean', 'min', 'max']})

            # Groups the results by temperature (T_set).
            return df_R_detail

        elif df_measurement_type["type_R"].iloc[0] == 1:
            # finding negative measurements!
            # Sets negative resistance values to None.
            df[df['R'] < 0] = None
            # Aggregates the median, mean, minimum, and maximum resistance values.
            df_R_detail = df.agg({'R': ['median', 'mean', 'min', 'max']})

        else:
            return 'Unknown measurement type or no valid data found.'

        return df_R_detail

    """
    The info_for_database method prepares information about resistance measurements for a database.
    It processes the data to generate a list of properties based on the type of measurements and temperature dependencies.
    """

    def info_for_database(self):
        df_measurement_type = self.file_division()
        df_min_max = self.find_min_max_resistance()
        count = 0
        # store temperature-dependent properties.
        properties_T_dependant = []
        # Check if the data is non-temperature dependent
        if (
                df_measurement_type["type_R1_R2_R3"].iloc[0] == 1 or
                df_measurement_type["type_R"].iloc[0] == 1
        ):
            # it processes the data as non-temperature dependent.
            properties_RT = [
                {
                    "Type": 1,
                    "Name": "Resistance",
                    "Value": df_min_max.loc['min']["R"],
                    "ValueEpsilon": None,
                    "SortCode": 0,
                    "Row": 1,
                    "Comment": "Minimal Resistance"
                },
                {
                    "Type": 1,
                    "Name": "Resistance",
                    "Value": df_min_max.loc['max']["R"],
                    "ValueEpsilon": None,
                    "SortCode": 0,
                    "Row": 2,
                    "Comment": "Maximal Resistance"
                }
            ]
            return {
                "DeletePreviousProperties": True,
                # Creates a list properties_RT containing dictionaries with the minimal and maximal resistance values.
                "Properties": properties_RT
            }
        # # Check if the data is temperature-dependent
        if (
                df_measurement_type["type_R_temperature_dependant"].iloc[0] == 1 or
                df_measurement_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1
        ):
            # processes the data as temperature-dependent.
            # Iterates through the index of df_min_max, which groups the data by temperature (T_set).
            for i in df_min_max.index:
                # For each temperature, it increments the count
                # appends a dictionary for the temperature, and appends dictionaries for the minimal and
                # maximal resistance values
                # at that temperature.
                count += 1
                properties_T_dependant.append({
                    "Type": 1,
                    "Name": "Temperature",
                    "Value": i,
                    "ValueEpsilon": None,
                    "SortCode": 10,
                    "Row": count,
                    "Comment": "Temperature"
                })
                properties_T_dependant.append(
                    {
                        "Type": 1,
                        "Name": "Resistance",
                        "Value": df_min_max.loc[i][("R", "min")],
                        "ValueEpsilon": None,
                        "SortCode": 20,
                        "Row": count,
                        "Comment": "Minimal Resistance"
                    })
                properties_T_dependant.append(
                    {
                        "Type": 1,
                        "Name": "Resistance",
                        "Value": df_min_max.loc[i][("R", "max")],
                        "ValueEpsilon": None,
                        "SortCode": 30,
                        "Row": count,
                        "Comment": "Maximal Resistance"
                    })
            # Returns a dictionary containing the properties_T_dependant list and a flag to delete previous properties.
            return {
                "DeletePreviousProperties": True,
                "Properties": properties_T_dependant
            }

    """
    The find_column_range method is designed to read data from a file and determine the minimum and maximum values
    for each column in the dataset.
    the function creates a DataFrame by finding the rows to skip and the column names,
    then calculates the minimum and maximum values for each column.
    """

    def find_column_range(self):
        # Calls find_skiprows to get the list of rows to skip when reading the file.
        skip_list, col_name = self.find_skiprows()
        # Calls find_column_name to get the names of the columns in the file.
        column_name = self.find_column_name()
        # empty dictionary data_ana to store the min and max values for each column.
        data_ana = {}
        # Checks the file extension.
        if self.file_extension == ".csv":
            # If the file is a CSV, it reads the file into a DataFrame using pd.read_csv with the specified column names
            # and rows to skip.
            df = pd.read_csv(self.file_path, names=column_name, skiprows=skip_list, encoding=self.encoding_pd_read_csv)
        else:
            # If the file is not a CSV, it reads the file line by line, splitting each line into columns and appending
            # the data to the DataFrame.
            df = pd.DataFrame(columns=column_name)
            count_l = 0
            with open(self.file_path, encoding=self.encoding_open_file) as f:
                for line in f:
                    col = line.split()
                    if len(col) > 1:
                        count_l += 1
                        df.loc[count_l] = col

            # Convert all columns to numeric, coercing errors to NaN
            df = df.apply(pd.to_numeric, errors='coerce')

        # Iterates over each column name.
        for cn in column_name:
            # Calculates the minimum value for the column using df[cn].min().
            min_col = df[cn].min()
            max_col = df[cn].max()
            # Stores the min and max values in the data_ana dictionary with the column name as the key.
            data_ana[cn] = [min_col, max_col]

        # Returns the data_ana dictionary containing the min and max values for each column.
        return data_ana


# testing Data_File:

def test_data_file():
    file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid.csv"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab.txt"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab_with-str.txt"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Messprotokoll_Fe-Co-O.txt"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/2020-11-09_1542_Cr-Mn-Fe-Co-Ni_Si + SiO2_49171_161202-K2-1 v5_Wid_000044642.xlsx"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/0005350_HTTS_4PP_all.xlsx"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/0008088_HTTS_4PP_Resistance.xlsx"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/0008088_HTTS_4PP_Resistance_ok.xlsx"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/MA_1_currents.xlsx"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/MA_1_resistances.xlsx"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/no column found.zip"
    # file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/results_RT.xlsx"

    try:
        data_file_instance = Data_File(file_path)
        print("Data_File instance created successfully.")
    except Exception as e:
        print("An error occurred:", e)


# Call the test function
test_data_file()

"""

# Testing function find_type_and_keyword
def test_find_type_and_keyword(file_path):
    try:
        print(f"Testing file: {file_path}")
        data_file_instance = Data_File(file_path)
        result = data_file_instance.find_type_and_keyword()
        print("find_type_and_keyword executed successfully.")
        print("Result:", result)
    except Exception as e:
        print(f"An error occurred while processing {file_path}: {e}")


# Test with the uploaded file
uploaded_file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid.xlsx"
test_find_type_and_keyword(uploaded_file_path)
"""

"""
# Testing function create_column_name:

def test_create_column_name(file_path):
    data_file_instance = Data_File(file_path)
    created_column_name = data_file_instance.create_column_name()

    # Print output for inspection
    print(f"File: {file_path}")
    print(f"Created Column Names: {created_column_name}")
    return created_column_name

# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Messprotokoll_Fe-Co-O.txt"
test_create_column_name(file_path)

"""

"""
# Testing function file_validation:
def test_file_validation(file_path):
    data_file_instance = Data_File(file_path)
    validation_result = data_file_instance.file_validation()

    # Print output for inspection
    print(f"File: {file_path}")
    print(f"Validation Result: {validation_result}")
    return validation_result


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Messprotokoll_Fe-Co-O.txt"
test_file_validation(file_path)
"""

"""
# Testing function file_temperature:
def test_file_temperature(file_path):
    data_file_instance = Data_File(file_path)
    labels, temperature_info = data_file_instance.file_temperature()

    # Print output for inspection
    print(f"File: {file_path}")
    print(f"Labels: {labels}")
    print(f"Temperature Info: {temperature_info}")
    return labels, temperature_info


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab.txt"
test_file_temperature(file_path)
"""

"""
Testing function find_skiprows:
def test_find_skiprows(file_path, expected_skip_list, expected_col_name):
    data_file_instance = Data_File(file_path)
    skip_list, col_name = data_file_instance.find_skiprows()

    # Print output for inspection
    print(f"File: {file_path}")
    print(f"Expected Skip List: {expected_skip_list}")
    print(f"Actual Skip List: {skip_list}")
    print(f"Expected Column Names: {expected_col_name}")
    print(f"Actual Column Names: {col_name}")

    # Assertions for testing
    assert skip_list == expected_skip_list, f"Expected {expected_skip_list} but got {skip_list}"
    assert col_name == expected_col_name, f"Expected {expected_col_name} but got {col_name}"
    print("Test passed!")


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab.txt" # Replace with the actual file path
expected_skip_list = [0, 1, 3]  # Expected skip list based on your test file content
expected_col_name = ["keyword1", "keyword2"]  # Expected column names based on your test file content
test_find_skiprows(file_path, expected_skip_list, expected_col_name)

"""

"""
# Testing function find_column_name:
def test_find_column_name(self):
    data_file_instance = Data_File(file_path)
    column_names = data_file_instance.find_column_name()

    # Print output for inspection
    print(f"Column Names: {column_names}")
    return column_names


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab.txt"
test_find_column_name(file_path)
"""

"""
# Testing function for file_division
def test_file_division(file_path):
    data_file_instance = Data_File(file_path)
    df_measurement_type = data_file_instance.file_division()

    # Print output for inspection
    print("Measurement Type DataFrame:\n", df_measurement_type)
    return df_measurement_type


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Messprotokoll_Fe-Co-O.txt"
test_file_division(file_path)
"""

"""  
# Example test function with debugging and improvements
def test_find_min_max_resistance_in_MA(file_path):
    data_file_instance = Data_File(file_path)
    try:
        df_MA, df = data_file_instance.find_min_max_resistance_in_MA()
        print(f"df_MA:\n{df_MA}")
        print(f"df:\n{df}")
        return df_MA, df

    except Exception as e:
        print(f"Exception occurred: {e}")
        raise


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab_with-str.txt"
test_find_min_max_resistance_in_MA(file_path)

"""

"""
def test_find_coordinate(file_path):
    # Create an instance of the Data_File class with the provided file path
    data_file_instance = Data_File(file_path)

    try:
        # Call the find_coordinate method
        df_cords = data_file_instance.find_coordinate()

        # Return the DataFrame for further inspection or assertions
        return df_cords

    except Exception as e:
        # Print the exception if any occurs during the method call
        print(f"Exception occurred while processing file {file_path}: {e}")
        return None


# Example usage with different file paths
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/0005350_HTTS_4PP_all.csv"
test_find_coordinate(file_path)
"""

"""
def test_table_of_df(file_path):
    # Create an instance of the Data_File class with the provided file path
    data_file_instance = Data_File(file_path)

    try:
        # Call the table_of_df method
        result = data_file_instance.table_of_df()
        # Return the result for further inspection or assertions
        return result

    except Exception as e:
        # Print the exception if any occurs during the method call
        print(f"Exception occurred while processing file {file_path}: {e}")
        return None


# Example usage with different file paths
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab.txt"
test_table_of_df(file_path)
"""

"""
def test_info_R_in_MA_for_database(file_path):
    # Create an instance of the Data_File class with the provided file path
    data_file_instance = Data_File(file_path)

    try:
        # Call the info_R_in_MA_for_database method
        result = data_file_instance.info_R_in_MA_for_database()

        # Return the result for further inspection or assertions
        return result

    except Exception as e:
        # Print the exception if any occurs during the method call
        print(f"Exception occurred while processing file {file_path}: {e}")
        return None


# Example usage with different file paths
file_path =
test_info_R_in_MA_for_database(file_path)

"""

"""
# Example test function
def test_find_min_max_resistance(file_path):
    data_file_instance = Data_File(file_path)
    try:
        df_R_detail = data_file_instance.find_min_max_resistance()
        print(f"df_R_detail:\n{df_R_detail}")
        return df_R_detail

    except Exception as e:
        print(f"Exception occurred: {e}")
        raise


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab_with-str.txt"
test_find_min_max_resistance(file_path)
"""

"""
# Example test function
def test_info_for_database(file_path):
    data_file_instance = Data_File(file_path)
    try:
        result = data_file_instance.info_for_database()
        print(f"Result:\n{result}")
        return result

    except Exception as e:
        print(f"Exception occurred: {e}")
        raise


# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab_with-str.txt"
test_info_for_database(file_path)
"""

"""
# Example test function
def test_info_for_database(file_path):
    data_file_instance = Data_File(file_path)
    try:
        result = data_file_instance.info_for_database()
        print(f"Result:\n{result}")
        return result

    except Exception as e:
        print(f"Exception occurred: {e}")
        raise

# Example usage
file_path = "C:/Users/Shirin/Desktop/Hiwi-MDI/test code/4PP_0004858_Fe-Co-O_Wid_tab_with-str.txt"
test_info_for_database(file_path)

"""
