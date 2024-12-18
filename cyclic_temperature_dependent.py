import pandas as pd
import os
import json
import numpy as np
import json
import pprint
import time

class TemperatureFileProcessor:

    def __init__(self, file_path):
        self.file_path = file_path
        self.file_name = os.path.splitext(file_path)[0]
        self.file_extension = os.path.splitext(file_path)[1]
        self.base_name = os.path.basename(self.file_name)
        self.dir_name = os.path.dirname(os.path.realpath(self.file_path))
        self.base_name_dir = os.path.basename(self.dir_name)
        self.encoding_read_file = "ISO-8859-1"
        self.encoding_pd_read_csv = 'unicode_escape'
        self.encoding_open_file = 'utf8'

    def file_validation_temp(self):
        validity_status = {
            "Code": None,
            "Message": None,
            "Warning": None
        }
        
        try:
            # Support both CSV and Excel files based on the file extension
            if self.file_extension == '.csv':
                df = pd.read_csv(self.file_path, encoding=self.encoding_pd_read_csv)
            elif self.file_extension == '.xlsx':
                df = pd.read_excel(self.file_path)
            else:
                validity_status["Code"] = 400
                validity_status["Message"] = "Unsupported file format. Only CSV and XLSX files are allowed."
                validity_status["Warning"] = None
                return validity_status  
        except Exception as e:
            validity_status["Code"] = 500
            validity_status["Message"] = f"Error reading file: {str(e)}"
            validity_status["Warning"] = None
            return validity_status

        # Validate file for having exactly 343 columns
        if df.shape[1] == 343:
            first_col = df.columns[0].strip().lower()
            if first_col in ['t', 'temperature']:
                for i in range(1, 343):
                    expected_col_1 = f"MA{i:03d}"  # Format MA001, MA002, ..., MA342
                    expected_col_2 = f"MA{i}"  # Format MA1, MA2, ..., MA342
                    actual_col = df.columns[i].strip()
                    if actual_col not in [expected_col_1, expected_col_2]:
                        validity_status["Code"] = 500
                        validity_status["Message"] = f"Column {i + 1} is not named '{expected_col_1}' or '{expected_col_2}', but '{actual_col}'."
                        validity_status["Warning"] = None
                        return validity_status
                validity_status["Code"] = 0
                validity_status["Message"] = None
                validity_status["Warning"] = "This file is valid and temperature-dependent."
            else:
                validity_status["Code"] = 400
                validity_status["Message"] = "The first column is not 'T' or 'Temperature'."
                validity_status["Warning"] = None
        else:
            validity_status["Code"] = 400
            validity_status["Message"] = f"File has {df.shape[1]} columns, expected 343."
            validity_status["Warning"] = None

        return validity_status
        
    def find_min_max_resistance_in_MA(self):
        """
        Finds the minimum and maximum resistance values and their corresponding MA columns 
        from the data in the specified file.
    
        Returns:
            dict: A dictionary containing resistance statistics or error messages.
        """
        # Initialize the output structure
        output = {
            "R_min": None,
            "absolute_min_MA_column": None,
            "R_max": None,
            "absolute_max_MA_column": None,
            "temperature_step": []
        }
    
        try:
            # Read the file based on its extension
            if self.file_extension == '.csv':
                df = pd.read_csv(self.file_path, encoding=self.encoding_pd_read_csv)
            elif self.file_extension == '.xlsx':
                df = pd.read_excel(self.file_path)
            else:
                output["Error"] = "Unsupported file format."
                return output  # Return the dictionary directly
        except Exception as e:
            output["Error"] = f"Failed to load file: {str(e)}"
            return output  # Return the dictionary directly
    
        # Check if the temperature column exists
        temp_column_name = None
        for col in ['T', 'Temperature']:
            if col in df.columns:
                temp_column_name = col
                break
    
        if temp_column_name is None:
            output["Error"] = "Temperature column 'T' or 'Temperature' not found."
            return output  # Return the dictionary directly
    
        # Drop the temperature column, keeping only resistance columns
        resistance_columns = df.drop(columns=[temp_column_name]).fillna(0)
    
        # Find the absolute minimum and maximum resistance values
        output["R_min"] = resistance_columns.min().min()
        output["R_max"] = resistance_columns.max().max()
    
        # Find the MA column for the absolute minimum and maximum resistance
        absolute_min_MA = resistance_columns.stack().idxmin()[1]
        absolute_max_MA = resistance_columns.stack().idxmax()[1]
        output["absolute_min_MA_column"] = absolute_min_MA
        output["absolute_max_MA_column"] = absolute_max_MA
    
        # Calculate the minimum and maximum for each temperature step (row-wise min/max)
        df['Min_R_Per_Temp'] = resistance_columns.min(axis=1)
        df['Max_R_Per_Temp'] = resistance_columns.max(axis=1)
    
        # Find the MA column for the min and max resistance per temperature
        df['Min_MA_Column'] = resistance_columns.idxmin(axis=1)
        df['Max_MA_Column'] = resistance_columns.idxmax(axis=1)
    
        # Add min and max resistance for each temperature step along with the MA column
        for index, row in df.iterrows():
            output["temperature_step"].append({
                "temperature": row[temp_column_name],
                "R_min": row['Min_R_Per_Temp'],
                "min_MA_column": row['Min_MA_Column'],
                "R_max": row['Max_R_Per_Temp'],
                "max_MA_column": row['Max_MA_Column']
            })
    
        # Return the output dictionary directly
        return output

    def table_of_df_temp(self):
        try:
            # Read the file (CSV or Excel)
            if self.file_extension == '.csv':
                df = pd.read_csv(self.file_path, encoding=self.encoding_pd_read_csv)
            elif self.file_extension == '.xlsx':
                df = pd.read_excel(self.file_path)
            else:
                return {"Error": "Unsupported file format."}  # Return a dictionary directly
        except Exception as e:
            return {"Error": f"Failed to load file: {str(e)}"}  # Return a dictionary directly

        # Check if the temperature column exists
        temp_column_name = None
        for col in ['T', 'Temperature']:
            if col in df.columns:
                temp_column_name = col
                break

        if temp_column_name is None:
            return {"Error": "Temperature column 'T' or 'Temperature' not found."}

        # Drop the temperature column, keeping only resistance columns
        resistance_columns = df.drop(columns=[temp_column_name])

        data_table = []

        # Loop over each row and construct the dictionary
        for index, row in df.iterrows():
            temperature = row[temp_column_name]  # Extract the temperature
            resistance_values = {}

            # For each MA column, add it to the resistance_values dictionary
            for col in resistance_columns.columns:
                resistance_values[col] = row[col]

            # Construct the final structure for each temperature
            data_table.append({
                "temperature": temperature,
                "R": resistance_values
            })

        # Return the final DataTable structure as JSON
        return {"DataTable": data_table}

    def analyze_cycles_and_store_resistance_descriptions(self):
        try:
            if self.file_extension == '.csv':
                df = pd.read_csv(self.file_path, encoding=self.encoding_pd_read_csv)
            elif self.file_extension == '.xlsx':
                df = pd.read_excel(self.file_path)
            else:
                raise ValueError("Unsupported file format. Only CSV and XLSX files are allowed.")
        except Exception as e:
            return {"Error": f"Failed to load file: {str(e)}"}

        df['Heating/Cooling'] = ''
        df['Cycle'] = 1
        cycle_count = 1
        previous_temp = df['T'].iloc[0]
        is_heating = True if df['T'].iloc[1] > previous_temp else False
        df.at[0, 'Heating/Cooling'] = 'heating' if is_heating else 'cooling'

        for i in range(1, len(df)):
            current_temp = df['T'].iloc[i]
            if current_temp > previous_temp:
                if not is_heating:
                    cycle_count += 1
                df.at[i, 'Heating/Cooling'] = 'heating'
                is_heating = True
            elif current_temp < previous_temp:
                if is_heating:
                    cycle_count += 1
                df.at[i, 'Heating/Cooling'] = 'cooling'
                is_heating = False
            else:
                df.at[i, 'Heating/Cooling'] = df['Heating/Cooling'].iloc[i - 1]

            df.at[i, 'Cycle'] = cycle_count
            previous_temp = current_temp

        total_cycles = df['Cycle'].max()
        resistance_data = []
        ma_columns = [col for col in df.columns if col.startswith('MA')]

        for i in range(len(df)):
            temperature = df['T'].iloc[i]
            phase = df['Heating/Cooling'].iloc[i]
            cycle = df['Cycle'].iloc[i]

            for col in ma_columns:
                resistance_value = df[col].iloc[i]

                if total_cycles > 1:
                    comment = f"R{temperature}_{phase}_cycle_{cycle}"
                    data_name = f"R{temperature}_{phase}{cycle}"
                else:
                    comment = f"R{temperature}_{phase}"
                    data_name = f"R{temperature}_{phase}"

                resistance_data.append({
                    "measurement_area": int(col[2:]),
                    "R": resistance_value,
                    "temperature": int(temperature),
                    "phase": phase,
                    "cycle": int(cycle),
                    "comment": comment,
                    "data_name": data_name
                })

        return resistance_data

    def info_R_in_MA_for_database_temp(self):
        try:
            resistance_data = self.analyze_cycles_and_store_resistance_descriptions()
        except Exception as e:
            return {"Error": f"Failed to analyze resistance data: {str(e)}"}

        final_output = {
            "compositionsForSampleUpdate": []
        }

        for data in resistance_data:
            measurement_area = data['measurement_area']
            resistance_value = data['R']

            composition = {
                "predicate": {
                    "properties": [
                        {
                            "Type": 2,
                            "Name": "Measurement Area",
                            "Value": measurement_area
                        }
                    ]
                },
                "deletePreviousProperties": False,
                "properties": [
                    {
                        "PropertyId": 0,
                        "Type": 1,
                        "Name": data['data_name'],
                        "Value": float(resistance_value),
                        "ValueEpsilon": None,
                        "SortCode": 10,
                        "Row": None,
                        "Comment": data['comment']
                    }
                ]
            }

            final_output["compositionsForSampleUpdate"].append(composition)

        return final_output


    def info_for_database_temp(self):
        """
        Generates a JSON structure with the resistance data for database storage.
        """
        # Initialize the final output structure
        final_output = {
            "DeletePreviousProperties": True,
            "Properties": []
        }
    
        # Call the function to get the minimum and maximum resistance values
        min_max_resistance = self.find_min_max_resistance_in_MA()  # No need to parse JSON, it's already a dictionary
    
        # Check for any errors returned in the dictionary
        if "Error" in min_max_resistance:
            return min_max_resistance  # Return the error as it is
    
        # Extract absolute minimum and maximum resistance values
        R_min = min_max_resistance.get("R_min", None)
        R_max = min_max_resistance.get("R_max", None)
        absolute_min_MA_column = min_max_resistance.get("absolute_min_MA_column", None)
        absolute_max_MA_column = min_max_resistance.get("absolute_max_MA_column", None)
    
        if R_min is None or R_max is None:
            return {"Error": "Min or Max resistance not found."}
    
        # Create the properties list for database entry
        properties_RT = [
            {
                "Type": 1,
                "Name": "R",
                "Value": R_min,
                "ValueEpsilon": None,
                "SortCode": 0,
                "Row": 1,
                "Comment": "Minimal Resistance"
            },
            {
               "Type": 1,
               "Name": "Measurement Area",
               "Value": absolute_min_MA_column,
               "ValueEpsilon": None,
               "SortCode": 0,
               "Row": 2,
               "Comment": "Measurement Area with Minimal Resistance"
            },
            {
                "Type": 1,
                "Name": "R",
                "Value": R_max,
                "ValueEpsilon": None,
                "SortCode": 0,
                "Row": 3,
                "Comment": "Maximal Resistance"
            },
            {
                "Type": 1,
                "Name": "Measurement Area",
                "Value": absolute_max_MA_column,
                "ValueEpsilon": None,
                "SortCode": 0,
                "Row": 4,
                "Comment": "Measurement Area with Maximal Resistance"
            }
        ]
    
        # Get per-temperature step data
        per_temperature_step = min_max_resistance.get("per_temperature_step", [])
        for idx, temp_step in enumerate(per_temperature_step, start=5):
            properties_RT.extend([
                {
                    "Type": 1,
                    "Name": "R",
                    "Value": temp_step['R_min'],
                    "ValueEpsilon": None,
                    "SortCode": 0,
                    "Row": idx,
                    "Comment": f"Min Resistance at {temp_step['temperature']} degree in MA {temp_step['min_MA_column']}"
                },
                {
                    "Type": 1,
                    "Name": "R",
                    "Value": temp_step['R_max'],
                    "ValueEpsilon": None,
                    "SortCode": 0,
                    "Row": idx + 1,
                    "Comment": f"Max Resistance at {temp_step['temperature']} degree in MA {temp_step['max_MA_column']}"
                }
            ])
    
        # Assign the properties list to the final output structure
        final_output["Properties"] = properties_RT
    
        return final_output
