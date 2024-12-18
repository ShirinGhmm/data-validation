import zipfile
import glob, os
import shutil
import pandas as pd 
import numpy as np
import re
from cyclic_temperature_dependent import TemperatureFileProcessor

class data_file:
    keywords = ["R1","R2","R3","R","Resistance"]
    relative_error_percent = 10
    min_required_rows_percent = 80
    temperature_range = range(-50, 300)
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_name = os.path.splitext(file_path)[0]
        self.file_extention = os.path.splitext(file_path)[1]
        self.base_name = os.path.basename(self.file_name)
        self.dir_name = os.path.dirname(os.path.realpath(self.file_path))
        self.base_name_dir = os.path.basename(self.dir_name)
        self.encoding_read_file = "ISO-8859-1"
        self.encoding_pd_read_csv = 'unicode_escape'
        self.encoding_open_file = 'utf8'
        self.cyclic_temp_processor = TemperatureFileProcessor(file_path)
        
        

            
    
    def find_type_and_keyword(self):
        """
        Goal: finding keywords and data type of each word(column) in a file.
        Generating a dictionary that shows:
        1- How many keywords was found in the file--> key = keyword,  value = number of keywords
        2- The number of lines that have similar number of columns which are floatable
        key = number of floatable columns, value: number of lines
        """
        file_path = self.file_path
        file_extention = self.file_extention
        count_col_no = 0
        count_l = 0
        float_list =[]
        same_col_dict = {}
        count_keyword = 0
        #file_path, file_extention =self.final_file_path()
        count_valid_data_file = 0
        if file_extention == '.csv' or file_extention == '.txt':
            with open(file_path, 'r',encoding = self.encoding_read_file) as f:
                
                for line in f:
                    count_fl =0
                    count_str= 0
                    count_l += 1

                    for word in data_file.keywords:
                        if word in line: 
                            count_keyword +=1
                            #print(line)
                            #break


                    if file_extention == ".csv":
                        col = line.split(",")
                    else:
                        col = line.split()

                    if len(col)>0:
                        for c in col:
                            c = c.replace(',','.')
                            try:
                                float(c)
                                count_fl +=1
                            except ValueError:
                                count_str +=1
                        if count_fl > 1:
                            float_list.append(count_fl)
                            if len(float_list)> 100:
                                break

                same_col_dict["keyword"] = count_keyword
                # same_list is useful for not counting a value in float list more than 1 time.
                same_list = []
                for i in float_list:
                #              keys= the number of columns with float type, 
                #              value = how many times this kind of columns are repeated
                    if float_list.count(i) > 5 and i not in same_list: 
                        val_fl = i
                        count_val_float = float_list.count(i)
                        same_list.append(i)
                        same_col_dict[val_fl] = count_val_float
                        
        if file_extention == '.xlsx':
            df = pd.read_excel(file_path)
            ### opening .xlsx file by pandas returns empty values by NaN that is not known by fastAPI
            ### to avoid this using following line NaN will be converted to None that is known by python and fastAPI
            df = df.where(pd.notnull(df), None)
            print(range(len(df.columns)))
            for index, row in df.iterrows():
                count_str = 0
                count_fl = 0
                row_values = row.tolist()
               # print(type(row))
                for value in row_values:
                    if pd.notna(value): 
                        try:
                            float(value)
                            count_fl +=1
                        except ValueError:
                            count_str +=1
            ### with the following condition if the number of floats values in a column is at least two, it will be saved in float_list
                if count_fl > 1:
                    float_list.append(count_fl)
                    if len(float_list)> 100:
                        break
                        
            for word in data_file.keywords:
                if word in df.columns:
                    count_keyword +=1
                for j in range(len(df.index)):
                    list_rows= list(df.iloc[j][0:len(df.columns)])
                    if word in list_rows:
                        count_keyword +=1
             
            same_col_dict["keyword"] = count_keyword
            same_list = []
            for i in float_list:
            #              keys= the number of columns with float type, 
            #              value = how many times this kind of columns are repeated
                if float_list.count(i) > 5 and i not in same_list: 
                    val_fl = i
                    count_val_float = float_list.count(i)
                    same_list.append(i)
                    same_col_dict[val_fl] = count_val_float
            
        return same_col_dict
    
    
    def create_column_name(self):
        """
        The purpose of this function is to create column_name for txt file which contain table of measurement
        However can not be validate due to lack of keyword that helps to find column name.
        """
        #file_path, file_extention = self.final_file_path()
        file_path = self.file_path
        file_extention = self.file_extention
        created_column_name = []
        same_dict= self.find_type_and_keyword()
        if same_dict["keyword"] == 0:
            if 5 in same_dict.keys():
                column_name= ['x','y','R1','R2','R3']
            elif 7 in same_dict.keys():
                column_name = ['x','y','R1','R2','R3','unknown_1','unknown_2']
            elif 6 in same_dict.keys():
                column_name = ['x','y','R1','R2','R3','unknown_1']
            elif 4 in same_dict.keys():
                column_name =['x','R1','R2','R3']
            else:
                column_name =[]
                #column_name = ['x','y','R','I','V','unknown_1','unknown_2']
                
            if len(column_name) > 0:
                df = pd.DataFrame(columns= column_name)
                count_l = 0
                f = open(file_path, encoding=self.encoding_open_file)
                for line in f:
                    if file_extention == ".csv":
                        col = line.split(",")
                    else:
                        col = line.split()
                    if len(col)>1:
                        count_l +=1
                        df.loc[count_l] = col
               
                for col_n in df.columns:
                    df[col_n] = df[col_n].apply(lambda x: x.replace(',','.'))
                
            ## The data of created dataframe in the above way are string so we need to make the float if possible for further evaluations!
                try: 
                    df= df.apply(pd.to_numeric)
                    if 'unknown_1' in column_name:
                        all_in_range_T = df['unknown_1'].apply(lambda x: x in data_file.temperature_range)
                        if all_in_range_T.all():
                            column_name[column_name.index('unknown_1')] = 'T_set'
                    if 'unknown_2' in column_name:
                        all_in_range_T = df['unknown_2'].apply(lambda x: x in data_file.temperature_range)
                        if all_in_range_T.all():
                            column_name[column_name.index('unknown_2')] = 'T_set'

                ## As it was seen in some csv files maybe the first column is temperature no coordinate so we check it to be sure
                    all_x_in_range_T = df['x'].apply(lambda x: x in data_file.temperature_range)
                    if all_x_in_range_T.all():
                        column_name[0] = 'T_set'
                        column_name[1] = 'R1'
                        column_name[2]='R2'
                        column_name[3]='R3'
                        if len(column_name)>4:
                            column_name[4]='unknown'

                ## Checking the columns attributed to 'R1', 'R2' and 'R3' by the following conditions:
                ## 1- the relative error percentage (difference of 'R1', 'R2' or 'R3' with their average/their average)*100 
                ## be less than relative_error_percent defined as a constant in data_file class
                ## 2- At least min_required_rows_percent of the rows of the columns attributed to 'R1', 'R2' or 'R3' follows the 1st condition

                    df['average_R']=abs(df['R1']+df['R2']+df['R3'])/3          
                    df['difference_1'] = abs(df['R1']-df['average_R'])
                    df['difference_2'] = abs(df['R2']-df['average_R'])
                    df['difference_3'] = abs(df['R3']-df['average_R'])

                    df['relative_Error_1']=(df['difference_1']/df['average_R'])*100
                    df['relative_Error_2']=(df['difference_2']/df['average_R'])*100
                    df['relative_Error_3']=(df['difference_3']/df['average_R'])*100

                    count_total = len(df['relative_Error_1'])+len(df['relative_Error_2'])+len(df['relative_Error_3'])
                    count_accept = (len(df[df['relative_Error_1']<data_file.relative_error_percent])+
                                    len(df[df['relative_Error_2']<data_file.relative_error_percent])+
                                    len(df[df['relative_Error_3']<data_file.relative_error_percent]))
                    accept_per_total = (count_accept/count_total)*100
                    if accept_per_total > data_file.min_required_rows_percent:
                        created_column_name = column_name
                except ValueError:
                    created_column_name = []
                    
            else:
                created_column_name = []

            ## Checking dependency of measurements on temperature
                
                
        return created_column_name
  
    def file_validation(self):
    
        
        same_col_dict = self.find_type_and_keyword()
        print (list(same_col_dict.keys()))
        found_col_no = list(same_col_dict.keys())[1] 
        print(found_col_no)
        
        if len(same_col_dict)<2:
            return {"Code":500,
                    "Message":"This file is not valid:Table of measurement was not found",
                    "Warning":None}
        
        created_column_name = self.create_column_name()
        column_name = []
        if len(same_col_dict)>1 and same_col_dict["keyword"] == 0 and len(created_column_name)>0 :
            column_name = created_column_name
        elif len(same_col_dict)>1 and same_col_dict["keyword"] == 0 and found_col_no == 343 :
            column_name = self.find_column_name()
        elif len(same_col_dict)>1 and same_col_dict["keyword"] > 0:
            column_name = self.find_column_name()
        col_len = len(column_name)
        #print(column_name)

        if col_len==0:
            return {"Code":400,
                   "Message":"Column names have not been defined correctly, name the column related to resistance measurement R,in case of more than once resistance measurement please name the related columns R1, R2, ...",
                   "Warning":None}
        elif col_len > 0 and col_len < 343:
            #coord_key = ['x', 'y', ]
            if 'MA' not in column_name and 'x' not in column_name:
                return {"Code":400,
                       "Message":" Coordinate was not found, there should be a column with name of MA or two columns with names of x and y to find coordinate",
                        "Warning":None}
        elif col_len == 343:
            return self.cyclic_temp_processor.file_validation_temp()
        
        
        df_MA, df = self.find_min_max_resistance_in_MA()
        
        if 'MA' in df.columns and same_col_dict["keyword"] > 0:
            return {"Code":0,
                    "Message":None,
                    "Warning":None}
            
        elif 'x' and 'y' in df.columns:
            
            df_coords = df[['x','y']].copy()
            df_coords = df_coords.sort_values(['y', 'x'], ascending=[True, True])
            df_coords = df_coords.drop_duplicates(keep='first')
            df_len = len(df_coords)
        
            if len(same_col_dict)>1 and same_col_dict["keyword"] == 0 and col_len>0 and df_len!=342 :
                return {"Code":300,
                       "Message":"The measurement areas are more or less than 342.File can be valid, if the coordinates are defined completely or the MA corresponding each coordinate be specified in a separate column",
                       "Warning": "Columns names are inferred from the file structure"}

            if len(same_col_dict)>1 and same_col_dict["keyword"] == 0 and col_len>0 and df_len==342 :
                return {"Code":0,
                       "Message":None,
                       "Warning": "Columns names are inferred from the file structure"}

            if len(same_col_dict)>1 and same_col_dict["keyword"] > 0 and df_len!=342:
                return {"Code":300,
                        "Message":"The measurement areas are more or less than 342.File can be valid, if the coordinates be defined completely or the MA coresponding each coordinate be specified in a seperate column",
                        "Warning":None}

            if len(same_col_dict)>1 and same_col_dict["keyword"] > 0 and df_len==342:
                return {"Code":0,
                        "Message":None,
                        "Warning":None}
            
        

            
           
    def file_temperature(self):
        """
        The file will be categorized based on the existence of "RT": room temperature
        "MA": time dependant and unknown when nor RT neither MA can be find in file name or its folder. 
        """
        #file_path, file_extention = self.final_file_path()
        file_path = self.file_path
        file_extention = self.file_extention
        T_list= [0,0,0,0]
        T_list[0]= file_path
        with open(file_path, 'r',encoding = self.encoding_read_file) as f:
            name_elements = self.base_name.split('_')
            dir_elements = self.base_name_dir.split('_')
            if "RT" in name_elements or "RT" in dir_elements:
                T_list[1]=1   
            elif "MA" in name_elements:
                T_list[2]=1 
            else:
                T_list[3]=1
        # List_el shows that each element of T list is related to what. 1 means yes, 0 means No.     
        list_el=["File_Path", "RT", "Temperature_dependent", "Unknown_T"]
        return list_el, T_list       

    def find_skiprows(self):
        """
        Finding skiprows is important for generating a dataframe from a file(csv, txt ,...)
        if the nember of columns in the rows of file are not similar. or the type of column in one row 
        is string while the other rows corresponds to measurement and are float.  
        """
        file_path = self.file_path
        file_extention = self.file_extention
        #file_path, file_extention = self.final_file_path()
        skip_list =[]
        count_l = 0
        count_w = 0
        if file_extention == '.csv' or file_extention == '.txt':
            
            with open(file_path, 'r',encoding = self.encoding_read_file) as f:
                for line in f:
                    col =line.split(",")
                    for i,n in enumerate(col):
                        if n != '' and n != '\n':
                            n = re.findall(r"[\w']+", n)[0]
                            col[i]=n
                    for word in data_file.keywords:
                        if word in col: 
                            count_w +=1
                            #print(word)
                            col_name = col
                    if count_w == 0 and count_l not in skip_list: skip_list.append(count_l)
                    count_l+=1
                if len(skip_list)>=1:
                    skip_list.append(skip_list[-1]+1)
                else:
                    skip_list.append(0)

                # This is the case when the keyword is repeated in more than one row
                if count_w >=4:
                    skip_list = skip_list+ [skip_list[-1]+1,skip_list[-1]+2]

                # This is the case when there is no keyword in the files such as txt files
                if count_w == 0:
                    skip_list = []
                    col_name = []
        if file_extention == '.xlsx':
            skip_list = []
            col_name = []
            
        return skip_list, col_name
    
                
    def find_column_name(self):
        """
        The column name is defined based on the the column name in the row which contain keywords. 
        that row will be taken and if the columns number is differen from the columns numbers of measurement
        the name of "unknown" will be associated to them.
        If the name is empty but counted as column the name of "header" will be associated to that column.
        """
        file_path = self.file_path
        file_extention = self.file_extention
        #file_path, file_extention = self.final_file_path()
        same_dict = self.find_type_and_keyword()
        skip_list, col_name = self.find_skiprows()
        print('Skip list and col name',skip_list, col_name)
        diff = list(same_dict.keys())[1]-len(col_name)
        print(list(same_dict.keys())[1])
        #print (diff)
        if diff == 0:
            column_name = col_name
        elif diff == 343 and file_extention == '.xlsx':
            df = pd.read_excel(file_path)
            column_name = list(df.columns)
        else:
            for i in range(diff):
                col_name.append(f"unknown_{i}")
            column_name = col_name
        
        for j,n in enumerate(column_name):
            if n == '':
                column_name[j]=f'header{j}'
            
            #n = re.findall(r"[\w']+", n)[0]
            #column_name[j] = n
            
            
            if "\n" in n:
                column_name[j]=column_name[j].strip("\n")
            if "X" in n:
                column_name[j]= "x"
            if "Y" in n: 
                column_name[j]= "y"
            if "Resistance" in n:
                column_name[j]= "R"

        return column_name
    
    
    
    def file_devision(self):
        """
        This function devides file based on the columns names in 5 categories:
        "type_R1_R2_R3",
        "type_R1_R2_R3_temperature_dependant"
        "type_R_temperature_dependant",
        "type_R",
        "new_type"
        """
        file_path = self.file_path
        file_extention = self.file_extention
        #file_path, file_extention = self.final_file_path()
        same_dict = self.find_type_and_keyword()
        created_column_name = self.create_column_name()
        if len(same_dict)>1 and same_dict["keyword"] == 0 and len(created_column_name)>0 :
            column_name = created_column_name
        elif len(same_dict)>1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()
        else:
            raise ValueError('File is not valid')
            
        name_elements = self.base_name.split('_')
        
        measurement_type = [0,0,0,0,0,0]
        measurement_type[0]= file_path
        measurement_type_header = ["file_path","type_R1_R2_R3_temperature_dependant", "type_R1_R2_R3",
                                   "type_R_temperature_dependant","type_R","new_type"]
    
        s1 = {"R1","R2","R3","T_set"}
        s2 = {"R1", "R2", "R3"}
        s3 = {"R", "T_set"}
        s4 = {"R"}   
        

#If list(set) and intersection method is used the order of elements in the list will be changed 
# and == condition will not work!
        if s1&set(column_name) == s1:
            measurement_type[1] = 1
        elif s2&set(column_name) == s2:
            measurement_type[2] = 1
        elif s3&set(column_name) == s3:
            measurement_type[3] = 1
        elif s4&set(column_name) == s4:
            measurement_type[4] = 1
        else:
            measurement_type[5] = 1
            
        df_measurment_type = pd.DataFrame(columns=measurement_type_header)
        df_measurment_type.loc[0]= measurement_type
        return df_measurment_type
    
########  finding max and min of resistance in each measurment Area
        
    def find_min_max_resistance_in_MA(self):
        """
            In this function two dataframe will be generated:
            df: a clean data frame from the initial row file that has the created column name or the found column name by previoues functions  
            df_MA: the df that is grouped by the coordinates of x, y. 
            The average and median of measured resistances will also added to the columns of initial row file will be added as new column to both dataframes.
            in addition to mean and median , df_MA also shows R_min and R_max for database.
        """
        #file_path, file_extention = self.final_file_path()
        file_path = self.file_path
        file_extention = self.file_extention
        df_measurment_type = self.file_devision()
        skip_list, col_name = self.find_skiprows()
        same_dict = self.find_type_and_keyword()
        created_column_name = self.create_column_name()
        if len(same_dict)>1 and same_dict["keyword"] == 0 and len(created_column_name)>0 :
            column_name = created_column_name
        elif len(same_dict)>1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()
        else:
            raise ValueError('file is not valid') 
        data_ana = {} 
        if file_extention == ".csv":
            df=pd.read_csv(file_path,names=column_name,skiprows= skip_list,encoding= self.encoding_pd_read_csv)
        else:
            df = pd.DataFrame(columns= column_name)
            count_l = 0
            f = open(file_path, encoding=self.encoding_open_file)
            for line in f:
                col = line.split()
                if len(col)>1:
                    count_l +=1
                    df.loc[count_l] = col
            for col_n in df.columns:
                df[col_n] = df[col_n].apply(lambda x: x.replace(',', '.'))
                
            try: 
                df= df.apply(pd.to_numeric)
            except ValueError:
                print(file_path,': contains non floatable columns')
                
        if 'MA' in column_name:
            df = df.sort_values(['MA'], ascending=[True])
        elif 'x' and 'y' in column_name: 
            df = df.sort_values(['y', 'x'], ascending=[True, True])
        
        if df_measurment_type["type_R1_R2_R3"].iloc[0] == 1:
            # finding negative measurements! Why should we?  I ignored this part to not lose any of 342 coordinates. as this is one of the measure of validity. 
            ###df[ df['R1'] < 0 ]= None 
            ###df[ df['R2'] < 0 ]= None 
            ###df[ df['R3'] < 0 ]= None 
            # finding mean, min, max of the same condition
            ## Here bothe mean and median of 3 R measurements are found but only median is used in further data processing
            df["R_ave"]= df[['R1','R2','R3']].mean(axis=1)
            df["R"]=df[['R1','R2','R3']].median(axis=1)
            ## If the mean can give better accuracy then 'R_ave' should be used instead of R
            if 'MA' in column_name:
                df_MA= df.groupby(["MA"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['MA'], ascending=[True])
                return df_MA, df
            elif 'x'and'y' in column_name:
                df_MA= df.groupby(["x","y"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['y', 'x'], ascending=[True, True])
                return df_MA, df
            else:
                raise ValueError('The column related to coordinate was not found')

            return df_MA,df
        
        if df_measurment_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1:
            # finding negative measurements! 
            ###df[ df['R1'] < 0 ]= None 
            ###df[ df['R2'] < 0 ]= None 
            ###df[ df['R3'] < 0 ]= None 
            # finding mean, min, max of the same condition
            df["R_ave"]= df[['R1','R2','R3']].mean(axis=1)
            # df[R] describes the median of R1,R2 and R3
            df["R"]=df[['R1','R2','R3']].median(axis=1)
            
            if 'MA' in column_name:
                df_MA= df.groupby(["MA"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['MA'], ascending=[True])
                return df_MA, df
            elif 'x' and 'y' in column_name:
                df_MA= df.groupby(["x","y"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['y', 'x'], ascending=[True, True])
                return df_MA, df
            else:
                raise ValueError('The column related to coordinate was not found')

        if df_measurment_type["type_R_temperature_dependant"].iloc[0] == 1:
            # finding negative measurements! 
            ###df[ df['R'] < 0 ]= None 
        
            if 'MA' in column_name:
                df_MA= df.groupby(["MA"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['MA'], ascending=[True])
                return df_MA, df
            elif 'x' and 'y' in column_name:
                df_MA= df.groupby(["x","y"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['y', 'x'], ascending=[True, True])
                return df_MA, df
            else:
                raise ValueError('The column related to coordinate was not found')
            
        if df_measurment_type["type_R"].iloc[0] == 1:
            # finding negative measurements! 
            ###df[ df['R'] < 0 ]= None 
            
            if 'MA' in column_name:
                df_MA= df.groupby(["MA"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['MA'], ascending=[True])
                return df_MA, df
            elif 'x' and 'y' in column_name:
                df_MA= df.groupby(["x","y"]).agg({'R': ['median','mean', 'min', 'max']})
                df_MA = df_MA.sort_values(['y', 'x'], ascending=[True, True])
                return df_MA, df
            else:
                raise ValueError('The column related to coordinate was not found')
        
        if df_measurment_type["new_type"].iloc[0] == 1:
            raise ValueError("The type of file is unknown")
            

    
    
    def find_coordinate(self):
        df_MA, df = self.find_min_max_resistance_in_MA()
        df_coords = df[['x','y']].copy()
        df_coords = df_coords.sort_values(['y', 'x'], ascending=[True, True])
        df_coords = df_coords.drop_duplicates(keep='first')
        
        if len(df_coords)== 342:
            df_coords = df_coords.assign(p=list(range(1,343)))
        else:
            raise ValueError('Please difine measurement area in a seperate column')
            #### In this Error as the MA is expected in column name, we should find a strategy for that!
            ### a solution is : if 'MA' in df.columns , However we never had MA in columns! 
            #### Right now only files containing all MA are accepted! so we may lose some files
            ### we didnt delete negative resistance in last function, can this make a trouble? 
        
        df_coords = df_coords[['p','x','y']]
        df_coords['coords']= df_coords[['x', 'y']].apply(tuple, axis=1)
        
        return df_coords
     
    def table_of_df(self):
        """
            In this function a dataframe generated by find_min_max_resistance_in_MA() will be read row by row 
            then the value of each cell of a row will be associated to its corresponding column name in a dictionary. 
            There for each row will have a dictionary. 
            All the dictionaries will be appended to a list (data_table). at the the jason file of DataTable will be returned. 
        """
       
        df_MA, df = self.find_min_max_resistance_in_MA()
        
        row_list = []
        data_table = []
        df_row_dict = {}
        cols = list(df.columns)
        if 'R_ave' in cols and 'R' in cols: 
            pos = cols.index('R')
            cols[pos] = 'R_median'
        for i in range(len(df)):
            for j,c in enumerate(cols):
                celll = list(df.iloc[i])[j] 

                ##### any method to take a cell value from a dataframe will return the value as numpy.float64 or numpy.int64 
                ##### numpy values cannot be used in dictionary in fastAPI , so I used item() method to just have normal int and float :<<<
                
                if type(celll) == np.float64 or type(celll) == np.int64 :
                    cell_val = celll.item()
                else:
                    cell_val = celll
                    
                #print(type(cell_val))
                #The below method for taking cell value didnt work for txt files but it is easier than iloc!
                #cell_val = df.at[i, c].item()
                
                df_row_dict[c] = cell_val
            data_table.append(df_row_dict)
            df_row_dict = {}
#               
        
        return {
                "DataTable":data_table
                }      

    def info_R_in_MA_for_database(self):
        
        same_dict = self.find_type_and_keyword()
        created_column_name = self.create_column_name()
        if len(same_dict)>1 and same_dict["keyword"] == 0 and len(created_column_name)>0 :
            column_name = created_column_name
        elif len(same_dict)>1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()
        else:
            raise ValueError('file is not valid') 
        
            
        df_measurment_type = self.file_devision() 
        df_min_max_in_MA, df= self.find_min_max_resistance_in_MA()
       
        
        
        if 'MA' in column_name:
            df_coords = df[['MA']]
            dict_coords =dict(zip(df_coords.MA, df_coords.MA))
            
        elif 'x' and 'y' in column_name and 'MA' not in column_name:
            df_coords = self.find_coordinate()
            dict_coords =dict(zip(df_coords.coords, df_coords.p))
        
        count = 0
        
        
        properties_MA_RT = []
        properties_overall_RT = []
        properties_MA_R_T =[]
        properties_overall_MA_R_T =[]
        
            
        
        if (
            df_measurment_type["type_R1_R2_R3"].iloc[0] == 1 or
            df_measurment_type["type_R"].iloc[0] == 1
            ):
            value_min_overall = df.agg({'R': ['min']}).iloc[0].R
            value_max_overall = df.agg({'R': ['max']}).iloc[0].R
            for i in df_min_max_in_MA.index:
                count+=1
                value_mean = df_min_max_in_MA.loc[i][("R","mean")]
                value_min = df_min_max_in_MA.loc[i][("R","min")]
                value_max = df_min_max_in_MA.loc[i][("R","max")]
              
                value_MA = dict_coords.get(i)

                

                properties_MA_RT.append({
                              "Predicate": {
                                "Properties": [
                                  {
                                    "Type": 2,  ### The value type is tuple how should type change?
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
                                  "Comment": f"Resistance (Room Temperature) for Measurement Area {value_MA}"
                                }
                              ]
                            })
                    
                ## last section - overall
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
                      "CompositionsForSampleUpdate":properties_MA_RT,
                      "DeletePreviousProperties": True,
                      "Properties": properties_overall_RT
                    }


        if (
            df_measurment_type["type_R_temperature_dependant"].iloc[0] == 1 or 
            df_measurment_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1
            ):
            value_min_overall = df.agg({'R': ['min']}).iloc[0].R
            value_max_overall = df.agg({'R': ['max']}).iloc[0].R
            T_value_min_overall = df[df['R']==value_min_overall].iloc[0].T_set
            T_value_max_overall = df[df['R']==value_max_overall].iloc[0].T_set
            T_min_overall = df.agg({'T_set': ['min']}).iloc[0].T_set
            T_max_overall = df.agg({'T_set': ['max']}).iloc[0].T_set
            for i in df_min_max_in_MA.index:
                count+=1
                value_mean = df_min_max_in_MA.loc[i][("R","mean")]
                value_min = df_min_max_in_MA.loc[i][("R","min")]
                value_max = df_min_max_in_MA.loc[i][("R","max")]
                
                T_min= df[df['R']==value_min].iloc[0].T_set
                T_max= df[df['R']==value_max].iloc[0].T_set
                value_MA = dict_coords.get(i)
                

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
                    
                ## last section - overall
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
            ### Strangly fastAPI doesnot accept T_min_overall directly like value_max_overall in resistance and raise ValueError!!!
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
                      "CompositionsForSampleUpdate":properties_MA_R_T,
                      "DeletePreviousProperties": True,
                      "Properties": properties_overall_MA_R_T
                    }



######## Finding max and min of resistance in each temperature
        
    def find_min_max_resistance(self):
        #file_path, file_extention = self.final_file_path()
        file_path = self.file_path
        file_extention = self.file_extention
        df_measurment_type = self.file_devision()
        skip_list, col_name = self.find_skiprows()
        same_dict = self.find_type_and_keyword()
        created_column_name = self.create_column_name()
        if len(same_dict)>1 and same_dict["keyword"] == 0 and len(created_column_name)>0 :
            column_name = created_column_name
        elif len(same_dict)>1 and same_dict["keyword"] > 0:
            column_name = self.find_column_name()
        else:
            return 'file is not valid' 
        data_ana = {} 
        if file_extention == ".csv":
            df=pd.read_csv(file_path,names=column_name,skiprows= skip_list,encoding= self.encoding_pd_read_csv)
        else:
            df = pd.DataFrame(columns= column_name)
            count_l = 0
            f = open(file_path, encoding=self.encoding_open_file)
            for line in f:
                col = line.split()
                if len(col)>1:
                    count_l +=1
                    df.loc[count_l] = col
                    
            for col_n in df.columns:
                df[col_n] = df[col_n].apply(lambda x: x.replace(',', '.'))
                
            try: 
                df= df.apply(pd.to_numeric)
            except ValueError:
                print(file_path,': contains non floatable columns')
        
        if df_measurment_type["type_R1_R2_R3"].iloc[0] == 1:
            # finding negative measurements! 
            df[ df['R1'] < 0 ]= None 
            df[ df['R2'] < 0 ]= None 
            df[ df['R3'] < 0 ]= None 
            # finding mean, min, max of the same condition
            df["R_ave"]= df[['R1','R2','R3']].mean(axis=1)
            df["R"]=df[['R1','R2','R3']].median(axis=1)
            df_R_detail = df.agg({'R': ['median','mean', 'min', 'max']})

            return df_R_detail
        
        if df_measurment_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1:
            # finding negative measurements! 
            df[ df['R1'] < 0 ]= None 
            df[ df['R2'] < 0 ]= None 
            df[ df['R3'] < 0 ]= None 
            # finding mean, min, max of the same condition
            df["R_ave"]= df[['R1','R2','R3']].mean(axis=1)
            # df[R] describes the median of R1,R2 and R3
            df["R"]=df[['R1','R2','R3']].median(axis=1)
            df_R_detail = df.groupby(["T_set"]).agg({'R': ['median','mean', 'min', 'max']})

            return df_R_detail
        
        if df_measurment_type["type_R_temperature_dependant"].iloc[0] == 1:
            # finding negative measurements! 
            df[ df['R'] < 0 ]= None 
            df_R_detail = df.groupby(["T_set"]).agg({'R': ['median','mean', 'min', 'max']})

            return df_R_detail
            
        if df_measurment_type["type_R"].iloc[0] == 1:
            # finding negative measurements! 
            df[ df['R'] < 0 ]= None 
            df_R_detail = df.agg({'R': ['median','mean','min','max']})

            return df_R_detail
        
    def info_for_database(self):
        df_measurment_type = self.file_devision() 
        df_min_max= self.find_min_max_resistance()
        count = 0
        properties_T_dependant = []
        if (
            df_measurment_type["type_R1_R2_R3"].iloc[0] == 1 or
            df_measurment_type["type_R"].iloc[0] == 1
            ):
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
                      "Properties": properties_RT
                    }
            
        if (
            df_measurment_type["type_R_temperature_dependant"].iloc[0] == 1 or 
            df_measurment_type["type_R1_R2_R3_temperature_dependant"].iloc[0] == 1
            ):
            for i in df_min_max.index:
                count+=1
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
                              "Value": df_min_max.loc[i][("R","min")],
                              "ValueEpsilon": None,
                              "SortCode": 20,
                              "Row": count,
                              "Comment": "Minimal Resistance"
                            })
                properties_T_dependant.append(
                            {
                              "Type": 1,
                              "Name": "Resistance",
                              "Value": df_min_max.loc[i][("R","max")],
                              "ValueEpsilon": None,
                              "SortCode": 30,
                              "Row": count,
                              "Comment": "Maximal Resistance"
                            })
            return {
                    "DeletePreviousProperties": True,
                    "Properties": properties_T_dependant
                    }
        
    def find_column_range(self):
        """
        After finding skiprows and column name the dataframe from a file can be generated precisely.
        by generating this DataFrame the min and max of each column can be easyly found by selecting that column. 
        """
        #file_path, file_extention = self.final_file_path()
        file_path = self.file_path
        file_extention = self.file_extention
        skip_list, col_name = self.find_skiprows()
        column_name = self.find_column_name() 
        data_ana = {} 
        if file_extention == ".csv":
            
            df=pd.read_csv(file_path,names=column_name,skiprows= skip_list,encoding= self.encoding_pd_read_csv)
        else:
            df = pd.DataFrame(columns= column_name)
            count_l = 0
            f = open(file_path, encoding=self.encoding_open_file)
            for line in f:
                col = line.split()
                if len(col)>1:
                    count_l +=1
                    df.loc[count_l] = col
            
        for cn in column_name:
            min_col = df[cn].min()
            max_col = df[cn].max()
            data_ana[cn]=[min_col, max_col]

        return data_ana                      
    