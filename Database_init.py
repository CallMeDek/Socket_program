# -*- coding: utf-8 -*-
import pandas as pd
import pyodbc
import datetime
import sys
import os


class Insert_excel_dat:
    def __init__(self):
        self.__files = []
        self.__attri_list = []
        self.__attri_list_2 = []
        self.__value_list = []
        self.__cnxn = None
        self.__cursor = None
        self.__table_name = None
        
        
    @property
    def table_name(self):
        return self.__table_name
    
    
    @table_name.setter
    def table_name(self, name=None):
        self.__table_name = name
               
            
    @property
    def files(self):
        return self.__files
    
    
    @files.setter
    def files(self, file_list=[]):
        self.__files = file_list
        
        
    @property
    def attri_list(self):
        return self.__attri_list
    
    
    @attri_list.setter
    def attri_list(self, attri=[]):
        self.__attri_list = attri
        
        
    @property
    def attri_list_2(self):
        return self.__attri_list_2
    
    
    @attri_list_2.setter
    def attri_list_2(self, attri=[]):
        self.__attri_list_2 = attri
        
        
    @property
    def value_list(self):
        return self.__value_list
    
    
    @value_list.setter
    def value_list(self, values=[]):
        self.__value_list = values
    
    
    @property
    def cnxn(self):
        return self.__cnxn
    
    
    @cnxn.setter
    def cnxn(self, conn=None):
        self.__cnxn = conn
        
        
    @property
    def cursor(self):
        return self.__cursor
    
    
    @cursor.setter
    def cursor(self, cur=None):
        self.__cursor = cur
    
        
    def main(self):
        self.db_connection()
        
        cnxn = self.cnxn
        cursor = self.cursor
        self.get_files(cnxn, cursor)
        
        file_list = self.files
        for i, file_with_path in enumerate(file_list):
            self.load_data(file_with_path)
            
            attri_list = self.attri_list
            cnxn = self.cnxn
            cursor = self.cursor
            self.create_table(attri_list, cnxn, cursor)

            attri_list = self.attri_list_2
            value_list = self.value_list
            cnxn = self.cnxn
            cursor = self.cursor
            t_name = self.table_name
            self.save_data(attri_list, value_list, cnxn, cursor, file_with_path, t_name)
        print("finished.")

        
        
    def get_files(self, cn=None, cu=None):
        def check_empty(cn=None, cu=None):
            try:
                sql_q = "select top 1 * from dbo.FILE_For_HeatData"
                cu.execute(sql_q)
            except Exception:
                return True
            else:
                return False
            
        if(check_empty(cn, cu)):
            sql_q = "create table dbo.FILE_For_HeatData(name varchar(900) not null primary key, created_date varchar(max), last_update_date varchar(900))"
            cu.execute(sql_q)
            cn.commit()
            
            files = []
            cwd = os.getcwd()
            file_count = int(input("Enter the number of files: "))
            for i in range(file_count):
                file_name = input("Enter the file name: ")
                files.append(file_name)
                sql_q = "insert into dbo.FILE_For_HeatData(name, created_date, last_update_date) values"
                sql_q += "('{0}', '{1}', '{2}')".format(file_name, str(datetime.datetime.now()), str(datetime.datetime.now()))
                cu.execute(sql_q)
                cn.commit()
                
            self.files = files
        else:
            possession_list = []
            sql_q = "select * from dbo.FILE_For_HeatData"
            cu.execute(sql_q)
            row = cu.fetchone()
            while row:
                possession_list.append(row[0])
                row = cu.fetchone()
                
            files = []
            cwd = os.getcwd()
            file_count = int(input("Enter the number of files: "))
            for i in range(file_count):
                file_name = input("Enter the file name: ")
                files.append(file_name)   
                
            for i in range(len(files)):
                if files[i] in possession_list:
                    print("{0} is under the control".format(files[i]))
                    files[i] = ""
                
            r_files = []
            for file in files:
                if file != "":
                    r_files.append(file)
                
            if len(r_files) == 0:
                print("Thers'no accurate file.\n")
                print("Finished.\n")
                sys.exit()
            else:
                for file in r_files:
                    sql_q = "insert into dbo.FILE_For_HeatData(name, created_date, last_update_date) values"
                    sql_q += "('{0}', '{1}', '{2}')".format(file, str(datetime.datetime.now()), str(datetime.datetime.now()))
                    cu.execute(sql_q)
                    cn.commit()
                    
            self.files = r_files
        
        
    def load_data(self, file_with_path=""):
        def transform_attri_names_for_create(attri=[], values=[]):
            temp = []
            for i,row in enumerate(values[0]):
                if type(row) == str:
                    temp.append("{0} varchar(MAX)".format(attri[i]))
                elif type(row) == int:
                    temp.append("{0} int".format(attri[i]))
                else:
                    temp.append("{0} float".format(attri[i]))
            return temp
            
        df = pd.read_excel(file_with_path)
        df.to_csv("temp.csv")
        df = pd.read_csv("temp.csv")
        value_list = df.values.tolist()

        for i, row in enumerate(value_list):
            flag_for_last = False
            for j, ele in enumerate(row):
                if pd.isna(ele):
                    del value_list[i][:]
                    
        value_list = [row for row in value_list if row != []]

        for i, row in enumerate(value_list):
            for j, ele in enumerate(row):
                try:
                    value_list[i][j] = float(ele)
                except Exception:
                    value_list[i][j] = str(ele)      
        self.value_list = value_list
        
        attri_list_n = len(value_list[0])
        attri_list = []
        for i in range(attri_list_n):
            col_name = input("Insert a column name: ")
            attri_list.append(col_name)
        self.attri_list = attri_list
        
        attri_list_origin = []
        for row in attri_list:
            attri_list_origin.append(row)
        self.attri_list_2 = attri_list_origin
        
        self.attri_list = transform_attri_names_for_create(attri_list, value_list)
       
    
    def save_data(self, attri=[], values=[], cn=None, cu=None, file_n=None, table_name=None):
        sql_q = """insert into dbo.{0}({1}, {2},""".format(table_name, 'name', 'date')
        for i, row in enumerate(attri):
            if i == len(attri)-1:
                sql_q += "{0}) ".format(row)
            else:
                sql_q += "{0}, ".format(row)

        sql_q += "values("
        sql_qu = sql_q
        for i, row in enumerate(values):
            values[i].insert(0, str(datetime.datetime.now()))
            values[i].insert(0, file_n)
        
        for i,row in enumerate(values):
            sql_q = sql_qu
            for j, ele in enumerate(row):
                if j == len(row) - 1:
                    if type(ele) == str:
                        sql_q += "'{0}')".format(ele)
                    else:
                        sql_q += str(ele) + ") "
                else:
                    if type(ele) == str:
                        sql_q += "'{0}', ".format(ele)
                    else:
                        sql_q += str(ele) + ", "
            try:
                cu.execute(sql_q)
                cn.commit()        
                sql_q = "update dbo.FILE_For_HeatData set last_update_date = '{0}' from dbo.FILE_For_HeatData where dbo.FILE_For_HeatData.name='{1}'".format(str(datetime.datetime.now()), file_n)
                cu.execute(sql_q)
                cn.commit()
            except Exception:
                continue
        
            
            
    def db_connection(self):
        def db_connect():
            server = None #It must be a server public ip address
            database = None # Database name
            username = None # User name
            password = None # User password
            cnxn = pyodbc.connect('DRIVER={ODBC Driver 11 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            cursor = cnxn.cursor()
            return cnxn, cursor
        
        cn, cu = db_connect()
        self.cnxn = cn
        self.cursor = cu
        
        
    def create_table(self, attri=[], cn=None, cu=None):
        def check_empty(cn=None, cu=None, table_name=""):
            try:
                sql_q = "select top 1 * from dbo.{}".format(table_name)
                cu.execute(sql_q)
            except Exception:
                return True
            else:
                return False
            
        while(True):
            print("\n------------------\n")
            table_name = input("Insert your table name: ")
            print("\n------------------\n")
            if(check_empty(cn, cu, table_name)):
                self.table_name = table_name
                sql_q = """create table dbo.{0}({1} varchar(900) FOREIGN KEY REFERENCES FILE_For_HeatData(name)""".format(table_name , "name")
                sql_q += ", date varchar(900)"
                for row in attri:
                    sql_q += ", {0}".format(row)
                sql_q += ")"
                cu.execute(sql_q)
                cn.commit()
                break
            else:
                print("\n-------- Warning: The table name has already used. --------\n")
                print("\n-------- Please Insert another name \n")
                
        
if __name__ == "__main__":
    obj = Insert_excel_dat()
    obj.main()
    
"""
Example:
Enter the number of files: 4
Enter the file name: 사용량 종합.xlsx
Enter the file name: 사용량종합현황.xls
Enter the file name: 아산에너지.xlsx
Enter the file name: 사용량 종합2.xlsx
Insert a column name: a
Insert a column name: b
Insert a column name: c
Insert a column name: d
Insert a column name: e
Insert a column name: f
Insert a column name: g
Insert a column name: h

------------------

Insert your table name: temp

------------------

Insert a column name: a
Insert a column name: b
Insert a column name: c
Insert a column name: d
Insert a column name: e
Insert a column name: f
Insert a column name: g
Insert a column name: h

------------------

Insert your table name: temp2

------------------

Insert a column name: a
Insert a column name: b
Insert a column name: c
Insert a column name: d

------------------

Insert your table name: temp3

------------------

Insert a column name: a
Insert a column name: b
Insert a column name: c
Insert a column name: d
Insert a column name: e
Insert a column name: f
Insert a column name: g
Insert a column name: h

------------------

Insert your table name: temp4

------------------

finished.

"""