# -*- coding: utf-8 -*-
import pyodbc
import os
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import sys
import socket

class Server:
    def __init__(self):
        self.__cnxn_for_insert = None
        self.__cursor_for_insert = None
        
              
    @property
    def cnxn_for_insert(self):
        if self.__cnxn_for_insert is None:
            print("cnxn_for_insert is none.\n")
            print("Finished.\n")
            sys.exit()
        else:
            return self.__cnxn_for_insert
    
    
    @cnxn_for_insert.setter
    def cnxn_for_insert(self, cn=None):
        if cn is None:
            print("cn_for_insert is none. cannot assign")
            print("Finished.\n")
            sys.exit()
        else:
            self.__cnxn_for_insert = cn
        
        
    @property
    def cursor_for_insert(self):
        if self.__cursor_for_insert is None:
            print("cnxn_for_crawling is none.\n")
            print("Finished.\n")
            sys.exit()
        else:
            return self.__cursor_for_insert
    
    
    @cursor_for_insert.setter
    def cursor_for_insert(self, cu=None):
        if cu is None:
            print("cu_for_insert is none. cannot assign")
            print("Finished.\n")
            sys.exit()
        else:
            self.__cursor_for_insert = cu
        
                      
    def run(self, port=4000):   
        self.__db_connection_for_insert()
        host = ''
        file_obj = open("temp_heat.txt", "w")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            s.listen(1)
            conn, addr = s.accept()
            print(addr, "\n")
            num = 1
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                file_obj.write(data.decode()+"\n")
                print("Received: ", data.decode())
                conn.sendall("OK! {0} : {1} : {2}".format(num, data.decode(), str(datetime.datetime.now())).encode())
                num += 1

            while True:
                data = conn.recv(1024)
                if not data:
                    break
                file_obj.write(data.decode()+"\n")
                print("Received: ", data.decode())
                conn.sendall("OK! {0} : {1} : {2}".format(num, data.decode(), str(datetime.datetime.now())).encode())
                num += 1
        print("done") 
        file_obj.close() 
        
        file_obj = open("temp_heat.txt", "r")
        data = file_obj.readlines()
        self.__insert_data(data)
        print("insert done")
        file_obj.close()
             
    def __insert_data(self, d_list = []):
        def under_control(cu):
            sql_q = "select name from dbo.FILE_For_HeatData"
            cu.execute(sql_q)
            return cu.fetchall()
        
        
        if d_list == "":
            print("Thers'no accurate data.\n")
            print("Finished.\n")
            sys.exit()
            
        cursor = self.cursor_for_insert
        conn = self.cnxn_for_insert
        under_control_list = under_control(cursor)
            
        if d_list[0].strip() not in under_control_list:
            sql_q = "insert into dbo.FILE_For_HeatData(name, created_date, last_update_date) values"
            sql_q += "('{0}', '{1}', '{2}')".format(d_list[0].strip(), str(datetime.datetime.now()), str(datetime.datetime.now()))
            cursor.execute(sql_q)
            conn.commit()
        
        d_list.pop(0)
        for data in d_list:
            row = data.strip().split("/")
            sql_q = "insert into dbo.Heat_consumption_Data(HCU_id, House_Name, name, Port4_Data, Port5_Data, YYYYMMDDHHMM) values("
            sql_q += "'{0}', '{1}', '{2}', {3}, {4}, '{5}'".format(row[0], row[1], row[2], int(row[3]), int(row[4]), row[5])
            sql_q += ")"
            cursor.execute(sql_q)
            sql_q = "update dbo.FILE_For_HeatData set last_update_date = '{0}' where name='{1}'".format(str(datetime.datetime.now()), row[2])
            cursor.execute(sql_q)
        conn.commit()
        
        
    def __db_connection_for_insert(self):
        def db_connect():
            server = None #It must be the server private ip address
            database = None #Database name
            username = None #User name 
            password = None #User password
            cnxn = pyodbc.connect('DRIVER={ODBC Driver 11 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            cursor = cnxn.cursor()
            return cnxn, cursor
        
        cn, cu = db_connect()
        self.cnxn_for_insert = cn
        self.cursor_for_insert = cu
        
        
if __name__ == "__main__":
    obj = Server()
#     obj.run()
    sched = BackgroundScheduler()
    sched.add_job(obj.run, 'interval', days=1, start_date="2018-11-29 23:35:00")
    sched.start()