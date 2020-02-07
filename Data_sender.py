# -*- coding: utf-8 -*-
import pypyodbc
import os
from os import listdir
from os.path import isfile
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import sys
import socket

class DataSender:
    def __init__(self):
        self.__cnxn_for_crawling = None
        self.__cursor_for_crawling = None
        self.__file = None
        self.__data = None
        self.__state = 0
        
        
    @property
    def cnxn_for_crawling(self):
        if self.__cnxn_for_crawling is None:
            print("cnxn_for_crawling is none.\n")
            print("Finished.\n")
            sys.exit()
        else:
            return self.__cnxn_for_crawling
    
    
    @cnxn_for_crawling.setter
    def cnxn_for_crawling(self, cn=None):
        if cn is None:
            print("cn_for_crawling is none. cannot assign")
            print("Finished.\n")
            sys.exit()
        else:
            self.__cnxn_for_crawling = cn
        
        
    @property
    def cursor_for_crawling(self):
        if self.__cursor_for_crawling is None:
            print("cursor_for_crawling is none.\n")
            print("Finished.\n")
            sys.exit()
        else:
            return self.__cursor_for_crawling
    
    
    @cursor_for_crawling.setter
    def cursor_for_crawling(self, cu=None):
        if cu is None:
            print("cu_for_crawling is none. cannot assign")
            print("Finished.\n")
            sys.exit()
        else:
            self.__cursor_for_crawling = cu
        
        
    @property
    def file(self):
        if self.__file is None:
            print("file is none.\n")
            print("Finished.\n")
            sys.exit()
        else:
            return self.__file
    
    
    @file.setter
    def file(self, f=None):
        if f is None:
            print("f is none. cannot assign")
            print("Finished.\n")
            sys.exit()
        else:
            self.__file = f
            
            
    @property
    def data(self):
        if self.__data is None:
            print("data_list is empty.\n")
            print("Finished.\n")
            sys.exit()
        else:
            return self.__data
    
    
    @data.setter
    def data(self, d_list=[]):
        if len(d_list) == 0:
            print("d_list is empty. cannot assign")
            print("Finished.\n")
            sys.exit()
        else:
            self.__data = d_list
            
            
    @property
    def state(self):
        return self.__state
    
    
    @state.setter
    def state(self, s=0):
        self.__state = s
    
        
    def run(self, port=4000):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            ip = None #It must be the server public ip address
            s.connect((ip, port))
            self.__check_and_insert()
            state = self.state
            while True:
                if (state):
                    line = self.file
                    state = 0
                else:
                    line = ''
                s.sendall(line.encode())
                if not line:
                    break
                data = s.recv(1024)
                print(data.decode(), "\n")
    
            file = self.file
            self.__crawaling_data(file)
            
            data = self.data
            idx = 0
            for row in data:
                if type(row) is list:
                    temp_row = [str(x) for x in row]
                    line = "/".join(temp_row)
                    s.sendall(line.encode())
                    msg = s.recv(1024)
                    print(msg.decode(), "\n")
                else:
                    continue                       
        print("done")
        
    
    def __check_and_insert(self):             
        files = [f for f in listdir(os.getcwd()) if isfile(f) and 'mdb' in f]

        """file_n = "{0}년{1}월.mdb".format(str(datetime.datetime.now()).split()[0].split("-")[0], str(datetime.datetime.now()).split()[0].split("-")[1])
        
        flag = 1
        for file in files:
            if file == file_n:
                if file not in under_control_files:
                    self.state = 1
                self.file = file   
                flag = 0"""

        self.state = 1
        self.file = "2018년10월.mdb"
        flag = 0
        
        if(flag):
            print("Thers'no accurate file.\n")
            print("Finished.\n")
            sys.exit()
         
         
    def __crawaling_data(self, f=""):
        def db_connect_for_crawaling(mdb_name=""):
            pypyodbc.lowercase = False
            conn = pypyodbc.connect(
                r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
                "Dbq=.\\{0};".format(mdb_name))
            cur = conn.cursor()
            return conn, cur
        
        if f == "":
            print("Thers'no accurate file.\n")
            print("Finished.\n")
            sys.exit()
        
        self.cnxn_for_crawling, self.cursor_for_crawling = db_connect_for_crawaling(f)
        
        day = str(datetime.datetime.now()).split()[0].split("-")[2]
        sql_q = "select HCU_id, House_Name, Port4_Data, Port5_Data from {0}".format(day)
        
        cnxn = self.cnxn_for_crawling
        cursor = self.cursor_for_crawling
        
        data_list = []
        cursor.execute(sql_q)
        row = cursor.fetchone()
        while row:
            data_list.append(list(row))
            row = cursor.fetchone()
        
        for i in range(len(data_list)):
            data_list[i].insert(2, f)
            data_list[i].append(str(datetime.datetime.now()))
            data_list[i].append(day)
        self.data = data_list
        
                
if __name__ == "__main__":
    obj = DataSender()
    obj.run()
    #sched = BackgroundScheduler()
    #sched.add_job(obj.run, 'interval', days=1, start_date="2018-11-29 23:40:00")
    #sched.start()