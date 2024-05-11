import mysql.connector
from datetime import datetime
import time
import random




class Mocker():
    def __init__(self, user, host, port, database, password):
        self.conn = Mocker.initConnection(user, host, port, database, password)
        self.cursor = self.conn.cursor()

    def initConnection(user, host, port, database, password):
        conn = mysql.connector.connect(
            user=user,
            host=host,
            password=password,
            port=port,
            database=database
        )
        return conn
         
    

    def create_DATAGUARD_STATS_TABLE(self):
        create_dataguard_stats_table = """
        CREATE TABLE V$DATAGUARD_STATS (
            SOURCE_DBID INTEGER,
            SOURCE_DB_UNIQUE_NAME VARCHAR (32),
            NAME VARCHAR (32),
            VALUE VARCHAR (64),
            UNIT VARCHAR (30),
            TIME_COMPUTED VARCHAR (30),
            DATUM_TIME VARCHAR (30),
            CON_ID INTEGER
        ) """
        self.cursor.execute(create_dataguard_stats_table)



    def randomTimeInterval():
        random_minute = random.randint(53, 59)
        random_second = random.randint(10, 50)
        
        return f"+00 00:{random_minute}:{random_second}"
        


    def generateVDATAGUARD_STATS(self):


        while True:    
            print("start generating") 
            head = "INSERT INTO V$DATAGUARD_STATS VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"    
            # random theseq 
            APPLY_FINISH_TIME_VALUE = Mocker.randomTimeInterval()  
            APPLY_LAG = Mocker.randomTimeInterval() 
            TRANSPORT_LAG =  Mocker.randomTimeInterval()


            val = [
                (112, "DB01", "APPLY_FINISH_TIME", f"{APPLY_FINISH_TIME_VALUE}", "interval", f"{str(datetime.now())}", f"{str(datetime.now())}", 1),
                (112, "DB01", "APPLY_LAG", f"{APPLY_LAG}", "interval", f"{str(datetime.now())}", f"{str(datetime.now())}", 1),
                (112, "DB01", "TRANSPORT_LAG", f"{TRANSPORT_LAG}", "interval", f"{str(datetime.now())}", f"{str(datetime.now())}", 1)
            ]
                
            self.cursor.executemany(head, val)
            self.conn.commit()
            print("waiting for 1s")
            time.sleep(1)










def main():

    try:
        mocker = Mocker(
            user="root",
            host="127.0.0.1",
            password="1q2w3e4r",
            port=3306,
            database="dataguard"

        )
    except:
        print("error")
    
    
    mocker.generateVDATAGUARD_STATS() 
    
    
    
if __name__ == "__main__":
    main()




"""


SOURCE_DBID = const
SOURCE_DB_UNIQUE_NAME = const
NAME = APPLY FINISH TIME
UNIT = time 
VALUE = [random]
TIME_COMPUTED = [x^]
DATUM_TIME = TIME_COMPUTED
CON_ID = nvm


jOURCE_DBID = const
SOURCE_DB_UNIQUE_NAME = const
NAME = APPLY LAG
UNIT = time 
VALUE = [random]
TIME_COMPUTED = [x^]
DATUM_TIME = TIME_COMPUTED
CON_ID = nvm



SOURCE_DBID = const
SOURCE_DB_UNIQUE_NAME = const
NAME = TRANSPORT LAG
UNIT = time 
VALUE = [random]
TIME_COMPUTED = [x^]
DATUM_TIME = TIME_COMPUTED
CON_ID = nvm


"""