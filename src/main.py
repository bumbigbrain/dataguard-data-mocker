import mysql.connector
from datetime import datetime
import time
import random




def create_DATAGUARD_STATS_TABLE():
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
    return create_dataguard_stats_table



def randomTimeInterval():
    random_minute = random.randint(53, 59)
    random_second = random.randint(10, 50)
    
    return f"+00 00:{random_minute}:{random_second}"
    


def Mocker(cursor, conn):


    for i in range(10):    
        print("start generating") 
        head = "INSERT INTO V$DATAGUARD_STATS VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"    
        # random theseq 
        APPLY_FINISH_TIME_VALUE = randomTimeInterval()  
        APPLY_LAG = randomTimeInterval() 
        TRANSPORT_LAG =  randomTimeInterval()


        val = [
            (112, "DB01", "APPLY_FINISH_TIME", f"{APPLY_FINISH_TIME_VALUE}", "interval", f"{str(datetime.now())}", f"{str(datetime.now())}", 1),
            (112, "DB01", "APPLY_LAG", f"{APPLY_LAG}", "interval", f"{str(datetime.now())}", f"{str(datetime.now())}", 1),
            (112, "DB01", "TRANSPORT_LAG", f"{TRANSPORT_LAG}", "interval", f"{str(datetime.now())}", f"{str(datetime.now())}", 1)
        ]
            
        cursor.executemany(head, val)
        conn.commit()
        print("waiting for 1s")
        time.sleep(3)





def main():
    try:
        conn = mysql.connector.connect(
            user="root",
            password="1q2w3e4r",
            host="127.0.0.1",
            port=3306,
            database="dataguard"
        )

    except mysql.connector.Error as e:
        print(f"{e}")
    

    cur = conn.cursor()
    # nowTime = str(datetime.now())
    # print(nowTime, type(nowTime))
    
    
    Mocker(cur, conn)
    
    
    
    
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