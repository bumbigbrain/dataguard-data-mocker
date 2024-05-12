import mysql.connector
import datetime
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


    def create_DATAGUARD_STATUS_TABLE(self):
        create_dataguard_status_table = """
        CREATE TABLE V$DATAGUARD_STATUS (
            FACILITY VARCHAR (24),
            SEVERITY VARCHAR (13),
            DEST_ID INT,
            MESSAGE_NUM INT,
            ERROR_CODE INT,
            CALLOUT VARCHAR (3),
            TIMESTAMP DATE,
            MESSAGE VARCHAR (256),
            CON_ID INT
        ) """
        self.cursor.execute(create_dataguard_status_table)



    def randomTimeStamp():
        # 05/10/2024 16:59:56  
        
        random_day = random.randint(10, 11)
        random_minute = random.randint(50, 59)
        random_second = random.randint(10,50)
        return f"05/{random_day}/2024 16:{random_minute}:{random_second}"
        


    def randomTimeInterval():
        random_minute = random.randint(53, 59)
        random_second = random.randint(10, 50)
        
        return f"+00 00:{random_minute}:{random_second}"


    def randomFacility():
        facility = ["Crash Recovery", "Log Transport Services", "Log Apply Services", "Role Management Services", "Remote File Server", "Fetch Archive Log", "Data Guard", "Network Services"]
        return facility[random.randint(0, len(facility)-1)] 
        
    def randomSeverity():
        severity = ["Information", "Warning", "Error", "Fatal", "Control"]
        return severity[random.randint(0, len(severity)-1)]
    

    def randomMessage():
        messages = ["eat pizza", "running", "swimming", "cat", "dog"]
        return messages[random.randint(0, len(messages)-1)]

    def generateVDATAGUARD_STATS(self):


        while True:    
            print("start generating") 
            head = "INSERT INTO V$DATAGUARD_STATS VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"    
            # random theseq 
            APPLY_FINISH_TIME_VALUE = Mocker.randomTimeInterval()  
            APPLY_LAG = Mocker.randomTimeInterval() 
            TRANSPORT_LAG =  Mocker.randomTimeInterval()
            TIMESTAMP = Mocker.randomTimeStamp()


            val = [
                (112, "DB01", "APPLY_FINISH_TIME", f"{APPLY_FINISH_TIME_VALUE}", "interval", TIMESTAMP, TIMESTAMP, 1),
                (112, "DB01", "APPLY_LAG", f"{APPLY_LAG}", "interval", TIMESTAMP, TIMESTAMP, 1),
                (112, "DB01", "TRANSPORT_LAG", f"{TRANSPORT_LAG}", "interval", TIMESTAMP, TIMESTAMP, 1)
            ]
                
            self.cursor.executemany(head, val)
            self.conn.commit()
            print("waiting for 1s")
            time.sleep(1)
    
    def generateVDATAGUARD_STATUS(self):
        
        while True:
            print("start generating")

            FACILITY = Mocker.randomFacility()
            SEVERITY = Mocker.randomSeverity()
            DEST_ID = 1
            MESSAGE_NUM = 1
            ERROR_CODE = 0
            CALLOUT = "co"
            TIMESTAMP = str(datetime.now())
            MESSAGE = Mocker.randomMessage()
            CON_ID = 0

            print(TIMESTAMP) 


            head = "INSERT INTO V$DATAGUARD_STATUS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"    
            val = [
                (FACILITY, SEVERITY, DEST_ID, MESSAGE_NUM, ERROR_CODE, CALLOUT, TIMESTAMP, MESSAGE, CON_ID)
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
    # mocker.create_DATAGUARD_STATUS_TABLE()
    # mocker.generateVDATAGUARD_STATUS()
    # today = datetime.date.today()
    # delta_time = datetime.timedelta(days=1)
    # print(today)
    # print(today-delta_time)

    
    
    
if __name__ == "__main__":
    main()




"""

V$DATAGUARD_STATS

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




"""


V$DATAGUARD_STATUS

FACILITY : [random(
    CrashRecovery, 
    LogTransportService, 
    LogApplyService, 
    RoleManagementService, 
    RemoteFileServer, 
    FetchArchiveLog, 
    DataGuard, 
    NetworkServices
    )]

SEVERITY : [random(
    InformationMessage,
    WarningMessage,
    ErrorMessage,
    Fatal,
    Control
    )]

DEST_ID : [const]

MESSAGE_NUM : [const]

ERROR_CODE : [const]

CALLOUT : [const]

TIMESTAMP : [str(datetime.now)]

MESSAGE : [random(string)]

CON_ID : [const]


"""
