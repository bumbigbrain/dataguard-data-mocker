import mysql.connector






def main():

    conn = mysql.connector.connect(
        user = "root",
        password = "1q2w3e4r",
        host = "127.0.0.1",
        port = "3306",
        database = "dataguard"
    )

    cursor = conn.cursor()

    print("ALL RECORD DESC ORDERD QUERY")
    all_record_desc_ordered_query = "SELECT * FROM V$DATAGUARD_STATS ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC;"
    cursor.execute(all_record_desc_ordered_query)
    for item in cursor:
        print(item)



    print("")    
    print("LATEST APPLY FINISH TIME QUERY")
    apply_finish_time_query = "SELECT * FROM V$DATAGUARD_STATS WHERE NAME='APPLY_FINISH_TIME' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(apply_finish_time_query)
    for item in cursor:
        print(item)



    print("") 
    print("LATEST APPLY LAG QUERY")
    latest_apply_lag_query = "SELECT * FROM V$DATAGUARD_STATS WHERE NAME='APPLY_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_apply_lag_query)
    for item in cursor:
        print(item)


    print("")    
    print("LATEST TRANSPORT LAG QUERY")
    latest_transport_lag_query = "SELECT * FROM V$DATAGUARD_STATS WHERE NAME='TRANSPORT_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_transport_lag_query)
    for item in cursor:
        print(item)



if __name__ == "__main__":
    main()
