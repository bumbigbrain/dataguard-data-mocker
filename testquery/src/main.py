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
    apply_finish_time_query = "SELECT VALUE FROM V$DATAGUARD_STATS WHERE NAME='APPLY_FINISH_TIME' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(apply_finish_time_query)
    for item in cursor:
        print(item)



    print("") 
    print("LATEST APPLY LAG QUERY")
    latest_apply_lag_query = "SELECT VALUE FROM V$DATAGUARD_STATS WHERE NAME='APPLY_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_apply_lag_query)
    for item in cursor:
        print(item)


    print("")    
    print("LATEST TRANSPORT LAG QUERY")
    latest_transport_lag_query = "SELECT VALUE FROM V$DATAGUARD_STATS WHERE NAME='TRANSPORT_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_transport_lag_query)
    for item in cursor:
        print(item)




    print("")    
    print("LATEST APPLY FINISH TIME (TO MILLISECOND) QUERY")
    apply_finish_time_query = "SELECT (CAST(SUBSTRING(VALUE, 5, 2) AS INT) * 3600000) + (CAST(SUBSTRING(VALUE, 8, 2) AS INT) * 60000) + (CAST(SUBSTRING(VALUE, 11, 2) AS INT) * 1000) AS milliseconds FROM V$DATAGUARD_STATS WHERE NAME='APPLY_FINISH_TIME' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(apply_finish_time_query)
    for item in cursor:
        print(item[0])
        print(type(item[0]))



    print("") 
    print("LATEST APPLY LAG QUERY")
    latest_apply_lag_query = "SELECT (CAST(SUBSTRING(VALUE, 5, 2) AS INT) * 3600000) + (CAST(SUBSTRING(VALUE, 8, 2) AS INT) * 60000) + (CAST(SUBSTRING(VALUE, 11, 2) AS INT) * 1000) AS milliseconds FROM V$DATAGUARD_STATS WHERE NAME='APPLY_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_apply_lag_query)
    for item in cursor:
        print(item)


    print("")    
    print("LATEST TRANSPORT LAG QUERY")
    latest_transport_lag_query = "SELECT (CAST(SUBSTRING(VALUE, 5, 2) AS INT) * 3600000) + (CAST(SUBSTRING(VALUE, 8, 2) AS INT) * 60000) + (CAST(SUBSTRING(VALUE, 11, 2) AS INT) * 1000) AS milliseconds FROM V$DATAGUARD_STATS WHERE NAME='TRANSPORT_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_transport_lag_query)
    for item in cursor:
        print(item)



    print("")    
    print("QUERY DICT")
    latest_transport_lag_query = "SELECT * FROM V$DATAGUARD_STATS WHERE NAME='TRANSPORT_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_transport_lag_query)
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]  # Get column names

    # Convert tuples to dictionaries (Method 1)
    for row in results:
        data_dict = dict(zip(column_names, row))
        result = data_dict    
    print(result)
    



if __name__ == "__main__":
    main()
