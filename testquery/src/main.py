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


    latest_transport_lag_query = "SELECT * FROM V$DATAGUARD_STATS WHERE NAME='TRANSPORT_LAG' ORDER BY CAST(TIME_COMPUTED AS DATETIME) DESC LIMIT 1;" 
    cursor.execute(latest_transport_lag_query)
    for item in cursor:
        for att in item:
            print(att)

if __name__ == "__main__":
    main()
