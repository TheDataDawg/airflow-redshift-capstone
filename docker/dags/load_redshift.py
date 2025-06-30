import pandas as pd
import psycopg2

def load_to_redshift(
    redshift_host,
    redshift_port,
    redshift_user,
    redshift_password,
    redshift_db,
    table_name,
    csv_path
):
    conn = psycopg2.connect(
        host=redshift_host,
        port=redshift_port,
        user=redshift_user,
        password=redshift_password,
        dbname=redshift_db
    )
    cursor = conn.cursor()

    # Only load the 8 relevant columns (updated)
    cols_to_use = ['YEAR', 'QUARTER', 'ORIGIN', 'DEST', 'REPORTING_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'MARKET_DISTANCE']
    df = pd.read_csv(csv_path, usecols=cols_to_use)

    insert_query = f"""
        INSERT INTO {table_name} (
            year, quarter, origin, dest, reporting_carrier, passengers, market_fare, market_distance
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Convert DataFrame to list of tuples
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # Execute in batches
    for i in range(0, len(data), 1000):
        cursor.executemany(insert_query, data[i:i+1000])

    conn.commit()
    cursor.close()
    conn.close()