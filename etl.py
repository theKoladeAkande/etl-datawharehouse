from connection_manger import psycopg2_error_handler, dbconnection_manager
import sql_queries
import config
@psycopg2_error_handler
def load_table(cursor, connection):
    """
    Load data from files stored in S3 to the staging
    tables using the queries written in the sql_queries script
    """
    print('Inserting logs data from json table')
    for query in sql_queries.COPY_TABLE_QUERIES:
        cursor.execute(query)
        connection.commit()

@psycopg2_error_handler
def insert_table(cursor, connection):
    """
    Extract and transform data from staging tables into the
    dimensional tables using the queries written in
    the sql_queries script
    """
    print('Transforming and  loading data from staging tables into analytics tables')
    for query in sql_queries.INSERT_TABLE_QUERUES:
        print('inserting:' +query)
        cursor.execute(query)
        conn.commit()


if __name__ == "__main__":

    with dbconnection_manager(dbname=config.DWH_DB, user=config.DWH_DB_USER,
                              password=config.DWH_DB_PASSWORD, host=config.DWH_ENDPOINT,
                              port=config.DWH_PORT) as (cursor, conn):
                load_table(cursor, conn)
                insert_table(cursor, conn)