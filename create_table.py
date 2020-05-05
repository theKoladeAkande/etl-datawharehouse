import psycopg2
import config
from connection_manger import dbconnection_manager, psycopg2_error_handler
import sql_queries
from cluster_manager import (create_ec2_resource, create_iam_access, create_s3_resource, launch_redshift_cluster)

@psycopg2_error_handler
def data_definition(query: str, cursor, execute_many=False, data=None):
    """
    Executes query to create or drop table
    :param query: string containing query
    :param cursor: cursor to the database
    :param execute_many: option for multiple queries execution
    :data: An iterable(preferably a list) of all data
    """
    if execute_many:
        cursor.executemany(query, data)
    else:
        cursor.execute(query)

    if "DROP" in query:
        print('Table dropped  succesfuly')
    elif "CREATE" in query:
        print("Table created successfuly")

def resource_creator():
    create_ec2_resource()
    s3 = create_s3_resource()
    create_iam_access()
    launch_redshift_cluster()

if __name__ == "__main__":

    resource_creator()

    with dbconnection_manager(dbname=config.DWH_DB, user=config.DWH_DB_USER,
                              password=config.DWH_DB_PASSWORD, host=config.DWH_ENDPOINT,
                              port=config.DWH_PORT) as (cursor, conn):
        for query in sql_queries.DROP_QUERIES:
                data_definition(query, cursor)

    with dbconnection_manager(dbname=config.DWH_DB, user=config.DWH_DB_USER,
                              password=config.DWH_DB_PASSWORD, host=config.DWH_ENDPOINT,
                              port=config.DWH_PORT) as (cursor, conn):
        for query in sql_queries.CREATE_QUERIES:
            data_definition(query, cursor)