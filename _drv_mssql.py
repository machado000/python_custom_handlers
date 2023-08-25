#!./venv/bin/python3
# ------------------------------------------------------------------------------------------
# This driver module is part of an ETL project (extract, transform, load).
# Can be imported by main.py and used to load dataframes to a MSSQL database
# v.20230620
# ------------------------------------------------------------------------------------------

import numpy as np
import pandas as pd
import pymssql
import _drv_hashicorp_vault

from datetime import date, datetime  # noqa


def test():
    print('INFO  - Auto-teste do modulo')
    data = datetime.now()

    sample_dict = {
        'Id': [1, 2, 3, 4, 5],
        'data': pd.to_datetime([data, data, data, data, data]),
        'status': [6, 7, 8, 9, 0],
        'origem': ['Text 1', None, 'Text 3', 'Text 4', 'Text 5'],
        'mensagem': ['Text 6', 'Text 7', None, 'Text 9', 'Text 10'],
    }
    sample_df = pd.DataFrame(sample_dict)

    database_name = "mistral_bi"
    handler = MSSQLHandler(database_name)

    handler.connect()

    handler.table_exists("OLAP_SAMPLE")
    handler.table_exists("LOGS")

    # handler.create_table_from_df("OLAP_SAMPLE", sample_df)

    # handler.drop_table_contents("OLAP_SAMPLE")
    handler.append_df_bulk("LOGS", sample_df)

    # handler.drop_table_contents("OLAP_SAMPLE")
    # handler.delete_table("OLAP_SAMPLE")


class MSSQLHandler:
    # ------------------------------------------------------------------------------------------
    # Class to hold MySQL connect and CRUD operations
    # ------------------------------------------------------------------------------------------
    def __init__(self, database_name):

        secret = 'holos_adm@mistral-bi-server.database.windows.net'
        status, metadata = _drv_hashicorp_vault.get_secret(secret)
        if status is True:
            self.server = metadata['server']
            self.port = metadata['port']
            self.user = metadata['username']
            self.password = metadata['password']
            self.database = database_name
            self.conn_status = False

            print(f"\nINFO  - Successful Hashi-Vault API request for secret {secret}")

        else:
            print(f"\nERROR  - Invalid Hashi-Vault API request for secret {secret}")
            raise Exception

    def connect(self):
        # ------------------------------------------------------------------------------------------
        # Initiate connection handler
        # ------------------------------------------------------------------------------------------
        try:
            self.connection = pymssql.connect(
                server=self.server.replace("tcp:", ""),
                user=self.user,
                port=self.port,
                password=self.password,
                database=self.database,
            )

            print(f"INFO  - Connected to database {self.database}")
            return True

        except pymssql.Error as e:
            print(f"\nERROR  - Failure connecting to MSSQL: {e}")
            return False

    def execute_query(self, query):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"USE {self.database};")
                cursor.execute(query)
                self.connection.commit()

        except pymssql.Error as e:
            print(f"ERROR  - Failed to execute the query. Error: {e}")
            raise Exception

    def table_exists(self, table_name):
        print(f"INFO  - Checking if table '{table_name}' exists")

        query = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0;"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()

            if result[0] == 1:
                print(f"INFO  - True -- table '{table_name}' exists.")
            else:
                print(f"INFO  - False -- table '{table_name}' does not exist.")

            return bool(result)

        except pymssql.Error as e:
            print(f"\nERROR  - Failure checking if table exists: {e}")
            return False

    def create_table_from_df(self, table_name, dataframe):
        print(f"INFO  - Creating table '{table_name}'")

        try:
            create_query = f"CREATE TABLE {table_name} ("
            columns = []
            for column in dataframe.columns:
                column_type = self._get_column_type(dataframe[column])
                columns.append(f"{column} {column_type}")
            columns.append("insert_time DATETIME")
            create_query += ", ".join(columns)
            create_query += ");"
            self.execute_query(create_query)

            print(f"INFO  - Success creating table '{table_name}'.")
            return True

        except pymssql.Error as e:
            print(f"\nERROR  - Failure creating table: {e}")
            return False

    def append_df_bulk(self, table_name, dataframe, batch_size=10000):
        print(f"INFO  - Appending dataframe to table '{table_name}' in bulk")

        # Transform the dataframe to match table
        # dataframe['insert_time'] = datetime.now()
        dataframe = dataframe.replace(np.nan, None)

        dataframe_columns = ", ".join(dataframe.columns)
        rows = [tuple(row) for row in dataframe.values.tolist()]
        placeholders = ", ".join(["%s"] * len(dataframe.columns))
        insert_query = f"INSERT INTO {table_name} ({dataframe_columns}) VALUES ({placeholders});"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"USE {self.database};")

                # Break the rows into chunks of commit_interval
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i:i + batch_size]
                    cursor.executemany(insert_query, chunk)
                    self.connection.commit()
                    print(f"Inserted {len(chunk)} rows into the table.")
                    print(f"INFO  - ... Batch {i+1}/{len(range(0, len(rows), batch_size))} commited")

            print("INFO  - Success appending dataframe to table.")
            return True

        except pymssql.Error as e:
            print(f"\nERROR  - Failure appending dataframe to table: {e}")
            return False

    def drop_table_contents(self, table_name):
        print(f"INFO  - Dropping contents of table '{table_name}'")

        try:
            query = f"DELETE FROM `{table_name}`"
            self.execute_query(query)

            print("INFO  - Success dropping table contents.")
            return True

        except pymssql.Error as e:
            print(f"\nERROR  - Failure dropping table contents: {e}")
            return False

    def delete_table(self, table_name):
        print(f"INFO  - Deleting table '{table_name}'")

        try:
            query = f"DROP TABLE IF EXISTS`{table_name}`"
            self.execute_query(query)

            print("INFO  - Success deleting table.")
            return True

        except pymssql.Error as e:
            print(f"\nERROR  - Failure deleting table: {e}")
            return False


if __name__ == '__main__':
    test()
