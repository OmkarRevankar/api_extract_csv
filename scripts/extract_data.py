import psycopg2
import os
import traceback
import logging
import pandas as pd
import urllib.request
from scripts.config_reader import config_reader
from scripts.postgre_conn import postgre_conn

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


class employee_details:

    def __init__(self):
        self.config_reader = config_reader()
        self.config = self.config_reader.get_config_pipeline()
        self.dest_folder = self.config.get('CONFIG_DETAIL', 'dest_folder')
        self.url = self.config.get('CONFIG_DETAIL', 'url')
        self.input_file = self.dest_folder + '\\Employee.csv'

    def get_data_from_github(self, url: str, dest_folder: str, input_file: str) -> bool:
        if not os.path.exists(str(dest_folder)):
            os.makedirs(str(dest_folder))
        try:
            urllib.request.urlretrieve(url, input_file)
            logging.info('Employee.csv file fetched from github successfully')
            return True
        except Exception as e:
            logging.error(f'Error while downloading the csv file due to: {e}')
            traceback.print_exc()
        return False

    def create_postgres_table(self) -> bool:
        conn, cur = postgre_conn().get_postgre_connection()
        try:
            cur.execute("""CREATE TABLE IF NOT EXISTS employee_details (Id INTEGER PRIMARY KEY,
            Name VARCHAR(20),Company VARCHAR(20),City VARCHAR(20),Country VARCHAR(20),Salary INTEGER)""")
            logging.info('Employee details created successfully')
            cur.execute("""CREATE TABLE IF NOT EXISTS employee_details_country (Country VARCHAR(20),Sum_Salary 
            INTEGER)""")
            logging.info('Employee_details_country details created successfully')
            conn.commit()
            cur.close()
            conn.close()
            return True
        except:
            logging.warning('Check if the tables exists')
        conn.commit()
        cur.close()
        conn.close()
        return False

    def read_input_csv_data(self, input_file: str):
        df = pd.read_csv(input_file)
        return df

    def write_to_employee_details_country(self):
        conn, cur = postgre_conn().get_postgre_connection()
        df = self.read_input_csv_data(self.input_file)
        df = df.groupby(['Country'])['Salary'].sum().reset_index()
        query = """INSERT INTO employee_details_country (Country,Sum_Salary) VALUES (%s,%s)"""
        row_count = 0
        for _, row in df.iterrows():
            values = (str(row[0]), int(row[1]))
            cur.execute(query, values)
            row_count += 1
            #logging.info(f'{row_count} rows from csv file inserted into employee_details_country table successfully')
        conn.commit()
        cur.close
        conn.close()

    def write_to_employee_details(self):
        conn, cur = postgre_conn().get_postgre_connection()
        df = self.read_input_csv_data(self.input_file)
        query = """INSERT INTO employee_details (Id,Name,Company,City,Country,Salary) VALUES (%s,%s,%s,%s,%s,%s)"""
        row_count = 0
        for _, row in df.iterrows():
            values = (int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), int(row[5]))
            cur.execute(query, values)
            row_count += 1
            #logging.info(f'{row_count} rows from csv file inserted into employee_details table successfully')
        conn.commit()
        cur.close
        conn.close()

    def get_data_from_postgresql(self) -> bool:
        try:
            conn, cur = postgre_conn().get_postgre_connection()
            query = """select * from employee_details"""
            cur.execute(query)
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=col_names)
            print("Employee_details:")
            print(df)
            query = """select * from employee_details_country"""
            cur.execute(query)
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=col_names)
            print("Employee_details_country:")
            print(df)
            logging.info(f'Read data from employee_details and employee_details_country successfully')
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            logging.error(f'Issue Occurred while reading data from postgresql due to: {e}')
            traceback.print_exc()
        conn.commit()
        cur.close
        conn.close()
        return False

    def delete_data_from_postgresql(self):
        conn, cur = postgre_conn().get_postgre_connection()
        query = """drop table if exists employee_details,employee_details_country"""
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f'drop data from employee_details and employee_details_country successfully')


if __name__ == '__main__':
    emp = employee_details()
    if emp.get_data_from_github(emp.url, emp.dest_folder, emp.input_file) and emp.create_postgres_table():
        emp.write_to_employee_details()
        emp.write_to_employee_details_country()
        if emp.get_data_from_postgresql():
            emp.delete_data_from_postgresql()
