from scripts.config_reader import config_reader
import psycopg2
import logging
import traceback

class postgre_conn:

    def get_postgre_connection(self):
        config = config_reader().get_config_pipeline()
        postgres_host = config.get('CONFIG_DETAIL', "postgres_host")
        postgres_database = config.get('CONFIG_DETAIL', "postgres_database")
        postgres_user = config.get('CONFIG_DETAIL', "postgres_user")
        postgres_password = config.get('CONFIG_DETAIL', "postgres_password")
        postgres_port = config.get('CONFIG_DETAIL', "postgres_port")
        try:
            conn = psycopg2.connect(
                host=postgres_host,
                database=postgres_database,
                user=postgres_user,
                password=postgres_password,
                port=postgres_port
            )
            cur = conn.cursor()
            logging.info('Postgres server connection is successful')
        except Exception as e:
            traceback.print_exc()
            logging.error("Couldn't create the Postgres connection")
        return conn,cur