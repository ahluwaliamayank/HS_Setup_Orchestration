import oracledb as oracle_conn


class OracleConnectionManager:
    def __init__(self, use_thick_client=False):
        if use_thick_client:
            oracle_conn.init_oracle_client()

    def get_connection_params(self, logger, connection_dict):
        logger.info('Creating oracle connection parameters')
        if connection_dict['use_sid']:
            connection_params = oracle_conn.ConnectParams(
                user=connection_dict['user_name'],
                password=connection_dict['password'],
                host=connection_dict['host'],
                port=connection_dict['port'],
                sid=connection_dict['sid_or_service']
            )
            return True, connection_params
        else:
            connection_params = oracle_conn.ConnectParams(
                user=connection_dict['user_name'],
                password=connection_dict['password'],
                host=connection_dict['host'],
                port=connection_dict['port'],
                service_name=connection_dict['sid_or_service'])
            return True, connection_params

    def create_jdbc_url(self, logger, connection_dict):
        logger.info('Creating oracle jdbc url')
        host = connection_dict['host']
        port = connection_dict['port']
        jdbc_url = f'jdbc:oracle:thin:@{host}:{port}'
        if connection_dict['use_sid']:
            jdbc_url = f"{jdbc_url}:{connection_dict['sid_or_service']}"
        else:
            jdbc_url = f"{jdbc_url}/{connection_dict['sid_or_service']}"

        return True, jdbc_url

    def create_connection(self, logger, connection_params):
        try:
            db_conn = oracle_conn.connect(params=connection_params)
        except Exception as excp:
            logger.error(f"Connection to source failed : {excp}")
            return False, None
        else:
            logger.info(f"Successfully connected to DB")
            return True, db_conn

    def close_connection(self, logger, db_conn):
        try:
            db_conn.close()
        except Exception as excp:
            logger.error(f"Failed to close connection : {excp}")
            return False
        else:
            logger.info("Closed connection to DB")
            return True
