import api_modules.api_abstractor as api


class Connector(api.GenericAPI):
    connector_resource = "database-connectors"
    base_header = {'Content-Type': 'application/json'}

    def create_db_connector(self, logger, base_url, auth_header, request_data):
        api_url = f"{base_url}/{self.connector_resource}"
        api_header = self.base_header | auth_header
        success, response = self.post_call(logger, api_url, api_header, request_data)
        if not success:
            logger.error('Failed to create connector')
            return False, None
        else:
            return True, response
