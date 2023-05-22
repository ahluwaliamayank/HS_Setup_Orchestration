import api_modules.api_abstractor as api


class Environment(api.GenericAPI):
    environment_resource = "environments"
    base_header = {'Content-Type': 'application/json'}

    def get_all_environments(self, logger, base_url, auth_header):
        api_url = f"{base_url}/{self.environment_resource}"
        api_header = self.base_header | auth_header
        success, response = self.get_call(logger, api_url, api_header)
        if not success:
            logger.error('Failed to get environments from environments endpoint')
            return False, None
        else:
            return True, response

    def create_environment(self, logger, base_url, auth_header, environment_name, application_id):
        api_url = f"{base_url}/{self.environment_resource}"
        api_header = self.base_header | auth_header
        request_data = dict()
        request_data['environmentName'] = environment_name
        request_data['applicationId'] = application_id
        request_data['purpose'] = 'MASK'
        success, response = self.post_call(logger, api_url, api_header, request_data)
        if not success:
            logger.error('Failed to create environment from environments endpoint')
            return False, None
        else:
            return True, response

    def delete_environment(self, logger, base_url, auth_header, environment_id):
        api_url = f"{base_url}/{self.environment_resource}/{environment_id}"
        api_header = self.base_header | auth_header
        success, response = self.delete_call(logger, api_url, api_header)
        if not success:
            logger.error('Failed to delete environments from environments endpoint')
            return False, None
        else:
            return True, response
