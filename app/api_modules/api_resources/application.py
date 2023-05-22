import api_modules.api_abstractor as api


class Application(api.GenericAPI):
    application_resource = "applications"
    base_header = {'Content-Type': 'application/json'}

    def get_all_applications(self, logger, base_url, auth_header):
        api_url = f"{base_url}/{self.application_resource}"
        api_header = self.base_header | auth_header
        success, response = self.get_call(logger, api_url, api_header)
        if not success:
            logger.error('Failed to get applications from applications endpoint')
            return False, None
        else:
            return True, response

    def create_application(self, logger, base_url, auth_header, application_name):
        api_url = f"{base_url}/{self.application_resource}"
        api_header = self.base_header | auth_header
        request_data = dict()
        request_data['applicationName'] = application_name
        success, response = self.post_call(logger, api_url, api_header, request_data)
        if not success:
            logger.error('Failed to create application from applications endpoint')
            return False, None
        else:
            return True, response
