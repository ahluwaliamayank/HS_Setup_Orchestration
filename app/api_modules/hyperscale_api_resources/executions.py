import api_modules.api_abstractor as api


class Executions(api.GenericAPI):
    execution_resource = 'executions'
    base_header = {'Content-Type': 'application/json'}

    def create_execution(self, logger, base_url, auth_header, request_data):
        api_url = f"{base_url}/{self.execution_resource}"
        api_header = self.base_header | auth_header
        success, response = self.post_call(logger, api_url, api_header, request_data, set_verify_false=True)
        if not success:
            logger.error('Failed to create job from jobs endpoint')
            return False, None
        else:
            return True, response

    def get_execution_summary(self, logger, base_url, auth_header, execution_id):
        api_url = f"{base_url}/{self.execution_resource}/{execution_id}/summary"
        api_header = self.base_header | auth_header
        success, response = self.get_call(logger, api_url, api_header, set_verify_false=True)
        if not success:
            logger.error('Failed to get execution summary from execution endpoint')
            return False, None
        else:
            return True, response
