import api_modules.api_abstractor as api


class Execution(api.GenericAPI):
    execution_resource = 'executions'

    def create_execution(self, logger, base_url, auth_header, job_id):
        api_url = f"{base_url}/{self.execution_resource}"
        api_header = self.base_header | auth_header
        request_data = dict()
        request_data['jobId'] = job_id
        success, response = self.post_call(logger, api_url, api_header, request_data)
        if not success:
            logger.error('Failed to create execution from executions endpoint')
            return False, None
        else:
            return True, response

    def get_status_of_execution(self, logger, base_url, auth_header, execution_id):
        api_url = f"{base_url}/{self.execution_resource}/{execution_id}"
        api_header = self.base_header | auth_header
        success, response = self.get_call(logger, api_url, api_header)
        if not success:
            logger.error('Failed to get execution for the given id from executions endpoint')
            return False, None
        else:
            return True, response

