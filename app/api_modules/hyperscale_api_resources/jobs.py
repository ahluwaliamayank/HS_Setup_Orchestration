import api_modules.api_abstractor as api


class Jobs(api.GenericAPI):
    jobs_resource = 'jobs'
    base_header = {'Content-Type': 'application/json'}

    def create_job(self, logger, base_url, auth_header, request_data):
        api_url = f"{base_url}/{self.jobs_resource}"
        api_header = self.base_header | auth_header
        success, response = self.post_call(logger, api_url, api_header, request_data, set_verify_false=True)
        if not success:
            logger.error('Failed to create job from jobs endpoint')
            return False, None
        else:
            return True, response

    def update_job(self, logger, base_url, auth_header, request_data, job_id):
        api_url = f"{base_url}/{self.jobs_resource}/{job_id}"
        api_header = self.base_header | auth_header
        success, response = self.put_call(logger, api_url, api_header, request_data, set_verify_false=True)
        if not success:
            logger.error('Failed to update job from jobs endpoint')
            return False, None
        else:
            return True, response
