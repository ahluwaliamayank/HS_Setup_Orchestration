import api_modules.api_abstractor as api


class AsyncTasks(api.GenericAPI):
    async_task_resource = "async-tasks"
    base_header = {'Content-Type': 'application/json'}

    def get_task_status(self, logger, base_url, auth_header, async_task_id):
        api_url = f"{base_url}/{self.async_task_resource}/{async_task_id}"
        api_header = self.base_header | auth_header
        success, response = self.get_call(logger, api_url, api_header)
        if not success:
            logger.error('Failed to get async task status from async tasks endpoint')
            return False, None
        else:
            return True, response
