import api_modules.api_abstractor as api


class DataSets(api.GenericAPI):
    datasets_resource = 'data-sets'
    base_header = {'Content-Type': 'application/json'}

    def create_dataset(self, logger, base_url, auth_header, request_data):
        api_url = f"{base_url}/{self.datasets_resource}"
        api_header = self.base_header | auth_header
        success, response = self.post_call(logger, api_url, api_header, request_data, set_verify_false=True)
        if not success:
            logger.error('Failed to create dataset from data-sets endpoint')
            return False, None
        else:
            return True, response

    def update_dataset(self, logger, base_url, auth_header, request_data, dataset_id):
        api_url = f"{base_url}/{self.datasets_resource}/{dataset_id}"
        api_header = self.base_header | auth_header
        success, response = self.put_call(logger, api_url, api_header, request_data, set_verify_false=True)
        if not success:
            logger.error('Failed to update dataset from data-sets endpoint')
            return False, None
        else:
            return True, response
