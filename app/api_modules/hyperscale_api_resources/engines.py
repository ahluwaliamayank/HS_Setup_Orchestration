import api_modules.api_abstractor as api


class Engines(api.GenericAPI):
    engine_resource = 'engines'
    base_header = {'Content-Type': 'application/json'}

    def get_registered_engines(self, logger, base_url, auth_header):
        api_url = f"{base_url}/{self.engine_resource}"
        api_header = self.base_header | auth_header
        success, response = self.get_call(logger, api_url, api_header, set_verify_false=True)
        if not success:
            logger.error('Failed to get engines from engines endpoint')
            return False, None
        else:
            return True, response
