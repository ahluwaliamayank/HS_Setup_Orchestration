import api_modules.api_abstractor as api


class Profiler_Set(api.GenericAPI):
    profile_set_resource = "profile-sets"
    base_header = {'Content-Type': 'application/json'}

    def get_all_profile_sets(self, logger, base_url, auth_header):
        api_url = f"{base_url}/{self.profile_set_resource}"
        api_header = self.base_header | auth_header
        success, response = self.get_call(logger, api_url, api_header)
        if not success:
            logger.error('Failed to get profile sets from applications endpoint')
            return False, None
        else:
            return True, response
