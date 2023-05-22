import api_modules.api_abstractor as api


class Login(api.GenericAPI):
    login_resource = "login"
    base_header = {'Content-Type': 'application/json'}

    def login(self, logger, base_url, user, password):
        api_url = f"{base_url}/{self.login_resource}"
        request_data = dict()
        request_data['username'] = user
        request_data['password'] = password
        success, response = self.post_call(logger, api_url, Login.base_header, request_data)
        if not success:
            logger.error('Failed to get authorization from login endpoint')
            return False, None
        else:
            return True, response
