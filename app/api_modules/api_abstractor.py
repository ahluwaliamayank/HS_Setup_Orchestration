import requests
from requests.packages import urllib3


class GenericAPI:

    valid_response_status_codes = [200, 201]

    def post_call(self, logger, url, request_headers, request_data, set_verify_false=False):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        try:
            if set_verify_false:
                response = requests.post(url, headers=request_headers, json=request_data, verify=False)
            else:
                response = requests.post(url, headers=request_headers, json=request_data)
        except Exception as excp:
            logger.error("Connection failed")
            return False, None
        else:
            if response.status_code not in self.valid_response_status_codes:
                logger.error(f"Post call failed for : {url}")
                logger.error(f"Error is : {response.text}")
                logger.error("Request was : ")
                logger.error(request_data)
                return False, None
            return True, response

    def get_call(self, logger, url, request_headers, request_data=None, query_params=None, set_verify_false=False):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        if request_data:
            if set_verify_false:
                response = requests.get(url, headers=request_headers, json=request_data, verify=False)
            else:
                response = requests.get(url, headers=request_headers, json=request_data)
        elif query_params:
            if set_verify_false:
                response = requests.get(url, headers=request_headers, params=query_params, verify=False)
            else:
                response = requests.get(url, headers=request_headers, params=query_params)
        else:
            if set_verify_false:
                response = requests.get(url, headers=request_headers, verify=False)
            else:
                response = requests.get(url, headers=request_headers)

        if response.status_code not in self.valid_response_status_codes:
            logger.error(f"Get call failed for : {url}")
            logger.error(f"Error is : {response.text}")
            logger.error("Request was : ")
            if request_data:
                logger.error(request_data)
            elif query_params:
                logger.error(query_params)
            return False, None

        return True, response

    def put_call(self, logger, url, request_headers, request_data, set_verify_false=False):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        if set_verify_false:
            response = requests.put(url, headers=request_headers, json=request_data, verify=False)
        else:
            response = requests.put(url, headers=request_headers, json=request_data)
        if response.status_code not in self.valid_response_status_codes:
            logger.error(f"Put call failed for : {url}")
            logger.error(f"Error is : {response.text}")
            logger.error("Request was : ")
            logger.error(request_data)
            return False, None

        return True, response

    def delete_call(self, logger, url, request_headers, request_data=None, set_verify_false=False):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        if request_data:
            if set_verify_false:
                response = requests.delete(url, headers=request_headers, json=request_data, verify=False)
            else:
                response = requests.delete(url, headers=request_headers, json=request_data)
        else:
            if set_verify_false:
                response = requests.delete(url, headers=request_headers, verify=False)
            else:
                response = requests.delete(url, headers=request_headers)

        if response.status_code not in self.valid_response_status_codes:
            logger.error(f"Delete call failed for : {url}")
            logger.error(f"Error is : {response.text}")
            logger.error("Request was : ")
            logger.error(request_data)
            return False, None

        return True, response
