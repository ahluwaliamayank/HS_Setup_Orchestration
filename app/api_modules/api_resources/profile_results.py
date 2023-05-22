import api_modules.api_abstractor as api

import math


class ProfileResults(api.GenericAPI):
    profile_results_resource = 'profile-results-database'

    def get_profiling_execution_results(self, logger, base_url, auth_header, execution_id):
        api_url = f"{base_url}/{self.profile_results_resource}/{execution_id}"
        api_header = self.base_header | auth_header
        results = []
        page_size = 1000
        page_number = 0
        total_pages_needed = 1
        while page_number < total_pages_needed:
            page_number += 1
            get_results_query_params = {
                'page_size' : page_size,
                'page_number' : page_number
            }
            success, response = self.get_call(logger, api_url, api_header, query_params=get_results_query_params)
            if not success:
                logger.error('Failed to get results for the given execution id from profile results endpoint')
                return False, None
            else:
                if page_number == 1:
                    if '_pageInfo' not in response.json():
                        logger.error('Could not determine number of pages in results')
                        return False, None
                    total_pages = response.json()['_pageInfo']['total']
                    total_pages_needed = math.ceil(total_pages / page_size)

                for each_response in response.json()['responseList']:
                    results.append(each_response)

        return True, results
