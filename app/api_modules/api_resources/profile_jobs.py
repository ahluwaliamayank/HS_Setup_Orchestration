import api_modules.api_abstractor as api


class ProfileJob(api.GenericAPI):
    profile_job_resource = "profile-jobs"

    def create_profiling_job(self, logger, base_url, auth_header, job_name, ruleset_id, profile_set_id, num_streams,
                             max_memory):
        api_url = f"{base_url}/{self.profile_job_resource}"
        api_header = self.base_header | auth_header
        request_data = dict()
        request_data['jobName'] = job_name
        request_data['profileSetId'] = profile_set_id
        request_data['rulesetId'] = ruleset_id
        request_data['numInputStreams'] = num_streams
        request_data['maxMemory'] = max_memory
        success, response = self.post_call(logger, api_url, api_header, request_data)
        if not success:
            logger.error('Failed to create profile job from profile jobs endpoint')
            return False, None
        else:
            return True, response
