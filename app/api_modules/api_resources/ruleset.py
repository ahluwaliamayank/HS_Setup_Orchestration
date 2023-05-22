import api_modules.api_abstractor as api


class RuleSet(api.GenericAPI):
    ruleset_resource = "database-rulesets"
    bulk_table_update_resource = 'bulk-table-update'
    base_header = {'Content-Type': 'application/json'}

    def create_ruleset(self, logger, base_url, auth_header, ruleset_name, connector_id):
        api_url = f"{base_url}/{self.ruleset_resource}"
        api_header = self.base_header | auth_header
        request_data = dict()
        request_data['rulesetName'] = ruleset_name
        request_data['databaseConnectorId'] = connector_id
        success, response = self.post_call(logger, api_url, api_header, request_data)
        if not success:
            logger.error('Failed to create ruleset from ruleset endpoint')
            return False, None
        else:
            return True, response

    def bulk_table_update(self, logger, base_url, auth_header, ruleset_id, list_of_tables):
        api_url = f"{base_url}/{self.ruleset_resource}/{ruleset_id}/{self.bulk_table_update_resource}"
        api_header = self.base_header | auth_header
        request_data = dict()
        table_metadata = []
        for each_table in list_of_tables:
            table_data = dict()
            table_data['tableName'] = each_table
            table_data['rulesetId'] = ruleset_id
            table_metadata.append(table_data)

        request_data['tableMetadata'] = table_metadata
        success, response = self.put_call(logger, api_url, api_header, request_data)
        if not success:
            logger.error('Failed to bulk add tables to ruleset from ruleset/bulk-table-update endpoint')
            return False, None
        else:
            return True, response

