import api_modules.hyperscale_api_resources.datasets as datasets


class Hyperscale_Ops(
    datasets.DataSets
):
    def __init__(self, config_dict):
        self.access_url = config_dict['hyperscale_access_url']
        self.auth_header = {"Authorization": config_dict['hyperscale_access_key']}
        self.connector_id = config_dict['hyperscale_connector_id']
        self.mount_id = config_dict['file_mount_id']
        self.date_domains = config_dict['date_domains']

    def refresh_datasets(self, logger, inventory, repo_conn, db_utils_inst, refresh_id):
        logger.info('Beginning refresh of hyperscale datasets')
        unique_schemas = list(set([each_inv[0] for each_inv in inventory]))
        for each_schema in unique_schemas:
            request_data = dict()
            request_data['connector_id'] = self.connector_id
            request_data['mount_filesystem_id'] = self.mount_id
            data_info = []
            logger.info(f'Processing schema : {each_schema}')
            schema_inv = [each_inv for each_inv in inventory if each_inv[0] == each_schema]
            unique_tables = list(set([each_inv[2] for each_inv in schema_inv]))
            for each_table in unique_tables:
                table_inv = [each_inv for each_inv in schema_inv if each_inv[2] == each_table]
                table_data = dict()
                source_details = dict()
                target_details = dict()
                table_masking_inventory = []
                source_details['schema_name'] = each_schema
                source_details['table_name'] = each_table
                num_rows = table_inv[0][7]
                if num_rows is None or num_rows < 1:
                    num_splits = 1
                else:
                    num_splits = min(num_rows // 100000000, 10)
                    if num_splits < 1:
                        num_splits = 1
                source_details['unload_split'] = num_splits
                target_details['schema_name'] = each_schema
                target_details['table_name'] = each_table
                target_details['stream_size'] = 65536
                table_data['source'] = source_details
                table_data['target'] = target_details
                for each_entry in table_inv:
                    column_data = dict()
                    column_data['field_name'] = each_entry[3]
                    column_data['domain_name'] = each_entry[4]
                    column_data['algorithm_name'] = each_entry[5]
                    if each_entry[4].lower() in self.date_domains:
                        if 'DATE' in each_entry[8].upper():
                            column_data['date_format'] = 'yyyy-MM-dd HH:mm:ss.SSS'
                        else:
                            if not each_entry[9] or each_entry[9] is None or len(each_entry[9]) < 2:
                                column_data['date_format'] = 'yyyy-MM-dd HH:mm:ss.SSS'
                            else:
                                column_data['date_format'] = each_entry[9]
                    table_masking_inventory.append(column_data)

                table_data['masking_inventory'] = table_masking_inventory
                data_info.append(table_data)

            request_data['data_info'] = data_info
            if schema_inv[0][1]:
                logger.info('Dataset already exists, put call will be needed')
                dataset_id = schema_inv[0][1]
                success, response = self.update_dataset(logger, self.access_url, self.auth_header, request_data,
                                                        dataset_id)
                if not success:
                    logger.error('Failed to create dataset using datasets endpoint')
                    return False
                if not response or not response.json() or 'id' not in response.json():
                    logger.error('Failed to create dataset using datasets endpoint')
                    return False

                success = db_utils_inst.update_dataset_id(logger, repo_conn, schema_inv[0][6], dataset_id,
                                                          refresh_id, update=True)
                if not success:
                    logger.error(f'Failed to insert dataset id')
                    return False
            else:
                logger.info("Dataset doesn't exist, post call will be needed")
                success, response = self.create_dataset(logger, self.access_url, self.auth_header, request_data)
                if not success:
                    logger.error('Failed to create dataset using datasets endpoint')
                    return False
                if not response or not response.json() or 'id' not in response.json():
                    logger.error('Failed to create dataset using datasets endpoint')
                    return False

                dataset_id = response.json()['id']

                success = db_utils_inst.update_dataset_id(logger, repo_conn, schema_inv[0][6], dataset_id,
                                                          refresh_id, update=False)
                if not success:
                    logger.error(f'Failed to insert dataset id')
                    return False

        return True
