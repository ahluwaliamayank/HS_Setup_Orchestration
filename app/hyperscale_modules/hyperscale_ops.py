import api_modules.hyperscale_api_resources.datasets as datasets
import api_modules.hyperscale_api_resources.jobs as jobs


class Hyperscale_Ops(
    datasets.DataSets,
    jobs.Jobs
):
    def __init__(self, config_dict):
        self.access_url = config_dict['hyperscale_access_url']
        self.auth_header = {"Authorization": config_dict['hyperscale_access_key']}
        self.connector_id = config_dict['hyperscale_connector_id']
        self.root_connector_id = config_dict['hyperscale_connector_id_for_root']
        self.mount_id = config_dict['file_mount_id']
        self.date_domains = config_dict['date_domains']
        self.masking_engine_ids = config_dict['masking_engine_ids']
        self.retain_execution_data = config_dict['retain_execution_data']
        self.masking_job_config = config_dict['masking_job_config']
        self.source_connections = config_dict['source_connections']
        self.target_connections = config_dict['target_connections']

    def refresh_datasets(self, logger, inventory, repo_conn, db_utils_inst, refresh_id):
        logger.info('Beginning refresh of hyperscale datasets')
        unique_schemas = list(set([each_inv[0] for each_inv in inventory]))
        for each_schema in unique_schemas:
            logger.info(f'Processing schema : {each_schema}')
            request_data = dict()
            if each_schema.upper() == 'ATHENA1':
                request_data['connector_id'] = self.root_connector_id
            else:
                request_data['connector_id'] = self.connector_id
            request_data['mount_filesystem_id'] = self.mount_id
            data_info = []
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
                dataset_id = schema_inv[0][1]
                logger.info(f'Dataset already exists with ID : {dataset_id}, put call will be needed')
                dataset_id = schema_inv[0][1]
                success, response = self.update_dataset(logger, self.access_url, self.auth_header, request_data,
                                                        dataset_id)
                if not success:
                    logger.error('Failed to update dataset using datasets endpoint')
                    return False
                if not response or not response.json() or 'id' not in response.json():
                    logger.error('Failed to update dataset using datasets endpoint')
                    return False

                success = db_utils_inst.update_dataset_id(logger, repo_conn, schema_inv[0][6], dataset_id,
                                                          refresh_id, update=True)
                if not success:
                    logger.error(f'Failed to update dataset id in DB')
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

                logger.info(f'Dataset successfully created for schema : {each_schema}, datasetID : {dataset_id}')

                logger.info('Updating repository with created dataset ID')

                success = db_utils_inst.update_dataset_id(logger, repo_conn, schema_inv[0][6], dataset_id,
                                                          refresh_id, update=False)
                if not success:
                    logger.error(f'Failed to insert dataset id')
                    return False

                logger.info(f'Successfully updated repository with dataset ID for schema : {each_schema}')

            job_request_data = dict()
            job_request_data['name'] = f'{each_schema}'
            job_request_data['masking_engine_ids'] = self.masking_engine_ids
            job_request_data['data_set_id'] = dataset_id
            job_request_data['app_name_prefix'] = f'app_{each_schema}'
            job_request_data['env_name_prefix'] = f'env_{each_schema}'
            job_request_data['retain_execution_data'] = self.retain_execution_data
            source_connection_config = {"max_concurrent_source_connection" : self.source_connections}
            target_connection_config = {"max_concurrent_target_connection" : self.target_connections}
            job_request_data['source_configs'] = source_connection_config
            job_request_data['target_configs'] = target_connection_config
            job_request_data['masking_job_config'] = self.masking_job_config
            if schema_inv[0][10]:
                job_id = schema_inv[0][10]
                logger.info(f'Dataset : {dataset_id} already has job defined with ID : {job_id}, updating the job')
                success, response = self.update_job(logger, self.access_url, self.auth_header, job_request_data, job_id)
                if not success:
                    logger.error('Failed to update job using jobs endpoint')
                    return False
                if not response or not response.json() or 'id' not in response.json():
                    logger.error('Failed to update job using jobs endpoint')
                    return False

            else:
                logger.info(f'No job exists for dataset : {dataset_id}, creating the job')
                success, response = self.create_job(logger, self.access_url, self.auth_header, job_request_data)
                if not success:
                    logger.error('Failed to create job using jobs endpoint')
                    return False
                if not response or not response.json() or 'id' not in response.json():
                    logger.error('Failed to create job using jobs endpoint')
                    return False

                job_id = response.json()['id']
                logger.info(f'Job successfully created for schema : {each_schema}, jobID : {job_id}')
                logger.info('Updating repository with created job ID')

            success = db_utils_inst.update_hyperscale_job_id(logger, repo_conn, schema_inv[0][6], job_id,
                                                             refresh_id)

            if not success:
                logger.error(f'Failed to update job id in DB')
                return False



        return True
