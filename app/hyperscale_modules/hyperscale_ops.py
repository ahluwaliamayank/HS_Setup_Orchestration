import api_modules.hyperscale_api_resources.datasets as datasets
import api_modules.hyperscale_api_resources.jobs as jobs
import api_modules.hyperscale_api_resources.executions as executions

import os
import time

class Hyperscale_Ops(
    datasets.DataSets,
    jobs.Jobs,
    executions.Executions
):
    def __init__(self, config_dict):
        self.session_ts = config_dict['session_ts']
        self.debug_mode = config_dict['debug_mode']
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
            job_request_data['name'] = f'{each_schema}_job'
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

    def job_coordinator(self, proc_num, utilities_inst, big_context_queue, small_context_queue, error_queue):
        execution_success = True
        contexts_processed_so_far = []
        execution_request_data = dict()
        if proc_num == 1:
            read_big_queue = 'y'
            read_small_queue = 'n'
        else:
            read_small_queue = 'y'
            read_big_queue = 'n'

        # Setup logging
        process_log_object = utilities_inst.setup_mp_logging(proc_num, self.session_ts, self.debug_mode, 0,
                                                             log_type='execution')

        module_name = f"Hyperscale_Execution_{proc_num}"
        execution_logger = process_log_object.get_module_logger(module_name)
        execution_logger.info(f"Starting Process : {proc_num}")
        execution_logger.info(f"Process ID : {os.getpid()}")

        while error_queue.qsize() == 0:
            try:
                if read_big_queue == 'y':
                    execution_logger.info('Reading from big context queue')
                    next_executable = big_context_queue.get_nowait()
                elif read_small_queue == 'y':
                    execution_logger.info('Reading from small context queue')
                    next_executable = small_context_queue.get_nowait()
                else:
                    raise Exception('Neither big nor small queue has any data left')
            except Exception:
                if read_big_queue == 'y' and read_small_queue not in ['y', 'x']:
                    read_small_queue = 'y'
                    read_big_queue = 'x'
                    continue
                elif read_small_queue == 'y' and read_big_queue not in ['y', 'x']:
                    read_big_queue = 'y'
                    read_small_queue = 'x'
                    continue
                else:
                    execution_logger.info(f"No more data to read")
                break
            else:
                context_name = next_executable[0]
                num_rows = next_executable[1]
                dataset_id = next_executable[2]
                job_id = next_executable[3]

                execution_logger.info(f'Creating execution for {context_name} with dataset ID : {dataset_id}, '
                                      f'job ID : {job_id}, having approx. total rows : {num_rows}')

                execution_request_data['job_id'] = job_id
                success, response = self.create_execution(execution_logger, self.access_url, self.auth_header,
                                                          execution_request_data)
                if not success:
                    execution_logger.error(f'Failed to create execution using executions endpoint '
                                           f'for job_id : {job_id}')
                    error_queue.put('error')
                    execution_success = False
                    break
                if not response or not response.json() or 'id' not in response.json():
                    execution_logger.error('Failed to create execution using executions endpoint')
                    execution_logger.error(f'Failed to create execution using executions endpoint '
                                           f'for job_id : {job_id}')
                    error_queue.put('error')
                    execution_success = False
                    break

                execution_id = response.json()['id']
                execution_logger.info(f'Successfully created execution : {execution_id}')

                execution_logger.info(f'Contexts processed so far : {contexts_processed_so_far}')
                execution_logger.info(f'Currently processing : {context_name} with dataset ID : {dataset_id}, '
                                      f'job ID : {job_id}, having approx. total rows : {num_rows}')

                execution_logger.info(f'Monitoring execution ID : {execution_id} for completion')

                while True:
                    execution_logger.debug(f'Contexts processed so far : {contexts_processed_so_far}')
                    execution_logger.debug(f'Currently processing : {context_name} with dataset ID : {dataset_id}, '
                                           f'job ID : {job_id}, having approx. total rows : {num_rows}')
                    execution_logger.debug(f'Sleeping 2 minutes to get status')
                    time.sleep(120)
                    success, response = self.get_execution_summary(execution_logger, self.access_url, self.auth_header,
                                                                   execution_id)
                    if not success or not response:
                        execution_logger.error(f'Failed to get summary for execution ID : {execution_id}')
                        error_queue.put('error')
                        break

                    if 'status' not in response.json():
                        execution_logger.error(f'Failed to get summary for execution ID : {execution_id}')
                        error_queue.put('error')
                        break

                    execution_logger.debug(response.json())

                    if response.json()['status'] == 'SUCCEEDED':
                        execution_logger.info(f'Context : {context_name} with dataset ID : {dataset_id}, '
                                              f'job ID : {job_id} completed successfully')
                        break

                    if response.json()['status'] == 'FAILED':
                        execution_logger.info(f'Context : {context_name} with dataset ID : {dataset_id}, '
                                              f'job ID : {job_id} failed')
                        break

                contexts_processed_so_far.append(context_name)
        ########################################################################

        if execution_success:
            execution_logger.info(f"Exiting process : {proc_num}")
        else:
            execution_logger.error(f"Aborting process : {proc_num}")

        # Terminate logging
        utilities_inst.terminate_logging(process_log_object, print_out=False)

        del execution_logger

        return execution_success
