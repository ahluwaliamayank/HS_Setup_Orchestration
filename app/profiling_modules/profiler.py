import os
import time
from queue import Queue
import threading

import api_modules.api_resources.login as login
import api_modules.api_resources.application as application
import api_modules.api_resources.environment as environment
import api_modules.api_resources.connector as connector
import api_modules.api_resources.ruleset as ruleset
import api_modules.api_resources.profiler_set as profiler_set
import api_modules.api_resources.profile_jobs as profiler_jobs
import api_modules.api_resources.execution as execution
import api_modules.api_resources.profile_results as profile_results
import api_modules.api_resources.async_tasks as async_tasks
import oracle_modules.oracle_operations_manager as ops_manager
import oracle_modules.oracle_connection_manager as conn_manager


class Profiler(
    login.Login,
    application.Application,
    environment.Environment,
    connector.Connector,
    ruleset.RuleSet,
    profiler_set.Profiler_Set,
    profiler_jobs.ProfileJob,
    execution.Execution,
    profile_results.ProfileResults,
    async_tasks.AsyncTasks,
    ops_manager.OracleOperationsManager,
    conn_manager.OracleConnectionManager
):

    compliance_app_name = 'incremental_profiler'
    database_type = 'ORACLE'

    def __init__(self, config_dict, inventory_map):
        self.masking_engines = config_dict['engines']
        self.num_engines = config_dict['num_engines']
        self.session_ts = config_dict['session_ts']
        self.jobs_per_engine = config_dict['jobs_per_engine']
        self.tables_per_dataset = config_dict['tables_per_dataset']
        self.debug_mode = config_dict['debug_mode']
        self.profile_set_name = config_dict['profile_set_name']
        self.streams_for_profile_job = config_dict['streams_for_profile_job']
        self.max_memory_for_profiling_job = config_dict['max_memory_for_profiling_job']

        self.inventory_map = inventory_map
        self.ignore_clob_columns = config_dict['ignore_clob_columns']
        self.metadata_inventory = config_dict['metadata_inventory']

    def get_engine_authorizations(self, logger):
        for key, value in self.masking_engines.items():
            logger.info(f"Getting authorization for engine : {key}")
            success, response = self.login(logger, value['url'], value['user'], value['password'])
            if not success:
                logger.error(f"Failed to get authorization header for : {key}")
                return False
            else:
                value['auth_header'] = response.json()
                self.masking_engines[key] = value

        return True

    def get_profile_set_ids(self, logger):

        for key, value in self.masking_engines.items():
            profile_set_found = False
            logger.info(f"Getting profiler set ID for engine : {key}")
            success, response = self.get_all_profile_sets(logger, value['url'], value['auth_header'])
            if not success:
                logger.error(f"Failed to get profiler set ID for : {key}")
                return False
            else:
                if 'responseList' not in response.json():
                    logger.error(f"Failed to get profiler set ID for : {key}")
                    return False
                elif not response.json()['responseList']:
                    logger.error(f"Profiler Set not found : {key}")
                    return False
                elif len(response.json()['responseList']) < 1:
                    logger.error(f"Profiler Set not found : {key}")
                    return False
                else:
                    for each_profile_set in response.json()['responseList']:
                        if each_profile_set['profileSetName'].lower() == self.profile_set_name.lower():
                            logger.info(f'Profile Set found for : {key}')
                            value['profile_set_id'] = each_profile_set['profileSetId']
                            profile_set_found = True
                            break

                    if not profile_set_found:
                        logger.error(f"Profiler Set not found : {key}")
                        return False

        return True

    def manage_application_in_compliance_engine(self, logger):

        for key, value in self.masking_engines.items():
            create_app = True
            logger.info(f"Getting applications for engine : {key}")
            success, response = self.get_all_applications(logger, value['url'], value['auth_header'])
            if not success:
                logger.error(f"Failed to get applications for : {key}")
                return False
            else:
                if 'responseList' not in response.json():
                    logger.error(f"Failed to get applications for : {key}")
                    return False
                elif not response.json()['responseList']:
                    create_app = True
                elif len(response.json()['responseList']) < 1:
                    create_app = True
                else:
                    for each_application in response.json()['responseList']:
                        if each_application['applicationName'].lower() == self.compliance_app_name.lower():
                            logger.info(f'Application already exists for : {key}')
                            value['Application_ID'] = each_application['applicationId']
                            create_app = False
                            break

                if create_app:
                    logger.info(f'Application needs to be created for : {key}')
                    success, response = self.create_application(logger, value['url'], value['auth_header'],
                                                                self.compliance_app_name)
                    if not success:
                        logger.error(f"Failed to create application for : {key}")
                        return False
                    else:
                        logger.info(f'Successfully created application for : {key}')
                        value['Application_ID'] = response.json()['applicationId']

        return True

    def manage_environment_in_compliance_engine(self, logger):
        for key, value in self.masking_engines.items():
            logger.info(f"Creating environment for engine : {key}")
            success, response = self.create_environment(logger, value['url'], value['auth_header'],
                                                        f'profiling_{self.session_ts}', value['Application_ID'])
            if not success:
                logger.error(f"Failed to create environment for : {key}")
                return False
            else:
                logger.info(f'Successfully created environment for : {key}')
                value['Environment_ID'] = response.json()['environmentId']

        return True

    def create_connectors(self, logger, config_dict, unique_contexts):
        request_data = dict()
        for key, value in self.masking_engines.items():
            value['Schema_Details'] = []
            logger.info(f'Creating connectors in : {key}')
            database_type = self.database_type
            environment_id = value['Environment_ID']
            jdbc = config_dict['source_details']['jdbc_url']
            user_name = config_dict['source_details']['user_name'].upper()
            password = config_dict['source_details']['password']
            for each_schema in unique_contexts:
                request_data.clear()
                schema_name = each_schema.upper()
                request_data['connectorName'] = f"Conn_{schema_name}"
                request_data['databaseType'] = database_type
                request_data['environmentId'] = environment_id
                request_data['jdbc'] = jdbc
                request_data['password'] = password
                request_data['username'] = user_name
                request_data['schemaName'] = schema_name
                logger.info(f'Creating connector for schema : {schema_name}')
                success, response = self.create_db_connector(logger, value['url'], value['auth_header'],
                                                             request_data)
                if not success:
                    logger.error(f"Failed to create connector for engine : {key}, for schema : {schema_name}")
                    return False
                else:
                    logger.info(f"Successfully created connector for engine : {key}, for schema : {schema_name}")
                    schema_info = dict()
                    schema_info['Schema_Name'] = schema_name
                    schema_info['Connector_ID'] = response.json()['databaseConnectorId']
                    value['Schema_Details'].append(schema_info)

        return True

    def split_into_datasets(self, logger, tables, unique_contexts):
        logger.info('Splitting available tables for profiling into datasets')
        datasets = []
        for each_context in unique_contexts:
            context_tables = [tup[1] for tup in tables if tup[0] == each_context]
            if not context_tables or len(context_tables) < 1:
                logger.error('No tables found for the context')
                return False, None
            low_val = 0
            high_val = self.tables_per_dataset
            while True:
                batch = context_tables[low_val:high_val]
                datasets.append((each_context, batch))
                if high_val >= len(context_tables):
                    break
                low_val = high_val
                high_val += self.tables_per_dataset

        return True, datasets

    def profiler(self, profile_logger, dataset_queue, engine_details, error_queue,
                 thread_num, results_queue, profile_run_id):
        profile_logger.info(f'Starting up thread : {thread_num}')
        ruleset_counter = thread_num * 1000
        while error_queue.qsize() == 0:
            try:
                next_executable = dataset_queue.get_nowait()
            except Exception:
                profile_logger.info(f"Thread{thread_num} - No more data to read")
                break
            else:
                schema_name = next_executable[0]
                connector_id = 0
                tables = next_executable[1]
                profile_logger.info(f'Thread{thread_num} - Processing for schema : {schema_name} '
                                    f'and table sample : {tables[:2]}')

                profile_logger.debug(f'Thread{thread_num} - Processing for schema : {schema_name} '
                                     f'and table sample : {tables}')

                for each_schema in engine_details['Schema_Details']:
                    if each_schema['Schema_Name'].lower() == schema_name.lower():
                        connector_id = each_schema['Connector_ID']
                        break

                if connector_id == 0:
                    profile_logger.error(f'Thread{thread_num} - Could not find connector ID for schema : {schema_name}')
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                if error_queue.qsize() > 0:
                    break

                try:
                    context_inventory_map = self.inventory_map[schema_name]
                except KeyError as ke:
                    profile_logger.error(f'Thread{thread_num} - Error in getting inventory map for : {schema_name}')
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                if not context_inventory_map or len(context_inventory_map) < 1:
                    profile_logger.error(f'Thread{thread_num} - Could not find inventory map for : {schema_name}')
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                ruleset_name = f'ruleset_{ruleset_counter}'
                ruleset_counter += 1
                success, response = self.create_ruleset(profile_logger, engine_details['url'],
                                                        engine_details['auth_header'], ruleset_name, connector_id)
                if not success:
                    profile_logger.error(f"thread{thread_num} - Failed to create ruleset")
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                if error_queue.qsize() > 0:
                    break

                ruleset_id = response.json()['databaseRulesetId']
                profile_logger.info(f'Thread{thread_num} - Created ruleset ID : {ruleset_id}')

                if error_queue.qsize() > 0:
                    break

                profile_logger.info(f'Thread{thread_num} - Attempting to add tables to ruleset')
                success, response = self.bulk_table_update(profile_logger, engine_details['url'],
                                                           engine_details['auth_header'], ruleset_id, tables)

                if not success:
                    profile_logger.error(f"Thread{thread_num} - Failed to add tables to ruleset")
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                profile_logger.info(f'Thread{thread_num} - Successfully submitted request for adding tables to ruleset')
                async_task_id = response.json()['asyncTaskId']
                profile_logger.info(f'Thread{thread_num} - Asynchronous task ID submitted is : {async_task_id}')

                if error_queue.qsize() > 0:
                    break

                profile_logger.info(f'Thread{thread_num} - Monitoring async task status and awaiting completion')

                while True:
                    if error_queue.qsize() > 0:
                        break

                    success, response = self.get_task_status(profile_logger, engine_details['url'],
                                                             engine_details['auth_header'], async_task_id)
                    if not success:
                        profile_logger.error(f"Thread{thread_num} - Failed to get async task status")
                        error_queue.put(f"error occurred in thread {thread_num}")
                        break

                    if response.json()['status'] == 'SUCCEEDED':
                        profile_logger.info(f'Thread{thread_num} - async task ID : {async_task_id} succeeded')
                        break
                    elif response.json()['status'] == "FAILED" or response.json()['status'] == "CANCELLED":
                        profile_logger.error(f"thread{thread_num} - async task ID : {async_task_id} failed")
                        error_queue.put(f"error occurred in thread {thread_num}")
                        break

                if error_queue.qsize() > 0:
                    break

                profile_logger.info(f'Thread{thread_num} - Attempting to create profile job')
                profile_job_name = f"profile_{ruleset_name}"
                success, response = self.create_profiling_job(profile_logger, engine_details['url'],
                                                              engine_details['auth_header'], profile_job_name,
                                                              ruleset_id, engine_details['profile_set_id'],
                                                              self.streams_for_profile_job,
                                                              self.max_memory_for_profiling_job)

                if not success:
                    profile_logger.error(f"thread{thread_num} - Failed to create profiling job")
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                profile_job_id = response.json()['profileJobId']
                profile_logger.info(f'Thread{thread_num} - Created profile job ID : {profile_job_id}')

                if error_queue.qsize() > 0:
                    break

                profile_logger.info(f'Thread{thread_num} - Attempting to execute profile job')
                success, response = self.create_execution(profile_logger, engine_details['url'],
                                                          engine_details['auth_header'], profile_job_id)

                if not success:
                    profile_logger.error(f"thread{thread_num} - Failed to create execution for profiling job")
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                execution_id = response.json()['executionId']
                profile_logger.info(f'Thread{thread_num} - Created execution ID : {execution_id}')
                profile_logger.info(f'Thread{thread_num} - Monitoring execution status and awaiting completion')

                while True:
                    if error_queue.qsize() > 0:
                        break

                    success, response = self.get_status_of_execution(profile_logger, engine_details['url'],
                                                                     engine_details['auth_header'], execution_id)
                    if not success:
                        profile_logger.error(f"Thread{thread_num} - Failed to get execution status")
                        error_queue.put(f"error occurred in thread {thread_num}")
                        break

                    if response.json()['status'] == 'SUCCEEDED':
                        profile_logger.info(f'Thread{thread_num} - execution ID : {execution_id} succeeded')
                        break
                    elif response.json()['status'] == "FAILED" or response.json()['status'] == "CANCELLED":
                        profile_logger.error(f"thread{thread_num} - execution ID : {execution_id} failed")
                        error_queue.put(f"error occurred in thread {thread_num}")
                        break

                if error_queue.qsize() > 0:
                    break

                profile_logger.info(f'Thread{thread_num} - Attempting to get results of profile job execution')
                success, results = self.get_profiling_execution_results(profile_logger, engine_details['url'],
                                                                        engine_details['auth_header'], execution_id)

                if not success:
                    profile_logger.error(f"Thread{thread_num} - Failed to get results")
                    error_queue.put(f"error occurred in thread {thread_num}")
                    break

                if not results or len(results) < 1:
                    profile_logger.info(f"Thread{thread_num} - No Results found")
                else:
                    profile_logger.info(f"Thread{thread_num} - successfully retrieved results")
                    profile_logger.info(f"Thread{thread_num} - Mapping retrieved results to inventory")
                    inventory_update_data = []
                    csv_output = []
                    profile_logger.info(f"Thread{thread_num} - inventory size : {len(context_inventory_map)}")
                    profile_logger.info(f"Thread{thread_num} - result size : {len(results)}")
                    result_dict = dict()
                    for each_result in results:
                        key = f"{each_result['tableName']}.{each_result['columnName']}"
                        if self.ignore_clob_columns:
                            if 'clob' in each_result['dataType'].lower():
                                continue
                            result_dict[key] = dict()
                            result_dict[key]['domainName'] = each_result['domainName']
                            result_dict[key]['algorithmName'] = each_result['algorithmName']

                    profile_logger.info(f"Thread{thread_num} - result dictionary size : {len(result_dict)}")

                    for key, value in context_inventory_map.items():
                        if key in result_dict:
                            inventory_update_data.append(
                                (result_dict[key]['domainName'], result_dict[key]['algorithmName'], profile_run_id,
                                 'No', value)
                            )
                            csv_output.append(
                                (schema_name, key.split('.')[0], key.split('.')[1],
                                 result_dict[key]['domainName'], result_dict[key]['algorithmName'], 'No'))
                        else:
                            inventory_update_data.append((None, None, profile_run_id, 'No', value))
                            csv_output.append(
                                (schema_name, key.split('.')[0], key.split('.')[1],
                                 None, None, 'No'))

                    results_queue.put((schema_name, inventory_update_data, csv_output))
                    profile_logger.info(f"Thread{thread_num} - Mapped retrieved results to inventory")

                if error_queue.qsize() > 0:
                    break

        profile_logger.info(f'Closing thread : {thread_num}')

    def profiling_coordinator(self, proc_num, dataset_queue, profile_run_id, utilities_inst, results_queue):
        profiling_success = True
        thread_list = []
        error_queue = Queue()
        engine_details = self.masking_engines[f'engine{proc_num}']

        # Setup logging
        process_log_object = utilities_inst.setup_mp_logging(proc_num, self.session_ts, self.debug_mode, profile_run_id,
                                                             log_type='profile')

        module_name = f"Profiler_{proc_num}"
        profile_logger = process_log_object.get_module_logger(module_name)
        profile_logger.info(f"Starting Process : {proc_num}")
        profile_logger.info(f"Process ID : {os.getpid()}")

        for x in range(1, self.jobs_per_engine+1):
            t = threading.Thread(
                target=self.profiler,
                args=(
                    profile_logger, dataset_queue, engine_details, error_queue, x,
                    results_queue, profile_run_id
                )
            )
            thread_list.append(t)
            t.start()

        for each_thread in thread_list:
            each_thread.join()

        if error_queue.qsize() > 0:
            profile_logger.error('Errors encountered')
            profiling_success = False

        ########################################################################

        if profiling_success:
            profile_logger.info(f"Exiting process : {proc_num}")
        else:
            profile_logger.error(f"Aborting process : {proc_num}")

        # Terminate logging
        utilities_inst.terminate_logging(process_log_object, print_out=False)

        del profile_logger

        return profiling_success


