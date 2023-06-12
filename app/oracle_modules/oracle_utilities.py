import oracle_modules.oracle_operations_manager as ops_manager
import oracle_modules.oracle_connection_manager as conn_manager
import pandas as pd
import os
import time


class OracleUtilities(ops_manager.OracleOperationsManager, conn_manager.OracleConnectionManager):
    def __init__(self, config_dict):
        super().__init__(config_dict['use_thick_client'])
        self.profiling_runs = config_dict['profiling_runs']
        self.context_master = config_dict['context_master']
        self.metadata_refreshes = config_dict['metadata_refreshes']
        self.metadata_inventory = config_dict['metadata_inventory']
        self.hyperscale_dataset_refreshes = config_dict['hyperscale_dataset_refreshes']
        self.hyperscale_datasets = config_dict['hyperscale_datasets']
        self.date_formats = config_dict['date_formats']
        self.raw_session_ts = config_dict['session_raw_ts']
        self.session_ts = config_dict['session_ts']
        self.debug_mode = config_dict['debug_mode']
        self.ignore_clob_columns = config_dict['ignore_clob_columns']

    def insert_refresh_run(self, logger, db_conn, run_ts):
        insert_refresh_sql = f"insert into {self.metadata_refreshes}(refresh_ts, status) values(:1, :2) " \
                             f"returning refresh_id into :3"
        insert_refresh_data = (run_ts, 'Submitted')

        success, new_refresh_id = self.oracle_insert_with_id_returned(logger, db_conn, insert_refresh_sql,
                                                                      insert_refresh_data)

        if not success:
            logger.error('Failed to insert a new refresh run')
            return False, None

        logger.info(f'New refresh inserted, refresh id : {new_refresh_id[0]}')

        return True, new_refresh_id[0]

    def update_refresh_run_status(self, logger, db_conn, refresh_id, status='Executing'):
        update_status_sql = f"update metadata_refreshes set status = :1 where refresh_id = :2"
        update_status_data = [(status, refresh_id)]
        success = self.oracle_dml_or_ddl(logger, db_conn, update_status_sql, query_data=update_status_data)
        if not success:
            logger.error(f'Failed to update status for refresh id : {refresh_id} to : {status}')
            return False

        logger.info(f'Successfully updated status for refresh id : {refresh_id} to : {status}')
        return True

    def handle_context(self, logger, db_conn, config_dict):
        get_context_sql = f"select context_id, context_name, status, context_log" \
                          f" from {self.context_master} where lower(context_name) = :1"
        get_context_data = (config_dict['context_name'].lower(), )
        if config_dict['add_context']:
            success, context_details = self.oracle_select(logger, db_conn, get_context_sql, query_data=get_context_data)
            if not success:
                logger.error('Failed to get context details')
                return False

            if len(context_details) == 0:
                add_context_sql = f"insert into {self.context_master}(context_name, status, context_log," \
                                  f"create_ts, last_modified_ts) values(:1, :2, :3, :4, :5)"

                add_context_data = [(config_dict['context_name'].upper(), 'ACTIVE', f"Added on : {self.session_ts}",
                                     self.raw_session_ts, self.raw_session_ts)]
                success = self.oracle_dml_or_ddl(logger, db_conn, add_context_sql, query_data=add_context_data)
                if not success:
                    logger.error('Failed to add context to repository')
                    return False

                logger.info('Context successfully added to repository')
                return True
            else:
                if context_details[0][2] == 'ACTIVE':
                    logger.info(f"Context already exists")
                    print(f"Context already exists")
                    return True
                else:
                    logger.info('Context already exists but status was inactive, changing it to active')
                    context_log = f"{context_details[0][3]}; Status changed to Active on : {self.session_ts}"
                    update_context_sql = f"update {self.context_master} set status = :1, last_modified_ts = :2, " \
                                         f"context_log = :3 where context_id = :4"
                    update_context_data = [('ACTIVE', self.raw_session_ts, context_log, context_details[0][0])]

                    success = self.oracle_dml_or_ddl(logger, db_conn, update_context_sql,
                                                     query_data=update_context_data)

                    if not success:
                        logger.error("Failed to update context")
                        return False

                    logger.info("Successfully updated context to active in repository")
                    return True
        else:
            success, context_details = self.oracle_select(logger, db_conn, get_context_sql, get_context_data)
            if not success:
                logger.error('Failed to get context details')
                return False

            if len(context_details) == 0:
                logger.error('Context not found to delete')
                return False

            if context_details[0][2] == 'INACTIVE':
                print('Context is already inactive')
                return True

            context_log = f"{context_details[0][3]}; Status changed to Inactive on : {self.session_ts}"
            update_context_sql = f"update {self.context_master} set status = :1, last_modified_ts = :2, " \
                                 f"context_log = :3 where context_id = :4"
            update_context_data = [('INACTIVE', self.raw_session_ts, context_log, context_details[0][0])]

            success = self.oracle_dml_or_ddl(logger, db_conn, update_context_sql, query_data=update_context_data)

            if not success:
                logger.error("Failed to remove context")
                return False

            logger.info("Successfully removed context from repository")
            return True

    def get_all_contexts(self, logger, db_conn):
        get_contexts_sql = f"select context_id, context_name from {self.context_master} where status = :1"
        get_contexts_data = ('ACTIVE', )
        success, contexts = self.oracle_select(logger, db_conn, get_contexts_sql, query_data=get_contexts_data)
        if not success:
            logger.error('Failed to get list of contexts')
            return False, None

        if len(contexts) == 0:
            logger.error('no contexts found')
            return False, None

        logger.info('Successfully retrieved list of contexts')

        return True, contexts

    def get_context_inventory(self, logger, db_conn, context_name):
        logger.info(f'Getting inventory for : {context_name}')
        get_context_inventory_sql = f"select tab.table_name, nvl(tab.num_rows, 0) as num_rows, col.column_name, " \
                                    f"col.data_type, col.data_length from " \
                                    f"all_tables tab join all_tab_columns col on " \
                                    f"tab.table_name = col.table_name and tab.owner = col.owner " \
                                    f"where tab.owner = :1"

        get_context_inventory_data = (context_name, )

        success, context_inventory, context_inv_columns = self.oracle_select(logger, db_conn, get_context_inventory_sql,
                                                                             query_data=get_context_inventory_data,
                                                                             desc_needed=True)

        if not success:
            logger.error(f'Failed to get inventory for : {context_name}')
            return False, None, None

        logger.info(f'Successfully retrieved inventory for : {context_name}')

        return True, context_inventory, context_inv_columns

    def get_existing_inventory(self, logger, db_conn, context_id, context_name):
        logger.info(f'Getting existing inventory for : {context_name}')
        get_existing_context_inventory_sql = f"select table_name, num_rows, column_name, data_type, " \
                                             f"data_length, inventory_log, inventory_id from metadata_inventory " \
                                             f"where context_id = :1 and inventory_status != :2"

        get_existing_context_inventory_data = (context_id, 'DROPPED')

        success, existing_inventory, existing_inv_columns = self.oracle_select(
            logger, db_conn, get_existing_context_inventory_sql, query_data=get_existing_context_inventory_data,
            desc_needed=True
        )

        if not success:
            logger.error(f'Failed to get existing inventory for : {context_name}')
            return False, None, None

        logger.info(f'Successfully retrieved existing inventory for : {context_name}')

        return True, existing_inventory, existing_inv_columns

    def refresh_inventory(self, logger, repo_conn, context_inventory, context_inv_columns, existing_inventory,
                          existing_inv_columns, context_name, context_id, session_ts, refresh_id):

        refresh_successful = True
        new_rows, existing_rows, dropped_rows = list(), list(), list()
        merge_on = ['TABLE_NAME', 'COLUMN_NAME']
        merge_suffixes = ("_APP", "_REPO")
        no_change_query = (
            "DATA_TYPE_APP == DATA_TYPE_REPO and DATA_LENGTH_APP == DATA_LENGTH_REPO"
        )
        mod_query = (
            "DATA_TYPE_APP != DATA_TYPE_REPO or DATA_LENGTH_APP != DATA_LENGTH_REPO"
        )

        insert_inventory_sql = f"insert into metadata_inventory(CONTEXT_ID, TABLE_NAME, NUM_ROWS, COLUMN_NAME," \
                               f"DATA_TYPE, DATA_LENGTH, INVENTORY_STATUS, INVENTORY_LOG, " \
                               f"LAST_PROFILING_RUN_ID, LAST_REFRESH_ID) " \
                               f"values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"

        # update_existing_inventory_sql = f"update metadata_inventory set INVENTORY_STATUS = :1, INVENTORY_LOG = :2, " \
        #                                 f"DATA_TYPE = :3, DATA_LENGTH = :4 where CONTEXT_ID = :5 AND " \
        #                                 f"TABLE_NAME = :6 AND COLUMN_NAME = :7"

        update_existing_inventory_sql = f"update metadata_inventory set INVENTORY_STATUS = :1, INVENTORY_LOG = :2, " \
                                        f"DATA_TYPE = :3, DATA_LENGTH = :4, NUM_ROWS = :5, LAST_REFRESH_ID = :6 " \
                                        f"where INVENTORY_ID = :7"

        # update_dropped_inventory_sql = f"update metadata_inventory set INVENTORY_STATUS = :1, INVENTORY_LOG = :2 " \
        #                                f"where CONTEXT_ID = :3 AND TABLE_NAME = :4 AND COLUMN_NAME = :5"

        update_dropped_inventory_sql = f"update metadata_inventory set INVENTORY_STATUS = :1, INVENTORY_LOG = :2, " \
                                       f"LAST_REFRESH_ID = :3 where INVENTORY_ID = :4"

        logger.info(f'Beginning inventory refresh for : {context_name}')

        df_repo_inv = pd.DataFrame(existing_inventory, columns=existing_inv_columns)
        df_app_inv = pd.DataFrame(context_inventory, columns=context_inv_columns)

        df_merge = pd.merge(
            df_app_inv,
            df_repo_inv,
            on=merge_on,
            indicator=True,
            how="outer",
            suffixes=merge_suffixes
        )

        del df_app_inv
        del df_repo_inv

        df_common = df_merge.query("_merge == 'both'", inplace=False)
        df_new = df_merge.query("_merge == 'left_only'", inplace=False)
        df_drop = df_merge.query("_merge == 'right_only'", inplace=False)

        del df_merge

        logger.info(f"Newly added rows in source are : {len(df_new)}")
        logger.info(f"Rows already found in repository are : {len(df_common)}")
        logger.info(f"Rows dropped are : {len(df_drop)}")

        while refresh_successful:
            if len(df_new):
                logger.info('Processing newly added rows in source')
                for row in df_new.itertuples(index=False):
                    new_rows.append(
                        (
                            context_id, getattr(row, 'TABLE_NAME'), getattr(row, 'NUM_ROWS_APP'),
                            getattr(row, 'COLUMN_NAME'), getattr(row, 'DATA_TYPE_APP'), getattr(row, 'DATA_LENGTH_APP'),
                            'NEW', f"Added on : {session_ts}", None, refresh_id
                        )
                    )

                refresh_successful = self.oracle_dml_or_ddl(logger, repo_conn, insert_inventory_sql,
                                                            query_data=new_rows)

                if not refresh_successful:
                    logger.error(f"Error encountered in inserting new rows in repository")
                    break

                logger.info('Successfully finished inserting rows in repository')

            if len(df_common):
                logger.info('Processing existing rows')
                df_no_change = df_common.query(no_change_query, inplace=False)
                df_modified = df_common.query(mod_query, inplace=False)

                logger.info(f"Rows without change are : {len(df_no_change)}")
                logger.info(f"Rows modified are : {len(df_modified)}")

                for row in df_no_change.itertuples(index=False):
                    # existing_rows.append(
                    #     (
                    #         'EXISTING', getattr(row, 'INVENTORY_LOG'), getattr(row, 'DATA_TYPE_APP'),
                    #         getattr(row, 'DATA_LENGTH_APP'), context_id, getattr(row, 'TABLE_NAME'),
                    #         getattr(row, 'COLUMN_NAME')
                    #     )
                    # )

                    existing_rows.append(
                        (
                            'EXISTING', getattr(row, 'INVENTORY_LOG'), getattr(row, 'DATA_TYPE_APP'),
                            getattr(row, 'DATA_LENGTH_APP'), getattr(row, 'NUM_ROWS_APP'), refresh_id,
                            getattr(row, 'INVENTORY_ID')
                        )
                    )

                for row in df_modified.itertuples(index=False):
                    # existing_rows.append(
                    #     (
                    #         'MODIFIED', f"{getattr(row, 'INVENTORY_LOG')}; data type or length found different on : "
                    #                     f"{session_ts}", getattr(row, 'DATA_TYPE_APP'),
                    #         getattr(row, 'DATA_LENGTH_APP'), context_id, getattr(row, 'TABLE_NAME'),
                    #         getattr(row, 'COLUMN_NAME')
                    #     )
                    # )

                    existing_rows.append(
                        (
                            'MODIFIED', f"{getattr(row, 'INVENTORY_LOG')}; Modified on : {session_ts}, "
                                        f"data type or length found different", getattr(row, 'DATA_TYPE_APP'),
                            getattr(row, 'DATA_LENGTH_APP'), getattr(row, 'NUM_ROWS_APP'),
                            refresh_id, getattr(row, 'INVENTORY_ID')
                        )
                    )

                refresh_successful = self.oracle_dml_or_ddl(logger, repo_conn, update_existing_inventory_sql,
                                                            query_data=existing_rows)

                if not refresh_successful:
                    logger.error(f"Error encountered in updating existing rows in repository")
                    break

                logger.info('Successfully finished updating existing rows in repository')

            if len(df_drop):
                logger.info('Processing dropped rows in source')
                for row in df_drop.itertuples(index=False):
                    # dropped_rows.append(
                    #     (
                    #         'DROPPED', f"{getattr(row, 'INVENTORY_LOG')}; found dropped on : {session_ts}",
                    #         context_id, getattr(row, 'TABLE_NAME'), getattr(row, 'COLUMN_NAME')
                    #     )
                    # )

                    dropped_rows.append(
                        (
                            'DROPPED', f"{getattr(row, 'INVENTORY_LOG')}; Dropped on : {session_ts}", refresh_id,
                            getattr(row, 'INVENTORY_ID')
                        )
                    )

                refresh_successful = self.oracle_dml_or_ddl(logger, repo_conn, update_dropped_inventory_sql,
                                                            query_data=dropped_rows)

                if not refresh_successful:
                    logger.error(f"Error encountered in updating dropped rows in repository")
                    break

                logger.info('Successfully finished updating dropped rows in repository')

            break

        return refresh_successful

    def metadata_inventory_refresh_coordinator(self, proc_num, shared_queue, refresh_id, utilities_inst):
        metadata_inventory_refresh_success = True

        # Setup logging
        process_log_object = utilities_inst.setup_mp_logging(proc_num, self.session_ts, self.debug_mode, refresh_id,
                                                             log_type='metadata_refresh')

        module_name = f"Inventory_Refresh_Coordinator_{proc_num}"
        refresh_logger = process_log_object.get_module_logger(module_name)
        refresh_logger.info(f"Starting Process : {proc_num}")
        refresh_logger.info(f"Process ID : {os.getpid()}")

        # Get connection to Repository
        refresh_logger.info(f"Establishing connection to repository")
        success, repo_connection_params = self.get_connection_params(
            refresh_logger, utilities_inst.config_dict['repository_details'])
        if not success:
            refresh_logger.error('Failed to create connection params for repository db')
            refresh_logger.error(f'Aborting process : {proc_num}')
            utilities_inst.terminate_logging(process_log_object, print_out=False)
            del refresh_logger
            return False

        success, repo_conn = self.create_connection(refresh_logger, repo_connection_params)
        if not success or not repo_conn:
            refresh_logger.error("Failed to create connection to repository db, aborting")
            refresh_logger.error(f'Aborting process : {proc_num}')
            utilities_inst.terminate_logging(process_log_object, print_out=False)
            del refresh_logger
            return False

        refresh_logger.info('Established connection to repository')

        # Get connection to Source DB
        refresh_logger.info(f"Establishing connection to source")
        success, source_connection_params = self.get_connection_params(
            refresh_logger, utilities_inst.config_dict['source_details'])

        if not success:
            refresh_logger.error('Failed to create connection params for source db')
            refresh_logger.error(f'Aborting process : {proc_num}')
            utilities_inst.terminate_logging(process_log_object, print_out=False)
            del refresh_logger
            return False

        success, source_conn = self.create_connection(refresh_logger, source_connection_params)
        if not success or not source_conn:
            refresh_logger.error("Failed to create connection to source db, aborting")
            success = self.close_connection(refresh_logger, repo_conn)
            if not success:
                refresh_logger.error('Failed to close connection to repository db')
            else:
                refresh_logger.info('Closed connection to repository db')
            refresh_logger.error(f'Aborting process : {proc_num}')
            utilities_inst.terminate_logging(process_log_object, print_out=False)
            del refresh_logger
            return False

        refresh_logger.info('Established connection to source DB')

        while True:
            try:
                next_executable = shared_queue.get_nowait()
            except Exception:
                refresh_logger.info("No more data to read")
                break
            else:
                context_name = next_executable[1]
                context_id = next_executable[0]
                refresh_logger.info(f'Processing context : {context_name}')
                success, context_inventory, context_inv_columns = self.get_context_inventory(
                    refresh_logger, source_conn, context_name)
                if not success:
                    refresh_logger.error('Failed to get new metadata')
                    metadata_inventory_refresh_success = False
                    break

                success, existing_inventory, existing_inv_columns = self.get_existing_inventory(
                    refresh_logger, repo_conn, context_id, context_name
                        )
                if not success:
                    refresh_logger.error('Failed to get existing metadata')
                    metadata_inventory_refresh_success = False
                    break

                success = self.refresh_inventory(refresh_logger, repo_conn, context_inventory,
                                                 context_inv_columns, existing_inventory,
                                                 existing_inv_columns, context_name, context_id,
                                                 self.session_ts, refresh_id)

                if not success:
                    refresh_logger.error('Failed to refresh metadata')
                    metadata_inventory_refresh_success = False
                    break

                refresh_logger.info(f'Finished processing context : {context_name}')

        ########################################################################
        # Close connection to repository
        success = self.close_connection(refresh_logger, repo_conn)
        if not success:
            refresh_logger.error('Failed to close connection to repository db')
        else:
            refresh_logger.info('Closed connection to repository db')

        # Close connection to source DB
        success = self.close_connection(refresh_logger, source_conn)
        if not success:
            refresh_logger.error('Failed to close connection to source db')
        else:
            refresh_logger.info('Closed connection to source db')

        if metadata_inventory_refresh_success:
            refresh_logger.info(f"Exiting process : {proc_num}")
        else:
            refresh_logger.error(f"Aborting process : {proc_num}")

        # Terminate logging
        utilities_inst.terminate_logging(process_log_object, print_out=False)

        del refresh_logger

        return metadata_inventory_refresh_success

    def get_tables_for_profiling(self, logger, db_conn):
        get_distinct_tables_for_profiling_sql = f"select distinct contexts.context_name, inventory.table_name " \
                                                f"from {self.metadata_inventory} inventory join " \
                                                f"{self.context_master} contexts on " \
                                                f"inventory.context_id = contexts.context_id where (" \
                                                f"inventory.last_profiling_run_id is null or " \
                                                f"inventory.last_profiling_run_id = 0 or " \
                                                f"inventory.inventory_status in ('NEW', 'MODIFIED')) and " \
                                                f"inventory.inventory_status not in ('DROPPED') and " \
                                                f"contexts.status = 'ACTIVE'"

        success, tables = self.oracle_select(logger, db_conn, get_distinct_tables_for_profiling_sql)
        if not success:
            logger.error("Failed to get list of tables for profiling")
            return False, None

        logger.info('Successfully retrieved list of tables')

        return True, tables

    def build_inventory_map(self, logger, db_conn):
        get_inventory_data_sql = f"select inventory.inventory_id, contexts.context_name || '.' || " \
                                 f"inventory.table_name || '.' || inventory.column_name as key_name " \
                                 f"from {self.metadata_inventory} inventory join " \
                                 f"{self.context_master} contexts " \
                                 f"on inventory.context_id = contexts.context_id " \
                                 f"where (inventory.last_profiling_run_id is null or " \
                                 f"inventory.last_profiling_run_id = 0 or " \
                                 f"inventory.inventory_status in ('NEW', 'MODIFIED')) and " \
                                 f"inventory.inventory_status not in ('DROPPED')"
        success, inventory_data = self.oracle_select(logger, db_conn, get_inventory_data_sql)
        if not success:
            logger.error("Failed to get inventory_data for profiling")
            return False, None

        logger.info('Successfully retrieved inventory_data')
        inventory_map = dict()
        for each_inventory in inventory_data:
            inventory_map[each_inventory[1]] = each_inventory[0]

        return True, inventory_map

    def build_context_inventory_map(self, logger, db_conn):
        get_inventory_data_sql = f"select inventory.inventory_id, contexts.context_name, " \
                                 f"inventory.table_name || '.' || inventory.column_name as key_name " \
                                 f"from {self.metadata_inventory} inventory join " \
                                 f"{self.context_master} contexts " \
                                 f"on inventory.context_id = contexts.context_id " \
                                 f"where (inventory.last_profiling_run_id is null or " \
                                 f"inventory.last_profiling_run_id = 0 or " \
                                 f"inventory.inventory_status in ('NEW', 'MODIFIED')) and " \
                                 f"inventory.inventory_status not in ('DROPPED') and contexts.status = 'ACTIVE'"
        success, inventory_data = self.oracle_select(logger, db_conn, get_inventory_data_sql)
        if not success:
            logger.error("Failed to get inventory_data for profiling")
            return False, None

        logger.info('Successfully retrieved inventory_data')

        unique_contexts = list(set([inv[1] for inv in inventory_data]))
        inventory_map = dict()
        for each_context in unique_contexts:
            context_inventory_map = dict()
            context_inventory_data = [inv for inv in inventory_data if inv[1] == each_context]
            for each_entry in context_inventory_data:
                context_inventory_map[each_entry[2]] = each_entry[0]
            inventory_map[each_context] = context_inventory_map

        return True, inventory_map

    def create_jdbc_url(self, logger, config_dict):
        logger.info('Creating JDBC URL for source DB')
        host = config_dict['source_details']['host']
        port = config_dict['source_details']['port']
        jdbc_url = f'jdbc:oracle:thin:@{host}:{port}'
        if config_dict['source_details']['use_sid']:
            jdbc_url = f"{jdbc_url}:{config_dict['source_details']['sid_or_service']}"
        else:
            jdbc_url = f"{jdbc_url}/{config_dict['source_details']['sid_or_service']}"

        logger.info('Successfully created JDBC URL')

        return jdbc_url

    def insert_profiling_run(self, logger, db_conn, run_ts):
        insert_profiling_run_sql = f"insert into {self.profiling_runs}(run_ts, status) values(:1, :2) " \
                                   f"returning profiling_run_id into :3"
        insert_profiling_run_data = (run_ts, 'Submitted')

        success, new_profiling_run_id = self.oracle_insert_with_id_returned(logger, db_conn, insert_profiling_run_sql,
                                                                            insert_profiling_run_data)

        if not success:
            logger.error('Failed to insert a new profiling run')
            return False, None

        logger.info(f'New profiling run inserted, run id : {new_profiling_run_id[0]}')

        return True, new_profiling_run_id[0]

    def update_profiling_run_status(self, logger, db_conn, run_id, status='Executing'):
        update_status_sql = f"update {self.profiling_runs} set status = :1 where profiling_run_id = :2"
        update_status_data = [(status, run_id)]
        success = self.oracle_dml_or_ddl(logger, db_conn, update_status_sql, query_data=update_status_data)
        if not success:
            logger.error(f'Failed to update status for profiling run id : {run_id} to : {status}')
            return False

        logger.info(f'Successfully updated status for profiling run id : {run_id} to : {status}')
        return True

    def update_profiling_results_old(self, logger, repo_conn, inventory_map, results_queue, profile_run_id):
        inventory_update_sql = f"update {self.metadata_inventory} set domain = :1, algorithm_applied = :2, " \
                               f"last_profiling_run_id = :3, reviewed = :4 where inventory_id = :5"

        inventory_update_data = []
        csv_output = []
        results = []
        logger.info('Preparing repository updates after profiling')
        while True:
            try:
                next_result = results_queue.get_nowait()
            except Exception as excp:
                logger.info('All results have been read')
                break
            else:
                results.append(next_result)

        for key, value in inventory_map.items():
            match_found = False
            for each_result in results:
                schema_name = each_result[0]
                inventory_key = f"{schema_name}.{each_result[1]['tableName']}.{each_result[1]['columnName']}"
                if key.lower() == inventory_key.lower():
                    if self.ignore_clob_columns:
                        if 'clob' in each_result[1]['dataType'].lower():
                            break
                    domain_name = each_result[1]['domainName']
                    algorithm_applied = each_result[1]['algorithmName']
                    inventory_update_data.append((domain_name, algorithm_applied, profile_run_id, 'No', value))
                    match_found = True
                    csv_output.append((schema_name, each_result[1]['tableName'], each_result[1]['columnName'],
                                       domain_name, algorithm_applied, 'No'))
                    break

            if not match_found:
                inventory_update_data.append((None, None, profile_run_id, 'No', value))
                inventory_info = key.split('.')
                csv_output.append((inventory_info[0], inventory_info[1], inventory_info[2], None, None, 'No'))

        success = self.oracle_dml_or_ddl(logger, repo_conn, inventory_update_sql, query_data=inventory_update_data)
        if not success:
            logger.error(f'Failed to update results for profiling run id : {profile_run_id}')
            return False, None

        logger.info(f'Successfully updated results for profiling run id : {profile_run_id}')
        return True, csv_output

    def insert_hyperscale_dataset_refresh_run(self, logger, db_conn, run_ts):
        insert_hyperscale_dataset_refresh_sql = f"insert into {self.hyperscale_dataset_refreshes}(refresh_ts, status)" \
                                                f" values(:1, :2)" \
                                                f" returning refresh_id into :3"
        insert_hyperscale_dataset_refresh_data = (run_ts, 'Submitted')

        success, new_hyperscale_dataset_refresh_id = \
            self.oracle_insert_with_id_returned(logger, db_conn, insert_hyperscale_dataset_refresh_sql,
                                                insert_hyperscale_dataset_refresh_data)

        if not success:
            logger.error('Failed to insert a new hyperscale dataset refresh')
            return False, None

        logger.info(f'New hyperscale dataset refresh inserted, refresh id : {new_hyperscale_dataset_refresh_id[0]}')

        return True, new_hyperscale_dataset_refresh_id[0]

    def update_hyperscale_dataset_refresh_run_status(self, logger, db_conn, refresh_id, status='Executing'):
        update_status_sql = f"update {self.hyperscale_dataset_refreshes} set status = :1 where refresh_id = :2"
        update_status_data = [(status, refresh_id)]
        success = self.oracle_dml_or_ddl(logger, db_conn, update_status_sql, query_data=update_status_data)
        if not success:
            logger.error(f'Failed to update status for hyperscale dataset refresh id : {refresh_id} to : {status}')
            return False

        logger.info(f'Successfully updated status for hyperscale dataset refresh id : {refresh_id} to : {status}')
        return True

    def get_inventory_for_masking(self, logger, db_conn):
        get_inventory_for_masking_sql = f"select cont.context_name, dataset.hyperscale_dataset_id, inv.table_name, " \
                                        f"inv.column_name, inv.domain, inv.algorithm_applied, cont.context_id," \
                                        f"inv.num_rows, inv.data_type, fmt.date_format from " \
                                        f"{self.metadata_inventory} inv join {self.context_master} cont on " \
                                        f"inv.context_id = cont.context_id left join " \
                                        f"{self.hyperscale_datasets} dataset on " \
                                        f"cont.context_id = dataset.context_id left join " \
                                        f"{self.date_formats} fmt on fmt.context_id = cont.context_id and " \
                                        f"lower(fmt.column_name) = lower(inv.column_name) " \
                                        f"where inv.domain is not null and " \
                                        f"cont.status = :1 and inv.inventory_status not in ('DROPPED')"

        get_inventory_for_masking_data = ('ACTIVE', )

        success, inventory = self.oracle_select(logger, db_conn, get_inventory_for_masking_sql,
                                                query_data=get_inventory_for_masking_data)
        if not success:
            logger.error("Failed to get inventory for masking")
            return False, None

        logger.info('Successfully retrieved inventory for masking')

        return True, inventory

    def update_dataset_id(self, logger, db_conn, context_id, dataset_id, refresh_id, update=True):
        if not update:
            update_dataset_id_sql = f"insert into {self.hyperscale_datasets}(context_id, hyperscale_dataset_id, " \
                                    f"last_hyperscale_dataset_refresh_id) values(:1, :2, :3)"
            update_dataset_id_data = [(context_id, dataset_id, refresh_id)]

            success = self.oracle_dml_or_ddl(logger, db_conn, update_dataset_id_sql,
                                             query_data=update_dataset_id_data)
            if not success:
                logger.error(f"Failed to update dataset id for context with ID : {context_id}")

            return success
        else:
            update_dataset_id_sql = f"update {self.hyperscale_datasets} set last_hyperscale_dataset_refresh_id = :1 " \
                                    f"where context_id = :2 and hyperscale_dataset_id = :3"
            update_dataset_id_data = [(refresh_id, context_id, dataset_id)]
            success = self.oracle_dml_or_ddl(logger, db_conn, update_dataset_id_sql,
                                             query_data=update_dataset_id_data)
            if not success:
                logger.error(f"Failed to update dataset id for context with ID : {context_id}")

            return success

    def update_profiling_results(self, logger, repo_conn, results_queue):
        inventory_update_sql = f"update {self.metadata_inventory} set domain = :1, algorithm_applied = :2, " \
                               f"last_profiling_run_id = :3, reviewed = :4 where inventory_id = :5"

        csv_output = []
        logger.info('Preparing repository updates after profiling')
        while True:
            try:
                next_result = results_queue.get_nowait()
            except Exception as excp:
                logger.info('All results have been read')
                break
            else:
                logger.debug(f'Updating repository with profiling results for : {next_result[0]}')
                logger.debug(f'Results to be updated : {len(next_result[1])}')
                logger.debug(f'Results for csv output : {len(next_result[2])}')
                for each_csv_result in next_result[2]:
                    csv_output.append(each_csv_result)

                success = self.oracle_dml_or_ddl(logger, repo_conn, inventory_update_sql, query_data=next_result[1])
                if not success:
                    logger.error(f'Failed to update results for context : {next_result[0]}')
                    return False, None
                logger.debug(f'Successfully updated results for context : {next_result[0]}')

        return True, csv_output
