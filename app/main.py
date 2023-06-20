import utilities.utils as utils
import oracle_modules.oracle_connection_manager as conn_manager
import oracle_modules.oracle_utilities as db_utils
import profiling_modules.profiler as profiler
import hyperscale_modules.hyperscale_ops as hyperscale_ops

import multiprocessing as mp
import os
import datetime as dt
import math

if __name__ == "__main__":
    start_ts = dt.datetime.now()
    num_errors = 0
    utils_obj = utils.Utilities()
    process_manager = mp.Manager()
    shared_queue = process_manager.Queue()
    dataset_queue = process_manager.Queue()
    results_queue = process_manager.Queue()
    execution_queue_big_contexts = process_manager.Queue()
    execution_queue_small_contexts = process_manager.Queue()
    error_queue = process_manager.Queue()
    utils_obj.read_app_config()

    log_object, app_logger = utils_obj.get_logger()
    app_logger.info(f'Starting Program, process ID : {os.getpid()}')

    if utils_obj.config_dict['get_metadata'] or utils_obj.config_dict['all']:
        app_logger.info('Beginning Metadata Refresh workflow')
        conn_manager_inst = conn_manager.OracleConnectionManager(utils_obj.config_dict['use_thick_client'])
        app_logger.info('Establishing connection to repository')

        success, repo_connection_params = conn_manager_inst.get_connection_params(
            app_logger, utils_obj.config_dict['repository_details'])

        if not success:
            app_logger.error('Failed to create connection params for repository db')
            utils_obj.exit_on_error(log_object)

        success, repo_conn = conn_manager_inst.create_connection(app_logger, repo_connection_params)
        if not success or not repo_conn:
            app_logger.error("Failed to create connection to repository db, aborting")
            utils_obj.exit_on_error(log_object)

        app_logger.info('Successfully connected to repository db')

        db_utils_inst = db_utils.OracleUtilities(utils_obj.config_dict)

        success, contexts = db_utils_inst.get_all_contexts(app_logger, repo_conn)

        if not success:
            app_logger.error('Failed to get list of contexts, aborting')

            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            utils_obj.exit_on_error(log_object)

        print(f'Total contexts : {len(contexts)}')

        success, new_refresh_id = db_utils_inst.insert_refresh_run(app_logger, repo_conn,
                                                                   utils_obj.config_dict['session_raw_ts'])

        if not success or not new_refresh_id:
            app_logger.error('Failed to insert new refresh request, aborting')

            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            utils_obj.exit_on_error(log_object)

        print(f'New refresh request submitted, refresh id : {new_refresh_id}')

        success = db_utils_inst.update_refresh_run_status(app_logger, repo_conn, new_refresh_id)

        if not success:
            app_logger.error('Failed to update status for refresh request, aborting')

            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            utils_obj.exit_on_error(log_object)

        num_processes = min(len(contexts), int(utils_obj.config_dict['max_parallel_processes']))

        app_logger.info(f'Parallel degree used for refreshing metadata : {num_processes}')

        for each_context in contexts:
            shared_queue.put([each_context[0], each_context[1]])

        refresh_args_list = list()

        for x in range(num_processes):
            refresh_args_list.append((x+1, shared_queue, new_refresh_id, utils_obj))

        app_logger.info('Distributing flow into individual processes')

        with mp.Pool(processes=num_processes) as pool:
            results = pool.starmap(db_utils_inst.metadata_inventory_refresh_coordinator, refresh_args_list)

        if sum(results) == num_processes:
            success = db_utils_inst.update_refresh_run_status(app_logger, repo_conn, new_refresh_id,
                                                              status='Completed')
        else:
            success = db_utils_inst.update_refresh_run_status(app_logger, repo_conn, new_refresh_id,
                                                              status='Failed')
            num_errors += 1

        if not success:
            app_logger.error('Failed to update status for refresh request, aborting')
            num_errors += 1

        success = conn_manager_inst.close_connection(app_logger, repo_conn)
        if not success:
            app_logger.error('Failed to close connection to repository db')
        else:
            app_logger.info('Closed connection to repository db')

        if num_errors > 0:
            app_logger.info('Metadata Refresh workflow completed with errors')
            utils_obj.exit_on_error(log_object)

        app_logger.info('Metadata Refresh workflow completed successfully')

        del repo_connection_params
        del repo_conn
        del db_utils_inst
        del conn_manager_inst
        del shared_queue

    if utils_obj.config_dict['profile'] or utils_obj.config_dict['all']:
        app_logger.info('Beginning Profiling workflow')

        conn_manager_inst = conn_manager.OracleConnectionManager(utils_obj.config_dict['use_thick_client'])
        app_logger.info('Establishing connection to repository')

        success, repo_connection_params = conn_manager_inst.get_connection_params(
            app_logger, utils_obj.config_dict['repository_details'])

        if not success:
            app_logger.error('Failed to create connection params for repository db')
            utils_obj.exit_on_error(log_object)

        success, repo_conn = conn_manager_inst.create_connection(app_logger, repo_connection_params)
        if not success or not repo_conn:
            app_logger.error("Failed to create connection to repository db, aborting")
            utils_obj.exit_on_error(log_object)

        app_logger.info('Successfully connected to repository db')

        db_utils_inst = db_utils.OracleUtilities(utils_obj.config_dict)
        success, tables = db_utils_inst.get_tables_for_profiling(app_logger, repo_conn)
        if not success:
            app_logger.error('Failed to get tables for profiling, aborting')
            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')
            utils_obj.exit_on_error(log_object)

        if not tables or len(tables) < 1:
            app_logger.info('No tables to profile')
            print('No tables to profile')
            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')
        else:
            success, inventory_map = db_utils_inst.build_context_inventory_map(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to build inventory map for profiling, aborting')
                success = conn_manager_inst.close_connection(app_logger, repo_conn)
                if not success:
                    app_logger.error('Failed to close connection to repository db')
                else:
                    app_logger.info('Closed connection to repository db')
                utils_obj.exit_on_error(log_object)

            if not inventory_map or len(inventory_map) < 1:
                app_logger.error('No inventory found for profiling, aborting')
                success = conn_manager_inst.close_connection(app_logger, repo_conn)
                if not success:
                    app_logger.error('Failed to close connection to repository db')
                else:
                    app_logger.info('Closed connection to repository db')
                utils_obj.exit_on_error(log_object)

            success, new_profiling_run_id = db_utils_inst.insert_profiling_run(app_logger, repo_conn,
                                                                               utils_obj.config_dict['session_raw_ts'])

            if not success or not new_profiling_run_id:
                app_logger.error('Failed to insert new profiling run request, aborting')

                success = conn_manager_inst.close_connection(app_logger, repo_conn)
                if not success:
                    app_logger.error('Failed to close connection to repository db')
                else:
                    app_logger.info('Closed connection to repository db')

                utils_obj.exit_on_error(log_object)

            print(f'New profiling run request submitted, run id : {new_profiling_run_id}')

            success = db_utils_inst.update_profiling_run_status(app_logger, repo_conn, new_profiling_run_id)

            if not success:
                app_logger.error('Failed to update status for profiling run, aborting')

                success = conn_manager_inst.close_connection(app_logger, repo_conn)
                if not success:
                    app_logger.error('Failed to close connection to repository db')
                else:
                    app_logger.info('Closed connection to repository db')

                utils_obj.exit_on_error(log_object)

            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            profiler_inst = profiler.Profiler(utils_obj.config_dict, inventory_map)
            success = profiler_inst.get_engine_authorizations(app_logger)
            if not success:
                app_logger.error('Failed to get engine authorizations')
                utils_obj.exit_on_error(log_object)

            success = profiler_inst.get_profile_set_ids(app_logger)
            if not success:
                app_logger.error('Failed to get profile set ids')
                utils_obj.exit_on_error(log_object)

            success = profiler_inst.manage_application_in_compliance_engine(app_logger)
            if not success:
                app_logger.error('Failed to manage applications in compliance engines')
                utils_obj.exit_on_error(log_object)

            success = profiler_inst.manage_environment_in_compliance_engine(app_logger)
            if not success:
                app_logger.error('Failed to create environment in compliance engines')
                utils_obj.exit_on_error(log_object)

            unique_contexts = list(set([tup[0] for tup in tables]))
            jdbc_url = db_utils_inst.create_jdbc_url(app_logger, utils_obj.config_dict)
            utils_obj.config_dict['source_details']['jdbc_url'] = jdbc_url

            success = profiler_inst.create_connectors(app_logger, utils_obj.config_dict, unique_contexts)
            if not success:
                app_logger.error('Failed to create connectors in compliance engines')
                utils_obj.exit_on_error(log_object)

            success, datasets = profiler_inst.split_into_datasets(app_logger, tables, unique_contexts)
            if not success:
                app_logger.error('Failed to split tables into datasets')
                utils_obj.exit_on_error(log_object)

            num_processes = utils_obj.config_dict['num_engines']
            app_logger.info(f'Parallel degree used for profiling : {num_processes}')

            for each_dataset in datasets:
                dataset_queue.put(each_dataset)

            profile_args_list = list()

            for x in range(num_processes):
                profile_args_list.append((x + 1, dataset_queue, new_profiling_run_id, utils_obj, results_queue))

            app_logger.info('Distributing flow into individual processes')

            with mp.Pool(processes=num_processes) as pool:
                results = pool.starmap(profiler_inst.profiling_coordinator, profile_args_list)

            if sum(results) == num_processes:
                pass
            else:
                num_errors += 1

            success, repo_conn = conn_manager_inst.create_connection(app_logger, repo_connection_params)
            if not success or not repo_conn:
                app_logger.error("Failed to create connection to repository db, aborting")
                utils_obj.exit_on_error(log_object)

            app_logger.info('Successfully connected to repository db')

            if num_errors > 0:
                app_logger.info('Profiling workflow completed with errors')
                success = db_utils_inst.update_profiling_run_status(app_logger, repo_conn, new_profiling_run_id,
                                                                    status='Failed')
                if not success:
                    app_logger.error('Failed to update status for profiling run, aborting')

                success = conn_manager_inst.close_connection(app_logger, repo_conn)
                if not success:
                    app_logger.error('Failed to close connection to repository db')
                else:
                    app_logger.info('Closed connection to repository db')
                utils_obj.exit_on_error(log_object)

            if results_queue.qsize() > 0:
                success, csv_output = db_utils_inst.update_profiling_results(app_logger, repo_conn, results_queue)
                if not success:
                    num_errors += 1
                else:
                    utils_obj.write_output_to_csv(app_logger, new_profiling_run_id, csv_output)

            if num_errors > 0:
                app_logger.info('Profiling workflow completed with errors')
                success = db_utils_inst.update_profiling_run_status(app_logger, repo_conn, new_profiling_run_id,
                                                                    status='Failed')
                if not success:
                    app_logger.error('Failed to update status for profiling run, aborting')

                success = conn_manager_inst.close_connection(app_logger, repo_conn)
                if not success:
                    app_logger.error('Failed to close connection to repository db')
                else:
                    app_logger.info('Closed connection to repository db')
                utils_obj.exit_on_error(log_object)

            success = db_utils_inst.update_profiling_run_status(app_logger, repo_conn, new_profiling_run_id,
                                                                status='Completed')
            if not success:
                app_logger.error('Failed to update status for profiling run, aborting')

            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            app_logger.info('Profiling workflow completed successfully')

            del profiler_inst
            del inventory_map

        del db_utils_inst
        del conn_manager_inst
        del dataset_queue
        del results_queue

    if utils_obj.config_dict['setup_hyperscale_dataset'] or utils_obj.config_dict['all']:
        app_logger.info('Beginning Hyperscale Ops workflow')

        num_errors = 0

        conn_manager_inst = conn_manager.OracleConnectionManager(utils_obj.config_dict['use_thick_client'])
        app_logger.info('Establishing connection to repository')

        success, repo_connection_params = conn_manager_inst.get_connection_params(
            app_logger, utils_obj.config_dict['repository_details'])

        if not success:
            app_logger.error('Failed to create connection params for repository db')
            utils_obj.exit_on_error(log_object)

        success, repo_conn = conn_manager_inst.create_connection(app_logger, repo_connection_params)
        if not success or not repo_conn:
            app_logger.error("Failed to create connection to repository db, aborting")
            utils_obj.exit_on_error(log_object)

        app_logger.info('Successfully connected to repository db')

        db_utils_inst = db_utils.OracleUtilities(utils_obj.config_dict)

        success, new_hyperscale_dataset_refresh_id = db_utils_inst.insert_hyperscale_dataset_refresh_run(
            app_logger, repo_conn, utils_obj.config_dict['session_raw_ts'])

        if not success or not new_hyperscale_dataset_refresh_id:
            app_logger.error('Failed to insert new hyperscale dataset refresh request, aborting')
            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            utils_obj.exit_on_error(log_object)

        print(f'New hyperscale dataset refresh request submitted, run id : {new_hyperscale_dataset_refresh_id}')

        success = db_utils_inst.update_hyperscale_dataset_refresh_run_status(
            app_logger, repo_conn, new_hyperscale_dataset_refresh_id)

        if not success:
            app_logger.error('Failed to update status for hyperscale dataset refresh, aborting')

            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            utils_obj.exit_on_error(log_object)

        success, inventory = db_utils_inst.get_inventory_for_masking(app_logger, repo_conn)
        if not success:
            app_logger.error('Failed to get inventory for masking, aborting')
            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')
            utils_obj.exit_on_error(log_object)

        if not inventory or len(inventory) < 1:
            app_logger.error('No inventory for masking, aborting')
            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')
            utils_obj.exit_on_error(log_object)

        hyperscale_ops_inst = hyperscale_ops.Hyperscale_Ops(utils_obj.config_dict)
        success = hyperscale_ops_inst.refresh_datasets(app_logger, inventory, repo_conn, db_utils_inst,
                                                       new_hyperscale_dataset_refresh_id)

        if not success:
            app_logger.error('Failed to update hyperscale datasets')
            app_logger.info('Hyperscale dataset refresh workflow completed with errors')
            success = db_utils_inst.update_hyperscale_dataset_refresh_run_status(
                app_logger, repo_conn, new_hyperscale_dataset_refresh_id, status='Failed')
            if not success:
                app_logger.error('Failed to update status for hyperscale dataset refresh, aborting')

            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')
            utils_obj.exit_on_error(log_object)

        success = db_utils_inst.update_hyperscale_dataset_refresh_run_status(
            app_logger, repo_conn, new_hyperscale_dataset_refresh_id, status='Completed')
        if not success:
            app_logger.error('Failed to update status for hyperscale dataset refresh, aborting')

        success = conn_manager_inst.close_connection(app_logger, repo_conn)
        if not success:
            app_logger.error('Failed to close connection to repository db')
        else:
            app_logger.info('Closed connection to repository db')

        app_logger.info('Hyperscale Ops workflow completed successfully')

        del hyperscale_ops_inst
        del db_utils_inst
        del conn_manager_inst

    if utils_obj.config_dict['execute_hyperscale_group'] or utils_obj.config_dict['all']:
        app_logger.info('Beginning hyperscale group execution')

        num_errors = 0

        conn_manager_inst = conn_manager.OracleConnectionManager(utils_obj.config_dict['use_thick_client'])
        app_logger.info('Establishing connection to repository')

        success, repo_connection_params = conn_manager_inst.get_connection_params(
            app_logger, utils_obj.config_dict['repository_details'])

        if not success:
            app_logger.error('Failed to create connection params for repository db')
            utils_obj.exit_on_error(log_object)

        success, repo_conn = conn_manager_inst.create_connection(app_logger, repo_connection_params)
        if not success or not repo_conn:
            app_logger.error("Failed to create connection to repository db, aborting")
            utils_obj.exit_on_error(log_object)

        app_logger.info('Successfully connected to repository db')

        db_utils_inst = db_utils.OracleUtilities(utils_obj.config_dict)

        success, jobs_ids = db_utils_inst.get_jobs_for_execution(app_logger, repo_conn)
        if not success:
            app_logger.error('Failed to get job ids for masking, aborting')
            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')
            utils_obj.exit_on_error(log_object)

        total_contexts = len(jobs_ids)
        print(f'Total contexts to be executed : {total_contexts}')
        app_logger.info(f'Total contexts to be executed : {total_contexts}')
        app_logger.debug(f'Context and job details : {jobs_ids}')

        num_processes = 2
        app_logger.info(f'Parallel degree used for execution : {num_processes}')

        success = conn_manager_inst.close_connection(app_logger, repo_conn)
        if not success:
            app_logger.error('Failed to close connection to repository db')
        else:
            app_logger.info('Closed connection to repository db')

        big_contexts = math.ceil(0.3 * total_contexts)

        big_context_jobs = jobs_ids[:big_contexts]
        small_context_jobs = jobs_ids[big_contexts:]

        small_context_jobs = sorted(small_context_jobs, key=lambda z: z[1])

        app_logger.info(f'Contexts to be processed as big : {len(big_context_jobs)}')
        app_logger.debug(f'Big contexts : {big_context_jobs}')
        app_logger.info(f'Contexts to be processed as small : {len(small_context_jobs)}')
        app_logger.debug(f'Small contexts : {small_context_jobs}')

        hyperscale_ops_inst = hyperscale_ops.Hyperscale_Ops(utils_obj.config_dict)

        if not utils_obj.config_dict['execution_preview_only']:

            for each_job in big_context_jobs:
                execution_queue_big_contexts.put(each_job)

            for each_job in small_context_jobs:
                execution_queue_small_contexts.put(each_job)

            execution_args = list()

            for x in range(num_processes):
                execution_args.append((x + 1, utils_obj, execution_queue_big_contexts, execution_queue_small_contexts,
                                       error_queue))

            app_logger.info('Distributing flow into individual processes')

            with mp.Pool(processes=num_processes) as pool:
                results = pool.starmap(hyperscale_ops_inst.job_coordinator, execution_args)

            if sum(results) == num_processes:
                pass
            else:
                num_errors += 1

        if num_errors > 0:
            app_logger.info('Hyperscale execution workflow completed with errors')
        else:
            app_logger.info('Hyperscale execution workflow completed successfully')

        del db_utils_inst
        del conn_manager_inst
        del hyperscale_ops_inst

    if utils_obj.config_dict['add_context'] or utils_obj.config_dict['remove_context']:
        conn_manager_inst = conn_manager.OracleConnectionManager(utils_obj.config_dict['use_thick_client'])
        app_logger.info('Establishing connection to repository')

        success, repo_connection_params = conn_manager_inst.get_connection_params(
            app_logger, utils_obj.config_dict['repository_details'])

        if not success:
            app_logger.error('Failed to create connection params for repository db')
            utils_obj.exit_on_error(log_object)

        success, repo_conn = conn_manager_inst.create_connection(app_logger, repo_connection_params)
        if not success or not repo_conn:
            app_logger.error("Failed to create connection to repository db, aborting")
            utils_obj.exit_on_error(log_object)

        app_logger.info('Successfully connected to repository db')

        db_utils_inst = db_utils.OracleUtilities(utils_obj.config_dict)

        success = db_utils_inst.handle_context(app_logger, repo_conn, utils_obj.config_dict)

        if not success:
            app_logger.error('Failed to handle context, aborting')
            success = conn_manager_inst.close_connection(app_logger, repo_conn)
            if not success:
                app_logger.error('Failed to close connection to repository db')
            else:
                app_logger.info('Closed connection to repository db')

            utils_obj.exit_on_error(log_object)

        success = conn_manager_inst.close_connection(app_logger, repo_conn)
        if not success:
            app_logger.error('Failed to close connection to repository db')
        else:
            app_logger.info('Closed connection to repository db')

        del repo_connection_params
        del repo_conn
        del db_utils_inst
        del conn_manager_inst

    app_logger.info('Application terminated')
    utils_obj.terminate_logging(log_object)

    print('End')
    end_ts = dt.datetime.now()
    print(f'Elapsed time: {end_ts - start_ts}')

    del utils_obj


