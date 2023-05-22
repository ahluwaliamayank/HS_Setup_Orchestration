from pathlib import Path
import argparse
import json
import datetime as dt
import sys
import csv

import log_manager.log_config as log_config
import utilities.encryption_manager as encryption_manager


class Utilities(encryption_manager.EncryptionManager):

    def __init__(self):

        self.config_file = Path(__file__).parent.resolve().with_name('config') / 'app_config.json'
        self.output_dir = Path(__file__).parent.parent.resolve().with_name('output_files')
        self.log_dir = Path(__file__).parent.parent.resolve().with_name('logs')
        self.metadata_refresh_logs = self.log_dir / 'metadata_refresh_logs'
        self.profiling_logs = self.log_dir / 'profiling_logs'
        self.proc_log_dir = self.log_dir / 'proc_logs'
        self.curr_raw_ts = dt.datetime.now()
        self.curr_ts = self.curr_raw_ts.strftime("%d%m%Y_%H%M%S")
        self.config_dict = dict()
        self.config_dict['session_raw_ts'] = self.curr_raw_ts
        self.config_dict['session_ts'] = self.curr_ts

        parser = argparse.ArgumentParser()
        parser.add_argument('--get_metadata', default=False, required=False, action='store_true')
        parser.add_argument('--profile', default=False, required=False, action='store_true')
        parser.add_argument('--setup_hyperscale_dataset', default=False, required=False, action='store_true')
        parser.add_argument('--add_context', default=False, required=False, action='store_true')
        parser.add_argument('--remove_context', default=False, required=False, action='store_true')
        parser.add_argument('-context_name', type=str, default=None, required=False)
        parser.add_argument('--all', default=True, required=False, action='store_true')
        parser.add_argument('--debug', default=False, required=False, action='store_true')
        args = parser.parse_args()
        if args.debug:
            self.config_dict['debug_mode'] = True
        else:
            self.config_dict['debug_mode'] = False
        if (
                args.get_metadata or args.profile or args.setup_hyperscale_dataset or args.add_context or
                args.remove_context
        ):
            args.all = False

        self.validate_terminal_options(args)

    def validate_terminal_options(self, args):
        self.config_dict['all'] = False
        self.config_dict['get_metadata'] = False
        self.config_dict['profile'] = False
        self.config_dict['setup_hyperscale_dataset'] = False
        self.config_dict['add_context'] = False
        self.config_dict['remove_context'] = False

        if args.all:
            print('Chosen option is to run entire workflow')
            self.config_dict['all'] = True
        elif args.get_metadata:
            print('Chosen option is to get and sync metadata')
            self.config_dict['get_metadata'] = True
        elif args.profile:
            print('Chosen option is to run profiling')
            self.config_dict['profile'] = True
        elif args.setup_hyperscale_dataset:
            print('Chosen option is to set up hyperscale dataset')
            self.config_dict['setup_hyperscale_dataset'] = True
        elif args.add_context:
            print('Chosen option is to add a new context')
            self.config_dict['add_context'] = True
            if not args.context_name:
                print('Context name is mandatory when adding a context, please supply context name')
                self.exit_on_error()
            else:
                self.config_dict['context_name'] = args.context_name
        elif args.remove_context:
            print('Chosen option is to remove a context')
            self.config_dict['remove_context'] = True
            if not args.context_name:
                print('Context name is mandatory when removing a context, please supply context name')
                self.exit_on_error()
            else:
                self.config_dict['context_name'] = args.context_name

        else:
            print('Please select a valid option')
            self.exit_on_error()

    def read_app_config(self):
        print(f'Reading config from : {self.config_file}')
        with open(self.config_file, encoding="utf-8") as f:
            application_properties = json.loads(f.read())

        if 'base_key' not in application_properties or not application_properties['base_key']:
            print('Base key is missing in config')
            self.exit_on_error()

        self.config_dict['base_key'] = application_properties['base_key']

        if 'secondary_key' not in application_properties or not application_properties['secondary_key']:
            print('Secondary key is missing in config')
            self.exit_on_error()

        self.config_dict['salt'] = application_properties['secondary_key']

        self.config_dict['profiling_runs'] = application_properties['repository_tables']['profiling_runs']
        self.config_dict['context_master'] = application_properties['repository_tables']['context_master']
        self.config_dict['metadata_refreshes'] = application_properties['repository_tables']['metadata_refreshes']
        self.config_dict['metadata_inventory'] = application_properties['repository_tables']['metadata_inventory']
        self.config_dict['hyperscale_datasets'] = application_properties['repository_tables']['hyperscale_datasets']
        self.config_dict['hyperscale_dataset_refreshes'] = \
            application_properties['repository_tables']['hyperscale_dataset_refreshes']
        self.config_dict['date_formats'] = application_properties['repository_tables']['date_formats']

        self.config_dict['max_parallel_processes'] = application_properties['max_parallel_processes']

        self.config_dict['repository_details'] = dict()

        if (
                'database_connections' not in application_properties or
                'repository' not in application_properties['database_connections'] or not
                application_properties['database_connections']['repository']
        ):
            print('Repository connection details missing in config')
            self.exit_on_error()

        self.config_dict['repository_details']['host'] = \
            application_properties['database_connections']['repository']['db_host']
        self.config_dict['repository_details']['port'] = \
            application_properties['database_connections']['repository']['db_port']
        self.config_dict['repository_details']['use_sid'] = \
            application_properties['database_connections']['repository']['use_sid']
        self.config_dict['repository_details']['sid_or_service'] = \
            application_properties['database_connections']['repository']['db_sid_or_service']
        self.config_dict['repository_details']['user_name'] = \
            self.decrypt(self.config_dict['base_key'], self.config_dict['salt'], 'repository_user',
                         application_properties['database_connections']['repository']['db_user'])
        self.config_dict['repository_details']['password'] = \
            self.decrypt(self.config_dict['base_key'], self.config_dict['salt'], 'repository_password',
                         application_properties['database_connections']['repository']['db_password'])

        self.config_dict['source_details'] = dict()

        if (
                'database_connections' not in application_properties or
                'data_source' not in application_properties['database_connections'] or not
                application_properties['database_connections']['data_source']
        ):
            print('Source connection details missing in config')
            self.exit_on_error()

        self.config_dict['source_details']['host'] = \
            application_properties['database_connections']['data_source']['db_host']
        self.config_dict['source_details']['port'] = \
            application_properties['database_connections']['data_source']['db_port']
        self.config_dict['source_details']['use_sid'] = \
            application_properties['database_connections']['data_source']['use_sid']
        self.config_dict['source_details']['sid_or_service'] = \
            application_properties['database_connections']['data_source']['db_sid_or_service']
        self.config_dict['source_details']['user_name'] = \
            self.decrypt(self.config_dict['base_key'], self.config_dict['salt'], 'data_source_user',
                         application_properties['database_connections']['data_source']['db_user'])
        self.config_dict['source_details']['password'] = \
            self.decrypt(self.config_dict['base_key'], self.config_dict['salt'], 'data_source_password',
                         application_properties['database_connections']['data_source']['db_password'])

        self.config_dict['engines'] = dict()
        self.config_dict['num_engines'] = len(application_properties['masking_engines'])
        for x in range(1, self.config_dict['num_engines']+1):
            engine_details = dict()
            if f'engine{x}' not in application_properties['masking_engines']:
                print(f'Invalid configuration for masking engines, '
                      f'each engine should have the key in format engine1, engine2...')
                self.exit_on_error()
            engine_details['url'] = f"{application_properties['masking_engines'][f'engine{x}']['access_url']}/api"
            engine_details['user'] = self.decrypt(
                self.config_dict['base_key'], self.config_dict['salt'], 'compliance_engine_user',
                application_properties['masking_engines'][f'engine{x}']['user']
            )
            engine_details['password'] = self.decrypt(
                self.config_dict['base_key'], self.config_dict['salt'], 'compliance-engine-password',
                application_properties['masking_engines'][f'engine{x}']['password']
            )
            self.config_dict['engines'][f'engine{x}'] = engine_details

        self.config_dict['jobs_per_engine'] = application_properties['max_jobs_per_engine']
        self.config_dict['tables_per_dataset'] = application_properties['max_tables_per_ruleset']
        self.config_dict['profile_set_name'] = application_properties['profile_set_name']
        self.config_dict['streams_for_profile_job'] = application_properties['num_streams_for_profile_job']
        self.config_dict['max_memory_for_profiling_job'] = application_properties['max_memory_for_profiling_job_in_MB']

        self.config_dict['hyperscale_access_url'] = application_properties['hyperscale_config']['access_url']
        self.config_dict['hyperscale_access_key'] = f"apk {application_properties['hyperscale_config']['api_key']}"
        self.config_dict['hyperscale_connector_id'] = application_properties['hyperscale_config']['connector_id']
        self.config_dict['file_mount_id'] = application_properties['hyperscale_config']['mount_filesystem_id']
        self.config_dict['use_thick_client'] = application_properties['use_thick_client']
        self.config_dict['date_domains'] = [domain.lower() for domain in application_properties['date_domains']]

    def get_logger(self):
        log_file = self.log_dir / f'app_log_{self.curr_ts}.log'
        if self.config_dict['debug_mode']:
            log_object = log_config.LogConfig(log_file, debug=True)
        else:
            log_object = log_config.LogConfig(log_file, debug=False)
        app_logger = log_object.get_module_logger()
        print(f'Check logs in : {log_file}')
        return log_object, app_logger

    def setup_mp_logging(self, proc_num, log_ts, debug_mode, refresh_id, log_type=None):
        if log_type == 'metadata_refresh':
            log_file = self.metadata_refresh_logs / f'{refresh_id}_proc_num_{proc_num}_{log_ts}.log'
        elif log_type == 'profile':
            log_file = self.profiling_logs / f'{refresh_id}_engine{proc_num}_{log_ts}.log'
        else:
            log_file = self.proc_log_dir / f'app_log_{log_ts}.log'

        if debug_mode:
            proc_log_object = log_config.LogConfig(log_file, debug=True)
        else:
            proc_log_object = log_config.LogConfig(log_file, debug=False)

        return proc_log_object

    def write_output_to_csv(self, logger, run_id, rows):
        logger.info('Writing to file')
        column_headers = [('Schema_Name', 'Table_Name', 'Column_Name', 'Domain', 'Algorithm_Applied', 'Reviewed')]
        file_name = self.output_dir / f'Profiling_Run_{run_id}.csv'
        with open(file_name, 'w+', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(column_headers)
            writer.writerows(rows)
        print(f'Please check output of profiling rnn in : {file_name}')

    def terminate_logging(self, log_object, print_out=True):
        if print_out:
            print('Terminating logging')
        log_object.close_handler()

    def exit_on_error(self, log_object=None):
        print('Operation failed, check logs')
        if log_object:
            self.terminate_logging(log_object)
        sys.exit(1)