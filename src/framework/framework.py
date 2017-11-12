import importlib
import logging
import os
import sys
from datetime import datetime

import pandas as pd


class Framework:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def read_df(file_path, names):
        df = pd.read_csv(file_path, delimiter='|', header=None, names=names, skipinitialspace=True). \
            dropna(axis=1, how='all')

        return df

    @staticmethod
    def merge_data(users, events):
        users_df = Framework.read_df(file_path=users[0], names=users[1])

        event_dfs = list()
        for (event_type, event_path, event_names) in events:
            event_df = Framework.read_df(file_path=event_path, names=event_names)
            event_dfs.append(event_df)
        events_df = pd.concat(event_dfs, ignore_index=True)

        extended_events_df = events_df.merge(right=users_df, on='user_id', how='left')

        extended_events_df['hour'] = pd.to_datetime(extended_events_df['timestamp'], unit='s'). \
            apply(lambda df: datetime(year=df.year, month=df.month, day=df.day, hour=df.hour))

        return extended_events_df

    @staticmethod
    def import_modules(module_names):
        modules = dict()
        for module_name in module_names:
            modules[module_name] = importlib.import_module(module_name)

        return modules

    @staticmethod
    def split_and_evaluate_metrics(events_df, modules):
        grouped_events_df = events_df.groupby(['user_id', 'hour'])

        metrics = list()

        for (user_id, hour), user_hour_df in grouped_events_df:
            logging.info('user: {}; hour: {}'.format(user_id, hour))

            user_hour_df = user_hour_df.sort_values(by=['timestamp'])

            user_hour_metrics = dict()

            user_hour_metrics['user_id'] = user_id
            user_hour_metrics['datetime'] = hour

            for (module_name, module_imp) in modules.items():
                try:
                    user_hour_metrics.update(module_imp.Module.mapper(user_hour_df))
                except:
                    logging.error('Exception in module {}: {}'.format(module_name, sys.exc_info()[0]))

            user_hour_metrics_df = pd.DataFrame(user_hour_metrics, index=[0])

            metrics.append(user_hour_metrics_df)

        return pd.concat(metrics, ignore_index=True)

    def process(self):
        modules = Framework.import_modules(self.config['modules'])

        events_df = Framework.merge_data(users=self.config['users'], events=self.config['events'])

        metrics_df = Framework.split_and_evaluate_metrics(events_df=events_df, modules=modules)

        metrics_df.to_csv(os.path.join(self.config['output_path'], 'metrics.csv'), index=False, sep='|')
