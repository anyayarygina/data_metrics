from interface import implements

from module import ModuleInterface


class Module(implements(ModuleInterface)):
    @staticmethod
    def mapper(user_hour_df):
        user_hour_df['timestamp_gap'] = user_hour_df['timestamp'] - user_hour_df['timestamp'].shift(1)
        metrics_dict = {'mean_session_gap': user_hour_df["timestamp_gap"].mean(),
                        'session_length': user_hour_df["timestamp_gap"].sum()}

        raise ValueError

        return metrics_dict
