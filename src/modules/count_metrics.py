from interface import implements
from pandasql import sqldf

from module import ModuleInterface


class Module(implements(ModuleInterface)):
    @staticmethod
    def mapper(user_hour_df):
        query = "select " \
                "   count(1) as total_events, " \
                "   sum(case when event_name like '%get_metadata%' then 1 else 0 end) as get_metadata_events " \
                "from " \
                "   user_hour_df;"

        metrics = sqldf(query, {'user_hour_df': user_hour_df})
        metrics_dict = {'total_events': int(metrics.iloc[0]['total_events']),
                        'get_metadata_events': int(metrics.iloc[0]['get_metadata_events'])}
        return metrics_dict
