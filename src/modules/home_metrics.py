from interface import implements

from module import ModuleInterface


class Module(implements(ModuleInterface)):
    @staticmethod
    def mapper(user_hour_df):
        home_visits = user_hour_df.loc[user_hour_df['page_name'].str.contains('home', na=False)].shape[0]

        solution_after_home_visits = 0
        is_home = False
        for index, row in user_hour_df.iterrows():
            if is_home and str(row['page_name']).find('solution') > -1:
                solution_after_home_visits = solution_after_home_visits + 1
                is_home = False
            elif str(row['page_name']).find('home') > -1:
                is_home = True

        metrics_dict = {'home_visits': home_visits,
                        'solution_after_home_visits': solution_after_home_visits}

        return metrics_dict
