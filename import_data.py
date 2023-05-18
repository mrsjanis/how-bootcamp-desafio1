from datetime import date
import pandas as pd
from utils.postgres import Postgres
from datetime import datetime

postgres = Postgres()


class ImportData:

    def __init__(self):
        self.postgres_conn = postgres.get_connection()
        self.current_datetime = datetime.today()
        self.df_file = None

    def read_file(self, file_name, file_path, file_type, file_extension):

        if file_extension == '.json':
            self.df_file = pd.read_json(file_path)
        elif file_extension == '.csv':
            self.df_file = pd.read_csv(file_path)
        else:
            raise Exception("File extension not accept")

        self.df_file.insert(1, 'insert_datetime', self.current_datetime)

        if file_type == 'NBA':
            if "Payroll" in file_name:
                table_name = 'nba_payroll'
                self.remove_caracters('payroll')
                self.remove_caracters('inflationAdjPayroll')
                self.column_to_float('payroll')
                self.column_to_float('inflationAdjPayroll')
            elif "Player Box Score Stats" in file_name:
                table_name = 'nba_box_score_stats'
            elif "Player Stats" in file_name:
                table_name = 'nba_player_stats'
                self.df_file = self.df_file.drop(columns=['Unnamed: 0.1'])
            elif "Salaries" in file_name:
                table_name = 'nba_player_salaries'
                self.remove_caracters('salary')
                self.remove_caracters('inflationAdjSalary')
                self.column_to_float('salary')
                self.column_to_float('inflationAdjSalary')
            else:
                raise Exception("File not accept")

            self.df_file = self.df_file.drop(columns=['Unnamed: 0'])
        elif file_type == 'TopTec':
            table_name = 'startups_hiring'

            self.df_file['jobs'] = self.df_file['jobs'].astype(str)
            self.df_file = self.df_file.drop(columns=['id'])
            self.remove_caracters('tags')
            self.remove_caracters('locations')
            self.remove_caracters('industries')
        else:
            raise Exception("File type not accept")

        self.df_file.to_sql(name=table_name, schema="public", con=self.postgres_conn, if_exists="replace", index=True,
                                index_label="id")
        postgres.create_pk(table_name, "id")

    def close_conn(self):
        postgres.close_connection()

    def remove_caracters(self, colum_name):
        self.df_file[colum_name] = self.df_file[colum_name].astype(str)
        self.df_file[colum_name] = self.df_file[colum_name].str.replace("'", '')
        self.df_file[colum_name] = self.df_file[colum_name].str.replace('[', '')
        self.df_file[colum_name] = self.df_file[colum_name].str.replace(']', '')
        self.df_file[colum_name] = self.df_file[colum_name].str.replace("$", '')

    def column_to_int(self, colum_name):
        self.df_file[colum_name] = self.df_file[colum_name].astype(int)

    def column_to_float(self, colum_name):
        self.df_file[colum_name] = self.df_file[colum_name].str.replace(",", '')
        self.df_file[colum_name] = self.df_file[colum_name].astype(float).round(2)