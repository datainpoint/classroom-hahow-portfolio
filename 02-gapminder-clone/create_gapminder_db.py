import sqlite3
import pandas as pd

class CreateGapminderDB:
    def __init__(self):
        self.file_names = ["ddf--datapoints--gdp_pcap--by--country--time",
                           "ddf--datapoints--lex--by--country--time",
                           "ddf--datapoints--pop--by--country--time",
                           "ddf--entities--geo--country"]
        self.file_keys = ["gdp_pcap", "lex", "pop", "geo"]
    def import_as_dataframe(self):
        df_dict = dict()
        for file_name, file_key in zip(self.file_names, self.file_keys):
            file_path = f"data/{file_name}.csv"
            df = pd.read_csv(file_path)
            df_dict[file_key] = df
        return df_dict
    def create_database(self):
        connection = sqlite3.connect("data/gapminder.db")
        df_dict = self.import_as_dataframe()
        for k, v in df_dict.items():
            v.to_sql(name=k, con=connection, index=False, if_exists="replace")
        drop_view_sql = """
        DROP VIEW IF EXISTS plotting;
        """
        create_view_sql = """
        CREATE VIEW plotting AS
        SELECT gdp_pcap.country AS country_alpha3,
               geo.name AS country_name,
               gdp_pcap.time AS year,
               gdp_pcap.gdp_pcap AS gdp_per_capita,
               geo.world_4region AS continent,
               lex.lex AS life_expectancy,
               pop.pop AS population
          FROM gdp_pcap
          JOIN geo
            ON gdp_pcap.country = geo.country
          JOIN lex
            ON gdp_pcap.country = lex.country AND
               gdp_pcap.time = lex.time
          JOIN pop
            ON gdp_pcap.country = pop.country AND
               gdp_pcap.time = pop.time
         WHERE gdp_pcap.time < 2024;
        """
        cur = connection.cursor()
        cur.execute(drop_view_sql)
        cur.execute(create_view_sql)
        connection.close()

create_gapminder_db = CreateGapminderDB()
create_gapminder_db.create_database()