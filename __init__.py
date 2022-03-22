import ast
import json
import os
import pandas as pd
import re
from sqlalchemy import create_engine

class Connection:
    def __init__(self):
        self.engine = create_engine(os.environ["POSTGRES_CONNECTION_URL"])
        self.connection = self.engine.raw_connection()

    def close(self):
        self.connection.close()
        self.engine.dispose()

class Table(Connection):
    def __init__(self, name, primary_key):
        super(Table, self).__init__()
        self.name = name
        self.primary_key = primary_key
        self.columns = ast.literal_eval((re.sub("\)$", "",
                     re.sub("^RMKeyView\(", "",
                        str((self.engine.execute("select * from " + self.name + " limit 1").keys()))))))

    def split_create_update_from_dataframe(self, dataframe):
        overlap_rows = []
        rows = self.engine.execute("select " + self.primary_key + " from " + self.name + " where " + self.primary_key + " in (" + ",".join([str(_id) for _id in dataframe[self.primary_key].to_list()]) + ")").fetchall()
        for row in rows:
            overlap_rows.append(row[0])
        return(dataframe[~dataframe[self.primary_key].isin(overlap_rows)], dataframe[dataframe[self.primary_key].isin(overlap_rows)])

    def update_rows(self, dataframe):
        overlapping_id_string = ",".join([str(_id) for _id in dataframe[self.primary_key].to_list()])

        current_database_rows = pd.read_sql(
            "select * from " + self.name + " where " + self.primary_key + " in (" + overlapping_id_string  + ")",
        con=self.engine)
        dataframe.set_index("_id")
        current_database_rows_rollback = current_database_rows.copy()
        current_database_rows.set_index("_id")
        current_database_rows.update(dataframe, join="left", overwrite=True)

        try:
            self.engine.execute("delete from " + self.name + " where " + self.primary_key + " in (" + overlapping_id_string + ")")
            result = current_database_rows.to_sql(name=self.name, con=self.engine, if_exists="append", index=False)
            return(result)
        except Exception as e:
            self.engine.execute(
                "delete from " + self.name + " where " + self.primary_key + " in (" + overlapping_id_string + ")")
            result = current_database_rows_rollback.to_sql(name=self.name, con=self.engine, if_exists="append", index=False)
            return(-1)

    def remove_invalid_columns(self, dataframe):
        return(dataframe.drop(columns=[col for col in dataframe if col not in self.columns]))

    def upsert_from_json(self, json_records):
        dataframe = pd.read_json(str(json.dumps(json_records)), orient="records")
        dataframe = self.remove_invalid_columns(dataframe)
        (create_dataframe, update_dataframe) = self.split_create_update_from_dataframe(dataframe=dataframe)

        if len(update_dataframe.index) > 0:
            update_result = self.update_rows(update_dataframe)
        if len(create_dataframe.index) > 0:
            create_result = create_dataframe.to_sql(name=self.name, con=self.engine, if_exists="append", index=False)
        return()