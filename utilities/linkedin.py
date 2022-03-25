import ast
from database_engine import Connection, Table
import hashlib
import json
import pandas as pd
import re

def parse_connections_json():
    connection = Connection()
    select_statment = """
    select
        _id
        , second_degree_connections
    from
        companies c
    where
        c.second_degree_connections is not null
        and c.second_degree_connections <> '[]'
    """
    dataframe = pd.read_sql(select_statment, con=connection.connection)
    output_dataframe = pd.DataFrame(columns=["_id", "company_id", "employee_name", "employee_title", "connection_name"])
    for index, row in dataframe.iterrows():
        # Have to do this because of json.loads' funk around single quotes.
        row_json = json.loads(json.dumps(ast.literal_eval(row["second_degree_connections"])))
        for i in range(0, len(row_json)):
            for j in range(0, len(row_json[i]["connections"])):
                output_dataframe.loc[len(output_dataframe.index)] = [hashlib.md5((
                    str(row["_id"]) + row_json[i]["name"] + row_json[i]["connections"][j]).encode()
                ).hexdigest(),
                                            row["_id"],
                                            row_json[i]["name"],
                                            row_json[i]["title"],
                                            row_json[i]["connections"][j]]
        break
    return(output_dataframe)

def update_connections_table(dataframe):
    update_table = Table(name="connections", primary_key="_id")
    update_table.upsert(object=dataframe, object_type="dataframe")