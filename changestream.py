import database_engine.auth__database_engine
import dictdiffer
import json
import os
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from queuing_engine import Queue
import re

class ChangeStream:
    def __init__(self, slot_name, table_name, schema_name="public"):
        pg_creds = {}
        (creds, locator) = re.sub(r"^postgresql://", "", os.environ["POSTGRES_CONNECTION_URL"]).split("@")
        (pg_creds["user"], pg_creds["password"]) = creds.split(":")
        (pg_creds["host"], parms) = locator.split(":")
        (pg_creds["port"], pg_creds["database"]) = parms.split("/")

        self.connection = psycopg2.connect("dbname='" + pg_creds["database"] + \
                               "' host='" + pg_creds["host"] + \
                               "' user='" + pg_creds["user"] + \
                               "' password='" + pg_creds["password"] + "'",
                               connection_factory = LogicalReplicationConnection)

        self.cursor = self.connection.cursor()
        self.slot_name = slot_name
        try:
            self.cursor.drop_replication_slot(self.slot_name)
        except Exception as e:
            print(e)
            pass

        self.cursor.create_replication_slot(self.slot_name, output_plugin="wal2json")
        self.cursor.start_replication(slot_name=self.slot_name, options={"pretty-print" : 1, "include-origin": 1, "add-tables": schema_name + "." + table_name}, decode=True)

class Consumer(ChangeStream):
    def __init__(self, slot_name, table_name, schema_name="public"):
        super(Consumer, self).__init__(slot_name=slot_name, table_name=table_name, schema_name=schema_name)
        self.queue_name = "changestream-" + table_name
        self.queue = Queue(queue_name=self.queue_name)

    def callback__consume(self, msg):
        payload = json.loads(msg.payload)
        self.queue_changestream(payload)

    def consume(self):
        self.cursor.consume_stream(self.callback__consume)

    def queue_changestream(self, payload):
        full_results_json = []
        for change in payload["change"]:
            changes_json = []
            new_keyvalues = {}
            try:
                for i in range(0, len(change["columnnames"])):
                    new_keyvalues[change["columnnames"][i]] = change["columnvalues"][i]
            except:
                pass

            current_keyvalues = {}
            try:
                for i in range(0, len(change["oldkeys"]["keynames"])):
                    current_keyvalues[change["oldkeys"]["keynames"][i]] = change["oldkeys"]["keyvalues"][i]
            except:
                pass

            if change["kind"] == "update":
                results = list(dictdiffer.diff(current_keyvalues, new_keyvalues))

                row_change_list = []
                for result in results:
                    if result[0] == "change":
                        row_change_list.append({
                            "column_name": result[1],
                            "new_value": result[2][1],
                            "current_value": result[2][0]
                        })
                changes_json.append({"_id": new_keyvalues["_id"], "kind": change["kind"], "changes": row_change_list})
            elif change["kind"] == "insert":
                changes_json.append({"_id": new_keyvalues["_id"], "kind": change["kind"], "changes": [new_keyvalues]})
            elif change["kind"] == "delete":
                changes_json.append({"_id": current_keyvalues["_id"], "kind": change["kind"]})

            full_results_json.append(changes_json)

        self.queue.publish({"records": changes_json})