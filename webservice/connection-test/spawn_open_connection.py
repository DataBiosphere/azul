#!/usr/bin/python

from sqlalchemy import create_engine, MetaData, Table, String, Float, Column
from time import sleep
import os

def luigiDBInit():
    # Initializes luigi database with the appropriate env variables
    #
    # Returns the connection and luigi table for SQL operations,
    # but abstracts the table's columns, db engine, and metadata.

    # POSTGRES_USER=monitor
    # POSTGRES_PASSWORD=ly06oB5klj6FR5SMUdiTaaOE10T1rr
    # POSTGRES_DB=monitor

    db_engine = create_engine('postgresql://monitor:ly06oB5klj6FR5SMUdiTaaOE10T1rr@db/monitor', echo=False)
    monitordb_connection = db_engine.connect()
    monitordb_table = Table('luigi', MetaData(db_engine),
                  Column("luigi_job", String(100)),
                  Column("status", String(20)),
                  Column("submitter_specimen_id", String(100)),
                  Column("specimen_uuid", String(100)),
                  Column("workflow_name", String(100)),
                  Column("center_name", String(100)),
                  Column("submitter_donor_id", String(100)),
                  Column("consonance_job_uuid", String(100), primary_key=True),
                  Column("submitter_donor_primary_site", String(100)),
                  Column("project", String(100)),
                  Column("analysis_type", String(100)),
                  Column("program", String(100)),
                  Column("donor_uuid", String(100)),
                  Column("submitter_sample_id", String(100)),
                  Column("submitter_experimental_design", String(100)),
                  Column("submitter_specimen_type", String(100)),
                  Column("workflow_version", String(100)),
                  Column("sample_uuid", String(100)),
                  Column("start_time", String(100)),
                  Column("last_updated", String(100)))

    if not db_engine.dialect.has_table(db_engine, monitordb_table):
        monitordb_table.create()

    return monitordb_connection, monitordb_table, db_engine

monitordb_connection, monitordb_table, db_engine = luigiDBInit();
#monitordb_connection.close()
#db_engine.dispose()
sleep(30)

