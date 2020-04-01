import os 
import sys
import yaml
from pathlib import Path
sys.path.insert(0, "/home/imanyakhin")
from src.io.db_utils import get_conn
import pandas as pd
import subprocess

project_path = Path("/home/imanyakhin")
with open(project_path.joinpath("config.yaml"), "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

tables_scripts =\
    project_path.joinpath("resources", "scripts", "tables")

for row in tables_scripts.iterdir():
    #print(row)
    with open(row, "r") as f:
        query = f.read()
    #print(query)
    conn = get_conn(cfg["postgres"])
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()


conn = get_conn(cfg["postgres"])
query = """
select * 
from pg_catalog.pg_tables
where 1=1
  and schemaname = 'public'
  """
tables_df = pd.io.sql.read_sql_query(query, conn)
conn.close()


# insert data
exec(open(project_path.joinpath("input_generator", "ref_tag.py").as_posix(), "r").read())
exec(open(project_path.joinpath("input_generator", "ref_widget.py").as_posix(), "r").read())
exec(open(project_path.joinpath("input_generator", "ref_search_service.py").as_posix(), "r").read())
exec(open(project_path.joinpath("input_generator", "ref_content_type.py").as_posix(), "r").read())
exec(open(project_path.joinpath("input_generator", "ref_content.py").as_posix(), "r").read())
exec(open(project_path.joinpath("input_generator", "user_features.py").as_posix(), "r").read())
exec(open(project_path.joinpath("input_generator", "clickstream_sa.py").as_posix(), "r").read())


preprocess_scripts =\
    project_path.joinpath("resources", "scripts", "preprocess")

for row in preprocess_scripts.iterdir():
    #print(row)
    with open(row, "r") as f:
        query = f.read()
    #print(query)
    conn = get_conn(cfg["postgres"])
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()