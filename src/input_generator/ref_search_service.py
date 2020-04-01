import os
import sys
import yaml
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
sys.path.insert(0, "/home/imanyakhin")
from src.io.db_utils import get_conn
from psycopg2.extras import execute_values


project_path = Path("/home/imanyakhin")
with open(project_path.joinpath("config.yaml"), "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)


df = pd.read_csv(
    project_path.joinpath("resources", "static", "search.txt"),
    sep="\t"
)

df = df[[
    "search_id",
    "search_code",
    "search_parent_id",
    "search_description"
]]

df["update_ts"] = datetime.now()


# insert db
conn = get_conn(cfg["postgres"])

with conn.cursor() as cur:
    execute_values(
        cur,
        "INSERT INTO ref_search_service (search_id, search_code, search_parent_id, search_desc, update_ts) VALUES %s",
        df.values
    )
    conn.commit()
    conn.close()


