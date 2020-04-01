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
    project_path.joinpath("resources", "static", "content_type.csv"),
    sep=";"
)

df = df[[
    "content_type",
    "content_type_desc",
    "delay"
]]

df["update_ts"] = datetime.now()


# insert db
conn = get_conn(cfg["postgres"])

with conn.cursor() as cur:
    execute_values(
        cur,
        "INSERT INTO ref_content_type (content_type, content_type_desc, delay, update_ts) VALUES %s",
        df.values
    )
    conn.commit()
    conn.close()
