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


# prepare data
df = pd.read_csv(
    project_path.joinpath("resources", "static", "CustomerKnowledgeCategory_20_03_2020_SA.csv"),
    sep=";"
)

df.columns = [
    "name",
    "id",
    "tag_id",
    "desc",
    "parent_name"
]

df.drop("id", axis=1, inplace=True)
df.drop("desc", axis=1, inplace=True)

df["parent_flg"] = (df["parent_name"].isnull()).astype(int)
df["parent_name"] = df["parent_name"].fillna("na")

parents = df[df["parent_flg"]==1][["name", "tag_id"]]
parents.columns = ["parent_name", "parent_tag_id"]
parents = parents.append(
    pd.DataFrame({"parent_name": "na", "parent_tag_id": 0}, index=[999])
)

df = pd.merge(
    df,
    parents,
    on="parent_name",
    how="left"
)

df.drop(["parent_name"], axis=1, inplace=True)
df["update_ts"] = datetime.now()


df = df[[
    "tag_id",
    "name",
    "parent_tag_id",
    "parent_flg",
    "update_ts"
]]


# insert db
conn = get_conn(cfg["postgres"])

with conn.cursor() as cur:
    execute_values(
        cur,
        "INSERT INTO ref_tag (tag_id, name, parent_tag_id, parent_flg, update_ts) VALUES %s",
        df.values
    )
    conn.commit()
    conn.close()

