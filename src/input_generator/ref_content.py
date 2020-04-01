import os
import sys
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
sys.path.insert(0, "/home/imanyakhin")
from src.io.db_utils import get_conn
from psycopg2.extras import execute_values


project_path = Path("/home/imanyakhin")
with open(project_path.joinpath("config.yaml"), "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)


def ts_value(x, add=7):
    """Прикидываем действие"""
    y = datetime.now() +\
        timedelta(days=7*x) + \
        timedelta(days=add) + \
        timedelta(hours=int(np.random.choice(10, 1)[0]))
    return y

# закачаем теги
conn = get_conn(cfg["postgres"])
query = "select * from ref_tag"
tags = pd.io.sql.read_sql_query(query, conn)
conn.close()

tags_dict = tags.groupby("parent_tag_id")["tag_id"].agg(list).to_dict()


def get_tags_str(tags_dict=tags_dict):
    """Функция для генерации тегов"""
    tags_1lvl_lst = [x for x in tags_dict.keys() if x != 0]
    tag_1lvl = np.random.choice(tags_1lvl_lst, 1)[0]
    tag_2lvl_lst = tags_dict[tag_1lvl]
    tag_2lvl = np.random.choice(tag_2lvl_lst, 1)[0]
    tags_str = ",".join([str(x) for x in (tag_1lvl, tag_2lvl)])
    return tags_str


n = cfg["input_generator"]["n_items"]

df = pd.DataFrame(
    {
      "content_type_id": [np.random.choice(
          (10, 11, 12, 13, 14, 15), p=[0.75, 0.05, 0.05, 0.05, 0.05, 0.05]
      ) for _ in range(n)],
      "content_id": [x for x in range(1,n+1)],
    "title": ["" for _ in range(n)],
    "actual_from": [ts_value(x//7) for x in range(n)],
    "actual_to":   [ts_value(x//7, 14) for x in range(n)],
    "tags": [get_tags_str() for _ in range(n)],
    "update_ts" : [datetime.now() for _ in range(n)]
    }
)

# insert db
conn = get_conn(cfg["postgres"])

with conn.cursor() as cur:
    execute_values(
        cur,
        "INSERT INTO ref_content (content_type_id, content_id, title, actual_from, actual_to, tags, update_ts) VALUES %s",
        df.values
    )
    conn.commit()
    conn.close()

