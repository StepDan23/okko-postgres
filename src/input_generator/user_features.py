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



# закачаем теги
conn = get_conn(cfg["postgres"])
query = "select * from ref_tag"
tags = pd.io.sql.read_sql_query(query, conn)
conn.close()

tags_dict = tags.groupby("parent_tag_id")["tag_id"].agg(list).to_dict()


def get_tags_str(tags_dict=tags_dict):
    """Функция для генерации тегов"""
    all_tags_lst = []

    tags_1lvl_lst = [x for x in tags_dict.keys() if x != 0]
    tag_1lvl = np.random.choice(tags_1lvl_lst, np.random.randint(0, 10, 1))

    for row in tag_1lvl:
        tag_2lvl_lst = tags_dict[row]
        tag_2lvl = np.random.choice(tag_2lvl_lst, np.random.randint(0, 5, 1))
        all_tags_lst.append(row)
        all_tags_lst = all_tags_lst + list(tag_2lvl)

    return all_tags_lst


n = cfg["input_generator"]["n_users"]

df = pd.DataFrame(
    {
      "epk_id": ["user_"+str(_) for _ in range(n)],
      "gender": [np.random.choice((1, 2, 3), p=[0.45, 0.5, 0.05]) for _ in range(n)],
      "age_grp": [np.random.choice((1, 2, 3, 4, 5, 6, 7), p=[0.1, 0.1, 0.2, 0.2, 0.2, 0.1, 0.1]) for _ in range(n)],
      "wage_grp": [np.random.choice((1, 2, 3, 4), p=[0.9, 0.05, 0.025, 0.025]) for _ in range(n)],
      "balance_grp": [np.random.choice((1, 2, 3, 4), p=[0.9, 0.05, 0.025, 0.025]) for _ in range(n)],
      "pos_grp": [np.random.choice((1, 2, 3, 4), p=[0.9, 0.05, 0.025, 0.025]) for _ in range(n)],
      "setllement_type_id": [np.random.choice((1, 2, 3, 4), p=[0.6, 0.2, 0.1, 0.1]) for _ in range(n)],
      "crm_channel_id": [np.random.choice((1, 2, 3, 4), p=[0.9, 0.05, 0.04, 0.01]) for _ in range(n)],
      "interest_tags": [get_tags_str() for _ in range(n)],
      "calculated_tags": [list(np.random.uniform(0, 1, 22)) for _ in range(n)],
      "update_ts": [datetime.now() for _ in range(n)]
    }
)

# cnt clusters
clusters_columns = [
    "gender",
    "age_grp",
    "balance_grp",
    "crm_channel_id"
]
clusters = df.groupby(clusters_columns)\
    .size()\
    .reset_index(name="cnt")\
    .reset_index()\
    .drop("cnt", axis=1)\
    .rename(columns={"index": "cluster_id"})


df = pd.merge(
    df,
    clusters,
    on=clusters_columns
)


df = df[[
       'epk_id', 'gender', 'age_grp', 'wage_grp', 'balance_grp', 'pos_grp',
       'setllement_type_id', 'crm_channel_id', 'interest_tags',
       'calculated_tags', 'cluster_id', 'update_ts'
]]


from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

# insert db
conn = get_conn(cfg["postgres"])

with conn.cursor() as cur:
    execute_values(
        cur,
        """INSERT INTO user_features (
        epk_id, 
        gender, 
        age_grp, 
        wage_grp, 
        balance_grp, 
        pos_grp, 
        settlement_type_id, 
        crm_channel_id, 
        interests_tags, 
        calculated_tags, 
        cluster_id, 
        update_ts) VALUES %s""",
        df.values
    )
    conn.commit()
    conn.close()

