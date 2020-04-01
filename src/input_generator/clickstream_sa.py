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

import pytz
from pytz import country_timezones
russian_tz = pytz.country_timezones("ru")

project_path = Path("/home/imanyakhin")
with open(project_path.joinpath("config.yaml"), "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    

# users
conn = get_conn(cfg["postgres"])
query = "select epk_id from user_features"
users = pd.io.sql.read_sql_query(query, conn)
conn.close()

# content
conn = get_conn(cfg["postgres"])
query = "select * from ref_content"
content = pd.io.sql.read_sql_query(query, conn)
conn.close()


# генерируем
n = cfg["input_generator"]["n_interactions"]

df = pd.DataFrame({
    "epk_id": np.random.choice(users.epk_id.values, n, replace=True),
    "content_id" : np.random.choice(content.content_id.values, n, replace=True),
    "event_type" : [
        np.random.choice([
            "Show", "Click", "Complete", "Like", "Dislike"
                          ], p=[0.8, 0.1, 0.05, 0.025, 0.025]) for _ in range(n)
    ]
})

df = pd.merge(
    df,
    content[["content_id", "actual_from", "actual_to"]],
    on="content_id"
)

# рандомное время
df["delta"] = (df["actual_to"] - df["actual_from"]).apply(lambda x: int(x.total_seconds()))
df["event_ts"] =\
    df["actual_from"] + timedelta(seconds=int(np.random.choice(df["delta"], 1)[0]))

# time зона по МСК
local_tz = pytz.timezone('Europe/Moscow')
df["event_ts"] =\
    df["event_ts"].apply(lambda x: local_tz.localize(x))

# рандомная тайм зона по РФ
df["event_ts"] =\
    df["event_ts"].apply(lambda x:
        x.astimezone(pytz.timezone(np.random.choice(russian_tz)))
                         )

# clean
df.drop(["actual_from", "actual_to", "delta"], axis=1, inplace=True)

# insert ts
df["insert_ts"] = datetime.now()


# insert db
conn = get_conn(cfg["postgres"])

with conn.cursor() as cur:
    execute_values(
        cur,
        """INSERT INTO clickstream_sa (epk_id, content_id, event_type, event_ts, insert_ts) VALUES %s""",
        df.values
    )
    conn.commit()
    conn.close()
