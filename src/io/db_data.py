import os
import sys
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from datetime import timedelta
from .db_utils import get_conn



def recent_row_filter(df, column_name):
    """ последняя актуальная строчка """
    df = (
        df.assign(rn=df.sort_values(["update_ts"], ascending=False)
                    .groupby(column_name)
                    .cumcount() + 1)
            .sort_values([column_name, "rn"])
    )
    df = df[df.rn == 1]
    df.drop("rn", axis=1, inplace=True)

    return df


class DbData:
    """ Класс для инкрементальной выгрузки данных """

    def __init__(
            self,
            db_cfg,
            current_state_ts=datetime.fromisoformat("2020-01-01"),
            tbl_feedbacks="user_feedbacks",
            tbl_item_features="ref_content",
            tbl_user_features="user_features",
            user_id_column="epk_id",
            item_id_column="content_id"
            ):
        """

        """
        self.db_cfg = db_cfg
        self.current_state_ts = current_state_ts
        self.feedbacks = pd.DataFrame()
        self.item_features = pd.DataFrame()
        self.user_features = pd.DataFrame()
        self._tbl_feedbacks = tbl_feedbacks
        self._tbl_item_features = tbl_item_features
        self._tbl_user_features = tbl_user_features
        self._user_id = user_id_column
        self._item_id = item_id_column


    def _get_feedbacks(self):
        """
        Выгружает инкремент по фидбекам
        """
        query = f"""
        SELECT * FROM {self._tbl_feedbacks} WHERE update_ts > '{self.current_state_ts.isoformat()}'
        """
        conn = get_conn(self.db_cfg)
        delta = pd.io.sql.read_sql_query(query, conn)
        conn.close()

        self.feedbacks = pd.concat(
            [self.feedbacks, delta], axis=1
        )


    def _get_item_features(self):
        """
        Выгружает инкремент по контенту
        """
        query = f"""
        SELECT * FROM {self._tbl_item_features} WHERE update_ts > '{self.current_state_ts.isoformat()}'
        """
        conn = get_conn(self.db_cfg)
        delta = pd.io.sql.read_sql_query(query, conn)
        conn.close()

        item_features = pd.concat(
            [self.item_features, delta], axis=1
        )
        item_features = recent_row_filter(item_features, self._item_id)
        self.item_features = item_features


    def _get_user_features(self):
        """
        Выгружает инкремент по пользователям
        """
        query = f"""
        SELECT * FROM {self._tbl_user_features} WHERE update_ts > '{self.current_state_ts.isoformat()}'
        """
        conn = get_conn(self.db_cfg)
        delta = pd.io.sql.read_sql_query(query, conn)
        conn.close()

        user_features = pd.concat(
            [self.user_features, delta], axis=1
        )
        user_features = recent_row_filter(user_features, self._user_id)
        self.user_features = user_features


    def get_data(self):
        """ загрузка данных из базы """
        self._get_feedbacks()
        self._get_item_features()
        self._get_user_features()
        self.current_state_ts = datetime.now()
