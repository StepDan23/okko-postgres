import psycopg2

def get_conn(db_cfg):
    """ create postgres connection """
    conn = psycopg2.connect(
        host=db_cfg["host"],
        port=db_cfg["port"],
        dbname=db_cfg["db"],
        user=db_cfg["user"],
        password=db_cfg["pwd"]
    )
    return conn