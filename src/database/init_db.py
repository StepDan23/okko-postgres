import pandas as pd
from src.utils import get_root_folder, insert_to_table
from codetiming import Timer


def insert_from_csv(root_path, path, table_name):
    path = root_path.joinpath(path).as_posix()
    bookmarks_df = pd.read_csv(path)
    n_elem = bookmarks_df.shape[0]
    with Timer(text=f"Inserted to '{table_name}' {n_elem} items in {{:.4f}} seconds"):
        insert_to_table(table_name, bookmarks_df.columns, bookmarks_df.values)
    return True


def load_data():
    project_path = get_root_folder()
    cat_path = project_path.joinpath('data/catalogue.json').as_posix()
    catalogue = pd.read_json(cat_path, orient='index')
    catalogue['element_uid'] = catalogue.index
    n_elem = catalogue.shape[0]
    with Timer(text=f"Inserted to 'catalogue' {n_elem} items in {{:.4f}} seconds"):
        insert_to_table('catalogue', catalogue.columns, catalogue.values)
    insert_from_csv(project_path, 'data/ratings.csv', 'ratings')
    insert_from_csv(project_path, 'data/bookmarks.csv', 'bookmarks')
    insert_from_csv(project_path, 'data/transactions.csv', 'transactions')
