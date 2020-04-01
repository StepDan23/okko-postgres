import os 
import sys
import yaml
from pathlib import Path
from src.io.db_utils import get_conn

project_path = Path(os.getcwd())
with open(project_path.joinpath("config.yaml"), "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

tables_scripts = project_path.joinpath("resources", "scripts", "tables")
tables_scripts.iterdir():