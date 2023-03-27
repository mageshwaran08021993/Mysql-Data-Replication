import os
from os import path, getcwd

import sys
import yaml

def find_resource(relative_path) -> str:
    dirs = [
        sys.prefix,
        "src///",
        f"{getcwd()}/src/resources",
        f"{getcwd()}/../src/resources",
    ]
    found_in = [path.normpath(path.join(d, relative_path))
                for d in dirs
                if path.exists(path.normpath(path.join(d, relative_path)))]

    if found_in:
        return found_in[0]
    return FileNotFoundError(fr"Resource not found - {relative_path}")

with open(find_resource("config.yaml")) as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
os.environ["ENV"]="local"

class Config:
    @staticmethod
    def get_env():
        return config[os.environ["ENV"]]

    @staticmethod
    def get_log_to_file():
        return config[os.environ["ENV"]].get("log_to_file","")

    @staticmethod
    def get_log_to_console():
        return config[os.environ["ENV"]].get("log_to_console", "")

    @staticmethod
    def get_log_file_path():
        return config[os.environ["ENV"]].get("log_file_path", "")

    @staticmethod
    def get_log_file_name():
        return config[os.environ["ENV"]].get("log_file_name", "")

    @staticmethod
    def get_target_db_host():
        return config[os.environ["ENV"]].get("db_host", "")

    @staticmethod
    def get_target_db_port():
        return config[os.environ["ENV"]].get("db_port", "")

    @staticmethod
    def get_target_db_user():
        return config[os.environ["ENV"]].get("db_user", "")

    @staticmethod
    def get_target_db_password():
        return config[os.environ["ENV"]].get("db_password", "")

    @staticmethod
    def get_target_db_name():
        return config[os.environ["ENV"]].get("db_name", "")

    @staticmethod
    def get_target_db_schema():
        return config[os.environ["ENV"]].get("db_schema", "")

    @staticmethod
    def get_target_database_type():
        return config[os.environ["ENV"]].get("database_type", "")

    @staticmethod
    def get_mysql_db_host():
        return config[os.environ["ENV"]].get("mysql_db_host", "")

    @staticmethod
    def get_mysql_db_port():
        return config[os.environ["ENV"]].get("mysql_db_port", "")

    @staticmethod
    def get_mysql_db_user():
        return config[os.environ["ENV"]].get("mysql_db_user", "")

    @staticmethod
    def get_mysql_db_password():
        return config[os.environ["ENV"]].get("mysql_db_password", "")

    @staticmethod
    def get_mysql_db_name():
        return config[os.environ["ENV"]].get("mysql_db_name", "")

    @staticmethod
    def get_mysql_db_schema():
        return config[os.environ["ENV"]].get("mysql_db_schema", "")

if __name__ == "__main__":
    Config.get_env()