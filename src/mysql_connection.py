from src.config import Config

def mysql_connection_details():
    con = {"host": Config.get_mysql_db_host(),
                      "port": Config.get_mysql_db_port(),
                      "user": Config.get_mysql_db_user(),
                      "passwd": Config.get_mysql_db_password()
                      }
    return con