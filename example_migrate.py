__author__ = 'em23'

import json
from simple_db_migrate.oracle import Oracle


def get_file_data(filename):
    with open(filename, 'r') as f:
        full_sql = f.read()
    return full_sql


def get_config_data():
    with open('config.txt') as f:
        data = f.read()
    return json.loads(data)


########################
### Setting up data ####
########################


config = get_config_data()

new_db_version      = "0.0.4"
migration_file_name = "migrate4"
sql                 = get_file_data('samples\sql.sql')
sql_up              = unicode(get_file_data('samples\sql_up.sql')   , "utf-8")
sql_down            = unicode(get_file_data('samples\sql_down.sql') , "utf-8")


def run_example():
    db = Oracle(config=config)
    db.change(sql=sql, new_db_version=new_db_version, migration_file_name=migration_file_name, sql_up=sql_up, sql_down=sql_down)



if __name__ == '__main__':
    run_example()
