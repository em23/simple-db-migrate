__author__ = 'em23'

import json
from simple_db_migrate.core.exceptions import MigrationException
from simple_db_migrate.oracle import Oracle
from sqlplus_commando import SqlplusCommando


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

new_db_version      = "0.0.3"
migration_file_name = "migrate3"
sql                 = get_file_data('samples\sql.sql')
sql_up              = unicode(get_file_data('samples\sql_up.sql')   , "utf-8")
sql_down            = unicode(get_file_data('samples\sql_down.sql') , "utf-8")


def run_example():
    # First trying to execute the sql using simple_db_migrate
    db = Oracle(config=config)
    try:
        db.change(sql=sql, new_db_version=new_db_version, migration_file_name=migration_file_name, sql_up=sql_up, sql_down=sql_down)

    except MigrationException as err:   # If simple_db_migrate fails because of Sql plus commands then try using SqlplusCommando
        # TODO: check that <err.msg> is "Invalid SQL Statement" and that <err.sql> is a Sql Plus command

        sqlplus = SqlplusCommando(configuration=config)

        with open(r'samples\temp.sql','w') as file:     # Creating temporary sql file so we can execute the whole script
            file.write(sql)

        result = sqlplus.run_script(r'samples\temp.sql')         # Execute script on oracle

        # Finish by using simple_db_migrate's method to record the migration
        db._change_db_version(new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None)

        # Saving oracle output to an html file
        with open('test.html', 'w') as file:
            file.write(result)


if __name__ == '__main__':
    run_example()
