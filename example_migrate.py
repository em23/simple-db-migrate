__author__ = 'em23'

from simple_db_migrate.core.exceptions import MigrationException
from simple_db_migrate.oracle import Oracle
from sqlplus_commando import SqlplusCommando


def get_file_data(filename):
    with open(filename, 'r') as f:
        full_sql = f.read()
    return full_sql


########################
### Setting up data ####
########################

config = {
    "database_script_encoding": "utf8",
    "database_encoding": "American_America.UTF8",
    "database_host": "54.203.84.28",
    "database_port": 1521,
    "database_user": "VDRDS",
    "database_password": "1qazxsw23",
    "database_name": 'VDDATA',
    "database_version_table": "db_version"
}
new_db_version      = "0.0.3"
migration_file_name = "migrate2"
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

        sqlplus.run_script(r'samples\temp.sql')         # Execute script on oracle

        # Finish by using simple_db_migrate's method to record the migration
        db._change_db_version(new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None)


if __name__ == '__main__':
    run_example()
