from core import SimpleDBMigrate
from helpers import Lists
from mysql import MySQL
from logging import Log

class Main(object):
    def __init__(self, options=None, args=None, mysql=None, db_migrate=None):
        self.__options = options
        self.__args = args
        
        self.__mysql = mysql
        if self.__mysql is None:
            self.__mysql = MySQL(self.__options.db_config_file)
        
        self.__db_migrate = db_migrate
        if self.__db_migrate is None:
            self.__db_migrate = SimpleDBMigrate(self.__options.migrations_dir)
    
    def execute(self):
        print "\nStarting DB migration..."
        if self.__options.create_migration:
            self._create_migration()
        else:
            self._migrate()
        print "\nDone.\n"
            
    def _create_migration(self):
        new_file = self.__db_migrate.create_migration(self.__options.create_migration)
        print "- Created file '%s'" % (new_file)
    
    def _migrate(self):
        destination_version = self._get_destination_version()
        current_version = self.__mysql.get_current_schema_version()
        
        if str(current_version) == str(destination_version):
            Log().error_and_exit("current and destination versions are the same (%s)" % current_version)

        print "- Current version is: %s" % current_version
        print "- Destination version is: %s" % destination_version

        # if current and destination versions are the same, 
        # will consider a migration up to execute remaining files
        is_migration_up = True
        if int(current_version) > int(destination_version):
            is_migration_up = False

        print "\nStarting migration %s!\n" % "up" if is_migration_up else "down"

        # do it!
        self._execute_migrations(current_version, destination_version, is_migration_up)

    def _get_destination_version(self):
        destination_version = self.__options.schema_version

        if destination_version == None:
            destination_version = self.__db_migrate.latest_schema_version_available()

        if not self.__db_migrate.check_if_version_exists(destination_version):
            Log().error_and_exit("version not found (%s)" % destination_version)

        return destination_version
        
    def _get_migration_files_to_be_executed(self, current_version, destination_version):
        mysql_versions = self.__mysql.get_all_schema_versions()
        migration_versions = self.__db_migrate.get_all_migration_versions()
        
        # migration up: the easy part
        if current_version <= destination_version:
            return Lists.subtract(migration_versions, mysql_versions)
        
        # migration down...
        down_versions = [version for version in mysql_versions if version <= current_version and version > destination_version]
        for version in down_versions:
            if version not in migration_versions:
                Log().error_and_exit("impossible to migrate down: one of the versions was not found (%s)" % version)
        down_versions.reverse()
        return down_versions
        
    def _execute_migrations(self, current_version, destination_version, is_migration_up):
        # getting only the migration sql files to be executed
        migration_files_to_be_executed = self._get_migration_files_to_be_executed(current_version, destination_version)
        
        sql_statements_executed = ""
        for sql_file in migration_files_to_be_executed:    

            file_version = self.__db_migrate.get_migration_version(sql_file)
            if not is_migration_up:
                file_version = destination_version
            
            print "===== executing %s (%s) =====" % (sql_file, "up" if is_migration_up else "down")
            sql = self.__db_migrate.get_sql_command(sql_file, is_migration_up)
            self.__mysql.change(sql, file_version, is_migration_up)
            
            #recording the last statement executed
            sql_statements_executed += sql
        
        if self.__options.show_sql:
            print "__________ SQL statements executed __________"
            print sql_statements_executed
            print "_____________________________________________\n"