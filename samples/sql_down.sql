WHENEVER SQLERROR EXIT ROLLBACK;
DECLARE

  deployed_version TMAG_DB_VERSION.script_name%TYPE;
  rollback_from_version TMAG_DB_VERSION.script_name%TYPE := 'patch_20_0_0_54_db.sql';
  BAD_VERSION EXCEPTION;

BEGIN

  SELECT script_name INTO deployed_version
        FROM  TMAG_DB_VERSION
        WHERE PATCH_APPLIED_DATE = (select max(PATCH_APPLIED_DATE) FROM  TMAG_DB_VERSION);

  IF (deployed_version != rollback_from_version) then
    RAISE BAD_VERSION;
  END if;

  EXCEPTION
  WHEN others then
    RAISE_APPLICATION_ERROR(-20000,'Current database version is '||nvl(deployed_version,'UNKNOWN')||'.  This script is intended to rollback from version ' || rollback_from_version);
END;
/
define varUserA      = amdocs;
define varUserB      = amdocs_user;
define varUserC	      = samstg;
define varUserD      = amdocs_admin;
--Run Rollback Script

delete from &varUserA..systemconfig where applicationid=19 and key ='container.dealer.ip';

/******************************************************************************************************************************************************/
--Set the current patch level

delete from  &varUserA..TMAG_DB_VERSION  where
major_version = 20 and
minor_version = 0 and
release_number = 0 and
patch_level = 54
and script_name ='patch_20_0_0_54_db.sql';
commit;