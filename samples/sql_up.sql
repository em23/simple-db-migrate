WHENEVER SQLERROR EXIT ROLLBACK;
-----------DONT RUN SCRIPT UNLESS PREREQUISITES HAVE BEEN RUN
DECLARE
   cnt               INTEGER := 0;
   cnt2              INTEGER := 0;

   prior_version     TMAG_DB_VERSION.script_name%TYPE
                                  := 'patch_20_0_0_52_db.sql';
   current_version   TMAG_DB_VERSION.script_name%TYPE
                                 := 'patch_20_0_0_54_db.sql';

  BAD_VERSION       EXCEPTION;
  ALREADY_RUN       EXCEPTION;
BEGIN
   SELECT COUNT (*)
     INTO cnt
     FROM TMAG_DB_VERSION
    WHERE script_name = prior_version;

   IF (cnt = 0)
   THEN
      RAISE BAD_VERSION;
   END IF;


   SELECT COUNT (*)
     INTO cnt2
     FROM TMAG_DB_VERSION
    WHERE script_name  = current_version;

   IF (cnt2 != 0)
   THEN
      RAISE ALREADY_RUN;
   END IF;
EXCEPTION
   WHEN BAD_VERSION
   THEN
      RAISE_APPLICATION_ERROR (
         -20000,
            'Database requirements are not met.  This script is intended to upgrade '
         || prior_version);
   WHEN ALREADY_RUN
   THEN
      RAISE_APPLICATION_ERROR (
         -20000,
            'Version '
         || current_version
         || ' has already been deployed in this database.');
END;
/
define varUserA       = amdocs;
define varUserB       = amdocs_user;
define varUserC	      = samstg;
define varUserD      = amdocs_admin;

-- ============================================================
--   SQL function Sequence
-- ============================================================

insert into &varUserA..systemconfig (applicationid, key, value) values (19, 'container.dealer.ip', '10.153.143.17');


/**********************************
 Set the current patch level
***********************************/
insert into &varUserA..TMAG_DB_VERSION
       (major_version,minor_version,release_number,patch_level,PATCH_CREATED_DATE,patch_applied_date,patch_applied_by,SCRIPT_NAME,change_comment)
values (20,0,0,54,null,sysdate,user,'patch_20_0_0_54_db.sql', 'New key for conatiner dealer ip');
commit;