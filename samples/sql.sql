WHENEVER SQLERROR EXIT ROLLBACK;

DEFINE tablename = users;

CREATE TABLE &tablename (
  id number(11) NOT NULL,
  oid varchar2(500) default NULL,
  first_name varchar2(255) default NULL,
  last_name varchar2(255) default NULL,
  email varchar2(255) default NULL,
  PRIMARY KEY  (id)
);


drop table &tablename;
