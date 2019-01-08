CREATE USER mcve
  IDENTIFIED BY oracle
  DEFAULT TABLESPACE users
  QUOTA 20M on users;

GRANT create session TO mcve;
GRANT create table TO mcve;
GRANT create view TO mcve;
GRANT create any trigger TO mcve;
GRANT create any procedure TO mcve;
GRANT create sequence TO mcve;
GRANT create synonym TO mcve;

CREATE TABLE mcve.test (
    id    NUMBER(10) NOT NULL,
    value NUMBER(10));
ALTER TABLE mcve.test ADD (CONSTRAINT test_pk PRIMARY KEY (ID));
