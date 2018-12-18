-- -----------------------------------------------------------------------------
-- Create a user called 'DOPA'
--
-- This code is specific to XE database running under Windows
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Create the DOPA_DATA tablespace in a Windows XE database
-- -----------------------------------------------------------------------------

CREATE BIGFILE TABLESPACE dopa_data
  DATAFILE 'C:\ORACLEXE\APP\ORACLE\ORADATA\XE\dopa_data.dbf'
    SIZE 50m
    AUTOEXTEND ON
    NEXT 5m
    MAXSIZE 250m;

-- -----------------------------------------------------------------------------
-- Create the DOPA user and have all of its objects in the DOPA_DATA tablespace
-- -----------------------------------------------------------------------------

CREATE USER dopa
  IDENTIFIED BY "Dopa$2018"
  DEFAULT TABLESPACE dopa_data 
  QUOTA UNLIMITED ON dopa_data;

-- -----------------------------------------------------------------------------
-- Grant minimal privileges to the DOPA user
-- -----------------------------------------------------------------------------

GRANT
  CREATE TABLE,
  CREATE SESSION
  TO dopa;