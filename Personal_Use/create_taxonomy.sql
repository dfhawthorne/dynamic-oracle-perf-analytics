-- -----------------------------------------------------------------------------
-- Create TAXONOMY table
-- -----------------------------------------------------------------------------

CREATE TABLE TAXONOMY
(
  "TAXONOMY_TYPE"
    VARCHAR2(32)
    NOT NULL,
  "STAT_SOURCE"
    VARCHAR2(32)
    NOT NULL,
  "METRIC_NAME"
    VARCHAR2(32)
    NOT NULL,
  "CATEGORY"
    VARCHAR2(32)
    NOT NULL,
  "SUB_CATEGORY"
    VARCHAR2(32)
    NOT NULL
);