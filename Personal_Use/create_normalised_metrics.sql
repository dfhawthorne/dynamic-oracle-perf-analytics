-- -----------------------------------------------------------------------------
-- Create table to hold the normalised metrics created by
--   DOPA_Normalized_Unioned_Metrics_3_9.sql
-- -----------------------------------------------------------------------------

CREATE TABLE normalised_metrics (
    "INSTANCE_NAME"
      VARCHAR2(8)
      NOT NULL,
    "HOST_NAME"
      VARCHAR2(128)
      NOT NULL,
    "VERSION"
      VARCHAR2(16)
      NOT NULL,
    "SNAP_ID"
      NUMBER
      NOT NULL,
    "BEGIN_INTERVAL_TIME"
      DATE
      NOT NULL,
    "METRIC_ID"
      NUMBER
      NOT NULL,
    "METRIC_NAME"
      VARCHAR2(128)
      NOT NULL,
    "AVERAGE"
      NUMBER
      NOT NULL,
    "DBID"
      NUMBER
      NOT NULL,
    "INSTANCE_NUMBER"
      NUMBER
      NOT NULL,
    "MIN_SNAP_ID"
      NUMBER
      NOT NULL,
    "STAT_SOURCE"
      VARCHAR2(32)
      NOT NULL
  )
;