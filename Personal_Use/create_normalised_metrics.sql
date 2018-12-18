-- -----------------------------------------------------------------------------
-- Create table to hold the normalised metrics created by
--   DOPA_Normalized_Unioned_Metrics_3_9.sql
-- -----------------------------------------------------------------------------

CREATE TABLE normalised_metrics (
    "SNAP_ID"
      NUMBER
      NOT NULL,
    "STAT_SOURCE"
      VARCHAR2(32)
      NOT NULL,
    "METRIC_NAME"
      VARCHAR2(128)
      NOT NULL,
    "AVERAGE"
      NUMBER
      NOT NULL
  )
;