-- ============================================================
-- BRONZE PIPELINE - COMPLETE FRESH SQL SCRIPT v3.0
-- Project: SAP Data Engineering Project
-- Author: Omkar Parab
-- Run this entire script from top to bottom
-- ============================================================


-- ============================================================
-- STEP 1: DROP EVERYTHING
-- ============================================================

-- Drop stored procedures
DROP PROCEDURE IF EXISTS dbo.usp_get_watermark
DROP PROCEDURE IF EXISTS dbo.usp_update_watermark
DROP PROCEDURE IF EXISTS dbo.usp_insert_file_tracking
DROP PROCEDURE IF EXISTS dbo.usp_insert_pipeline_run_audit
DROP PROCEDURE IF EXISTS dbo.usp_update_pipeline_run_audit
DROP PROCEDURE IF EXISTS dbo.usp_get_run_counts

-- Drop tables
DROP TABLE IF EXISTS dbo.file_tracking
DROP TABLE IF EXISTS dbo.pipeline_run_audit
DROP TABLE IF EXISTS dbo.watermark
DROP TABLE IF EXISTS dbo.table_config


-- ============================================================
-- STEP 2: CREATE TABLES
-- ============================================================

-- Watermark table
CREATE TABLE dbo.watermark (
    table_name      VARCHAR(100)    NOT NULL PRIMARY KEY,
    last_modified   DATETIME        NOT NULL DEFAULT '1900-01-01 00:00:00'
)

-- Table config
CREATE TABLE dbo.table_config (
    folder_path              VARCHAR(100)    NOT NULL PRIMARY KEY,
    is_active                BIT             NOT NULL DEFAULT 1,
    expected_column_count    INT             NOT NULL,
    file_pattern             VARCHAR(100)    NOT NULL,
    expected_file_extension  VARCHAR(10)     NOT NULL DEFAULT 'csv',
    last_updated             DATETIME        NOT NULL DEFAULT GETUTCDATE(),
    updated_by               VARCHAR(100)    NOT NULL DEFAULT SYSTEM_USER
)

-- File tracking table
CREATE TABLE dbo.file_tracking (
    id                  INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id     VARCHAR(100)    NOT NULL,
    table_name          VARCHAR(100)    NOT NULL,
    file_name           VARCHAR(500)    NOT NULL,
    last_modified       DATETIME        NULL,
    status              VARCHAR(50)     NOT NULL,
    load_time           DATETIME        NOT NULL DEFAULT GETUTCDATE(),
    file_size_bytes     BIGINT          NULL DEFAULT 0,
    rows_copied         INT             NULL DEFAULT 0,
    error_message       VARCHAR(MAX)    NULL
)

-- Pipeline run audit table
CREATE TABLE dbo.pipeline_run_audit (
    id              INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    run_id          VARCHAR(100)    NOT NULL,
    pipeline_name   VARCHAR(100)    NOT NULL,
    folder_name     VARCHAR(100)    NOT NULL,
    start_time      DATETIME        NOT NULL,
    end_time        DATETIME        NULL,
    status          VARCHAR(50)     NOT NULL,
    files_copied    INT             NULL DEFAULT 0,
    files_skipped   INT             NULL DEFAULT 0,
    files_failed    INT             NULL DEFAULT 0,
    watermark_used  DATETIME        NULL,
    new_watermark   DATETIME        NULL
)


-- ============================================================
-- STEP 3: CREATE INDEXES
-- ============================================================

CREATE INDEX IX_file_tracking_run_id
ON dbo.file_tracking (pipeline_run_id)

CREATE INDEX IX_file_tracking_status
ON dbo.file_tracking (status)

CREATE INDEX IX_file_tracking_table
ON dbo.file_tracking (table_name)

CREATE INDEX IX_file_tracking_last_modified
ON dbo.file_tracking (last_modified)

CREATE INDEX IX_file_tracking_table_status
ON dbo.file_tracking (table_name, status)

CREATE INDEX IX_pipeline_audit_run_id
ON dbo.pipeline_run_audit (run_id)

CREATE INDEX IX_pipeline_audit_folder
ON dbo.pipeline_run_audit (folder_name)

-- Unique constraint: prevent duplicate Success records per file
ALTER TABLE dbo.file_tracking
ADD CONSTRAINT UQ_file_success
UNIQUE (table_name, file_name, status)


-- ============================================================
-- STEP 4: INSERT INITIAL DATA
-- ============================================================

INSERT INTO dbo.watermark (table_name, last_modified)
VALUES
    ('ekko', '1900-01-01 00:00:00'),
    ('ekpo', '1900-01-01 00:00:00')

INSERT INTO dbo.table_config
    (folder_path, is_active, expected_column_count, file_pattern, expected_file_extension)
VALUES
    ('ekko', 1, 143, 'ekko_YYYY-MM-DD.csv', 'csv'),
    ('ekpo', 1, 307, 'ekpo_YYYY-MM-DD.csv', 'csv')


-- ============================================================
-- STEP 5: CREATE STORED PROCEDURES
-- ============================================================

-- SP 1: Get Watermark (no SQL injection)
CREATE OR ALTER PROCEDURE dbo.usp_get_watermark
    @table_name VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON
    SELECT last_modified
    FROM dbo.watermark
    WHERE table_name = @table_name
END
GO

-- SP 2: Update Watermark (max lastModified + 1 second)
CREATE OR ALTER PROCEDURE dbo.usp_update_watermark
    @table_name VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON

    UPDATE dbo.watermark
    SET last_modified = DATEADD(SECOND, 1,
        ISNULL(
            (SELECT MAX(last_modified)
             FROM dbo.file_tracking
             WHERE table_name = @table_name
             AND status = 'Success'),
        '1900-01-01 00:00:00')
    )
    WHERE table_name = @table_name

    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO dbo.watermark (table_name, last_modified)
        VALUES (@table_name, '1900-01-01 00:00:00')
    END
END
GO

-- SP 3: Insert File Tracking (with idempotency check)
CREATE OR ALTER PROCEDURE dbo.usp_insert_file_tracking
    @pipeline_run_id    VARCHAR(100),
    @table_name         VARCHAR(100),
    @file_name          VARCHAR(500),
    @last_modified      DATETIME        = NULL,
    @status             VARCHAR(50),
    @load_time          DATETIME        = NULL,
    @file_size_bytes    BIGINT          = 0,
    @rows_copied        INT             = 0,
    @error_message      VARCHAR(MAX)    = NULL
AS
BEGIN
    SET NOCOUNT ON

    -- Idempotency: skip if Success record already exists for this file
    IF EXISTS (
        SELECT 1 FROM dbo.file_tracking
        WHERE table_name = @table_name
        AND file_name = @file_name
        AND status = 'Success'
    )
    BEGIN
        RETURN
    END

    INSERT INTO dbo.file_tracking
        (pipeline_run_id, table_name, file_name, last_modified,
         status, load_time, file_size_bytes, rows_copied, error_message)
    VALUES
        (@pipeline_run_id, @table_name, @file_name, @last_modified,
         @status, ISNULL(@load_time, GETUTCDATE()),
         @file_size_bytes, @rows_copied, @error_message)
END
GO

-- SP 4: Insert Pipeline Run Audit
CREATE OR ALTER PROCEDURE dbo.usp_insert_pipeline_run_audit
    @run_id         VARCHAR(100),
    @pipeline_name  VARCHAR(100),
    @folder_name    VARCHAR(100),
    @start_time     DATETIME,
    @status         VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON

    INSERT INTO dbo.pipeline_run_audit
        (run_id, pipeline_name, folder_name, start_time, status)
    VALUES
        (@run_id, @pipeline_name, @folder_name, @start_time, @status)
END
GO

-- SP 5: Update Pipeline Run Audit
CREATE OR ALTER PROCEDURE dbo.usp_update_pipeline_run_audit
    @run_id         VARCHAR(100),
    @folder_name    VARCHAR(100),
    @end_time       DATETIME,
    @status         VARCHAR(50),
    @files_copied   INT             = 0,
    @files_skipped  INT             = 0,
    @files_failed   INT             = 0,
    @new_watermark  DATETIME        = NULL
AS
BEGIN
    SET NOCOUNT ON

    UPDATE dbo.pipeline_run_audit
    SET
        end_time        = @end_time,
        status          = @status,
        files_copied    = @files_copied,
        files_skipped   = @files_skipped,
        files_failed    = @files_failed,
        new_watermark   = @new_watermark
    WHERE
        run_id          = @run_id
        AND folder_name = @folder_name
END
GO

-- SP 6: Get actual run counts from file_tracking
CREATE OR ALTER PROCEDURE dbo.usp_get_run_counts
    @run_id         VARCHAR(100),
    @table_name     VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON

    SELECT
        SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) AS files_copied,
        SUM(CASE WHEN status = 'Skipped' THEN 1 ELSE 0 END) AS files_skipped,
        SUM(CASE WHEN status IN ('Failed', 'SchemaInvalid', 'InvalidFilename', 'EmptyFile') THEN 1 ELSE 0 END) AS files_failed
    FROM dbo.file_tracking
    WHERE pipeline_run_id = @run_id
    AND table_name = @table_name
END
GO


-- ============================================================
-- STEP 6: VERIFY EVERYTHING
-- ============================================================

SELECT 'watermark' AS table_name, COUNT(*) AS records FROM dbo.watermark
UNION ALL
SELECT 'table_config', COUNT(*) FROM dbo.table_config
UNION ALL
SELECT 'file_tracking', COUNT(*) FROM dbo.file_tracking
UNION ALL
SELECT 'pipeline_run_audit', COUNT(*) FROM dbo.pipeline_run_audit

SELECT * FROM dbo.watermark
SELECT * FROM dbo.table_config
SELECT name FROM sys.procedures ORDER BY name
SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.file_tracking')
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'file_tracking'
ORDER BY ORDINAL_POSITION


-- ============================================================
-- RESET FOR TESTING (run when needed)
-- ============================================================

/*
UPDATE dbo.watermark SET last_modified = '1900-01-01 00:00:00'
DELETE FROM dbo.file_tracking
DELETE FROM dbo.pipeline_run_audit
DBCC CHECKIDENT ('dbo.file_tracking', RESEED, 0)
DBCC CHECKIDENT ('dbo.pipeline_run_audit', RESEED, 0)
*/


-- ============================================================
-- MONITORING QUERIES
-- ============================================================

-- Latest pipeline run per folder
SELECT
    p.folder_name,
    p.run_id,
    p.start_time,
    p.end_time,
    p.status,
    p.files_copied,
    p.files_skipped,
    p.files_failed,
    p.new_watermark,
    DATEDIFF(SECOND, p.start_time, p.end_time) AS duration_seconds
FROM dbo.pipeline_run_audit p
INNER JOIN (
    SELECT folder_name, MAX(start_time) AS max_start
    FROM dbo.pipeline_run_audit
    GROUP BY folder_name
) latest ON p.folder_name = latest.folder_name
        AND p.start_time = latest.max_start
ORDER BY p.folder_name

-- Daily summary
SELECT
    CAST(load_time AS DATE)     AS load_date,
    table_name,
    COUNT(*)                    AS total_files,
    SUM(CASE WHEN status = 'Success'         THEN 1 ELSE 0 END) AS success,
    SUM(CASE WHEN status = 'Skipped'         THEN 1 ELSE 0 END) AS skipped,
    SUM(CASE WHEN status = 'Failed'          THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'SchemaInvalid'   THEN 1 ELSE 0 END) AS schema_invalid,
    SUM(CASE WHEN status = 'InvalidFilename' THEN 1 ELSE 0 END) AS invalid_filename,
    SUM(CASE WHEN status = 'EmptyFile'       THEN 1 ELSE 0 END) AS empty_file,
    SUM(file_size_bytes)        AS total_bytes
FROM dbo.file_tracking
GROUP BY CAST(load_time AS DATE), table_name
ORDER BY load_date DESC, table_name

-- Current watermarks
SELECT
    table_name,
    last_modified,
    DATEDIFF(HOUR, last_modified, GETUTCDATE()) AS hours_since_last_load
FROM dbo.watermark
ORDER BY last_modified DESC

-- Failed files last 7 days
SELECT
    pipeline_run_id,
    table_name,
    file_name,
    status,
    error_message,
    load_time
FROM dbo.file_tracking
WHERE status IN ('Failed', 'SchemaInvalid', 'InvalidFilename', 'EmptyFile')
AND load_time >= DATEADD(DAY, -7, GETUTCDATE())
ORDER BY load_time DESC
