CREATE SCHEMA stg;
GO

CREATE SCHEMA audit;
GO

-- Purpose:
-- stg   → staging layer (temporary data)
-- audit → logging + error tracking

-- 2. Staging Table
CREATE TABLE stg.DIM_LICENSES_STG
(
    BatchUID UNIQUEIDENTIFIER NOT NULL,
    LICENSE_ID VARCHAR(50) NULL,
    PERSON_ID VARCHAR(50) NULL,
    LICENSE_TYPE VARCHAR(100) NULL,
    ISSUE_DATE DATETIME NULL,
    EXPIRY_DATE DATETIME NULL,
    LICENSE_STATUS VARCHAR(50) NULL,
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

--Purpose:
-- Temporary landing area for raw data from Snowflake
-- Stores ALL records (valid + invalid)
-- BatchUID → tracks which pipeline run inserted data

--3. Batch Log Table
CREATE TABLE audit.PipelineBatchLog
(
    BatchUID UNIQUEIDENTIFIER PRIMARY KEY,
    PipelineName VARCHAR(200),
    PipelineRunId VARCHAR(100),
    SourceSystem VARCHAR(100),
    TargetTable VARCHAR(200),
    StartTime DATETIME2,
    EndTime DATETIME2,
    Status VARCHAR(50),
    RowsCopied BIGINT,
    RowsInserted BIGINT,
    RowsUpdated BIGINT,
    ErrorMessage VARCHAR(MAX),
    CreatedDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

--Purpose
-- Tracks full pipeline execution (start → end)
-- One row per pipeline run
-- Used for monitoring and reporting

--4. Activity Log Table
CREATE TABLE audit.PipelineActivityLog
(
    ActivityLogId INT IDENTITY(1,1) PRIMARY KEY,
    BatchUID UNIQUEIDENTIFIER,
    ActivityName VARCHAR(200),
    ActivityStatus VARCHAR(50),
    ActivityStartTime DATETIME2,
    ActivityEndTime DATETIME2,
    Message VARCHAR(MAX),
    CreatedDate DATETIME2 DEFAULT SYSUTCDATETIME()
);
--Purpose
-- Tracks each step inside pipeline
-- Example:
-- TRUNCATE → SUCCESS
-- VALIDATE → SUCCESS
-- MERGE → FAILED

--5. Error Table
CREATE TABLE audit.DIM_LICENSES_ERROR
(
    ErrorId INT IDENTITY(1,1) PRIMARY KEY,
    BatchUID UNIQUEIDENTIFIER,
    LICENSE_ID VARCHAR(50),
    PERSON_ID VARCHAR(50),
    LICENSE_TYPE VARCHAR(100),
    ISSUE_DATE DATETIME,
    EXPIRY_DATE DATETIME,
    LICENSE_STATUS VARCHAR(50),
    ErrorReason VARCHAR(500),
    ErrorDate DATETIME2 DEFAULT SYSUTCDATETIME()
);
--Purpose 
-- Stores invalid records
-- Prevents pipeline failure due to bad data
-- Used for data quality tracking

--6. Start Batch Procedure
    CREATE OR ALTER PROCEDURE audit.usp_StartPipelineBatch
    (
        @BatchUID UNIQUEIDENTIFIER,
        @PipelineName VARCHAR(200),
        @PipelineRunId VARCHAR(100),
        @SourceSystem VARCHAR(100),
        @TargetTable VARCHAR(200)
    )
    AS
    BEGIN
        INSERT INTO audit.PipelineBatchLog
        (
            BatchUID,
            PipelineName,
            PipelineRunId,
            SourceSystem,
            TargetTable,
            StartTime,
            Status
        )
        VALUES
        (
            @BatchUID,
            @PipelineName,
            @PipelineRunId,
            @SourceSystem,
            @TargetTable,
            SYSUTCDATETIME(),
            'STARTED'
        );
    END;
--Purpose
-- Marks pipeline start
-- Creates batch record
-- Tracks execution lifecycle

--7. Truncate Staging Procedure
CREATE PROCEDURE stg.usp_Prepare_DIM_LICENSES_Staging
(
    @BatchUID UNIQUEIDENTIFIER
)
AS
BEGIN
    TRUNCATE TABLE stg.DIM_LICENSES_STG;
END;
--Purpose
-- Clears old staging data
-- Ensures fresh load every run
-- Avoids mixing batches

--8. Validation Procedure
CREATE PROCEDURE stg.usp_Validate_DIM_LICENSES_Staging
(
    @BatchUID UNIQUEIDENTIFIER
)
AS
BEGIN
    INSERT INTO audit.DIM_LICENSES_ERROR
    SELECT *, 'LICENSE_ID is NULL'
    FROM stg.DIM_LICENSES_STG
    WHERE BatchUID = @BatchUID
      AND LICENSE_ID IS NULL;
END;

--Purpose 
-- Separates bad data
-- Logs invalid records
-- Prevents pipeline failure

--9. Merge (Upsert) Procedure
CREATE PROCEDURE dbo.SP_MERGE_DIM_LICENSES
(
    @BatchUID UNIQUEIDENTIFIER
)
AS
BEGIN
    MERGE dbo.DIM_LICENSES AS TARGET
    USING
    (
        SELECT *
        FROM stg.DIM_LICENSES_STG
        WHERE BatchUID = @BatchUID
          AND LICENSE_ID IS NOT NULL
    ) AS SOURCE
    ON TARGET.LICENSE_ID = SOURCE.LICENSE_ID

    WHEN MATCHED THEN
        UPDATE SET
            TARGET.PERSON_ID = SOURCE.PERSON_ID

    WHEN NOT MATCHED THEN
        INSERT (LICENSE_ID, PERSON_ID)
        VALUES (SOURCE.LICENSE_ID, SOURCE.PERSON_ID);
END;

--Purpose:
-- Performs UPSERT
-- Updates existing records
-- Inserts new records
-- Processes only valid data

--10. End Batch Success
CREATE PROCEDURE audit.usp_EndPipelineBatch_Success
(
    @BatchUID UNIQUEIDENTIFIER,
    @RowsCopied BIGINT
)
AS
BEGIN
    UPDATE audit.PipelineBatchLog
    SET
        EndTime = SYSUTCDATETIME(),
        Status = 'SUCCESS',
        RowsCopied = @RowsCopied
    WHERE BatchUID = @BatchUID;
END;
--Purpose
-- Marks pipeline completion
-- Stores execution metrics
-- Used for reporting

--11. End Batch Failure
CREATE PROCEDURE audit.usp_EndPipelineBatch_Failure
(
    @BatchUID UNIQUEIDENTIFIER,
    @ErrorMessage VARCHAR(MAX)
)
AS
BEGIN
    UPDATE audit.PipelineBatchLog
    SET
        EndTime = SYSUTCDATETIME(),
        Status = 'FAILED',
        ErrorMessage = @ErrorMessage
    WHERE BatchUID = @BatchUID;
END;

--Captures failure details
-- Helps debugging
-- Triggers alerting

--FINAL UNDERSTANDING
-- ADF → moves data
-- SQL → validates + processes

--Pipeline flow:
--Start → Truncate → Copy → Validate → Merge → End → Email