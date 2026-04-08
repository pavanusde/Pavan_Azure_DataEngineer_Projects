/*
--Sample Log table 
CREATE TABLE dbo.ApiPipelineLog
(
    LogId INT IDENTITY(1,1),
    PipelineName VARCHAR(200),
    SourceName VARCHAR(100),
    TargetName VARCHAR(100),
    RunStatus VARCHAR(50),
    RunTime DATETIME,
    FilePath VARCHAR(500),
    ErrorMessage VARCHAR(1000)
)
--Sample Stored Procedure 
CREATE OR ALTER  PROCEDURE dbo.usp_LogApiPipelineRun
    @PipelineName VARCHAR(200),
    @SourceName VARCHAR(100),
    @TargetName VARCHAR(100),
    @RunStatus VARCHAR(50),
    @RunTime DATETIME,
    @FilePath VARCHAR(500),
    @ErrorMessage VARCHAR(1000)
AS
BEGIN
    INSERT INTO dbo.ApiPipelineLog
    (
        PipelineName,
        SourceName,
        TargetName,
        RunStatus,
        RunTime,
        FilePath,
        ErrorMessage
    )
    VALUES
    (
        @PipelineName,
        @SourceName,
        @TargetName,
        @RunStatus,
        @RunTime,
        @FilePath,
        @ErrorMessage
    )
END

*/