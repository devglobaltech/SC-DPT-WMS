USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 03:33 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from DESARROLLO_906 to WMS_ELECTRO_906_MATCH'
GO

IF @@TRANCOUNT > 0 COMMIT TRANSACTION
GO

SET NUMERIC_ROUNDABORT OFF
SET ANSI_PADDING, ANSI_NULLS, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT * FROM tempdb..sysobjects WHERE id=OBJECT_ID('tempdb..#tmpErrors')) DROP TABLE #tmpErrors
GO

CREATE TABLE #tmpErrors (Error int)
GO

SET XACT_ABORT OFF
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO

BEGIN TRANSACTION
GO

CREATE VIEW [dbo].[vw_aspnet_Applications]
AS
SELECT CAST(NULL AS nvarchar(256)) AS [ApplicationName], CAST(NULL AS nvarchar(256)) AS [LoweredApplicationName], CAST(NULL AS uniqueidentifier) AS [ApplicationId], CAST(NULL AS nvarchar(256)) AS [Description]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_MembershipUsers]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [UserId], CAST(NULL AS int) AS [PasswordFormat], CAST(NULL AS nvarchar(16)) AS [MobilePIN], CAST(NULL AS nvarchar(256)) AS [Email], CAST(NULL AS nvarchar(256)) AS [LoweredEmail], CAST(NULL AS nvarchar(256)) AS [PasswordQuestion], CAST(NULL AS nvarchar(128)) AS [PasswordAnswer], CAST(NULL AS bit) AS [IsApproved], CAST(NULL AS bit) AS [IsLockedOut], CAST(NULL AS datetime) AS [CreateDate], CAST(NULL AS datetime) AS [LastLoginDate], CAST(NULL AS datetime) AS [LastPasswordChangedDate], CAST(NULL AS datetime) AS [LastLockoutDate], CAST(NULL AS int) AS [FailedPasswordAttemptCount], CAST(NULL AS datetime) AS [FailedPasswordAttemptWindowStart], CAST(NULL AS int) AS [FailedPasswordAnswerAttemptCount], CAST(NULL AS datetime) AS [FailedPasswordAnswerAttemptWindowStart], CAST(NULL AS ntext) AS [Comment], CAST(NULL AS uniqueidentifier) AS [ApplicationId], CAST(NULL AS nvarchar(256)) AS [UserName], CAST(NULL AS nvarchar(16)) AS [MobileAlias], CAST(NULL AS bit) AS [IsAnonymous], CAST(NULL AS datetime) AS [LastActivityDate]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_Profiles]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [UserId], CAST(NULL AS datetime) AS [LastUpdatedDate], CAST(NULL AS int) AS [DataSize]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_Roles]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [ApplicationId], CAST(NULL AS uniqueidentifier) AS [RoleId], CAST(NULL AS nvarchar(256)) AS [RoleName], CAST(NULL AS nvarchar(256)) AS [LoweredRoleName], CAST(NULL AS nvarchar(256)) AS [Description]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_Users]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [ApplicationId], CAST(NULL AS uniqueidentifier) AS [UserId], CAST(NULL AS nvarchar(256)) AS [UserName], CAST(NULL AS nvarchar(256)) AS [LoweredUserName], CAST(NULL AS nvarchar(16)) AS [MobileAlias], CAST(NULL AS bit) AS [IsAnonymous], CAST(NULL AS datetime) AS [LastActivityDate]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_UsersInRoles]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [UserId], CAST(NULL AS uniqueidentifier) AS [RoleId]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_WebPartState_Paths]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [ApplicationId], CAST(NULL AS uniqueidentifier) AS [PathId], CAST(NULL AS nvarchar(256)) AS [Path], CAST(NULL AS nvarchar(256)) AS [LoweredPath]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_WebPartState_Shared]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [PathId], CAST(NULL AS int) AS [DataSize], CAST(NULL AS datetime) AS [LastUpdatedDate]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE VIEW [dbo].[vw_aspnet_WebPartState_User]
AS
SELECT CAST(NULL AS uniqueidentifier) AS [PathId], CAST(NULL AS uniqueidentifier) AS [UserId], CAST(NULL AS int) AS [DataSize], CAST(NULL AS datetime) AS [LastUpdatedDate]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_Applications]
  AS SELECT [dbo].[aspnet_Applications].[ApplicationName], [dbo].[aspnet_Applications].[LoweredApplicationName], [dbo].[aspnet_Applications].[ApplicationId], [dbo].[aspnet_Applications].[Description]
  FROM [dbo].[aspnet_Applications]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_MembershipUsers]
  AS SELECT [dbo].[aspnet_Membership].[UserId],
            [dbo].[aspnet_Membership].[PasswordFormat],
            [dbo].[aspnet_Membership].[MobilePIN],
            [dbo].[aspnet_Membership].[Email],
            [dbo].[aspnet_Membership].[LoweredEmail],
            [dbo].[aspnet_Membership].[PasswordQuestion],
            [dbo].[aspnet_Membership].[PasswordAnswer],
            [dbo].[aspnet_Membership].[IsApproved],
            [dbo].[aspnet_Membership].[IsLockedOut],
            [dbo].[aspnet_Membership].[CreateDate],
            [dbo].[aspnet_Membership].[LastLoginDate],
            [dbo].[aspnet_Membership].[LastPasswordChangedDate],
            [dbo].[aspnet_Membership].[LastLockoutDate],
            [dbo].[aspnet_Membership].[FailedPasswordAttemptCount],
            [dbo].[aspnet_Membership].[FailedPasswordAttemptWindowStart],
            [dbo].[aspnet_Membership].[FailedPasswordAnswerAttemptCount],
            [dbo].[aspnet_Membership].[FailedPasswordAnswerAttemptWindowStart],
            [dbo].[aspnet_Membership].[Comment],
            [dbo].[aspnet_Users].[ApplicationId],
            [dbo].[aspnet_Users].[UserName],
            [dbo].[aspnet_Users].[MobileAlias],
            [dbo].[aspnet_Users].[IsAnonymous],
            [dbo].[aspnet_Users].[LastActivityDate]
  FROM [dbo].[aspnet_Membership] INNER JOIN [dbo].[aspnet_Users]
      ON [dbo].[aspnet_Membership].[UserId] = [dbo].[aspnet_Users].[UserId]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_Profiles]
  AS SELECT [dbo].[aspnet_Profile].[UserId], [dbo].[aspnet_Profile].[LastUpdatedDate],
      [DataSize]=  DATALENGTH([dbo].[aspnet_Profile].[PropertyNames])
                 + DATALENGTH([dbo].[aspnet_Profile].[PropertyValuesString])
                 + DATALENGTH([dbo].[aspnet_Profile].[PropertyValuesBinary])
  FROM [dbo].[aspnet_Profile]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_Roles]
  AS SELECT [dbo].[aspnet_Roles].[ApplicationId], [dbo].[aspnet_Roles].[RoleId], [dbo].[aspnet_Roles].[RoleName], [dbo].[aspnet_Roles].[LoweredRoleName], [dbo].[aspnet_Roles].[Description]
  FROM [dbo].[aspnet_Roles]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_Users]
  AS SELECT [dbo].[aspnet_Users].[ApplicationId], [dbo].[aspnet_Users].[UserId], [dbo].[aspnet_Users].[UserName], [dbo].[aspnet_Users].[LoweredUserName], [dbo].[aspnet_Users].[MobileAlias], [dbo].[aspnet_Users].[IsAnonymous], [dbo].[aspnet_Users].[LastActivityDate]
  FROM [dbo].[aspnet_Users]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_UsersInRoles]
  AS SELECT [dbo].[aspnet_UsersInRoles].[UserId], [dbo].[aspnet_UsersInRoles].[RoleId]
  FROM [dbo].[aspnet_UsersInRoles]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_WebPartState_Paths]
  AS SELECT [dbo].[aspnet_Paths].[ApplicationId], [dbo].[aspnet_Paths].[PathId], [dbo].[aspnet_Paths].[Path], [dbo].[aspnet_Paths].[LoweredPath]
  FROM [dbo].[aspnet_Paths]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_WebPartState_Shared]
  AS SELECT [dbo].[aspnet_PersonalizationAllUsers].[PathId], [DataSize]=DATALENGTH([dbo].[aspnet_PersonalizationAllUsers].[PageSettings]), [dbo].[aspnet_PersonalizationAllUsers].[LastUpdatedDate]
  FROM [dbo].[aspnet_PersonalizationAllUsers]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER VIEW [dbo].[vw_aspnet_WebPartState_User]
  AS SELECT [dbo].[aspnet_PersonalizationPerUser].[PathId], [dbo].[aspnet_PersonalizationPerUser].[UserId], [DataSize]=DATALENGTH([dbo].[aspnet_PersonalizationPerUser].[PageSettings]), [dbo].[aspnet_PersonalizationPerUser].[LastUpdatedDate]
  FROM [dbo].[aspnet_PersonalizationPerUser]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO
/*
GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Applications]
TO [aspnet_Membership_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Applications]
TO [aspnet_Profile_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Applications]
TO [aspnet_Roles_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Applications]
TO [aspnet_Personalization_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_MembershipUsers]
TO [aspnet_Membership_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Profiles]
TO [aspnet_Profile_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Roles]
TO [aspnet_Roles_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Users]
TO [aspnet_Membership_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Users]
TO [aspnet_Profile_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Users]
TO [aspnet_Roles_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_Users]
TO [aspnet_Personalization_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_UsersInRoles]
TO [aspnet_Roles_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_WebPartState_Paths]
TO [aspnet_Personalization_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_WebPartState_Shared]
TO [aspnet_Personalization_ReportingAccess]
GO

GRANT SELECT
ON OBJECT::[dbo].[vw_aspnet_WebPartState_User]
TO [aspnet_Personalization_ReportingAccess]
GO
*/
IF @@TRANCOUNT > 0
BEGIN
   IF EXISTS (SELECT * FROM #tmpErrors)
       ROLLBACK TRANSACTION
   ELSE
       COMMIT TRANSACTION
END
GO

DROP TABLE #tmpErrors
GO