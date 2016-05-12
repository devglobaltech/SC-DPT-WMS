USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 03:52 p.m.
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