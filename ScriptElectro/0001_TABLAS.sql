USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 12/04/2013 03:38 p.m.
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

CREATE TABLE [dbo].[aspnet_Applications] (
	[ApplicationName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[LoweredApplicationName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[ApplicationId] uniqueidentifier NOT NULL DEFAULT (newid()),
	[Description] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	PRIMARY KEY NONCLUSTERED ([ApplicationId] ASC) ON [PRIMARY],
	UNIQUE ([LoweredApplicationName] ASC) ON [PRIMARY],
	UNIQUE ([ApplicationName] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE CLUSTERED INDEX [aspnet_Applications_Index]
 ON [dbo].[aspnet_Applications] ([LoweredApplicationName])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_Membership] (
	[ApplicationId] uniqueidentifier NOT NULL,
	[UserId] uniqueidentifier NOT NULL,
	[Password] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[PasswordFormat] int NOT NULL DEFAULT ((0)),
	[PasswordSalt] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[MobilePIN] nvarchar(16) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Email] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[LoweredEmail] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PasswordQuestion] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PasswordAnswer] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[IsApproved] bit NOT NULL,
	[IsLockedOut] bit NOT NULL,
	[CreateDate] datetime NOT NULL,
	[LastLoginDate] datetime NOT NULL,
	[LastPasswordChangedDate] datetime NOT NULL,
	[LastLockoutDate] datetime NOT NULL,
	[FailedPasswordAttemptCount] int NOT NULL,
	[FailedPasswordAttemptWindowStart] datetime NOT NULL,
	[FailedPasswordAnswerAttemptCount] int NOT NULL,
	[FailedPasswordAnswerAttemptWindowStart] datetime NOT NULL,
	[Comment] ntext COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	PRIMARY KEY NONCLUSTERED ([UserId] ASC) ON [PRIMARY]
) TEXTIMAGE_ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

EXECUTE [sp_tableoption]
	@TableNamePattern  = N'[dbo].[aspnet_Membership]',
	@OptionName  = 'text in row',
	@OptionValue  = '3000'
GO

CREATE CLUSTERED INDEX [aspnet_Membership_index]
 ON [dbo].[aspnet_Membership] ([ApplicationId],
	[LoweredEmail])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_Paths] (
	[ApplicationId] uniqueidentifier NOT NULL,
	[PathId] uniqueidentifier NOT NULL DEFAULT (newid()),
	[Path] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[LoweredPath] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	PRIMARY KEY NONCLUSTERED ([PathId] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE UNIQUE CLUSTERED INDEX [aspnet_Paths_index]
 ON [dbo].[aspnet_Paths] ([ApplicationId],
	[LoweredPath])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_PersonalizationAllUsers] (
	[PathId] uniqueidentifier NOT NULL,
	[PageSettings] image NOT NULL,
	[LastUpdatedDate] datetime NOT NULL,
	PRIMARY KEY ([PathId] ASC) ON [PRIMARY]
) TEXTIMAGE_ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

EXECUTE [sp_tableoption]
	@TableNamePattern  = N'[dbo].[aspnet_PersonalizationAllUsers]',
	@OptionName  = 'text in row',
	@OptionValue  = '6000'
GO

CREATE TABLE [dbo].[aspnet_PersonalizationPerUser] (
	[Id] uniqueidentifier NOT NULL DEFAULT (newid()),
	[PathId] uniqueidentifier NULL,
	[UserId] uniqueidentifier NULL,
	[PageSettings] image NOT NULL,
	[LastUpdatedDate] datetime NOT NULL,
	PRIMARY KEY NONCLUSTERED ([Id] ASC) ON [PRIMARY]
) TEXTIMAGE_ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

EXECUTE [sp_tableoption]
	@TableNamePattern  = N'[dbo].[aspnet_PersonalizationPerUser]',
	@OptionName  = 'text in row',
	@OptionValue  = '6000'
GO

CREATE UNIQUE CLUSTERED INDEX [aspnet_PersonalizationPerUser_index1]
 ON [dbo].[aspnet_PersonalizationPerUser] ([PathId],
	[UserId])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE UNIQUE INDEX [aspnet_PersonalizationPerUser_ncindex2]
 ON [dbo].[aspnet_PersonalizationPerUser] ([UserId],
	[PathId])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_Profile] (
	[UserId] uniqueidentifier NOT NULL,
	[PropertyNames] ntext COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[PropertyValuesString] ntext COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[PropertyValuesBinary] image NOT NULL,
	[LastUpdatedDate] datetime NOT NULL,
	PRIMARY KEY ([UserId] ASC) ON [PRIMARY]
) TEXTIMAGE_ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

EXECUTE [sp_tableoption]
	@TableNamePattern  = N'[dbo].[aspnet_Profile]',
	@OptionName  = 'text in row',
	@OptionValue  = '6000'
GO

CREATE TABLE [dbo].[aspnet_Roles] (
	[ApplicationId] uniqueidentifier NOT NULL,
	[RoleId] uniqueidentifier NOT NULL DEFAULT (newid()),
	[RoleName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[LoweredRoleName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[Description] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	PRIMARY KEY NONCLUSTERED ([RoleId] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE UNIQUE CLUSTERED INDEX [aspnet_Roles_index1]
 ON [dbo].[aspnet_Roles] ([ApplicationId],
	[LoweredRoleName])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_SchemaVersions] (
	[Feature] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[CompatibleSchemaVersion] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[IsCurrentVersion] bit NOT NULL,
	PRIMARY KEY ([Feature] ASC, [CompatibleSchemaVersion] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_Users] (
	[ApplicationId] uniqueidentifier NOT NULL,
	[UserId] uniqueidentifier NOT NULL DEFAULT (newid()),
	[UserName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[LoweredUserName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[MobileAlias] nvarchar(16) COLLATE SQL_Latin1_General_CP1_CI_AS NULL DEFAULT (NULL),
	[IsAnonymous] bit NOT NULL DEFAULT ((0)),
	[LastActivityDate] datetime NOT NULL,
	PRIMARY KEY NONCLUSTERED ([UserId] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE UNIQUE CLUSTERED INDEX [aspnet_Users_Index]
 ON [dbo].[aspnet_Users] ([ApplicationId],
	[LoweredUserName])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE INDEX [aspnet_Users_Index2]
 ON [dbo].[aspnet_Users] ([ApplicationId],
	[LastActivityDate])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_UsersInRoles] (
	[UserId] uniqueidentifier NOT NULL,
	[RoleId] uniqueidentifier NOT NULL,
	PRIMARY KEY ([UserId] ASC, [RoleId] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE INDEX [aspnet_UsersInRoles_index]
 ON [dbo].[aspnet_UsersInRoles] ([RoleId])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[aspnet_WebEvent_Events] (
	[EventId] char(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[EventTimeUtc] datetime NOT NULL,
	[EventTime] datetime NOT NULL,
	[EventType] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[EventSequence] decimal(19, 0) NOT NULL,
	[EventOccurrence] decimal(19, 0) NOT NULL,
	[EventCode] int NOT NULL,
	[EventDetailCode] int NOT NULL,
	[Message] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ApplicationPath] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ApplicationVirtualPath] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[MachineName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[RequestUrl] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ExceptionType] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Details] ntext COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	PRIMARY KEY ([EventId] ASC) ON [PRIMARY]
) TEXTIMAGE_ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[CargaSeriesLog] (
	[IDPROCESO] numeric(20, 0) NOT NULL,
	[CLIENTE_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[NRO_BULTO] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[PRODUCTO_ID] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[SERIE] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[FECHA_ALTA] datetime NULL,
	[TERMINAL] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[USUARIO] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ARCHIVO] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[VALIDA] varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CARGADA] varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[CONFIGURACION_CONTENEDORAS] (
	[CLIENTE_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[PRODUCTO_ID] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[ORDEN_COMPRA] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[NRO_CONTENEDORA] numeric(10, 0) NULL,
	[CANTIDAD] numeric(20, 5) NULL,
	[USUARIO] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TERMINAL] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA] datetime NULL,
	[PROCESADO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL DEFAULT ((0)),
	[NRO_LOTE] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_PARTIDA] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[INFORME_PEDIDOS_EMPAQUE_ERP] (
	[CLIENTE_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[VIAJE_ID] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[FECHA] datetime NOT NULL,
	[USUARIO] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[TERMINAL] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[ResultadosCargaSeriesLog] (
	[IDPROCESO] numeric(20, 0) NULL,
	[MENSAJE] varchar(2000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PRIORIDAD_MSG] numeric(2, 0) NULL
) ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[SERIES_EGRESADAS] (
	[CLIENTE_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_BULTO] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PRODUCTO_ID] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_SERIE] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[VIAJE_ID] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[USUARIO_ID] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TERMINAL] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_INSERT] datetime NULL
) ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[SNAP_EXISTENCIAS] (
	[SNAP_ID] numeric(20, 0) IDENTITY(1, 1),
	[F_SNAP] datetime NOT NULL,
	[RL_ID] numeric(20, 0) NOT NULL,
	[DOC_TRANS_ID] numeric(20, 0) NULL,
	[NRO_LINEA_TRANS] numeric(10, 0) NULL,
	[POSICION_ANTERIOR] numeric(20, 0) NULL,
	[POSICION_ACTUAL] numeric(20, 0) NULL,
	[CANTIDAD] numeric(20, 5) NOT NULL,
	[TIPO_MOVIMIENTO_ID] varchar(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ULTIMA_ESTACION] varchar(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ULTIMA_SECUENCIA] numeric(3, 0) NULL,
	[NAVE_ANTERIOR] numeric(20, 0) NULL,
	[NAVE_ACTUAL] numeric(20, 0) NULL,
	[DOCUMENTO_ID] numeric(20, 0) NULL,
	[NRO_LINEA] numeric(10, 0) NULL,
	[DISPONIBLE] varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[DOC_TRANS_ID_EGR] numeric(20, 0) NULL,
	[NRO_LINEA_TRANS_EGR] numeric(10, 0) NULL,
	[DOC_TRANS_ID_TR] numeric(20, 0) NULL,
	[NRO_LINEA_TRANS_TR] numeric(10, 0) NULL,
	[CLIENTE_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CAT_LOG_ID] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CAT_LOG_ID_FINAL] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[EST_MERC_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT [PK_SNAP_EXISTENCIAS] PRIMARY KEY ([SNAP_ID] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE INDEX [IDX_SE_FSNAP]
 ON [dbo].[SNAP_EXISTENCIAS] ([CLIENTE_ID],
	[F_SNAP])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[spt_values] (
	[name] nvarchar(35) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[number] int NOT NULL,
	[type] nchar(3) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[low] int NULL,
	[high] int NULL,
	[status] int NULL DEFAULT ((0))
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE UNIQUE CLUSTERED INDEX [spt_valuesclust]
 ON [dbo].[spt_values] ([type],
	[number],
	[name])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE INDEX [ix2_spt_values_nu_nc]
 ON [dbo].[spt_values] ([number],
	[type])
WITH (FILLFACTOR=100)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[SYS_INT_DET_DOCUMENTO_HISTORICO] (
	[DOC_EXT] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[NRO_LINEA] numeric(20, 0) NOT NULL,
	[CLIENTE_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[PRODUCTO_ID] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[CANTIDAD_SOLICITADA] numeric(20, 5) NOT NULL,
	[CANTIDAD] numeric(20, 5) NULL,
	[EST_MERC_ID] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CAT_LOG_ID] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_BULTO] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[DESCRIPCION] varchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_LOTE] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_PALLET] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_VENCIMIENTO] datetime NULL,
	[NRO_DESPACHO] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_PARTIDA] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[UNIDAD_ID] varchar(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[UNIDAD_CONTENEDORA_ID] varchar(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PESO] numeric(10, 3) NULL,
	[UNIDAD_PESO] varchar(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[VOLUMEN] numeric(10, 3) NULL,
	[UNIDAD_VOLUMEN] varchar(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PROP1] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PROP2] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PROP3] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[LARGO] numeric(10, 3) NULL,
	[ALTO] numeric(10, 3) NULL,
	[ANCHO] numeric(10, 3) NULL,
	[DOC_BACK_ORDER] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ESTADO] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_ESTADO] datetime NULL,
	[ESTADO_GT] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_ESTADO_GT] datetime NULL,
	[DOCUMENTO_ID] numeric(20, 0) NULL,
	[NAVE_ID] numeric(20, 0) NULL,
	[NAVE_COD] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[sysdiagrams] (
	[name] sysname NOT NULL,
	[principal_id] int NOT NULL,
	[diagram_id] int IDENTITY(1, 1),
	[version] int NULL,
	[definition] varbinary(max) NULL,
	PRIMARY KEY ([diagram_id] ASC) ON [PRIMARY],
	CONSTRAINT [UK_principal_name] UNIQUE ([principal_id] ASC, [name] ASC) ON [PRIMARY]
) TEXTIMAGE_ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[TMP_EMPAQUE_CONTENEDORA] (
	[NRO_REMITO] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PICKING_ID] numeric(20, 0) NOT NULL,
	[DOCUMENTO_ID] numeric(20, 0) NOT NULL,
	[NRO_LINEA] numeric(10, 0) NULL,
	[CLIENTE_ID] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PRODUCTO_ID] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[VIAJE_ID] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TIPO_CAJA] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[DESCRIPCION] varchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CANTIDAD] numeric(20, 5) NULL,
	[NAVE_COD] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[POSICION_COD] varchar(45) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[RUTA] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PROP1] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_INICIO] datetime NULL,
	[FECHA_FIN] datetime NULL,
	[USUARIO] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CANT_CONFIRMADA] numeric(20, 5) NULL,
	[PALLET_PICKING] numeric(20, 0) NULL,
	[SALTO_PICKING] int NULL,
	[PALLET_CONTROLADO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[USUARIO_CONTROL_PICK] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ST_ETIQUETAS] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ST_CAMION] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FACTURADO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FIN_PICKING] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ST_CONTROL_EXP] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_CONTROL_PALLET] datetime NULL,
	[TERMINAL_CONTROL_PALLET] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_CONTROL_EXP] datetime NULL,
	[USUARIO_CONTROL_EXP] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TERMINAL_CONTROL_EXP] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FECHA_CONTROL_FAC] datetime NULL,
	[USUARIO_CONTROL_FAC] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TERMINAL_CONTROL_FAC] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[VEHICULO_ID] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PALLET_COMPLETO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[HIJO] numeric(20, 0) NULL,
	[QTY_CONTROLADO] numeric(20, 0) NULL,
	[PALLET_FINAL] numeric(20, 0) NULL,
	[PALLET_CERRADO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[USUARIO_PF] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TERMINAL_PF] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[REMITO_IMPRESO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_REMITO_PF] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[PICKING_ID_REF] numeric(20, 0) NULL,
	[BULTOS_CONTROLADOS] varchar(8000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[BULTOS_NO_CONTROLADOS] varchar(8000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FLG_PALLET_HOMBRE] varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TRANSF_TERMINADA] varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_LOTE] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_PARTIDA] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NRO_SERIE] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE INDEX [idx_cli_ped_prod]
 ON [dbo].[TMP_EMPAQUE_CONTENEDORA] ([CLIENTE_ID],
	[NRO_REMITO],
	[PRODUCTO_ID])
WITH (FILLFACTOR=100,
	 ALLOW_ROW_LOCKS=OFF,
	 ALLOW_PAGE_LOCKS=OFF)
ON [PRIMARY]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

CREATE TABLE [dbo].[UsuarioWebClientes] (
	[UserId] uniqueidentifier NOT NULL,
	[cliente_id] varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[UserName] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	CONSTRAINT [PK_UsersClientes] PRIMARY KEY ([UserId] ASC, [cliente_id] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_Membership]
 ADD FOREIGN KEY ([ApplicationId]) REFERENCES [dbo].[aspnet_Applications] ([ApplicationId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_Membership]
 ADD FOREIGN KEY ([UserId]) REFERENCES [dbo].[aspnet_Users] ([UserId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_Paths]
 ADD FOREIGN KEY ([ApplicationId]) REFERENCES [dbo].[aspnet_Applications] ([ApplicationId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_PersonalizationAllUsers]
 ADD FOREIGN KEY ([PathId]) REFERENCES [dbo].[aspnet_Paths] ([PathId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_PersonalizationPerUser]
 ADD FOREIGN KEY ([PathId]) REFERENCES [dbo].[aspnet_Paths] ([PathId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_PersonalizationPerUser]
 ADD FOREIGN KEY ([UserId]) REFERENCES [dbo].[aspnet_Users] ([UserId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_Profile]
 ADD FOREIGN KEY ([UserId]) REFERENCES [dbo].[aspnet_Users] ([UserId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_Roles]
 ADD FOREIGN KEY ([ApplicationId]) REFERENCES [dbo].[aspnet_Applications] ([ApplicationId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_Users]
 ADD FOREIGN KEY ([ApplicationId]) REFERENCES [dbo].[aspnet_Applications] ([ApplicationId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_UsersInRoles]
 ADD FOREIGN KEY ([UserId]) REFERENCES [dbo].[aspnet_Users] ([UserId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[aspnet_UsersInRoles]
 ADD FOREIGN KEY ([RoleId]) REFERENCES [dbo].[aspnet_Roles] ([RoleId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[UsuarioWebClientes]
 ADD FOREIGN KEY ([cliente_id]) REFERENCES [dbo].[CLIENTE] ([CLIENTE_ID]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

ALTER TABLE [dbo].[UsuarioWebClientes]
 ADD FOREIGN KEY ([UserId]) REFERENCES [dbo].[aspnet_Users] ([UserId]) 
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

EXECUTE [sp_addextendedproperty]
	@name = N'microsoft_database_tools_support',
	@value = 1,
	@level0type = 'SCHEMA',
	@level0name = N'dbo',
	@level1type = 'TABLE',
	@level1name = N'sysdiagrams'
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