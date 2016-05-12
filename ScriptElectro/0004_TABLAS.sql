USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 03:02 p.m.
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

ALTER TABLE [dbo].[Picking_Historico]
DROP CONSTRAINT [HIST_PICKING]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

EXECUTE [sp_rename]
	@objname  = N'[dbo].[PICKING_HISTORICO]',
	@newname  = N'tmp_0a1cfb63c0b84272bf9795968e6ca9c0',
	@objtype  = 'OBJECT'
GO

CREATE TABLE [dbo].[Picking_Historico] (
	[PICKING_ID] numeric(20, 0) IDENTITY(1, 1),
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
	[PALLET_CONTROLADO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL DEFAULT ('0'),
	[USUARIO_CONTROL_PICK] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ST_ETIQUETAS] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ST_CAMION] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FACTURADO] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL DEFAULT ('0'),
	[FIN_PICKING] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL DEFAULT ('0'),
	[ST_CONTROL_EXP] char(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL DEFAULT ('0'),
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
	[NRO_SERIE] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT [HIST_PICKING] PRIMARY KEY ([PICKING_ID] ASC) ON [PRIMARY]
)

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET IDENTITY_INSERT [dbo].[Picking_Historico] ON
GO

INSERT INTO [dbo].[Picking_Historico] (
	[PICKING_ID],
	[DOCUMENTO_ID],
	[NRO_LINEA],
	[CLIENTE_ID],
	[PRODUCTO_ID],
	[VIAJE_ID],
	[TIPO_CAJA],
	[DESCRIPCION],
	[CANTIDAD],
	[NAVE_COD],
	[POSICION_COD],
	[RUTA],
	[PROP1],
	[FECHA_INICIO],
	[FECHA_FIN],
	[USUARIO],
	[CANT_CONFIRMADA],
	[PALLET_PICKING],
	[SALTO_PICKING],
	[PALLET_CONTROLADO],
	[USUARIO_CONTROL_PICK],
	[ST_ETIQUETAS],
	[ST_CAMION],
	[FACTURADO],
	[FIN_PICKING],
	[ST_CONTROL_EXP],
	[FECHA_CONTROL_PALLET],
	[TERMINAL_CONTROL_PALLET],
	[FECHA_CONTROL_EXP],
	[USUARIO_CONTROL_EXP],
	[TERMINAL_CONTROL_EXP],
	[FECHA_CONTROL_FAC],
	[USUARIO_CONTROL_FAC],
	[TERMINAL_CONTROL_FAC],
	[VEHICULO_ID],
	[PALLET_COMPLETO],
	[HIJO],
	[QTY_CONTROLADO],
	[PALLET_FINAL],
	[PALLET_CERRADO],
	[USUARIO_PF],
	[TERMINAL_PF],
	[REMITO_IMPRESO],
	[NRO_REMITO_PF],
	[PICKING_ID_REF],
	[BULTOS_CONTROLADOS],
	[BULTOS_NO_CONTROLADOS],
	[FLG_PALLET_HOMBRE],
	[TRANSF_TERMINADA],
	[NRO_LOTE],
	[NRO_PARTIDA],
	[NRO_SERIE])
SELECT
	[PICKING_ID],
	[DOCUMENTO_ID],
	[NRO_LINEA],
	[CLIENTE_ID],
	[PRODUCTO_ID],
	[VIAJE_ID],
	[TIPO_CAJA],
	[DESCRIPCION],
	[CANTIDAD],
	[NAVE_COD],
	[POSICION_COD],
	[RUTA],
	[PROP1],
	[FECHA_INICIO],
	[FECHA_FIN],
	[USUARIO],
	[CANT_CONFIRMADA],
	[PALLET_PICKING],
	[SALTO_PICKING],
	[PALLET_CONTROLADO],
	[USUARIO_CONTROL_PICK],
	[ST_ETIQUETAS],
	[ST_CAMION],
	[FACTURADO],
	[FIN_PICKING],
	[ST_CONTROL_EXP],
	[FECHA_CONTROL_PALLET],
	[TERMINAL_CONTROL_PALLET],
	[FECHA_CONTROL_EXP],
	[USUARIO_CONTROL_EXP],
	[TERMINAL_CONTROL_EXP],
	[FECHA_CONTROL_FAC],
	[USUARIO_CONTROL_FAC],
	[TERMINAL_CONTROL_FAC],
	[VEHICULO_ID],
	[PALLET_COMPLETO],
	[HIJO],
	[QTY_CONTROLADO],
	[PALLET_FINAL],
	[PALLET_CERRADO],
	[USUARIO_PF],
	[TERMINAL_PF],
	[REMITO_IMPRESO],
	[NRO_REMITO_PF],
	[PICKING_ID_REF],
	[BULTOS_CONTROLADOS],
	[BULTOS_NO_CONTROLADOS],
	[FLG_PALLET_HOMBRE],
	[TRANSF_TERMINADA],
	NULL,
	NULL,
	NULL
FROM [dbo].[tmp_0a1cfb63c0b84272bf9795968e6ca9c0]

GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET IDENTITY_INSERT [dbo].[Picking_Historico] OFF
GO

DROP TABLE [dbo].[tmp_0a1cfb63c0b84272bf9795968e6ca9c0]
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
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