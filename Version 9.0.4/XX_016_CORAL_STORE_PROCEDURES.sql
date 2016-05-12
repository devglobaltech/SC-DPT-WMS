
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 04:54 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from V9 to CORAL'
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

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER     PROCEDURE [dbo].[IMPRESION_AUDITORIA_CAT_LOGICA] 
	@P_CLIENTE AS VARCHAR (50) OUTPUT, 
	@P_PRODUCTO_ID As VARCHAR (50) OUTPUT, 
	@P_FechaDesde As VARCHAR (50) OUTPUT, 
	@P_FechaHasta As VARCHAR (50) OUTPUT, 
	@P_USUARIO As VARCHAR (50) OUTPUT, 
	@P_OLD As VARCHAR (50) OUTPUT, 
	@P_NEW As VARCHAR (50) OUTPUT,
	@P_PALLET as Varchar (100) OUTPUT
AS
BEGIN

	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)

	Set @StrWhere = ''
	
	SELECT 	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal 
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT AUDITORIA_ID AS ID' + char(13)
	Set @StrSql = @StrSql + ' ,CAST(DD.PRODUCTO_ID AS VARCHAR) AS PRODUCTO_ID' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(PRO.DESCRIPCION AS VARCHAR) AS PRODUCTO_COD' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CLI.CLIENTE_ID AS VARCHAR) AS CLIENTE_ID' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(CLI.RAZON_SOCIAL AS VARCHAR) AS CLIENTE_COD' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.OLD AS VARCHAR) AS OLDID' + CHAR(13)
	Set @StrSql = @StrSql + ' ,CAST(CL.DESCRIPCION AS VARCHAR) AS OLDDESC '  + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.NEW AS VARCHAR) AS NEWID' + char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CL2.DESCRIPCION AS VARCHAR) AS NEWDESC '+ Char(13)
	Set @StrSql = @StrSql + ' ,SA.QTY_NEW AS QTY_NEW ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.USUARIO_ID ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.TERMINAL ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.FECHA ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(P.POSICION_COD,N.NAVE_COD) AS POS ' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)) AS FECHA_VENCIMIENTO ' + Char(13)
	--Set @StrSql = @StrSql + ' ,' + Char(39) + '(Nro.Pallet:' + char(39) + ' + ISNULL(DD.PROP1, ' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Nro.Lote:' + char(39) + ' + ISNULL(DD.NRO_LOTE, ' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + '+ char(39) + 'Nro.Partida:' + char(39) + ' + ISNULL(DD.NRO_PARTIDA,' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Nro.Bulto:' + char(39) + ' + ISNULL(DD.NRO_BULTO,' + char(39) + '-' + char(39)+ ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Nro.Despacho:' + char(39) + ' + ISNULL(DD.NRO_DESPACHO,' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Fecha Vto.:' + char(39) + ' + CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4))' + ' + ' + char(39) + ' )' + char(39) + ' AS DETALLES ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.PROP1,' + char(39) + ' - ' + char(39) + ') AS PALLET ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_LOTE, ' + char(39) + ' - ' + char(39) + ') AS LOTE ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_PARTIDA,' + char(39) + ' - ' + char(39) + ') AS PARTIDA ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_BULTO, ' + char(39) + ' - ' + char(39) + ') AS BULTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_DESPACHO, ' + char(39) + ' - ' + char(39) + ') AS DESPACHO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)), ' + char(39) + ' - ' + char(39) + ') AS FECHA_VENCIMIENTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,SA.PREFIJO ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + ' FROM 	SYS_AUDITORIA_CAT_MERC SA ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN vDET_DOCUMENTO  DD ' + Char(13)
	Set @StrSql = @StrSql + ' ON(DD.DOCUMENTO_ID=SA.DOCUMENTO_ID AND DD.NRO_LINEA=SA.NRO_LINEA) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CATEGORIA_LOGICA CL ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.OLD=CL.CAT_LOG_ID AND SA.CLIENTE_ID=CL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CATEGORIA_LOGICA CL2 ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NEW=CL2.CAT_LOG_ID AND SA.CLIENTE_ID=CL2.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN POSICION P ' + Char(13)
	Set @StrSql = @StrSql + ' ON(P.POSICION_ID=SA.POSICION_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN NAVE N ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NAVE_ID=N.NAVE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN PRODUCTO PRO ON(PRO.PRODUCTO_ID = DD.PRODUCTO_ID AND PRO.CLIENTE_ID=DD.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CLIENTE CLI ON(CLI.CLIENTE_ID = SA.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' WHERE SA.PREFIJO = ' + CHAR(39) + 'CATEGORIA LOGICA' + CHAR(39) + Char(13)

	If @P_CLIENTE Is not null and  @P_CLIENTE <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.CLIENTE_ID =' + Char(39) + @P_CLIENTE + Char(39) + Char(13)
		End

	If @P_PRODUCTO_ID Is not null and @P_PRODUCTO_ID <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PRODUCTO_ID =' + Char(39) + @P_PRODUCTO_ID + Char(39) + Char(13)
		End

	If @P_USUARIO Is not null and @P_USUARIO <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.USUARIO_ID =' + Char(39) + @P_USUARIO + Char(39) + Char(13)
		End

	If @P_OLD Is not null and @P_OLD <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.OLD =' + Char(39) + @P_OLD + Char(39) + Char(13)
		End

	If @P_NEW Is not null and @P_NEW <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.NEW =' + Char(39) + @P_NEW + Char(39) + Char(13)
		End

	If @P_PALLET Is not null and @P_PALLET <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PROP1 =' + Char(39) + @P_PALLET + Char(39) + Char(13)
		End

	if @P_FechaDesde is not null and @P_FechaHasta is not null and @P_FechaDesde <> '' and @P_FechaHasta <> ''
		Begin

			Set @StrWhere = @StrWhere + 'and cast(sa.fecha as datetime) between cast(' + char(39) + @P_FechaDesde + char(39)+  ' as datetime) and cast(' + char(39) + @P_FechaHasta + char(39) + ' as datetime)'		
		End 

	Set @strsql =  @strsql + isnull(@StrWhere, '')

	EXECUTE SP_EXECUTESQL @StrSql 

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER     PROCEDURE [dbo].[IMPRESION_AUDITORIA_EST_MERC]
	@P_CLIENTE AS VARCHAR (50) OUTPUT, 
	@P_PRODUCTO_ID As VARCHAR (50) OUTPUT, 
	@P_FechaDesde As VARCHAR (50) OUTPUT, 
	@P_FechaHasta As VARCHAR (50) OUTPUT, 
	@P_USUARIO As VARCHAR (50) OUTPUT, 
	@P_OLD As VARCHAR (50) OUTPUT, 
	@P_NEW As VARCHAR (50) OUTPUT,
	@P_PALLET as Varchar (100) OUTPUT
AS
BEGIN

	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @cWhere		as INT
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)

	Set @cWhere = 0
	Set @StrWhere = ''
	
	SELECT 	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal 
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT AUDITORIA_ID AS ID' + char(13)
	Set @StrSql = @StrSql + ' ,CAST(DD.PRODUCTO_ID AS VARCHAR) AS PRODUCTO_ID' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(PRO.DESCRIPCION AS VARCHAR) AS PRODUCTO_COD' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CLI.CLIENTE_ID AS VARCHAR) AS CLIENTE_ID' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(CLI.RAZON_SOCIAL AS VARCHAR) AS CLIENTE_COD' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.OLD AS VARCHAR) AS OLDID' + CHAR(13)
	Set @StrSql = @StrSql + ' ,CAST(CL.DESCRIPCION AS VARCHAR) AS OLDDESC '  + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.NEW AS VARCHAR) AS NEWID' + char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CL2.DESCRIPCION AS VARCHAR) AS NEWDESC '+ Char(13)
	Set @StrSql = @StrSql + ' ,SA.QTY_NEW AS QTY_NEW ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.USUARIO_ID ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.TERMINAL ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.FECHA ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(P.POSICION_COD,N.NAVE_COD) AS POS ' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)) AS FECHA_VENCIMIENTO ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.PROP1,' + char(39) + ' - ' + char(39) + ') AS PALLET ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_LOTE, ' + char(39) + ' - ' + char(39) + ') AS LOTE ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_PARTIDA,' + char(39) + ' - ' + char(39) + ') AS PARTIDA ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_BULTO, ' + char(39) + ' - ' + char(39) + ') AS BULTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_DESPACHO, ' + char(39) + ' - ' + char(39) + ') AS DESPACHO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)), ' + char(39) + ' - ' + char(39) + ') AS FECHA_VENCIMIENTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,SA.PREFIJO ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + ' FROM 	SYS_AUDITORIA_CAT_MERC SA ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN vDET_DOCUMENTO  DD ' + Char(13)
	Set @StrSql = @StrSql + ' ON(DD.DOCUMENTO_ID=SA.DOCUMENTO_ID AND DD.NRO_LINEA=SA.NRO_LINEA) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN ESTADO_MERCADERIA_RL CL ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.OLD=CL.EST_MERC_ID AND SA.CLIENTE_ID=CL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN ESTADO_MERCADERIA_RL CL2 ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NEW=CL2.EST_MERC_ID AND SA.CLIENTE_ID=CL2.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN POSICION P ' + Char(13)
	Set @StrSql = @StrSql + ' ON(P.POSICION_ID=SA.POSICION_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN NAVE N ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NAVE_ID=N.NAVE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN PRODUCTO PRO ON(PRO.PRODUCTO_ID = DD.PRODUCTO_ID AND PRO.CLIENTE_ID=DD.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CLIENTE CLI ON(CLI.CLIENTE_ID = SA.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' WHERE SA.PREFIJO = ' + CHAR(39) + 'ESTADO MERCADERIA' + CHAR(39) + Char(13)
	If @P_CLIENTE Is not null and  @P_CLIENTE <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.CLIENTE_ID =' + Char(39) + @P_CLIENTE + Char(39) + Char(13)
		End

	If @P_PRODUCTO_ID Is not null and @P_PRODUCTO_ID <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PRODUCTO_ID =' + Char(39) + @P_PRODUCTO_ID + Char(39) + Char(13)
		End

	If @P_USUARIO Is not null and @P_USUARIO <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.USUARIO_ID =' + Char(39) + @P_USUARIO + Char(39) + Char(13)
		End

	If @P_OLD Is not null and @P_OLD <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.OLD =' + Char(39) + @P_OLD + Char(39) + Char(13)
		End

	If @P_NEW Is not null and @P_NEW <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.NEW =' + Char(39) + @P_NEW + Char(39) + Char(13)
		End

	If @P_PALLET Is not null and @P_PALLET <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PROP1 =' + Char(39) + @P_PALLET + Char(39) + Char(13)
		End

	if @P_FechaDesde is not null and @P_FechaHasta is not null and @P_FechaDesde <> '' and @P_FechaHasta <> ''
		Begin

			Set @StrWhere = @StrWhere + 'and cast(sa.fecha as datetime) between cast(' + char(39) + @P_FechaDesde + char(39)+  ' as datetime) and cast(' + char(39) + @P_FechaHasta + char(39) + ' as datetime)'		
		End 

	Set @strsql =  @strsql + isnull(@StrWhere, '')
	EXECUTE SP_EXECUTESQL @StrSql 

END
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

ALTER       PROCEDURE [dbo].[IMPRESION_HISTORICO_PRODUCTO_RL]
	@CLIENTE AS VARCHAR (50) OUTPUT, 
	@PRODUCTO As VARCHAR (50) OUTPUT, 
	@FECHA_AUD_DESDE As VARCHAR (50) OUTPUT,
	@FECHA_AUD_HASTA As VARCHAR (50) OUTPUT
AS
BEGIN

	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)
	DECLARE @CWhere		AS NUMERIC(1,0)

	Set @StrWhere = ''
	Set @CWhere = 0
	
	SELECT 	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal 
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT	DD.CLIENTE_ID AS CLIENTE_ID' + CHAR(13)
	Set @StrSql = @StrSql + '	,C.RAZON_SOCIAL AS CLIENTE_COD' + CHAR(13)
	Set @StrSql = @StrSql + '	,DD.PRODUCTO_ID AS PRODUCTO_ID' + Char(13)
	Set @StrSql = @StrSql + '	,P.DESCRIPCION AS PRODUCTO_COD' + CHAR(13)  
	Set @StrSql = @StrSql + '	,CAST(DAY(HPRL.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(HPRL.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(HPRL.FECHA) AS VARCHAR(4)) AS FECHA_AUD ' + Char(13)
	Set @StrSql = @StrSql + '	,HPRL.CANTIDAD' + CHAR(13)
	Set @StrSql = @StrSql + '	,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS UBICACION' + CHAR(13)
	Set @StrSql = @StrSql + '	,CAST(DAY(D.FECHA_ALTA_GTW) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(D.FECHA_ALTA_GTW) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(D.FECHA_ALTA_GTW) AS VARCHAR(4)) AS FECHA_ING ' + Char(13)
	Set @StrSql = @StrSql + '	,CAST(HPRL.CAT_LOG_ID AS VARCHAR) AS CAT_LOG_ID ' + char(13) 
	Set @StrSql = @StrSql + '	,CAST(CL.DESCRIPCION AS VARCHAR) AS CAT_LOG_ID_FINAL'  + Char(13)
	Set @StrSql = @StrSql + '	,CAST(HPRL.EST_MERC_ID AS VARCHAR) AS EST_MERC_ID' + char(13)
	Set @StrSql = @StrSql + '	,CAST(EMRL.DESCRIPCION AS VARCHAR) AS EST_MERC_COD '+ Char(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_SERIE, ' + char(39) + ' - ' + char(39) + ') AS SERIE ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_BULTO, ' + char(39) + ' - ' + char(39) + ') AS BULTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_LOTE, ' + char(39) + ' - ' + char(39) + ') AS LOTE ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)), ' + char(39) + ' - ' + char(39) + ') AS FECHA_VENCIMIENTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_DESPACHO, ' + char(39) + ' - ' + char(39) + ') AS DESPACHO ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_PARTIDA,' + char(39) + ' - ' + char(39) + ') AS PARTIDA ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.PROP1,' + char(39) + ' - ' + char(39) + ') AS PALLET ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + 'FROM	HISTORICO_PRODUCTO_RL HPRL' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (HPRL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND HPRL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN DOCUMENTO D ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN PRODUCTO P ON (P.PRODUCTO_ID=DD.PRODUCTO_ID AND P.CLIENTE_ID =DD.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN CLIENTE C ON (C.CLIENTE_ID = DD.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN CATEGORIA_LOGICA CL ON(HPRL.CAT_LOG_ID = CL.CAT_LOG_ID AND DD.CLIENTE_ID=CL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + '	LEFT JOIN ESTADO_MERCADERIA_RL EMRL ON(HPRL.EST_MERC_ID = EMRL.EST_MERC_ID AND DD.CLIENTE_ID=EMRL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + '	LEFT JOIN POSICION POS ON(POS.POSICION_ID = HPRL.POSICION_ACTUAL)' + CHAR(13) 	Set @StrSql = @StrSql + '	LEFT JOIN NAVE N ON(HPRL.NAVE_ACTUAL=N.NAVE_ID) ' + CHAR(13)

	If @CLIENTE Is not null and  @CLIENTE <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where DD.CLIENTE_ID =' + Char(39) + @CLIENTE + Char(39) + Char(13)
				Set @CWhere = 1
			End

	If @PRODUCTO Is not null and @PRODUCTO <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where DD.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and DD.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
			End

	if @FECHA_AUD_DESDE is not null and @FECHA_AUD_HASTA is not null and @FECHA_AUD_DESDE <> '' and @FECHA_AUD_HASTA <> ''
		If @CWhere = 0
			Begin
				Set @StrWhere = @StrWhere + 'Where cast(HPRL.FECHA as datetime) between cast(' + char(39) + @FECHA_AUD_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_AUD_HASTA + char(39) + ' as datetime)'		
				Set @CWhere = 1
			End 
		Else
			Begin	
				Set @StrWhere = @StrWhere + 'and cast(HPRL.FECHA as datetime) between cast(' + char(39) + @FECHA_AUD_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_AUD_HASTA + char(39) + ' as datetime)'		
			End 

	Set @StrSql = @StrSql + @StrWhere 

	EXECUTE SP_EXECUTESQL @StrSql 

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[IMPRESION_HISTORICO_SALDO_PRODUCTO]
	@CLIENTE AS VARCHAR (50) OUTPUT, 
	@PRODUCTO As VARCHAR (50) OUTPUT, 
	@FECHA_DESDE As VARCHAR (50) OUTPUT,
	@FECHA_HASTA As VARCHAR (50) OUTPUT,
	@CAT_LOG_ID As VARCHAR (50) OUTPUT, 
	@EST_MERC_ID As VARCHAR (50) OUTPUT
AS
BEGIN
	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)
	DECLARE @CWhere		AS NUMERIC(1,0)

	Set @StrWhere = ''
	Set @CWhere = 0
	
	SELECT	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT	HSP.CLIENTE_ID' + CHAR(13)
	Set @StrSql = @StrSql + '	,CLI.RAZON_SOCIAL' + CHAR(13)
	Set @StrSql = @StrSql + '	,HSP.PRODUCTO_ID' + CHAR(13)
	Set @StrSql = @StrSql + '	,P.DESCRIPCION AS PDESC' + CHAR(13)
	Set @StrSql = @StrSql + '	,HSP.CANTIDAD' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(HSP.CAT_LOG_ID, ' + char(39) + ' - ' + char(39) + ') AS CAT_LOG_ID ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(CL.DESCRIPCION, ' + char(39) + ' - ' + char(39) + ') AS CDESC ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(HSP.EST_MERC_ID, ' + char(39) + ' - ' + char(39) + ') AS EST_MERC_ID ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(EM.DESCRIPCION, ' + char(39) + ' - ' + char(39) + ') AS EDESC ' + CHAR(13)
	Set @StrSql = @StrSql + '	,CAST(DAY(HSP.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(HSP.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(HSP.FECHA) AS VARCHAR(4)) AS FECHA ' + Char(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + 'FROM	HISTORICO_SALDO_PRODUCTO HSP' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN CLIENTE CLI' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.CLIENTE_ID = CLI.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN PRODUCTO P' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.PRODUCTO_ID = P.PRODUCTO_ID AND HSP.CLIENTE_ID = P.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	LEFT JOIN CATEGORIA_LOGICA CL' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.CAT_LOG_ID = CL.CAT_LOG_ID AND HSP.CLIENTE_ID = CL.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	LEFT JOIN ESTADO_MERCADERIA_RL EM' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.EST_MERC_ID = EM.EST_MERC_ID AND HSP.CLIENTE_ID = EM.CLIENTE_ID)' + CHAR(13)


	If @CLIENTE Is not null and  @CLIENTE <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.CLIENTE_ID =' + Char(39) + @CLIENTE + Char(39) + Char(13)
				Set @CWhere = 1
			End

	If @PRODUCTO Is not null and @PRODUCTO <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and HSP.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
			End

	If @FECHA_DESDE is not null and @FECHA_HASTA is not null and @FECHA_DESDE <> '' and @FECHA_HASTA <> ''
		If @CWhere = 0
			Begin
				Set @StrWhere = @StrWhere + 'Where CAST(HSP.FECHA as datetime) between cast(' + char(39) + @FECHA_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_HASTA + char(39) + ' as datetime)'		
				Set @CWhere = 1
			End 
		Else
			Begin	
				Set @StrWhere = @StrWhere + 'and CAST(HSP.FECHA as datetime) between cast(' + char(39) + @FECHA_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_HASTA + char(39) + ' as datetime)'		
			End 

	If @CAT_LOG_ID Is not null and @CAT_LOG_ID <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.CAT_LOG_ID =' + Char(39) + @CAT_LOG_ID + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and HSP.CAT_LOG_ID =' + Char(39) + @CAT_LOG_ID + Char(39) + Char(13)
			End


	If @EST_MERC_ID Is not null and @EST_MERC_ID <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.EST_MERC_ID =' + Char(39) + @EST_MERC_ID + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and HSP.EST_MERC_ID =' + Char(39) + @EST_MERC_ID + Char(39) + Char(13)
			EnD

	Set @StrSql = @StrSql + @StrWhere 

	EXECUTE SP_EXECUTESQL @StrSql 

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[ING_MATCH_CODE]
@DOCUMENTO_ID 	NUMERIC(20,0),
@NRO_LINEA		NUMERIC(10,0),
@CODE				VARCHAR(50),
@CONTROL			CHAR(1) OUT
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	DECLARE @CONTADOR FLOAT

	SET @CONTROL='0'


	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS PC (NOLOCK) INNER JOIN DET_DOCUMENTO DD (NOLOCK)
			ON(PC.CLIENTE_ID=DD.CLIENTE_ID AND PC.PRODUCTO_ID=DD.PRODUCTO_ID)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA

	IF @CONTADOR=0
	BEGIN
		RAISERROR ('El producto tiene marcado validación al ingreso, pero no se definieron códigos EAN13/DUN14. Por favor, verifique el maestro de productos',16,1)
		RETURN
	END

	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS PC (NOLOCK) INNER JOIN DET_DOCUMENTO DD (NOLOCK)
			ON(PC.CLIENTE_ID=DD.CLIENTE_ID AND PC.PRODUCTO_ID=DD.PRODUCTO_ID)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
			AND PC.CODIGO=@CODE		

	IF @CONTADOR>0 
	BEGIN
		SET @CONTROL='1'
	END
	ELSE
	BEGIN
		RAISERROR('El codigo ingresado no se corresponde con los cargados en el Maestro de productos.',16,1)
		return
	END
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

/*
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREACION: 		29-06-2007
VERSION:		1.3
AUTOR:			SEBASTIAN GOMEZ.
DESCRIPCION:	PROCEDIMIENTO ALMACENADO. DADO UN DOCUMENTO_ID ALIMENTA MEDIANTE UN QUERY A LA TABLA DE PICKING.
				ADICIONALMENTE EVALUA SI LA OPERACION ES UNA OPERACION DE EGRESO CASO CONTRARIO SE TERMINA 
				LA EJECUCION DEL PROCEDIMIENTO
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
ALTER                         PROCEDURE [dbo].[INGRESA_PICKING]
	@DOCUMENTO_ID NUMERIC(20,0) OUTPUT
AS
BEGIN
	--DECLARACIONES.
	DECLARE @TIPO_OPERACION VARCHAR(5)
	DECLARE @CANT			AS INT


	DECLARE @TCUR				CURSOR
	DECLARE @VIAJEID			VARCHAR(100)
	DECLARE @PRODUCTO_ID		VARCHAR(30)
	DECLARE @POSICION_COD	VARCHAR(50)
	DECLARE @PALLET			VARCHAR(100)
	DECLARE @RUTA				VARCHAR(100)
	DECLARE @ID				NUMERIC(20,0)		

	--START
	SELECT 	@TIPO_OPERACION = TIPO_OPERACION_ID
	FROM	DOCUMENTO
	WHERE 	DOCUMENTO_ID=@DOCUMENTO_ID

	IF @TIPO_OPERACION <> 'EGR'
		BEGIN
			--SI LA OPERACION NO ES UN EGRESO ENTONCES...
			RAISERROR ('EL NRO. DE DOCUMENTO INGRESADO NO CORRESPONDE A UNA OPERACION DE EGRESO.', 16, 1)
		END
	ELSE
		BEGIN
			SELECT 	@CANT=COUNT(VIAJE_ID) 
			FROM 	PICKING P INNER JOIN DOCUMENTO DD
					ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			WHERE 	DD.DOCUMENTO_ID=@DOCUMENTO_ID

			IF @CANT>0 
			BEGIN
				RAISERROR('El picking ya fue ingresado.',16,1)
				RETURN
			END			

			INSERT INTO PICKING 
			SELECT 	 DISTINCT
					 DD.DOCUMENTO_ID
					,DD.NRO_LINEA
					,DD.CLIENTE_ID
					,DD.PRODUCTO_ID 
					,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
					,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
					,P.DESCRIPCION
					,DD.CANTIDAD
					,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
					,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
					,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.SUCURSAL_DESTINO)),ISNULL(D.NRO_REMITO,LTRIM(RTRIM(D.DOCUMENTO_ID)))))AS RUTA
					,DD.PROP1
					,NULL AS FECHA_INICIO
					,NULL AS FECHA_FIN
					,NULL AS USUARIO
					,NULL AS CANT_CONFIRMADA
					,NULL AS PALLET_PICKING
					,0 	  AS SALTO_PICKING
					,'0'  AS PALLET_CONTROLADO
					,NULL AS USUARIO_CONTROL_PICKING
					,'0'  AS ST_ETIQUETAS
					,'0'  AS ST_CAMION
					,'0'  AS FACTURADO
					,'0'  AS FIN_PICKING
					,'0'  AS ST_CONTROL_EXP
					,NULL AS FECHA_CONTROL_PALLET
					,NULL AS TERMINAL_CONTROL_PALLET
					,NULL AS FECHA_CONTROL_EXP
					,NULL AS USUARIO_CONTROL_EXP
					,NULL AS TERMINAL_CONTROL_EXPEDICION
					,NULL AS FECHA_CONTROL_FAC
					,NULL AS USUARIO_CONTROL_FAC
					,NULL AS TERMINAL_CONTROL_FAC
					,NULL AS VEHICULO_ID
					,NULL AS PALLET_COMPLETO
					,NULL AS HIJO
					,NULL AS QTY_CONTROLADO
					,NULL AS PALLET_FINAL
					,NULL AS PALLET_CERRADO
					,NULL AS USUARIO_PF
					,NULL AS TERMINAL_PF
					,'0'  AS REMITO_IMPRESO
					,NULL AS NRO_REMITO_PF
					,NULL AS PICKING_ID_REF
					,NULL AS BULTOS_CONTROLADOS
					,NULL AS BULTOS_NO_CONTROLADOS
					,C.FLG_PALLET_HOMBRE
					,'0'  AS TRANSF_TERMINADA
					,DD.NRO_LOTE AS NRO_LOTE
					,DD.NRO_PARTIDA AS NRO_PARTIDA
					,DD.NRO_SERIE AS NRO_SERIE
			FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
					ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
					INNER JOIN PRODUCTO P
					ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
					LEFT JOIN POSICION POS
					ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
					LEFT JOIN NAVE N2
					ON(POS.NAVE_ID=N2.NAVE_ID)
					INNER JOIN CLIENTE_PARAMETROS C
					ON(D.CLIENTE_ID = C.CLIENTE_ID)
			WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID


------CONTROLO QUE SERIES FUERON OBLIGATORIAS Y CUALES NO.
	
	UPDATE DET_DOCUMENTO
	SET NRO_SERIE = NULL
	WHERE DOCUMENTO_ID = @DOCUMENTO_ID
			AND NOT EXISTS (SELECT 1 FROM SYS_INT_DET_DOCUMENTO SS
							INNER JOIN SYS_INT_DOCUMENTO S ON (SS.CLIENTE_ID = S.CLIENTE_ID AND SS.DOC_EXT = S.DOC_EXT)
							WHERE S.DOC_EXT = (SELECT NRO_REMITO FROM DOCUMENTO WHERE DOCUMENTO_ID = @DOCUMENTO_ID)
									AND PROP3=DET_DOCUMENTO.NRO_SERIE)

------

		END --FIN ELSE
END --FIN PROCEDURE
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

ALTER   procedure [dbo].[IngresaMandatorioInicial]
@cliente_id as varchar(15),
@OPERACION AS VARCHAR(5)
as

declare @articulo as varchar(30)

declare pcur cursor for
select distinct producto_id 
from producto 
where cliente_id=@cliente_id and producto_id not in(	select producto_id
							from mandatorio_producto
							where cliente_id=@cliente_id
						    );


open pcur
fetch next from pcur into @articulo
while @@fetch_status = 0
begin
	insert into mandatorio_producto	values(
			 UPPER(LTRIM(RTRIM(@cliente_id)))
			,UPPER(LTRIM(RTRIM(@articulo)))
			,UPPER(LTRIM(RTRIM(@OPERACION)))
			,'CANTIDAD')
	fetch next from PCUR into @articulo
end
CLOSE PCUR
DEALLOCATE PCUR
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INGRESO_CONTENEDORAS]
	@CLIENTE_ID			varchar(15),
	@PRODUCTO_ID		varchar(30),
	@ORDEN_COMPRA		varchar(100),
	@CANTIDAD			numeric(20,5),
	@CONTENEDORA		int,
  @LOTEPROVEEDOR  VARCHAR(100),
  @PARTIDA        VARCHAR(100)                            
AS
begin
	DECLARE @USUARIO	VARCHAR(50)
	DECLARE @TERMINAL VARCHAR(100)

SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	SET @TERMINAL=HOST_NAME()
	
INSERT INTO CONFIGURACION_CONTENEDORAS(CLIENTE_ID, PRODUCTO_ID, ORDEN_COMPRA, CANTIDAD, NRO_CONTENEDORA, USUARIO, TERMINAL, FECHA, NRO_LOTE, NRO_PARTIDA)                    
VALUES                (@CLIENTE_ID,@PRODUCTO_ID,@ORDEN_COMPRA,@CANTIDAD,@CONTENEDORA,@USUARIO,@TERMINAL,GETDATE(),@LOTEPROVEEDOR,@PARTIDA)
end
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER Procedure [dbo].[Ingreso_CrossDock]
@Documento_id	Numeric(20,0) Output,
@Nro_Linea		Numeric(10,0) Output	
As
Begin
	Set xAct_Abort on 
	Declare @pCur 	Cursor
	Declare @RL_Id	Numeric(20,0)

	Set @pCur=Cursor For
		Select 	Rl.Rl_Id
		from	Det_Documento DD (NoLock) inner join Det_Documento_Transaccion DDT (NoLock)
				on(dd.Documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
				Inner Join Rl_Det_Doc_Trans_Posicion Rl (NoLock)
				on(ddt.doc_trans_id=rl.doc_trans_id And ddt.nro_linea_trans=rl.nro_linea_trans)
		Where	dd.Documento_id=@Documento_id
				and dd.nro_linea=@Nro_linea

	Open @pCur
	Fetch Next from @pCur into @RL_Id
	While @@Fetch_Status=0
	Begin
		Update Rl_Det_Doc_Trans_posicion Set Disponible='1',  Cat_Log_ID=Cat_Log_Id_Final Where Rl_Id=@Rl_ID
		Fetch Next from @pCur into @RL_Id
	End
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INGRESO_OC_ACTUALIZA]
	@CLIENTE_ID			varchar(15),
	@PRODUCTO_ID		varchar(30),
	@ORDEN_COMPRA		varchar(100),
	@CANTIDAD			numeric(20,5),
    @CANT_CONTENEDORAS	numeric(20,5)=NULL,
    @LOTEPROVEEDOR		VARCHAR(100),
	@PARTIDA			VARCHAR(100),
  @DOC_EXT      VARCHAR(100)
AS
UPDATE    INGRESO_OC
SET       CANTIDAD = @CANTIDAD, CANT_CONTENEDORAS = @CANT_CONTENEDORAS, NRO_LOTE = @LOTEPROVEEDOR, NRO_PARTIDA = @PARTIDA
WHERE     (CLIENTE_ID = @CLIENTE_ID) AND (PRODUCTO_ID = @PRODUCTO_ID) AND (ORDEN_COMPRA = @ORDEN_COMPRA) AND DOC_EXT = @DOC_EXT
	/* SET NOCOUNT ON */ 
	/*RETURN*/
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INGRESO_OC_ALTA]
	@CLIENTE_ID			varchar(15),
	@PRODUCTO_ID		varchar(30),
	@ORDEN_COMPRA		varchar(100),
	@CANTIDAD			numeric(20,5),
	@CANT_CONTENEDORAS	numeric(20,5)=NULL,
	@FECHA				datetime,
	@procesado			char(1),
	@LOTEPROVEEDOR		VARCHAR(100),
	@PARTIDA			VARCHAR(100),
  @DOC_EXT      VARCHAR(100)
AS
begin
	DECLARE @USUARIO	VARCHAR(50)
	DECLARE @TERMINAL VARCHAR(100)

SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	SET @TERMINAL=HOST_NAME()
	
INSERT INTO INGRESO_OC(CLIENTE_ID, PRODUCTO_ID, ORDEN_COMPRA, CANTIDAD, CANT_CONTENEDORAS, USUARIO, TERMINAL, FECHA, PROCESADO, NRO_LOTE, NRO_PARTIDA, DOC_EXT)                    
VALUES                (@CLIENTE_ID,@PRODUCTO_ID,@ORDEN_COMPRA,@CANTIDAD,@CANT_CONTENEDORAS,@USUARIO,@TERMINAL,@FECHA,@PROCESADO, @LOTEPROVEEDOR, @PARTIDA, @DOC_EXT)
end
	/* SET NOCOUNT ON */ 
	/*RETURN*/
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INGRESO_OC_BORRAR]
	@CLIENTE_ID		varchar(15),
	@PRODUCTO_ID	varchar(30),
	@ORDEN_COMPRA	varchar(100)
AS
DELETE FROM INGRESO_OC
WHERE     (CLIENTE_ID = @cliente_id) AND (PRODUCTO_ID = @producto_id) AND (ORDEN_COMPRA = @orden_compra)
	/* SET NOCOUNT ON */ 
	/*RETURN*/
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INGRESO_OC_EXISTE_PROD]
	@CLIENTE_ID		varchar(15),
	@PRODUCTO_ID	varchar(30),
	@ORDEN_COMPRA	varchar(100),
	@LOTE_PROVEEDOR	varchar(100),
	@PARTIDA		varchar(100)
AS
	SELECT	producto_id
	FROM	INGRESO_OC
	WHERE   CLIENTE_ID = @CLIENTE_ID
			AND PRODUCTO_ID = @PRODUCTO_ID
			AND ORDEN_COMPRA = @ORDEN_COMPRA
			AND ISNULL(PROCESADO,'0') = '0'
			AND NRO_LOTE=@LOTE_PROVEEDOR
			AND NRO_PARTIDA = @PARTIDA
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    procedure [dbo].[IngVerificaIntermedia]
@Doc_trans_id numeric(20,0) output,
@Out int output
As
Begin

	Declare @vRlId  as Numeric(20,0)
	Declare @Q1		as int
	Declare @Q2		as int
	Declare @Return as int

	Declare Cur_VerIntIng cursor For
		Select 	Rl_id
		from	Rl_Det_Doc_trans_posicion rl inner join Det_documento_transaccion ddt
				on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans =ddt.nro_linea_trans)
		Where	ddt.doc_trans_id=@Doc_trans_id


	Open Cur_VerIntIng
		
	Fetch Next from Cur_VerIntIng Into @vRlId
	While @@Fetch_Status=0
		Begin
		
			SELECT 	@Q1=COUNT(RL_ID)
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	RL.RL_ID=@vRlId
					AND N.INTERMEDIA='1'
		
		
		
		
			SELECT 	@Q2=COUNT(RL_ID)
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	RL.RL_ID=@vRlId
					AND P.INTERMEDIA='1'
		
			
		
			If @Q1=1 Or @Q2=1
				Begin
					set @Return=1
					Break
				End
			Else
				Begin
					set @Return=0
				End
	
			Fetch Next from Cur_VerIntIng Into @vRlId
					
		End --Fin While
	set @Out=@Return

	Close Cur_VerIntIng
	deallocate Cur_VerIntIng
End --Fin Procedure
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INSERT_CONTENEDORAS]
@PRODUCTO_ID	VARCHAR(30),
@CANTIDAD		FLOAT
AS
BEGIN
	DECLARE @UNIDAD_ID	VARCHAR(5)
	DECLARE @CLIENTE_ID	VARCHAR(15)
	SET @CLIENTE_ID='LEADER PRICE'

	SELECT @UNIDAD_ID=UNIDAD_ID FROM PRODUCTO WHERE CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
	
	INSERT INTO RL_PRODUCTO_UNIDAD_CONTENEDORA (CLIENTE_ID, PRODUCTO_ID, UNIDAD_ID,CANTIDAD,FLG_PICKING, INGRESO)
	VALUES(@CLIENTE_ID, @PRODUCTO_ID, @UNIDAD_ID, @CANTIDAD,'1','1')

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[Insert_Picking_Wave]
@PWave_id			as Numeric(20,0) Output,
@PDocTransId		as Numeric(20,0) Output
As

Begin
	Declare @Secuencia 	as Numeric(20,0)
	Declare @Usuario_id	as Varchar(20)

	If @PWave_id is null
	Begin
		Exec Dbo.Get_Value_For_Sequence 'PICKING_WAVE', @Secuencia Output
		Set @PWave_id=@Secuencia
	End;

	Select @Usuario_id=Usuario_Id	From #Temp_Usuario_Loggin
		
	Insert into Sys_Picking_Wave (Wave_Id, Doc_Trans_id, Fecha, Usuario_Id) 
	Values(
			 @PWave_id
			,@PDocTransId
			,GetDate()
			,@Usuario_Id
			);
End;
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INSERT_RL_VH]
@POSICION	VARCHAR(45),
@VEHICULO 	VARCHAR(50)
AS
BEGIN
	DECLARE @POSICION_ID	AS BIGINT
	
	SELECT 	@POSICION_ID=POSICION_ID
	FROM	POSICION
	WHERE 	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION)))

	IF @VEHICULO='NO'
	BEGIN
		RETURN
	END 
	IF @POSICION_ID IS NOT NULL
	BEGIN
		INSERT INTO RL_VEHICULO_POSICION (VEHICULO_ID, POSICION_ID)
		VALUES(@VEHICULO,@POSICION_ID )
	END
	
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[INSERT_RLPPP]
@CLIENTE_ID		VARCHAR(15),
@PRODUCTO_ID	VARCHAR(30),
@NAVE_COD		VARCHAR(15),
@POSICION_COD	VARCHAR(45)
As
Begin

	Declare @Nave_ID 		as Numeric(20,0)
	Declare @Posicion_Id	as Numeric(20,0)
	Declare @Control		as float(1)
	Declare @Msg			as varchar(4000)
	Declare @error_var		as int

	--Obtengo la Posicion Id en caso de que no sea null
	If @POSICION_COD is not null
	Begin
		Set @Posicion_Id=Dbo.Get_Posicion_id(@Posicion_Cod)
	End
	Else
	Begin
		Set @Posicion_Id=Null
	End	

	--Obtengo la Posicion Id en caso de que no sea null
	If @NAVE_COD is not null
	Begin
		Select 	@Nave_ID=Nave_Id
		From 	Nave
		Where	nave_cod=ltrim(rtrim(upper(@NAVE_COD)))
	End
	Else
	Begin
		Set @Nave_ID=Null
	End	

	If (@Producto_id Is null) Or (ltrim(rtrim(upper(@Producto_Id)))='')
	Begin
		Set @Msg='El campo producto no puede estar vacio.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	--Controlo el producto
	Select 	@Control=Count(*)
	from 	Producto
	Where	Cliente_id=ltrim(rtrim(Upper(@Cliente_id)))
			and Producto_id=ltrim(rtrim(Upper(@Producto_id)))

	If @Control=0
	Begin
		Set @Msg='Producto inexistente, por favor verifique estos valores.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	-- Controlo que no se ingrese basura a la tabla, al menos uno deberia tener valores.
	If (@Nave_id is null) and (@posicion_id is null)
	Begin
		Set @Msg='La nave o la posicion no existen, por favor verifique estos valores.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	--Inserto en la tabla
	INSERT INTO RL_PRODUCTO_POSICION_PERMITIDA (CLIENTE_ID, PRODUCTO_ID, NAVE_ID, POSICION_ID) 
	VALUES(@Cliente_id, @Producto_Id, @Nave_Id, @Posicion_id)


	--Controlo la condicion de error.

	SELECT @error_var = @@ERROR
	If @error_var<> 0 
	Begin
		Set @Msg='Ocurrio un error inesperado al insertar en la tabla Rl_Producto_Posicion_Permitida. - COD. ERROR: ' + CAST(@error_var AS VARCHAR(10))
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg );
		Return
	End

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Jb_Close_Documents_Egr]
As
Begin
	Set xAct_abort On
	Declare @Doc_trans	numeric(20,0)
	Declare @CloseDoc	Cursor

	Set @CloseDoc=Cursor for
		select	ddt.doc_trans_id
		from	picking p inner join det_documento dd
				on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
				inner join documento d 
				on(d.documento_id=dd.documento_id)
				inner join det_documento_transaccion ddt
				on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
		where	d.status='D30'
				and p.facturado='1'
				and p.fin_picking='2'
		group by
				d.documento_id, d.status, ddt.doc_trans_id
	Open @CloseDoc
	Fetch Next From @CloseDoc into @Doc_Trans
	While @@Fetch_Status=0
	Begin
		exec Egr_aceptar_job @Doc_Trans
		Fetch Next From @CloseDoc into @Doc_Trans
	End
	Close @CloseDoc
	Deallocate @CloseDoc
End	--End Job.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[Job_Ft_Rl_Doc_Trans_Posicion]
As
Begin
	DECLARE @FECHA	DATETIME
	DECLARE @AUDITA	CHAR(1)

	SELECT @AUDITA=AUDITABLE FROM PARAMETROS_AUDITORIA WHERE TIPO_AUDITORIA_ID=13

	IF @AUDITA='1'
	BEGIN

		SET @FECHA=GETDATE()
		
		INSERT INTO DBO.RL_DET_DOC_TRANS_POSICION_HISTORICO 
		SELECT 	 RL_ID
				,DOC_TRANS_ID
				,NRO_LINEA_TRANS
				,POSICION_ANTERIOR
				,POSICION_ACTUAL
				,CANTIDAD
				,TIPO_MOVIMIENTO_ID
				,ULTIMA_ESTACION
				,ULTIMA_SECUENCIA
				,NAVE_ANTERIOR
				,NAVE_ACTUAL
				,DOCUMENTO_ID
				,NRO_LINEA
				,DISPONIBLE
				,DOC_TRANS_ID_EGR
				,NRO_LINEA_TRANS_EGR
				,DOC_TRANS_ID_TR
				,NRO_LINEA_TRANS_TR
				,CLIENTE_ID
				,CAT_LOG_ID
				,CAT_LOG_ID_FINAL
				,EST_MERC_ID
				,@FECHA
		FROM 	RL_DET_DOC_TRANS_POSICION
	END
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Job_Libera_Tareas]
As
Begin
	Declare @Dif		as Int
	Declare @Cur	Cursor
	Declare @Doc	as Numeric(20,0)
	Declare @Line	as Numeric(10,0)
	Declare @Fecha	as DateTime
	
	Set @Cur= Cursor For
		Select	Documento_Id, Nro_Linea, Fecha_Lock
		From	Sys_Lock_Pallet
		Where	Lock='1'

	Open @Cur

	Fetch Next From @Cur Into @Doc, @Line, @Fecha
	While @@Fetch_Status=0
	Begin
		Select @Dif=DateDiff(mi, @Fecha, Getdate())	
		if @Dif >= 15
		Begin
			Update Sys_Lock_Pallet Set Lock='0' Where Documento_Id=@Doc and Nro_Linea=@Line
		End
		Fetch Next From @Cur Into @Doc, @Line, @Fecha
	End	
	Close @Cur
	Deallocate @Cur
End -- Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[JOB_PROD_AGRUPADO_HISTORICO]
AS
BEGIN

	INSERT INTO DBO.PRODUCTO_AGRUPADO_HISTORICO
	SELECT 	RL.CLIENTE_ID, DD.PRODUCTO_ID, SUM(RL.CANTIDAD), GETDATE()
	FROM	RL_DET_DOC_TRANS_POSICION RL
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DOCUMENTO_TRANSACCION DT
			ON(DT.DOC_TRANS_ID=DDT.DOC_TRANS_ID)
			INNER JOIN DET_DOCUMENTO DD 
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN DOCUMENTO D
			ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			LEFT JOIN NAVE N ON(RL.NAVE_ACTUAL=N.NAVE_ID)
	WHERE	D.STATUS='D40'
	GROUP BY
			RL.CLIENTE_ID, DD.PRODUCTO_ID


END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER     Procedure [dbo].[Libera_Lockeo_Pallet]
@Pallet		Varchar(100) Output
As
Begin
	Declare @Documento_Id		as Numeric(20,0)
	Declare @Nro_Linea			as Numeric(10,0)

	Select 	@Documento_id=Documento_id--, @Nro_Linea=Nro_Linea
	From	Det_Documento
	Where	Prop1=Ltrim(Rtrim(Upper(@Pallet)))

	Update Sys_Lock_Pallet	Set	Lock='0' Where	Documento_Id=@Documento_Id And  Pallet=@Pallet --Nro_Linea=@Nro_Linea And
	
	If @@RowCount=0
	Begin
		Raiserror('No se actualizo ningun registro. Libera_Lockeo_Pallet.',16,1)
		Return
	End
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[LIBERAR_POSLOCKEADA]
	@POSICION_ID	NUMERIC(20,0) OUTPUT
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	
	UPDATE POSICION SET POS_LOCKEADA='0' WHERE POSICION_ID=@POSICION_ID
	DELETE FROM lockeo_posicion WHERE POSICION_ID=@POSICION_ID

END
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