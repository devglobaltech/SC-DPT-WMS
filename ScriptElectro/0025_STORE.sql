USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 05:43 p.m.
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

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[AUDITORIA_HIST_AJUSTE]
@RL_ID			NUMERIC(20,0) OUTPUT,
@QTY			FLOAT	OUTPUT
AS
BEGIN
	DECLARE @USUARIO	VARCHAR(15)

	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	INSERT INTO AUDITORIA_HISTORICOS
	SELECT 	 15									[TIPO_AUDITORIA_ID]
			,RL.CLIENTE_ID		
			,D.NRO_DESPACHO_IMPORTACION			[DOC_EXT]
			,NULL								[NRO_LINEA_EXT]
			,DD.DOCUMENTO_ID	
			,DD.NRO_LINEA		
			,DDT.DOC_TRANS_ID	
			,DDT.NRO_LINEA_TRANS
			,DD.PRODUCTO_ID		
			,DD.NRO_SERIE		
			,DD.NRO_SERIE_PADRE
			,RL.EST_MERC_ID
			,RL.CAT_LOG_ID
			,DD.NRO_BULTO
			,DD.NRO_LOTE
			,DD.FECHA_VENCIMIENTO
			,DD.NRO_DESPACHO
			,DD.NRO_PARTIDA
			,DD.UNIDAD_ID
			,DD.PESO
			,DD.UNIDAD_PESO
			,DD.VOLUMEN
			,DD.UNIDAD_VOLUMEN
			,DD.BUSC_INDIVIDUAL
			,DD.TIE_IN
			,DD.NRO_TIE_IN_PADRE
			,DD.NRO_TIE_IN
			,DD.ITEM_OK
			,RL.CAT_LOG_ID_FINAL
			,DD.MONEDA_ID
			,DD.COSTO
			,DD.PROP1
			,DD.PROP2
			,DD.PROP3
			,DD.LARGO
			,DD.ALTO
			,DD.ANCHO
			,DD.VOLUMEN_UNITARIO
			,PESO_UNITARIO
			,@QTY
			,RL.RL_ID
			,@USUARIO							[USUARIO_ID]
			,HOST_NAME()						[TERMINAL]
			,GETDATE()							[FECHA_AUDITORIA]
			,NULL								[NAVE_ID_SUG]
			,NULL								[POSICION_ID_SUG]
			,RL.NAVE_ACTUAL
			,RL.POSICION_ACTUAL
	FROM	RL_DET_DOC_TRANS_POSICION RL (NOLOCK)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT (NOLOCK)	ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD	(NOLOCK)				ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D (NOLOCK)						ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
	WHERE	RL.RL_ID=@RL_ID
			

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

ALTER      PROCEDURE [dbo].[AUDITORIA_HIST_INSERT_CATLOG]
	@vRL_ID 	AS NUMERIC(20,0)	OUTPUT,
	@vCATOLD	AS VARCHAR(50)	OUTPUT,
	@vCATNEW     	AS VARCHAR(50)	OUTPUT,
	@vQTY		AS NUMERIC(25,5)	OUTPUT

AS
BEGIN

	DECLARE @Usuario			AS VARCHAR (30)
	DECLARE @Terminal			AS VARCHAR (100)
	DECLARE @FLAG				AS CHAR (1)

	SELECT @FLAG = AUDITABLE FROM PARAMETROS_AUDITORIA WHERE TIPO_AUDITORIA_ID = 5

	SELECT 	@Usuario=Usuario_id FROM #Temp_Usuario_Loggin
	SELECT  @Terminal=Host_Name()

	IF @FLAG = '1'
		BEGIN
			--NEGATIVO
			INSERT INTO AUDITORIA_HISTORICOS 
			SELECT	5
				,DD.CLIENTE_ID
				,D.NRO_DESPACHO_IMPORTACION
				,NULL
				,DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,RL.DOC_TRANS_ID
				,RL.NRO_LINEA_TRANS
				,DD.PRODUCTO_ID
				,DD.NRO_SERIE
				,DD.NRO_SERIE_PADRE
				,RL.EST_MERC_ID
				,@vCATOLD
				,DD.NRO_BULTO
				,DD.NRO_LOTE 
				,DD.FECHA_VENCIMIENTO
				,DD.NRO_DESPACHO
				,DD.NRO_PARTIDA
				,DD.UNIDAD_ID
				,DD.PESO 
				,DD.UNIDAD_PESO
				,DD.VOLUMEN 
				,DD.UNIDAD_VOLUMEN
				,DD.BUSC_INDIVIDUAL
				,DD.TIE_IN 
				,DD.NRO_TIE_IN_PADRE
				,DD.NRO_TIE_IN
				,DD.ITEM_OK 
				,RL.CAT_LOG_ID_FINAL
				,DD.MONEDA_ID
				,DD.COSTO
				,DD.PROP1
				,DD.PROP2
				,DD.PROP3
				,DD.LARGO
				,DD.ALTO 
				,DD.ANCHO
				,DD.VOLUMEN_UNITARIO
				,DD.PESO_UNITARIO
				,@vQTY * -1
				,@vRL_ID
				,@Usuario
				,@Terminal
				,GETDATE()
				,NULL
				,NULL
				,RL.NAVE_ACTUAL
				,RL.POSICION_ACTUAL
			FROM 	DOCUMENTO D
				INNER JOIN DET_DOCUMENTO DD ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT. NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
			WHERE 	RL.RL_ID = @vRL_ID
		
			--POSITIVO
			INSERT INTO AUDITORIA_HISTORICOS 
			SELECT	5
				,DD.CLIENTE_ID
				,D.NRO_DESPACHO_IMPORTACION
				,NULL
				,DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,RL.DOC_TRANS_ID
				,RL.NRO_LINEA_TRANS
				,DD.PRODUCTO_ID
				,DD.NRO_SERIE
				,DD.NRO_SERIE_PADRE
				,RL.EST_MERC_ID
				,@vCATNEW
				,DD.NRO_BULTO
				,DD.NRO_LOTE 
				,DD.FECHA_VENCIMIENTO
				,DD.NRO_DESPACHO
				,DD.NRO_PARTIDA
				,DD.UNIDAD_ID
				,DD.PESO 
				,DD.UNIDAD_PESO
				,DD.VOLUMEN 
				,DD.UNIDAD_VOLUMEN
				,DD.BUSC_INDIVIDUAL
				,DD.TIE_IN 
				,DD.NRO_TIE_IN_PADRE
				,DD.NRO_TIE_IN
				,DD.ITEM_OK 
				,RL.CAT_LOG_ID_FINAL
				,DD.MONEDA_ID
				,DD.COSTO
				,DD.PROP1
				,DD.PROP2
				,DD.PROP3
				,DD.LARGO
				,DD.ALTO 
				,DD.ANCHO
				,DD.VOLUMEN_UNITARIO
				,DD.PESO_UNITARIO
				,@vQTY
				,@vRL_ID
				,@Usuario
				,@Terminal
				,DATEADD(SS,1,GETDATE())
				,NULL
				,NULL
				,RL.NAVE_ACTUAL
				,RL.POSICION_ACTUAL
			FROM 	DOCUMENTO D
				INNER JOIN DET_DOCUMENTO DD ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT. NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
			WHERE 	RL.RL_ID = @vRL_ID
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

ALTER   PROCEDURE [dbo].[AUDITORIA_HIST_INSERT_ESTMERC]
	@vRL_ID 	AS NUMERIC(20,0)	OUTPUT,
	@vESTOLD	AS VARCHAR(50)	OUTPUT,
	@vESTNEW     	AS VARCHAR(50)	OUTPUT,
	@vQTY		AS NUMERIC(25,5)	OUTPUT

AS
BEGIN

	DECLARE @Usuario			AS VARCHAR (30)
	DECLARE @Terminal			AS VARCHAR (100)
	DECLARE @FLAG				AS CHAR (1)

	SELECT @FLAG = AUDITABLE FROM PARAMETROS_AUDITORIA WHERE TIPO_AUDITORIA_ID = 6

	SELECT 	@Usuario=Usuario_id FROM #Temp_Usuario_Loggin
	SELECT  @Terminal=Host_Name()

	IF @FLAG = '1'
		BEGIN
			--NEGATIVO
			INSERT INTO AUDITORIA_HISTORICOS 
				SELECT	6
				,DD.CLIENTE_ID
				,D.NRO_DESPACHO_IMPORTACION
				,NULL
				,DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,RL.DOC_TRANS_ID
				,RL.NRO_LINEA_TRANS
				,DD.PRODUCTO_ID
				,DD.NRO_SERIE
				,DD.NRO_SERIE_PADRE
				,@vESTOLD
				,ISNULL (RL.CAT_LOG_ID, RL.CAT_LOG_ID_FINAL)
				,DD.NRO_BULTO
				,DD.NRO_LOTE 
				,DD.FECHA_VENCIMIENTO
				,DD.NRO_DESPACHO
				,DD.NRO_PARTIDA
				,DD.UNIDAD_ID
				,DD.PESO 
				,DD.UNIDAD_PESO
				,DD.VOLUMEN 
				,DD.UNIDAD_VOLUMEN
				,DD.BUSC_INDIVIDUAL
				,DD.TIE_IN 
				,DD.NRO_TIE_IN_PADRE
				,DD.NRO_TIE_IN
				,DD.ITEM_OK 
				,RL.CAT_LOG_ID_FINAL
				,DD.MONEDA_ID
				,DD.COSTO
				,DD.PROP1
				,DD.PROP2
				,DD.PROP3
				,DD.LARGO
				,DD.ALTO 
				,DD.ANCHO
				,DD.VOLUMEN_UNITARIO
				,DD.PESO_UNITARIO
				,@vQTY * -1
				,@vRL_ID
				,@Usuario
				,@Terminal
				,GETDATE()
				,NULL
				,NULL
				,RL.NAVE_ACTUAL
				,RL.POSICION_ACTUAL
			FROM 	DOCUMENTO D
				INNER JOIN DET_DOCUMENTO DD ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT. NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
			WHERE 	RL.RL_ID = @vRL_ID
		
			--POSITIVO
			INSERT INTO AUDITORIA_HISTORICOS 
			SELECT	6
				,DD.CLIENTE_ID
				,D.NRO_DESPACHO_IMPORTACION
				,NULL
				,DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,RL.DOC_TRANS_ID
				,RL.NRO_LINEA_TRANS
				,DD.PRODUCTO_ID
				,DD.NRO_SERIE
				,DD.NRO_SERIE_PADRE
				,@vESTNEW
				,ISNULL (RL.CAT_LOG_ID, RL.CAT_LOG_ID_FINAL)
				,DD.NRO_BULTO
				,DD.NRO_LOTE 
				,DD.FECHA_VENCIMIENTO
				,DD.NRO_DESPACHO
				,DD.NRO_PARTIDA
				,DD.UNIDAD_ID
				,DD.PESO 
				,DD.UNIDAD_PESO
				,DD.VOLUMEN 
				,DD.UNIDAD_VOLUMEN
				,DD.BUSC_INDIVIDUAL
				,DD.TIE_IN 
				,DD.NRO_TIE_IN_PADRE
				,DD.NRO_TIE_IN
				,DD.ITEM_OK 
				,RL.CAT_LOG_ID_FINAL
				,DD.MONEDA_ID
				,DD.COSTO
				,DD.PROP1
				,DD.PROP2
				,DD.PROP3
				,DD.LARGO
				,DD.ALTO 
				,DD.ANCHO
				,DD.VOLUMEN_UNITARIO
				,DD.PESO_UNITARIO
				,@vQTY
				,@vRL_ID
				,@Usuario
				,@Terminal
				,DATEADD(SS,1,GETDATE())
				,NULL
				,NULL
				,RL.NAVE_ACTUAL
				,RL.POSICION_ACTUAL
			FROM 	DOCUMENTO D
				INNER JOIN DET_DOCUMENTO DD ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT. NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
			WHERE 	RL.RL_ID = @vRL_ID
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

ALTER PROCEDURE [dbo].[AUDITORIA_HIST_INSERT_TR]
	@DOC		AS NUMERIC(20,0)	OUTPUT,
	@NRO_LINEA	AS NUMERIC(10,0)	OUTPUT,
	@NAVE_O	AS NUMERIC(20,0)	OUTPUT,
	@NAVE_D	AS NUMERIC(20,0)	OUTPUT,
	@POSICION_O	AS NUMERIC(20,0)	OUTPUT,
	@POSICION_D	AS NUMERIC(20,0)	OUTPUT
AS
BEGIN

	DECLARE @Usuario			AS VARCHAR (30)
	DECLARE @Terminal			AS VARCHAR (100)
	DECLARE @FLAG				AS CHAR (1)

	SELECT @FLAG = AUDITABLE FROM PARAMETROS_AUDITORIA WHERE TIPO_AUDITORIA_ID = 14

	SELECT 	@Usuario=Usuario_id FROM #Temp_Usuario_Loggin
	SELECT  @Terminal=Host_Name()

	IF @FLAG = '1'
		BEGIN
			--NEGATIVO
			INSERT INTO AUDITORIA_HISTORICOS 
			SELECT	14
				,DD.CLIENTE_ID
				,D.NRO_DESPACHO_IMPORTACION
				,NULL
				,DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,RL.DOC_TRANS_ID
				,RL.NRO_LINEA_TRANS
				,DD.PRODUCTO_ID
				,DD.NRO_SERIE
				,DD.NRO_SERIE_PADRE
				,RL.EST_MERC_ID
				,RL.CAT_LOG_ID 
				,DD.NRO_BULTO
				,DD.NRO_LOTE 
				,DD.FECHA_VENCIMIENTO
				,DD.NRO_DESPACHO
				,DD.NRO_PARTIDA
				,DD.UNIDAD_ID
				,DD.PESO 
				,DD.UNIDAD_PESO
				,DD.VOLUMEN 
				,DD.UNIDAD_VOLUMEN
				,DD.BUSC_INDIVIDUAL
				,DD.TIE_IN 
				,DD.NRO_TIE_IN_PADRE
				,DD.NRO_TIE_IN
				,DD.ITEM_OK 
				,RL.CAT_LOG_ID_FINAL
				,DD.MONEDA_ID
				,DD.COSTO
				,DD.PROP1
				,DD.PROP2
				,DD.PROP3
				,DD.LARGO
				,DD.ALTO 
				,DD.ANCHO
				,DD.VOLUMEN_UNITARIO
				,DD.PESO_UNITARIO
				,RL.CANTIDAD * -1
				,RL.RL_ID
				,@Usuario
				,@Terminal
				,GETDATE()
				,NULL
				,NULL
				,@NAVE_O
				,@POSICION_O
			FROM 	DOCUMENTO D
				INNER JOIN DET_DOCUMENTO DD ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT. NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
			WHERE 	RL.DOC_TRANS_ID_TR = @DOC AND RL.NRO_LINEA_TRANS_TR = @NRO_LINEA

			--POSITIVO
			INSERT INTO AUDITORIA_HISTORICOS 
			SELECT	14
				,DD.CLIENTE_ID
				,D.NRO_DESPACHO_IMPORTACION
				,NULL
				,DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,RL.DOC_TRANS_ID
				,RL.NRO_LINEA_TRANS
				,DD.PRODUCTO_ID
				,DD.NRO_SERIE
				,DD.NRO_SERIE_PADRE
				,RL.EST_MERC_ID
				,RL.CAT_LOG_ID 
				,DD.NRO_BULTO
				,DD.NRO_LOTE 
				,DD.FECHA_VENCIMIENTO
				,DD.NRO_DESPACHO
				,DD.NRO_PARTIDA
				,DD.UNIDAD_ID
				,DD.PESO 
				,DD.UNIDAD_PESO
				,DD.VOLUMEN 
				,DD.UNIDAD_VOLUMEN
				,DD.BUSC_INDIVIDUAL
				,DD.TIE_IN 
				,DD.NRO_TIE_IN_PADRE
				,DD.NRO_TIE_IN
				,DD.ITEM_OK 
				,RL.CAT_LOG_ID_FINAL
				,DD.MONEDA_ID
				,DD.COSTO
				,DD.PROP1
				,DD.PROP2
				,DD.PROP3
				,DD.LARGO
				,DD.ALTO 
				,DD.ANCHO
				,DD.VOLUMEN_UNITARIO
				,DD.PESO_UNITARIO
				,RL.CANTIDAD
				,RL.RL_ID
				,@Usuario
				,@Terminal
				,DATEADD(SS,1,GETDATE())
				,NULL
				,NULL
				,@NAVE_D
				,@POSICION_D
			FROM 	DOCUMENTO D
				INNER JOIN DET_DOCUMENTO DD ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT. NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
			WHERE 	RL.DOC_TRANS_ID_TR = @DOC AND RL.NRO_LINEA_TRANS_TR =@NRO_LINEA

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

ALTER            Procedure [dbo].[AutoCompletarEstacion]
@pDocumento_id 	as Numeric(20,0) Output--,
--@Control		as Char(1) Output
As
Begin
	---------------------------------------------------
	--		Declaraciones.
	---------------------------------------------------
	Declare @strSql				as nvarchar(4000)
	Declare @SumaCantidad 		as float
	Declare @i 					as BigInt
	Declare @okUbic 			as Char(1)
	Declare @p_Cantidad			as numeric(20, 5)
	---------------------------------------------------
	--		Cursor @RsDatos.
	---------------------------------------------------
	Declare @Documento_idD 		as numeric(20, 0) 
	Declare @Nro_lineaD 		as numeric(10, 0) 
	Declare @Cliente_idD 		as varchar (15)  
	Declare @Producto_idD 		as varchar (30)  
	Declare @CantidadD 			as numeric(20, 5) 
	Declare @Nro_serieD 		as varchar (50)  
	Declare @Nro_serie_padreD 	as varchar (50)  
	Declare @Est_merc_idD 		as varchar (15)  
	Declare @Cat_log_idD 		as varchar (50)  
	Declare @Nro_bultoD 		as varchar (50)  
	Declare @DescripcionD 		as varchar (200)  
	Declare @Nro_loteD 			as varchar (50)  
	Declare @Fecha_vencimientoD as datetime 
	Declare @Nro_despachoD 		as varchar (50)  
	Declare @Nro_partidaD 		as varchar (50)  
	Declare @Unidad_idD 		as varchar (5)  
	Declare @PesoD 				as numeric(20, 5) 
	Declare @Unidad_pesoD 		as varchar (5)  
	Declare @VolumenD 			as numeric(20, 5) 
	Declare @Unidad_volumenD 	as varchar (5)  
	Declare @Busc_individualD 	as varchar (1)  
	Declare @Tie_inD 			as varchar (1)
	Declare @Nro_tie_in_padreD 	as varchar (100)  
	Declare @Nro_tie_inD 		as varchar (100)  
	Declare @Item_okD 			as varchar (1)  
	Declare @Cat_log_id_finalD 	as varchar (50)  
	Declare @Moneda_idD 		as varchar (20)  
	Declare @CostoD 			as numeric(10, 3) 
	Declare @Prop1D 			as varchar (100)  
	Declare @Prop2D 			as varchar (100)  
	Declare @Prop3D 			as varchar (100)  
	Declare @LargoD 			as numeric(10, 3) 
	Declare @AltoD 				as numeric(10, 3) 
	Declare @AnchoD 			as numeric(10, 3) 
	Declare @Volumen_unitarioD 	as varchar (1)
	Declare @Peso_unitarioD 	as varchar (1)
	Declare @Cant_solicitadaD 	as numeric(20, 5) 
	Declare @Trace_back_orderD 	as varchar (1)  
	Declare @Doc_Trans_idD		as numeric(20, 0)
	Declare @Nro_Linea_TransD	as numeric(10, 0)
	Declare @pTieIND			as varchar(1)
	---------------------------------------------------
	--		Cursor @RsPosicion.
	---------------------------------------------------
	Declare @Documento_idP		as Numeric(20, 0)
	Declare @ClienteidP			as varchar(15)
	Declare @ProductoidP		as varchar(30)
	Declare @CantidadP			as numeric(20,5)
	Declare @Unidad_idP			as varchar(5)
	Declare @Nro_serieP			as varchar(50)
	Declare @Nro_loteP			as varchar(50)
	Declare @Fecha_vencimientoP	as DateTime
	Declare @Nro_despachoP		as varchar(50)
	Declare @Nro_bultoP			as varchar(50)
	Declare @Nro_partidaP		as varchar(50)
	Declare @PesoP				as numeric(20, 5)
	Declare @VolumenP			as numeric(20, 5)
	Declare @KitP				as varchar(50)
	Declare @Tie_inP			as varchar(50)
	Declare @Nro_tie_in_padreP	as varchar(50)
	Declare @Nro_tie_inP		as varchar(50)
	Declare @StorageP			as varchar(45)--nave_cod
	Declare @NaveidP			as numeric(20, 0)
	Declare @Calle_codP			as varchar(15)
	Declare @Calle_idP			as numeric(20, 0)
	Declare @Columna_codP		as varchar(15)
	Declare @Columna_idP		as numeric(20, 0)
	Declare @Nivel_codP			as varchar(15)
	Declare @Nivel_idP			as numeric(20, 0)
	Declare @Cat_log_idP		as varchar(50)
	Declare @Est_merc_idP		as varchar(50)
	Declare @PosicionidP		as numeric(20, 0)
	Declare @Prop1P				as varchar(100)
	Declare @Prop2P				as varchar(100)
	Declare @Prop3P				as varchar(100)
	Declare @Fecha_cpteP		as Datetime
	Declare @Fecha_alta_gtwP	as Datetime
	Declare @Rl_idP				as numeric(20, 0)
	Declare @Unidad_pesoP		as varchar(5)
	Declare @Unidad_volumenP	as varchar(5)
	Declare @Moneda_idP			as varchar(20)
	Declare @CostoP				as numeric(20,5)
	Declare @Orden_pickingP		as numeric(3, 0)
	---------------------------------------------------
	--		Cursores.
	---------------------------------------------------
	Declare @RsDatos			as Cursor
	Declare @RsPosicion			as Cursor
	Declare @rsParametros		as Cursor
	---------------------------------------------------

	Create Table #Tmp_Q2(
		Documento_id 			numeric(20,0),
		Clienteid 				varchar(15),
		Productoid 				varchar(30),
		Cantidad				numeric(20,5),
		Unidad_id 				varchar(5),
		Nro_serie 				varchar(50),
		Nro_lote				varchar(50),
		Fecha_vencimiento 		datetime,
		Nro_despacho			varchar(50),
		Nro_bulto 				varchar(50),
		Nro_partida 			varchar(50),
		Peso 					numeric(20,5),
		Volumen 				numeric(20,5),
		Kit						varchar(30),
		Tie_in					varchar(1),
		Tie_in_padre			varchar(100),
		Nro_tie_in 				varchar(100),
		Storage					varchar(15),
		Naveid 					numeric(20,0),
		Callecod				varchar(15),
		Calleid 				numeric(20,0),
		Columnacod				varchar(15),
		Columnaid				numeric(20,0),
		Nivelcod				varchar(15),
		Nivelid					numeric(20,0),
		Cat_log_id				varchar(50),
		Est_merc_id				varchar(15),
		Posicionid				numeric(20,0),
		Prop1					varchar(100),
		Prop2					varchar(100),
		Prop3					varchar(100),
		Fecha_cpte				datetime,
		Fecha_alta_gtw			datetime,
		Rl_id					numeric(20,0),
		Unidad_peso				varchar(5),
		Unidad_volumen 			varchar(5),
		Moneda_id				varchar(50),
		Costo					numeric(20,5),
		Orden_picking			numeric(20,0)
	)

	SET NOCOUNT ON;

	--Set @Control='0'

	Set @RsDatos = Cursor For
		select	dd.*,ddt.doc_trans_id,ddt.nro_linea_trans
		from 	det_documento dd 
				inner join det_documento_transaccion ddt on (dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
		where 	dd.documento_id= @pDocumento_id 
    
	Open @RsDatos
    
	Fetch Next From @RsDatos into     @Documento_idD 		 	, @Nro_lineaD 			, @Cliente_idD 		  
									, @Producto_idD 		  	, @CantidadD 		 	, @Nro_serieD 		  
									, @Nro_serie_padreD 	  	, @Est_merc_idD 	 	, @Cat_log_idD 		  
									, @Nro_bultoD 		  		, @DescripcionD 		, @Nro_loteD 			  
									, @Fecha_vencimientoD  		, @Nro_despachoD 		, @Nro_partidaD 		  
									, @Unidad_idD 				, @PesoD 				, @Unidad_pesoD 	
									, @VolumenD 			 	, @Unidad_volumenD 		, @Busc_individualD 
									, @Tie_inD 					, @Nro_tie_in_padreD 	, @Nro_tie_inD 		  
									, @Item_okD 				, @Cat_log_id_finalD 	, @Moneda_idD 		
									, @CostoD 					, @Prop1D 			  	, @Prop2D 			  
									, @Prop3D 			  		, @LargoD 				, @AltoD 			
									, @AnchoD 					, @Volumen_unitarioD	, @Peso_unitarioD 	
									, @Cant_solicitadaD 	 	, @Trace_back_orderD 	, @Doc_Trans_idD		
									, @Nro_Linea_TransD	
	While @@Fetch_Status=0
	Begin

		If @Tie_inD= '0'
		Begin
			Set @pTieIND = Null
		End

		If @CostoD = 0 
		Begin
			Set @CostoD = Null
		End

		Truncate table #Tmp_Q2

		Exec Locator_Api#Get_Productos_Locator_Rl     	 @Cliente_IdD
														,@Producto_idD
														,@CantidadD
														,@pTieIND
														,@Nro_serieD
														,@Nro_loteD
														,@Fecha_vencimientoD
														,@nro_despachoD
														,@nro_bultoD
														,@nro_partidaD
														,@Cat_Log_id_finalD
														,@pesoD
														,@volumenD
														,Null
														,Null
														,Null
														,Null
														,'EGR'
														,'1'
														,@Est_merc_idD
														,Null
														,@Prop1D
														,@Prop2D
														,@Prop3D
														,@Unidad_idD
														,@Unidad_pesoD
														,@unidad_volumenD
														,@Documento_idD
														,@Doc_trans_idD
														,@moneda_idD
														,@CostoD
														,'1'
														,'1'
														,Null
														,Null
														--,@RsPosicion Output
		Set @RsPosicion= Cursor For
			Select 	* 
			from	#Tmp_Q2
	
		Open @RsPosicion	

		Set @SumaCantidad=0

		Fetch Next From @RsPosicion into  @Documento_idP		, @ClienteidP			, @ProductoidP
										, @CantidadP			, @Unidad_idP			, @Nro_serieP
										, @Nro_loteP			, @Fecha_vencimientoP	, @Nro_despachoP
										, @Nro_bultoP			, @Nro_partidaP			, @PesoP
										, @VolumenP				, @KitP					, @Tie_inP
										, @Nro_tie_in_padreP	, @Nro_tie_inP			, @StorageP
										, @NaveidP				, @Calle_codP			, @Calle_idP
										, @Columna_codP			, @Columna_idP			, @Nivel_codP
										, @Nivel_idP			, @Cat_log_idP			, @Est_merc_idP
										, @PosicionidP			, @Prop1P				, @Prop2P
										, @Prop3P				, @Fecha_cpteP			, @Fecha_alta_gtwP
										, @Rl_idP				, @Unidad_pesoP			, @Unidad_volumenP
										, @Moneda_idP			, @CostoP				, @Orden_pickingP

		While @@Fetch_Status=0
		Begin

			If @CantidadP <= @SumaCantidad
			Begin
				Set @p_Cantidad = @CantidadP
				Set @SumaCantidad = @SumaCantidad + @CantidadP
			End
			Else
			Begin
				Set @p_Cantidad = @CantidadD - @SumaCantidad
				Set @SumaCantidad = @SumaCantidad + @p_Cantidad
			End

			Exec funciones_estacion_api#GrabarDocTREgreso	 @Doc_trans_idD			,@Nro_linea_transD
															,@ClienteIdP			,@ProductoidP
															,@Fecha_vencimientoP	,@Nro_serieP
															,@Nro_partidaP			,@Nro_despachoP
															,@Nro_bultoP			,@Nro_loteP
															,@p_Cantidad			,@NaveidP
															,@PosicionidP			,2
															,Null					,@cat_log_idP
															,@Est_merc_idP			,@Prop1P
															,@Prop2P				,@Prop3P
															,@PesoP					,@volumenP
															,@Unidad_idP			,@Unidad_pesoP
															,@Unidad_volumenP		,@Moneda_idP
															,@CostoP



			Fetch Next From @RsPosicion into  @Documento_idP		, @ClienteidP			, @ProductoidP
											, @CantidadP			, @Unidad_idP			, @Nro_serieP
											, @Nro_loteP			, @Fecha_vencimientoP	, @Nro_despachoP
											, @Nro_bultoP			, @Nro_partidaP			, @PesoP
											, @VolumenP				, @KitP					, @Tie_inP
											, @Nro_tie_in_padreP	, @Nro_tie_inP			, @StorageP
											, @NaveidP				, @Calle_codP			, @Calle_idP
											, @Columna_codP			, @Columna_idP			, @Nivel_codP
											, @Nivel_idP			, @Cat_log_idP			, @Est_merc_idP
											, @PosicionidP			, @Prop1P				, @Prop2P
											, @Prop3P				, @Fecha_cpteP			, @Fecha_alta_gtwP
											, @Rl_idP				, @Unidad_pesoP			, @Unidad_volumenP
											, @Moneda_idP			, @CostoP				, @Orden_pickingP
		
		End	--Fin While @RsPosicion.
							
		Fetch Next From @RsDatos into     @Documento_idD 		 	, @Nro_lineaD 			, @Cliente_idD 		  
										, @Producto_idD 		  	, @CantidadD 		 	, @Nro_serieD 		  
										, @Nro_serie_padreD 	  	, @Est_merc_idD 	 	, @Cat_log_idD 		  
										, @Nro_bultoD 		  		, @DescripcionD 		, @Nro_loteD 			  
										, @Fecha_vencimientoD  		, @Nro_despachoD 		, @Nro_partidaD 		  
										, @Unidad_idD 				, @PesoD 				, @Unidad_pesoD 	
										, @VolumenD 			 	, @Unidad_volumenD 		, @Busc_individualD 
										, @Tie_inD 					, @Nro_tie_in_padreD 	, @Nro_tie_inD 		  
										, @Item_okD 				, @Cat_log_id_finalD 	, @Moneda_idD 		
										, @CostoD 					, @Prop1D 			  	, @Prop2D 			  
										, @Prop3D 			  		, @LargoD 				, @AltoD 			
										, @AnchoD 					, @Volumen_unitarioD	, @Peso_unitarioD 	
										, @Cant_solicitadaD 	 	, @Trace_back_orderD 	, @Doc_Trans_idD		
										, @Nro_Linea_TransD	

	End	--End While @RsDatos.
	Set NoCount Off;
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

ALTER              PROCEDURE [dbo].[MOB_CONSULTASTOCK2]
@Codigo as nvarchar(100),
@TipoOperacion as integer--,
--@Cliente as varchar(15)
as
DECLARE @EXISTE AS INTEGER

IF @TipoOperacion=1
BEGIN

	SET 		@EXISTE = (SELECT count(prop1) as EXISTE
	FROM	DET_DOCUMENTO
	WHERE 	prop1 = UPPER(LTRIM(RTRIM(@Codigo))))

	IF @EXISTE =0
	BEGIN
		RAISERROR ('El pallet ingresado no existe.', 16, 1)
		RETURN
	END

SELECT 	C.Razon_Social
		,X.ProductoID 
		,X.DESCRIPCION	
		,x.Unidad_id
		,cast(sum(X.cantidad)as int) AS Cantidad 
		,isnull(x.Posicion_Cod,X.Storage) as POSICION
		,ISNULL(X.EST_MERC_ID,'') AS EST_MERC_ID
		,ISNULL(X.CategLogID,'') AS CategLogID
		,ISNULL(X.Nro_Lote,'') AS Nro_Lote
		,ISNULL(X.prop1,'') AS Property_1
		,ISNULL(CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103),'') as Fecha_Vencimiento
		,PR.DESCRIPCION AS PRODUCTO1
FROM 	CLIENTE C, PRODUCTO PR 
		,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID
				,prod.DESCRIPCION AS DESCRIPCION
				,cast(sum(rl.cantidad)as int) AS Cantidad 
				,dd.unidad_id ,dd.moneda_id ,dd.costo 
				,dd.nro_serie AS Nro_Serie 
				,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
				,dd.Nro_Partida 
				,dd.Nro_Despacho, dd.Nro_Bulto 
				,dd.Prop1, dd.Prop2, dd.Prop3 
				,dd.Peso ,dd.Unidad_Peso 
				,dd.Volumen ,dd.Unidad_Volumen 
				,prod.kit AS Kit 
				,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
				,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
				,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
				,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
				,ISNULL(caln.calle_cod,Null) AS CalleCod 
				,ISNULL(caln.calle_id,Null) AS CalleID 
				,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
				,ISNULL(coln.columna_id,Null) AS ColumnaID
				,ISNULL(nn.nivel_cod,Null) AS NivelCod 
				,ISNULL(nn.nivel_id,Null) AS NivelID 
				,rl.cat_log_id as CategLogID
				,p.posicion_cod as posicion_cod
		FROM 
				 rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
				 ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
				 left join nave n  ON rl.nave_actual=n.nave_id 
				 left join posicion p  ON rl.posicion_actual=p.posicion_id 
				 left join nave n2 ON p.nave_id=n2.nave_id 
				 left join calle_nave caln ON  p.calle_id=caln.calle_id 
				 left join columna_nave coln ON p.columna_id=coln.columna_id 
				 left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
				 inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
				 inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
				 inner join cliente c ON dd.cliente_id=c.cliente_id 
				 inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
				 inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
		 WHERE 	1<>0  
				--AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
				AND dd.prop1 = UPPER(LTRIM(RTRIM(@Codigo)))
		GROUP BY dd.cliente_id ,dd.producto_id,PROD.DESCRIPCION
				,dd.unidad_id, dd.moneda_id, dd.costo 
				,dd.Nro_Serie 
				,dd.Nro_lote, dd.Fecha_vencimiento 
				,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
				,dd.Prop1, dd.Prop2, dd.Prop3 
				,dd.Peso ,dd.unidad_peso 
				,dd.Volumen ,dd.unidad_volumen 
				,rl.nave_actual,p.nave_id,n.nave_cod 
				,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
				,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
				,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
				,dd.nro_tie_in , RL.est_merc_id 
				,rl.cat_log_id 
				,p.posicion_cod
		UNION ALL  
		SELECT 	dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID  
				,prod.DESCRIPCION AS DESCRIPCION
				,cast(sum(rl.cantidad)as int) AS Cantidad  
				,dd.unidad_id ,dd.moneda_id ,dd.costo  
				,dd.nro_serie AS Nro_Serie  
				,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23), dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
				,dd.Nro_Partida  
				,dd.Nro_Despacho, dd.Nro_Bulto  
				,dd.Prop1, dd.Prop2, dd.Prop3  
				,cast(dd.Peso as float) AS Peso ,dd.Unidad_Peso  
				,cast(dd.Volumen as float) AS Volumen,dd.Unidad_Volumen  
				,prod.kit AS Kit  
				,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
				,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
				,n.nave_cod AS Storage  
				,rl.nave_actual as NaveID  
				,null AS CalleCod  
				,null AS CalleID  
				,null AS ColumnaCod  
				,null AS ColumnaID  
				,null AS NivelCod  
				,null AS NivelID 
				,rl.cat_log_id as CategLogID  
				,n.nave_cod as Posicion_Cod
		FROM  
				rl_det_doc_trans_posicion rl inner join det_documento dd  
				ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
				left join nave n  ON rl.nave_actual=n.nave_id  
				inner join cliente c  ON dd.cliente_id=c.cliente_id  
				inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
				inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
		WHERE 1<>0  
				--AND dd.Cliente_ID = @Cliente
				AND dd.prop1 = @Codigo
		GROUP BY dd.cliente_id ,dd.producto_id,PROD.DESCRIPCION
				,dd.unidad_id, dd.moneda_id, dd.costo 
				,dd.Nro_Serie 
				,dd.Nro_lote ,dd.Fecha_vencimiento 
				,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
				,dd.Prop1, dd.Prop2, dd.Prop3
				,dd.Peso ,dd.unidad_peso 
				,dd.Volumen ,dd.unidad_volumen 
				,rl.nave_actual,n.nave_cod 
				,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
				,dd.nro_tie_in , RL.est_merc_id 
				,rl.cat_log_id 
				,n.nave_cod
				) x 
WHERE 	C.CLIENTE_ID = X.CLIENTEID 
		AND PR.CLIENTE_ID = X.CLIENTEID 
		AND PR.PRODUCTO_ID = X.PRODUCTOID 
		AND ((X.CategLogID<>'TRAN_ING') OR (X.CategLogID<>'TRAN_EGR'))
group by X.ClienteID, X.ProductoID,X.DESCRIPCION,x.Unidad_id
		 ,X.Storage 
		 ,X.NaveID 
		 ,X.CalleCod 
		 ,X.CalleID 
		 ,X.ColumnaCod 
		 ,X.ColumnaID 
		 ,X.NivelCod 
		 ,X.NivelID 
		 ,X.EST_MERC_ID 
		 ,X.CategLogID 
		 ,X.Nro_Serie 
		 ,X.Nro_Bulto 
		 ,X.Nro_Lote 
		 ,X.Nro_Despacho 
		 ,X.Nro_Partida 
		 ,X.prop1 
		 ,X.prop2 
		 ,X.prop3 
		 ,X.Fecha_Vencimiento 
		 ,X.Peso 
		 ,X.Unidad_Peso 
		 ,X.Volumen 
		 ,X.Unidad_Volumen 
		 ,X.Kit 
		 ,X.TIE_IN  ,X.TIE_IN_PADRE 
		 ,X.NRO_TIE_IN 
		 ,C.RAZON_SOCIAL 
		 ,PR.DESCRIPCION 
		 ,X.unidad_id 
		 ,X.moneda_id 
		 ,x.costo 
		 ,x.posicion_cod
END
ELSE
	BEGIN
	IF  @TipoOperacion=2
		BEGIN
		SET @EXISTE = (SELECT COUNT(X.TIPO) AS EXISTE
			FROM
			(
				SELECT     POSICION_ID,'POS' AS TIPO
				FROM       POSICION
				WHERE     (POSICION_COD = UPPER(LTRIM(RTRIM(@Codigo))))
				UNION ALL
				SELECT     NAVE_ID, 'NAVE' AS TIPO
				FROM       NAVE
				WHERE     (NAVE_COD = UPPER(LTRIM(RTRIM(@Codigo))))
			) AS X)

		IF @EXISTE =0
		BEGIN
		    RAISERROR ('El ubicación no existe.', 16, 1)
		END

		--CONSULTA UBICACION
			SELECT X.*
			FROM
				(
					SELECT DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID, cast(SUM(RL.CANTIDAD)as int) AS CANTIDAD_TOTAL, ISNULL(CONVERT(VARCHAR(23), DD.FECHA_VENCIMIENTO, 103),'') as FECHA_VENCIMIENTO, ISNULL(DD.NRO_LOTE,'') AS NRO_LOTE, ISNULL(DD.PROP1,'') AS PROP1
					FROM RL_DET_DOC_TRANS_POSICION RL INNER JOIN
				        POSICION P ON RL.POSICION_ACTUAL = P.POSICION_ID INNER JOIN
				        DET_DOCUMENTO_TRANSACCION DDT ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND 
				        RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS INNER JOIN
				        DOCUMENTO_TRANSACCION DT ON DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID INNER JOIN
				        DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA
						INNER JOIN PRODUCTO PROD ON DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID
					WHERE     --(RL.POSICION_ACTUAL = @Codigo) SGG
						  P.POSICION_COD=UPPER(LTRIM(RTRIM(@Codigo)))
					GROUP BY DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID, DD.FECHA_VENCIMIENTO, DD.NRO_LOTE, DD.PROP1
			
			
					UNION ALL
			
					SELECT 	DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID,cast(SUM(RL.CANTIDAD)as int) AS CANTIDAD_TOTAL, ISNULL(CONVERT(VARCHAR(23), DD.FECHA_VENCIMIENTO, 103),'') as FECHA_VENCIMIENTO, ISNULL(DD.NRO_LOTE,'') AS NRO_LOTE, ISNULL(DD.PROP1,'') AS PROP1
					FROM 	RL_DET_DOC_TRANS_POSICION RL 
							INNER JOIN NAVE N
							ON(RL.NAVE_ACTUAL=N.NAVE_ID)
							INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
							ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS =DDT.NRO_LINEA_TRANS)
							INNER JOIN DET_DOCUMENTO DD 
							ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
							INNER JOIN PRODUCTO PROD ON DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID
					WHERE     --(RL.POSICION_ACTUAL = @Codigo) SGG
						  	N.NAVE_COD=UPPER(LTRIM(RTRIM(@Codigo)))
					GROUP BY DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID, DD.FECHA_VENCIMIENTO, DD.NRO_LOTE, DD.PROP1
				)AS X
		END
	ELSE
BEGIN
--------------------------------------
--		CONSULTA PRODUCTO		    --
--------------------------------------
--	SELECT     PRODUCTO_ID, CLIENTE_ID
--	FROM         PRODUCTO
SET @EXISTE = (SELECT count(Producto_ID) as EXISTE
FROM PRODUCTO
WHERE Producto_ID = UPPER(LTRIM(RTRIM(@Codigo))))
IF @EXISTE =0
BEGIN
    RAISERROR ('El producto ingresado no existe.', 16, 1)
END



SELECT	C.razon_social
		,X.ProductoID 
	 ,PR.DESCRIPCION
	 ,X.UNIDAD_ID
     ,cast(sum(X.cantidad)as int) AS Cantidad 
     ,ISNULL(X.EST_MERC_ID,'') AS EST_MERC_ID
	 ,isnull(X.POSICION_COD,X.STORAGE) AS POSICION
	 ,ISNULL(X.Storage,'') AS STORAGE
     ,ISNULL(X.CategLogID,'') AS CategLogID
     ,ISNULL(X.Nro_Lote,'') AS Nro_Lote
     ,ISNULL(X.prop1,'') AS Property_1
     ,ISNULL(CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103),'') as Fecha_Vencimiento
     ,PR.DESCRIPCION AS PRODUCTO1
FROM CLIENTE C, PRODUCTO PR 
     ,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID
             ,cast(sum(rl.cantidad)as int) AS Cantidad 
             ,dd.unidad_id ,dd.moneda_id ,dd.costo 
             ,dd.nro_serie AS Nro_Serie 
             ,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
             ,dd.Nro_Partida 
             ,dd.Nro_Despacho, dd.Nro_Bulto 
             ,dd.Prop1, dd.Prop2, dd.Prop3 
             ,cast(dd.Peso as float) as Peso,dd.Unidad_Peso 
             ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen 
             ,prod.kit AS Kit 
             ,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
             ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
             ,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
             ,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
             ,ISNULL(caln.calle_cod,Null) AS CalleCod 
             ,ISNULL(caln.calle_id,Null) AS CalleID 
             ,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
             ,ISNULL(coln.columna_id,Null) AS ColumnaID
             ,ISNULL(nn.nivel_cod,Null) AS NivelCod 
             ,ISNULL(nn.nivel_id,Null) AS NivelID 
             ,rl.cat_log_id as CategLogID 
			 ,P.POSICION_COD AS POSICION_COD
     FROM 
         rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
         ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
         left join nave n  ON rl.nave_actual=n.nave_id 
         left join posicion p  ON rl.posicion_actual=p.posicion_id 
         left join nave n2 ON p.nave_id=n2.nave_id 
         left join calle_nave caln ON  p.calle_id=caln.calle_id 
         left join columna_nave coln ON p.columna_id=coln.columna_id 
         left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
         inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
         inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
         inner join cliente c ON dd.cliente_id=c.cliente_id 
         inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
         inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
     WHERE 1<>0  
   --AND dd.Cliente_ID = @Cliente
   	AND dd.Producto_ID = @Codigo
	--AND n.Pre_ingreso <>'1'
	--AND N.PRE_EGRESO <>'1'	

GROUP BY dd.cliente_id ,dd.producto_id
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote, dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3 
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,p.nave_id,n.nave_cod 
     ,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
     ,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
     ,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
	 ,P.POSICION_COD
UNION ALL  
     SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID
           ,cast(sum(rl.cantidad)as int) AS Cantidad  
           ,dd.unidad_id ,dd.moneda_id ,dd.costo  
           ,dd.nro_serie AS Nro_Serie  
           ,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23),dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
           ,dd.Nro_Partida  
           ,dd.Nro_Despacho, dd.Nro_Bulto  
           ,dd.Prop1, dd.Prop2, dd.Prop3  
           ,cast(dd.Peso as float) as Peso ,dd.Unidad_Peso  
           ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen  
           ,prod.kit AS Kit  
           ,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
           ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
           ,n.nave_cod AS Storage  
           ,rl.nave_actual as NaveID  
           ,null AS CalleCod  
           ,null AS CalleID  
           ,null AS ColumnaCod  
           ,null AS ColumnaID  
           ,null AS NivelCod  
           ,null AS NivelID 
           ,rl.cat_log_id as CategLogID  
		   ,N.NAVE_COD AS POSICION_COD
     FROM  
           rl_det_doc_trans_posicion rl inner join det_documento dd  
           ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
           left join nave n  ON rl.nave_actual=n.nave_id  
           inner join cliente c  ON dd.cliente_id=c.cliente_id  
           inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
           inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
     WHERE 1<>0    
 --AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
 AND dd.Producto_ID = UPPER(LTRIM(RTRIM(@Codigo)))
	--AND n.Pre_ingreso <>'1'
	--AND N.PRE_EGRESO <>'1'
GROUP BY dd.cliente_id ,dd.producto_id
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote ,dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,n.nave_cod 
     ,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
	 ,N.NAVE_COD
     ) x 
WHERE C.CLIENTE_ID = X.CLIENTEID 
     AND PR.CLIENTE_ID = X.CLIENTEID 
     AND PR.PRODUCTO_ID = X.PRODUCTOID 
     group by X.ClienteID, X.ProductoID
     ,X.Storage 
     ,X.NaveID 
     ,X.CalleCod 
     ,X.CalleID 
     ,X.ColumnaCod 
     ,X.ColumnaID 
     ,X.NivelCod 
     ,X.NivelID 
     ,X.EST_MERC_ID 
     ,X.CategLogID 
     ,X.Nro_Serie 
     ,X.Nro_Bulto 
     ,X.Nro_Lote 
     ,X.Nro_Despacho 
     ,X.Nro_Partida 
     ,X.prop1 
     ,X.prop2 
     ,X.prop3 
     ,X.Fecha_Vencimiento 
     ,X.Peso 
     ,X.Unidad_Peso 
     ,X.Volumen 
     ,X.Unidad_Volumen 
     ,X.Kit 
     ,X.TIE_IN  ,X.TIE_IN_PADRE 
     ,X.NRO_TIE_IN 
     ,C.RAZON_SOCIAL 
     ,PR.DESCRIPCION 
     ,X.unidad_id 
     ,X.moneda_id 
     ,x.costo 
	 ,X.POSICION_COD
	
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

ALTER         PROCEDURE [dbo].[Mob_Eliminar_Locator_Ing]
@DOCUMENTO_ID	AS NUMERIC(20),
@NRO_LINEA		AS NUMERIC(20)

AS
BEGIN
	DECLARE @DOC 				AS INTEGER
	DECLARE @DOC_TRANS_ID	AS NUMERIC(20,0)
	DECLARE @PALLET			AS VARCHAR(100)

	SET 	@DOC = (	SELECT 	COUNT (DOCUMENTO_ID) 
					FROM 	SYS_LOCATOR_ING 
					WHERE 	DOCUMENTO_ID=@DOCUMENTO_ID
							AND NRO_LINEA = @NRO_LINEA
				)
/*
IF @DOC = 0
BEGIN
RAISERROR ('NO EXISTE EL DOCUMENTO EN LA BASE', 16, 1)
END
*/
	DELETE FROM SYS_LOCATOR_ING 
	WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	AND NRO_LINEA = @NRO_LINEA

	DELETE 	FROM SYS_LOCATOR_ING 
	WHERE 	POSICION_ID IS NULL AND NAVE_ID IS NULL


	SELECT @PALLET=PROP1 FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

	SELECT 	@DOC_TRANS_ID= DOC_TRANS_ID
	FROM	DET_DOCUMENTO DD 
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA

	UPDATE SYS_LOCK_PALLET SET LOCK='0' WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND PALLET=@PALLET

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

ALTER  PROCEDURE [dbo].[MOB_FIN_TRANSFERENCIA]
	@PDOCTRANS 	NUMERIC(20,0),
	@USUARIO 	VARCHAR(20)
AS
BEGIN
	DECLARE @IORDEN 		AS NUMERIC(3,0)
	DECLARE @STATION 		AS VARCHAR(15)
	DECLARE @TRANSACCION_ID AS VARCHAR(15)
	DECLARE @STATUS			AS VARCHAR(3)
	DECLARE @FLG_FIN		AS CHAR(1)
	DECLARE @FLG_ACT_STOCK	AS CHAR(1)
	DECLARE @NEXT_STATION 	AS VARCHAR(15)
	DECLARE @NEXT_ORDEN		AS VARCHAR(15)

	--OBTENGO EL ORDEN DE LA ESTACION.
	SELECT 	@IORDEN=DBO.GETORDENESTACIONFORDOCTRID(@PDOCTRANS)

	SELECT 	@STATION=ESTACION_ACTUAL,@TRANSACCION_ID=TRANSACCION_ID,
			@STATUS=STATUS
	FROM  	DOCUMENTO_TRANSACCION
	WHERE 	DOC_TRANS_ID=@PDOCTRANS


	SELECT 	@FLG_FIN=FIN, @FLG_ACT_STOCK=ACTUALIZA_STOCK
	FROM  	RL_TRANSACCION_ESTACION
	WHERE 	TRANSACCION_ID 	=@TRANSACCION_ID
	     	AND ESTACION_ID	=@STATION
	     	AND ORDEN		=@IORDEN

	/*
	SELECT
			TRANSACCION_ID,
			ESTACION_ACTUAL,
			ORDEN_ESTACION
	FROM 	DOCUMENTO_TRANSACCION
	WHERE 	DOC_TRANS_ID = @PDOCTRANS
			AND TRANSACCION_ID = @TRANSACCION_ID
			AND (STATUS = 'T10' OR STATUS = 'T20')

	SELECT
				RTE.TRANSACCION_ID,
				RTE.ESTACION_ID,
				RTE.ORDEN
	FROM 		RL_TRANSACCION_ESTACION RTE
	WHERE 		RTE.TRANSACCION_ID = @TRANSACCION_ID
	ORDER BY 	TRANSACCION_ID, ORDEN
	*/
	EXEC DBO.UPDATEESTACIONACTUAL_STOCK_TRANS	@DOC_TRANS_ID=@PDOCTRANS, @USUARIO=@USUARIO

	EXEC DBO.UPDATEESTACIONACTUAL  @DOC_TRANS_ID=@PDOCTRANS


END --FIN DEL PROCEDURE
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

ALTER  Procedure [dbo].[Mob_Get_Printers]
As
Begin

	Select 	Device, Descripcion	 
	From 	sys_impresora
	Where	Activa='1'
	Order By
			Orden	

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

ALTER  Procedure [dbo].[Mob_GetCantEnvase]
				@Cliente_Id as varchar(15),
				@Variable as numeric(4,0) Output
As
Begin

	Select 	@Variable = Count(*)
	from 	producto
	where 	Cliente_id=@cliente_id and envase='1'

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
SET QUOTED_IDENTIFIER OFF
GO

ALTER   Procedure [dbo].[Mob_GetClient]
				@Viaje_Id as varchar(50),
				@Pallet_Picking as numeric(20,0),
				@Cliente_ID as varchar(15) output
As
Begin

	IF @Pallet_Picking<>0
	BEGIN
		Select @Cliente_ID = Cliente_ID
		From picking 
		Where VIAJE_ID = @Viaje_Id 
			and PALLET_PICKING = @Pallet_Picking
	END 
	ELSE
	BEGIN
		Select DISTINCT @Cliente_ID = Cliente_ID
		From picking 
		Where VIAJE_ID = @Viaje_Id 

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

ALTER procedure [dbo].[Mob_GetDocTransId]
@DocumentoId 	as Numeric(20,0),
@DocTransId  	as Numeric(20,0) Output
As
Begin

	Select	@DocTransId=Doc_trans_id
	from	Det_Documento_Transaccion
	where	Documento_id=@DocumentoId

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
SET QUOTED_IDENTIFIER OFF
GO

ALTER       Procedure [dbo].[Mob_GetProdDescCant]
				@Viaje AS varchar(30) output,
				@Secuencia as numeric(20,0)Output

As
Begin

	Declare @ValorSequencia as Numeric(20,0)
	/*
	exec Get_Value_For_Sequence 'VALE_ENVASE', @ValorSequencia Output
	*/
	Select 	P.Producto_id, P.Descripcion, sum(DD.Cantidad) as CANTIDAD,
			d.nro_despacho_importacion as Viaje_ID, @secuencia as Numero,dbo.fx_trunc_Fecha(Getdate()) as Fecha
	From 	Producto P
			inner join det_documento DD on (DD.Producto_id = P.Producto_id)
			inner join documento D on (D.Documento_id = DD.documento_id)
	where 	P.Envase = '1' and d.NRO_DESPACHO_IMPORTACION = ltrim(rtrim(Upper(@Viaje)))
	Group by P.Producto_id, P.Descripcion,d.nro_despacho_importacion
	

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
SET QUOTED_IDENTIFIER OFF
GO

ALTER     Procedure [dbo].[Mob_GetProductoEnvase]
	@Cliente_Id as varchar(15)
As
Begin
	Select producto_id as PRODUCTO_ID,descripcion AS DESCRIPCION,0 as QTY
	From producto 
	Where	cliente_id = @Cliente_Id and envase='1'
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

ALTER  Procedure [dbo].[Mob_GrabarDocumento]
@Viaje_Id 			as varchar(100),
@Documento_Id		as Numeric(20,0)
As
Begin
	
	Declare @Seq	as Numeric(20,0)

	Exec Get_Value_For_Sequence 'VALE_ENVASE', @Seq Output
	
	Insert Into Rl_Env_Documento_Viaje 	(Viaje_id,Documento_Id,Nro_Vale) values	(Ltrim(Rtrim(Upper(@Viaje_id)))	,@Documento_id	,@Seq)

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

ALTER PROCEDURE [dbo].[MOB_GUARDADO_PENDIENTES]
@DOCUMENTO_ID	NUMERIC(20,0)
AS
BEGIN

				SELECT	DD.PRODUCTO_ID,
				CASE WHEN P.FLG_CONTENEDORA=1 THEN DD.NRO_BULTO 
					ELSE 0 END AS [NRO_BULTO],
				CASE WHEN P.FLG_CONTENEDORA=1 THEN COUNT(RL.CANTIDAD) 
					ELSE ISNULL(SUM(RL.CANTIDAD),0)END AS [CANTIDAD],
				DD.DESCRIPCION
			FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
					INNER JOIN PRODUCTO P ON (DD.PRODUCTO_ID = P.PRODUCTO_ID AND DD.CLIENTE_ID= P.CLIENTE_ID)
			WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
					AND RL.NAVE_ACTUAL='1'
			GROUP BY
					DD.PRODUCTO_ID, DD.DESCRIPCION,DD.NRO_BULTO,P.FLG_CONTENEDORA
            HAVING	ISNULL(SUM(RL.CANTIDAD),0)>0
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

ALTER PROCEDURE [dbo].[Mob_Guardado_Prod]
	@DOCUMENTO_ID	NUMERIC(20,0),
	@CLIENTE		VARCHAR(30),
	@PRODUCTO		VARCHAR(30) OUTPUT,		--OK
	@DESCRIPCION	VARCHAR(50) OUTPUT,		--OK
	@LINEA			SMALLINT	OUTPUT,		--OK
	@QTY			FLOAT		OUTPUT,		
	@FRACC			CHAR(1)		OUTPUT		--OK
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	--DECLARACION DE VARIABLES.
	DECLARE @COUNT	SMALLINT
	
	--PRIMERO TENGO QUE SABER SI ES EL PRODUCTO O UN EAN/DUN.
	SELECT	@COUNT=COUNT(*)
	FROM	PRODUCTO
	WHERE	CLIENTE_ID=@CLIENTE
			AND PRODUCTO_ID=@PRODUCTO
	
	IF @COUNT=0
	BEGIN
		--NO ES UN PRODUCTO, TENGO QUE SACAR EL EAN/DUN.
		SET @COUNT=NULL
		SELECT	@COUNT=COUNT(*)
		FROM	RL_PRODUCTO_CODIGOS
		WHERE	CLIENTE_ID=@CLIENTE
				AND CODIGO=@PRODUCTO
			 
		
		IF @COUNT=0
		BEGIN
			--NO EXISTE EL PRODUCTO Y EL CODIGO ES INVALIDO.
			RAISERROR('El producto ingresado es inexistente.',16,1)
			RETURN
		END
		ELSE
		BEGIN
			--ENCONTRE EL PRODUCTO A PARTIR DEL CODIGO Y RECUPERO EL PRODUCTO_ID.
			SELECT	@PRODUCTO=PRODUCTO_ID 
			FROM	RL_PRODUCTO_CODIGOS
			WHERE	CLIENTE_ID=@CLIENTE
					AND CODIGO=@PRODUCTO
		END
	END
	--SACO LA DESCRIPCION DEL PRODUCTO Y SI ES O NO FRACCIONABLE.
	SELECT	@DESCRIPCION=DESCRIPCION, @FRACC=ISNULL(FRACCIONABLE,'0')
	FROM	PRODUCTO
	WHERE	CLIENTE_ID=@CLIENTE
			AND PRODUCTO_ID=@PRODUCTO
	
	--OBTENGO EL NUMERO DE LINEA DEL DOCUMENTO.
	SELECT	@LINEA=DD.NRO_LINEA
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.CLIENTE_ID=@CLIENTE
			AND DD.PRODUCTO_ID=@PRODUCTO
			AND RL.NAVE_ACTUAL='1'
			AND RL.CAT_LOG_ID='TRAN_ING'
	ORDER BY
			1;
			
	--OBTENGO LA CANTIDAD A UBICAR DEL PRODUCTO.
	SELECT	@QTY=SUM(RL.CANTIDAD)
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.PRODUCTO_ID=@PRODUCTO
			AND DD.NRO_LINEA=@LINEA
			AND RL.NAVE_ACTUAL='1'
			AND RL.CAT_LOG_ID='TRAN_ING'
		
	IF (@QTY IS NULL) OR (@QTY=0)
	BEGIN
		RAISERROR('No hay pendientes de ubicacion para el producto seleccionado.',16,1)
		return
	END
END--FIN PROCEDURE.
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

ALTER                  PROCEDURE [dbo].[Mob_IngresarViajes]
@Codigo as nvarchar(100)

AS	
	DECLARE @CONTROLADO AS INT
	DECLARE @FINALIZADO AS INT
	DECLARE @CONTROL 	AS INT
	DECLARE @RC			AS INT
	DECLARE @EXISTE_V	AS INT
	DECLARE @QTY		AS INT
	DECLARE @ENV		AS INT
	DECLARE @Controla	AS CHAR(1)

	SELECT 	@EXISTE_V=COUNT(PICKING_ID)
	FROM	PICKING
	WHERE	VIAJE_ID=LTRIM(RTRIM(UPPER(@CODIGO)))
	
	Select	@Controla=isnull(c.flg_control_exp,'0')
	from	picking p inner join cliente_parametros c
			on(p.cliente_id=c.cliente_id)
	where	viaje_id=ltrim(rtrim(upper(@CODIGO)))
	
	IF @EXISTE_V > 0
		BEGIN
			SELECT @FINALIZADO=DBO.STATUS_PICKING(@CODIGO)
		END
	ELSE
		BEGIN
			RAISERROR('El viaje no existe',16,1)
			RETURN
		END
	
	IF @FINALIZADO =2 
		BEGIN
			-- Agregado para control de Carga.
			SELECT 	@QTY=COUNT(DD.DOC_EXT)
			FROM 	SYS_INT_DET_DOCUMENTO DD
					INNER JOIN SYS_INT_DOCUMENTO D ON (DD.CLIENTE_ID=D.CLIENTE_ID AND DD.DOC_EXT=D.DOC_EXT)
					INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID)
			WHERE 	DD.ESTADO_GT IS NULL AND D.CODIGO_VIAJE=LTRIM(RTRIM(UPPER(@Codigo)))

			IF (@QTY>0) BEGIN
				RAISERROR('EL PICKING/VIAJE AUN TIENE PRODUCTOS PENDIENTES POR PROCESAR!',16,1)
				RETURN
			END 
			
			--Aca hago un Update para que no levante los pallet
			--que, sumado el total, den igual a 0
			update picking set st_control_exp=1 
			where  viaje_id=ltrim(rtrim(upper(@CODIGO)))
					and pallet_picking in(	select 	pallet_picking
											from	picking
											where	viaje_id=ltrim(rtrim(upper(@CODIGO)))
											group by
													pallet_picking
											having 	sum(cant_confirmada)=0
										)


			SELECT  DISTINCT   
					PALLET_PICKING as NRO_PALLET, VIAJE_ID as NRO_VIAJE,
					ST_CONTROL_EXP AS ST_CONTROL_EXP
			FROM    PICKING
			WHERE 	VIAJE_ID =LTRIM(RTRIM(UPPER(@Codigo)))
					AND FECHA_INICIO IS NOT NULL
					AND FECHA_FIN IS NOT NULL
					AND USUARIO IS NOT NULL
					AND CANT_CONFIRMADA IS NOT NULL
					AND PALLET_PICKING IS NOT NULL
					AND ISNULL(ST_CONTROL_EXP,'0')='0'
					AND ((@Controla='0') OR (FACTURADO=0))

			IF @@ROWCOUNT =0 
			BEGIN
				SELECT @ENV=COUNT(*) FROM RL_ENV_DOCUMENTO_VIAJE WHERE VIAJE_ID=Ltrim(Rtrim(Upper(@Codigo)))
				IF @ENV=1
				BEGIN
					RAISERROR('El viaje ya fue controlado',16,1)				
				END
				ELSE
				BEGIN

					SELECT 1 AS EXISTE
				END

			END
		END
		ELSE
		BEGIN
			RAISERROR('El viaje se encuentra en proceso de Picking',16,1)
			RETURN
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

ALTER         Procedure [dbo].[Mob_IngresarViajes_Controlado]
	@ViajeId As Varchar(100)
As
Begin

	Select 	Distinct
			ISNULL(cast(Pallet_Picking as varchar),'') AS NRO_PALLET
	From	Picking (nolock)
	Where	Viaje_Id=Ltrim(Rtrim(Upper(@ViajeID)))
			And St_Control_Exp='1'
	Group By
			Pallet_Picking
	having	sum(cant_confirmada)>0

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

ALTER         Procedure [dbo].[Mob_IngresarViajes_Pendiente]
	@ViajeId As Varchar(100)
As
Begin

	Select 	
			ISNULL(cast(Pallet_Picking as varchar),'') As NRO_PALLET
	From	Picking (nolock)
	Where	Viaje_Id=Ltrim(Rtrim(Upper(@ViajeId)))
			And isnull(St_Control_Exp,'0')='0'
	Group by pallet_picking
	Having 	 sum(cant_confirmada)>0

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

ALTER PROCEDURE [dbo].[MOB_INGRESO_OC_SEL]
	@CLIENTE_ID		varchar(15),
	@ORDEN_COMPRA	varchar(100)
AS
BEGIN
	SELECT	INGRESO_OC.PRODUCTO_ID					AS PRODUCTO, 
			INGRESO_OC.CANTIDAD						AS CANTIDAD, 
			ISNULL(INGRESO_OC.CANT_CONTENEDORAS,0)	AS CANT_CONTENEDORAS, 
			PRODUCTO.DESCRIPCION					AS DESCRIPCION, 
			INGRESO_OC.NRO_LOTE						AS NRO_LOTE, 
			INGRESO_OC.NRO_PARTIDA					AS NRO_PARTIDA,
			INGRESO_OC.ING_ID						AS ING_ID
	FROM    INGRESO_OC INNER JOIN PRODUCTO 
			ON(INGRESO_OC.PRODUCTO_ID = PRODUCTO.PRODUCTO_ID AND INGRESO_OC.CLIENTE_ID = PRODUCTO.CLIENTE_ID)
	WHERE   INGRESO_OC.CLIENTE_ID = @CLIENTE_ID 
			AND INGRESO_OC.ORDEN_COMPRA = @ORDEN_COMPRA 
			AND PROCESADO ='0'
	ORDER BY 
			INGRESO_OC.PRODUCTO_ID
END;
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

ALTER   procedure [dbo].[Mob_IngVerificaIntermedia]
@Doc_trans_id numeric(20,0) output,
@Out int output
As
Begin

	Declare @vRlId  as Numeric(20,0)
	Declare @Q1		as int
	Declare @Q2		as int
	Declare @Return as int

	Declare C_VerIntIng cursor For
		Select 	Rl_id
		from	Rl_Det_Doc_trans_posicion rl inner join Det_documento_transaccion ddt
				on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans =ddt.nro_linea_trans)
		Where	ddt.doc_trans_id=@Doc_trans_id


	Open C_VerIntIng
		
	Fetch Next from C_VerIntIng Into @vRlId
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
	
			Fetch Next from C_VerIntIng Into @vRlId
					
		End --Fin While
	set @Out=@Return

	Close C_VerIntIng
	deallocate C_VerIntIng
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

ALTER Procedure [dbo].[MOB_INSERT_SERIE_PICKING]
	@Picking_id NUMERIC(20,0) OUTPUT,
	@Nro_Serie VARCHAR(100) OUTPUT
AS
BEGIN

INSERT INTO [Produ].[dbo].[SeriePicking]
           ([Picking_id]
           ,[Nro_Serie]
           ,[Fecha])
     VALUES
           (@Picking_id
           ,@Nro_Serie
           ,GETDATE())

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

ALTER  PROCEDURE [dbo].[Mob_Permisos_Menu]
@usuario_id as nvarchar(20),
@codigo_id as integer
as

SELECT CODIGO_MENU
FROM SYS_PERMISOS_HH
WHERE (USUARIO_ID = @usuario_id AND CODIGO_MENU = @codigo_id)
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

ALTER   PROCEDURE [dbo].[Mob_Pwd_Correcto]
@password_handheld as nvarchar(50)
as

select NOMBRE from SYS_USUARIO where  RTRIM(LTRIM(upper(password_handheld))) = upper(@password_handheld)
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

ALTER                          PROCEDURE [dbo].[MOB_TRANSF_PREPICKING]
	@POSICION_O	AS VARCHAR(45),
	@POSICION_D AS VARCHAR(45),
	@USUARIO 	AS VARCHAR(30),
	@PALLET		AS VARCHAR(100),
	@CONTENEDORA AS VARCHAR(50),
	@DOCUMENTO_ID AS NUMERIC(20,0),
	@NRO_LINEA	AS NUMERIC(10,0)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	DECLARE @CLIENTE_ID		AS VARCHAR(5)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(1,0)
	DECLARE @LIMITE			AS NUMERIC(20,0)
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)
	---------	Variables para determinar el doc trans de egreso.
	DECLARE @DC_EGR			AS NUMERIC(20,0)
	DECLARE @LN_EGR			AS NUMERIC(20,0)
	---------	Var. para la cat log de rl
	DECLARE @RL_CATLOG		AS VARCHAR(50)
	
	SET XACT_ABORT ON
	--DETERMINO EL DOC Y LA LINEA DE TRANSACCION.
	SELECT	@DC_EGR=DDT.DOC_TRANS_ID,@LN_EGR=DDT.NRO_LINEA_TRANS
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.NRO_LINEA=@NRO_LINEA


	EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
	IF @OUT='1'
		BEGIN
			RETURN
		END
	if ltrim(rtrim(@CONTENEDORA))=''
	begin
		SET @CONTENEDORA=NULL
	end



	--OBTENGO LA RL DEL PALLET EN UN CURSOR POR SI HAY MAS DE UNA LINEA EN RL.--
	DECLARE CUR_RL_TR CURSOR FOR
		/*
		SELECT 	RL.RL_ID,DD.CLIENTE_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ANTERIOR  =(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ANTERIOR=(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND ((RL.DOC_TRANS_ID_EGR=@DC_EGR AND RL.NRO_LINEA_TRANS_EGR=@LN_EGR) OR (RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL))
				AND RL.CANTIDAD >0*/
		SELECT 	RL.RL_ID,DD.CLIENTE_ID, RL.CAT_LOG_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ANTERIOR  =(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ANTERIOR=(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND (RL.DOC_TRANS_ID_EGR=@DC_EGR AND RL.NRO_LINEA_TRANS_EGR=@LN_EGR) 
				AND RL.CANTIDAD >0
		UNION ALL
		SELECT 	RL.RL_ID,DD.CLIENTE_ID, RL.CAT_LOG_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ACTUAL  =(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL 
				AND RL.CANTIDAD >0				

		--Aca verifico que el pallet este en la posicion indicada.
		SELECT 	@CONTROL=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ANTERIOR=	(	SELECT 	NAVE_ID		FROM NAVE		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ANTERIOR=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		IF @CONTROL=0
			BEGIN
				RAISERROR('1-El pallet no esta en la posicion especificada.',16,1)
				DEALLOCATE CUR_RL_TR
				RETURN
			END
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		--	Controlo que la categoria logica no sea TRAN_ING, TRAN_EGR
		/*
		Exec Mob_Transf_VerificaCatLog @Pallet, @Posicion_O, @TRANSFIERE OUTPUT
		If @Transfiere=0
		Begin
			Return
		End */
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		-- Voy por las posiciones de Picking
		Exec Verifica_Picking @PALLET, @PICKING OUTPUT
		If @Picking=1
		Begin
			--Aca tengo que verificar que la posicion de destino sea de picking.
			Select @Picking=Dbo.IsPosPicking(@POSICION_D)
			If @Picking=0
			Begin
				RAISERROR('1- La ubicacion destino no es una ubicacion de Picking.',16,1,@CAT_LOG)
				DEALLOCATE CUR_RL_TR
				RETURN
			End
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		SELECT 	@LIMITE=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET)))
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.DOC_TRANS_ID_EGR=@DC_EGR AND RL.NRO_LINEA_TRANS_EGR=@LN_EGR
				AND RL.CANTIDAD >0

		SELECT @EXISTE = COUNT(*)
		FROM  TRANSACCION T
			  INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
			  INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
			  AND RTC.CLIENTE_ID IN (SELECT CLIENTE_ID FROM CLIENTE
									 WHERE (SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
										   FROM   RL_SYS_CLIENTE_USUARIO
										   WHERE  CLIENTE_ID = RTC.CLIENTE_ID
										   AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)WHERE T.TIPO_OPERACION_ID='TR' AND RTE.ORDEN=1	
		IF @EXISTE = 0
		BEGIN
			RAISERROR('El usuario %s no posee clientes asignados',16,1,@USUARIO)
			return
		END

		--GENERO EL DOCUMENTO DE TRANSACCION .--
		EXEC CREAR_DOC_TRANSFERENCIA @USUARIO=@USUARIO
	
		--OBTENGO EL DOC_TRANS_ID INSERTADO.--
		SET @VDOCNEW=@@IDENTITY
	
		UPDATE DOCUMENTO_TRANSACCION SET TR_POS_COMPLETA= '0' WHERE DOC_TRANS_ID=@VDOCNEW
	
		--ABRO EL CURSOR PARA SU POSTERIOR USO	
		OPEN CUR_RL_TR
	
		SET @CONT_LINEA= 0
		SET @LIM_CONT=0

		FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID,@RL_CATLOG
		WHILE (@@FETCH_STATUS=0)
		BEGIN
				SET @CONT_LINEA= @CONT_LINEA + 1 
				INSERT INTO DET_DOCUMENTO_TRANSACCION (
				        DOC_TRANS_ID,     NRO_LINEA_TRANS,
				        DOCUMENTO_ID,     NRO_LINEA_DOC,
				        MOTIVO_ID,        EST_MERC_ID,
				        CLIENTE_ID,       CAT_LOG_ID,
				        ITEM_OK,          MOVIMIENTO_PENDIENTE,
				        DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF)
				VALUES (
				        @VDOCNEW
				        ,@CONT_LINEA --NRO DE LINEA DE DET_DOCUMENTO_TRANSACCION
				        ,NULL   ,NULL   ,NULL     ,NULL
				        ,@CLIENTE_ID
				        ,NULL ,'0' ,'0' ,NULL     ,NULL)
	
		
				SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@POSICION_D) AS INT)
				SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@POSICION_D) AS INT)
	
				IF @RL_CATLOG='TRAN_EGR'
				BEGIN
					--TRATO LAS QUE SON TRAN EGR.
					INSERT INTO RL_DET_DOC_TRANS_POSICION
							   (DOC_TRANS_ID,
								NRO_LINEA_TRANS,
								POSICION_ANTERIOR,
								POSICION_ACTUAL,
								CANTIDAD,
								TIPO_MOVIMIENTO_ID,
								ULTIMA_SECUENCIA,
								NAVE_ANTERIOR,
								NAVE_ACTUAL,
								DOCUMENTO_ID,
								NRO_LINEA,
								DISPONIBLE,
								DOC_TRANS_ID_EGR,
								NRO_LINEA_TRANS_EGR,
								DOC_TRANS_ID_TR,
								NRO_LINEA_TRANS_TR,
								CLIENTE_ID,
								CAT_LOG_ID,
								EST_MERC_ID)
								(SELECT   DOC_TRANS_ID 
										, NRO_LINEA_TRANS
										, POSICION_ACTUAL
										, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
										, CANTIDAD
										, NULL
										, NULL
										, NAVE_ACTUAL
										, @NEWNAVE
										, NULL
										, NULL
										, 0
										, DOC_TRANS_ID_EGR
										, NRO_LINEA_TRANS_EGR									
										, @vDocNew
										, 1 
										, CLIENTE_ID
										, CAT_LOG_ID
										, EST_MERC_ID
								 FROM RL_DET_DOC_TRANS_POSICION
								 WHERE RL_ID = @vRLID
								 ) 
			END
			ELSE
			BEGIN
					--ACA TRATO LAS QUE NO SON TRAN EGR.
					INSERT INTO RL_DET_DOC_TRANS_POSICION
								(DOC_TRANS_ID,
								NRO_LINEA_TRANS,
								POSICION_ANTERIOR,
								POSICION_ACTUAL,
								CANTIDAD,
								TIPO_MOVIMIENTO_ID,
								ULTIMA_SECUENCIA,
								NAVE_ANTERIOR,
								NAVE_ACTUAL,
								DOCUMENTO_ID,
								NRO_LINEA,
								DISPONIBLE,
								DOC_TRANS_ID_TR,
								NRO_LINEA_TRANS_TR,
								CLIENTE_ID,
								CAT_LOG_ID,
								EST_MERC_ID)
					(SELECT   DOC_TRANS_ID 
							, NRO_LINEA_TRANS
							, POSICION_ACTUAL
							, @NEWPOS
							, CANTIDAD
							, NULL
							, NULL
							, NAVE_ACTUAL							
							, @NEWNAVE
							, NULL
							, NULL
							, 1
							, @vDocNew
							, 1 
							, CLIENTE_ID
							, CAT_LOG_ID
							, EST_MERC_ID
					FROM	RL_DET_DOC_TRANS_POSICION
					WHERE	RL_ID = @vRLID
					) 
			END
			EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE
			DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			----------------------------------------------------------------------------
			--	ESTO ES SEGURAMENTE UN BUG DE SQL SERVER. REVISAR PARA CORREGIR
			--	ESTE ALGORITMO.
			SET @LIM_CONT=@LIM_CONT +1
			IF (@LIMITE=@LIM_CONT)
				BEGIN
					BREAK
				END
			----------------------------------------------------------------------------
			FETCH NEXT FROM CUR_RL_TR INTO @VRLID, @CLIENTE_ID, @RL_CATLOG
			
		END

		-- DEVOLUCION
		EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW
	
		--FINALIZA LA TRANSFERENCIA	
		EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
			
		-- BORRO POR SI QUEDA ALGUNA RL EN 0
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE CANTIDAD =0 AND DOC_TRANS_ID=@VDOCNEW
	
		UPDATE POSICION SET POS_VACIA='0' WHERE POSICION_ID IN (SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
			
		UPDATE POSICION SET POS_VACIA='1' WHERE POSICION_ID  NOT IN
		(SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
	
		CLOSE CUR_RL_TR
		DEALLOCATE CUR_RL_TR
			
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

ALTER PROCEDURE [dbo].[Mob_Transf_Verifica_Cat_Log]
--@Pallet			as varchar(100),
@Posicion		as varchar(45),
@Producto_id	as varchar(30),
@Transfiere		as Char(1) Output

As
Begin
	Declare @CatLog		Cursor
	Declare @vCatLog	Varchar(50)
	

	Set @Transfiere=Null

	Set @CatLog=  Cursor For
		SELECT 	RL.CAT_LOG_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	(RL.NAVE_ACTUAL	 =	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@Posicion))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION 	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@Posicion)))))
				and dd.producto_id=@Producto_id
				AND RL.CANTIDAD >0
	--PROP1=LTRIM(RTRIM(UPPER(@PALLET))) AND 
	
	Open @Catlog
	Fetch Next From @CatLog into @vCatLog
	While @@Fetch_Status=0
	Begin
		IF (@vCatLog='TRAN_ING')OR(@vCatLog='TRAN_EGR')
		BEGIN
			Set @Transfiere=0
			RAISERROR('1- No es posible Transferir con Categoria Logica %s.',16,1,@vCatLog)
			RETURN
		END
		Fetch Next From @CatLog into @vCatLog
	End
	If (@Transfiere is null)
	Begin
		Set @Transfiere=1
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

ALTER  Procedure [dbo].[Mob_Transf_VerificaCatLog]
@Pallet			as varchar(100),
@Posicion		as varchar(45),
@Transfiere		as Char(1) Output
As
Begin
	Declare @CatLog		Cursor
	Declare @vCatLog	Varchar(50)

	Set @Transfiere=Null

	Set @CatLog=  Cursor For
		SELECT 	RL.CAT_LOG_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND (RL.NAVE_ACTUAL	 =	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@Posicion))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION 	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@Posicion)))))
				AND RL.CANTIDAD >0

	Open @Catlog
	Fetch Next From @CatLog into @vCatLog
	While @@Fetch_Status=0
	Begin
		IF (@vCatLog='TRAN_ING')OR(@vCatLog='TRAN_EGR')
		BEGIN
			Set @Transfiere=0
			RAISERROR('1- No es posible Transferir un pallet con Categoria Logica %s.',16,1,@vCatLog)
			RETURN
		END
		Fetch Next From @CatLog into @vCatLog
	End
	If (@Transfiere is null)
	Begin
		Set @Transfiere=1
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

ALTER PROCEDURE [dbo].[MOB_TRANSFERENCIA]
	@POSICION_O	AS VARCHAR(45),
	@POSICION_D AS VARCHAR(45),
	@USUARIO 	AS VARCHAR(30),
	@PALLET		AS VARCHAR(100)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	DECLARE @CLIENTE_ID		AS VARCHAR(5)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(20,0) -- LRojas Tracker Id 5156 23/05/2012: Aumento tamanio para corregir error reportado
	DECLARE @LIMITE			AS NUMERIC(20,0)
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)

	EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
	IF @OUT='1'
		BEGIN
			RETURN
		END

	--OBTENGO LA RL DEL PALLET EN UN CURSOR POR SI HAY MAS DE UNA LINEA EN RL.--
	DECLARE CUR_RL_TR CURSOR FOR
		SELECT 	RL.RL_ID,DD.CLIENTE_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		--Aca verifico que el pallet este en la posicion indicada.
		SELECT 	@CONTROL=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		IF @CONTROL=0
			BEGIN
				RAISERROR('1-El pallet no esta en la posicion especificada.',16,1)
				DEALLOCATE CUR_RL_TR
				RETURN
			END
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		--Controlo que la categoria logica no sea TRAN_ING, TRAN_EGR
		Exec Mob_Transf_VerificaCatLog @Pallet, @Posicion_O, @TRANSFIERE OUTPUT
		If @Transfiere=0
		Begin
			Return
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		-- Voy por las posiciones de Picking
		Exec Verifica_Picking @PALLET, @PICKING OUTPUT
		If @Picking=1
		Begin
			--Aca tengo que verificar que la posicion de destino sea de picking.
			Select @Picking=Dbo.IsPosPicking(@POSICION_D)
			If @Picking=0
			Begin
				RAISERROR('1- La ubicacion destino no es una ubicacion de Picking.',16,1,@CAT_LOG)
				DEALLOCATE CUR_RL_TR
				RETURN
			End
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		SELECT 	@LIMITE=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		SELECT @EXISTE = COUNT(*)
		FROM  TRANSACCION T
			  INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
			  INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
			  AND RTC.CLIENTE_ID IN (SELECT CLIENTE_ID FROM CLIENTE
									 WHERE (SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
										   FROM   RL_SYS_CLIENTE_USUARIO
										   WHERE  CLIENTE_ID = RTC.CLIENTE_ID
										   AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)WHERE T.TIPO_OPERACION_ID='TR' AND RTE.ORDEN=1	
		IF @EXISTE = 0
		BEGIN
			RAISERROR('El usuario %s no posee clientes asignados',16,1,@USUARIO)
			return
		END

		--GENERO EL DOCUMENTO DE TRANSACCION .--
		EXEC CREAR_DOC_TRANSFERENCIA @USUARIO=@USUARIO
	
		--OBTENGO EL DOC_TRANS_ID INSERTADO.--
		SET @VDOCNEW=@@IDENTITY
	
		UPDATE DOCUMENTO_TRANSACCION SET TR_POS_COMPLETA= '0' WHERE DOC_TRANS_ID=@VDOCNEW
	
		--ABRO EL CURSOR PARA SU POSTERIOR USO	
		OPEN CUR_RL_TR
	
		SET @CONT_LINEA= 0
		SET @LIM_CONT=0

		FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID
		WHILE (@@FETCH_STATUS=0)
		BEGIN
				SET @CONT_LINEA= @CONT_LINEA + 1 
				INSERT INTO DET_DOCUMENTO_TRANSACCION (
				        DOC_TRANS_ID,     NRO_LINEA_TRANS,
				        DOCUMENTO_ID,     NRO_LINEA_DOC,
				        MOTIVO_ID,        EST_MERC_ID,
				        CLIENTE_ID,       CAT_LOG_ID,
				        ITEM_OK,          MOVIMIENTO_PENDIENTE,
				        DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF)
				VALUES (
				        @VDOCNEW
				        ,@CONT_LINEA --NRO DE LINEA DE DET_DOCUMENTO_TRANSACCION
				        ,NULL   ,NULL   ,NULL     ,NULL
				        ,@CLIENTE_ID
				        ,NULL ,'0' ,'0' ,NULL     ,NULL)
	
		
				SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@POSICION_D) AS INT)
				SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@POSICION_D) AS INT)
	
				INSERT INTO RL_DET_DOC_TRANS_POSICION
				           (DOC_TRANS_ID,
				            NRO_LINEA_TRANS,
				            POSICION_ANTERIOR,
				            POSICION_ACTUAL,
				            CANTIDAD,
				            TIPO_MOVIMIENTO_ID,
				            ULTIMA_SECUENCIA,
				            NAVE_ANTERIOR,
				            NAVE_ACTUAL,
				            DOCUMENTO_ID,
				            NRO_LINEA,
				            DISPONIBLE,
				            DOC_TRANS_ID_TR,
				            NRO_LINEA_TRANS_TR,
				            CLIENTE_ID,
				            CAT_LOG_ID,
				            EST_MERC_ID)
				            (SELECT   DOC_TRANS_ID 
									, NRO_LINEA_TRANS
									, POSICION_ACTUAL
									, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
									, CANTIDAD
									, NULL
									, NULL
									, NAVE_ACTUAL
									, @NEWNAVE
									, NULL
									, NULL
									, 0
									, @vDocNew
				                    , 1 
									, CLIENTE_ID
									, CAT_LOG_ID
									, EST_MERC_ID
				             FROM RL_DET_DOC_TRANS_POSICION
				             WHERE RL_ID = @vRLID
				             ) 
			EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE
			DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			----------------------------------------------------------------------------
			--	ESTO ES SEGURAMENTE UN BUG DE SQL SERVER. REVISAR PARA CORREGIR
			--	ESTE ALGORITMO.
			SET @LIM_CONT=@LIM_CONT +1
			IF (@LIMITE=@LIM_CONT)
				BEGIN
					BREAK
				END
			----------------------------------------------------------------------------
			FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID
			
		END
	
		-- DEVOLUCION
		EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW
	
		--FINALIZA LA TRANSFERENCIA	
		EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
	
		-- BORRO POR SI QUEDA ALGUNA RL EN 0
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE CANTIDAD =0 AND DOC_TRANS_ID=@VDOCNEW
	
		--ACTUALIZO LAS POSICIONES DE PICKING
		EXEC ACTUALIZA_POS_PICKING_TR @POSICION_O,@POSICION_D,@PALLET

		UPDATE	POSICION SET POS_VACIA='0' 
		WHERE	POSICION_ID IN (SELECT	DISTINCT POSICION_ACTUAL FROM	RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)

		UPDATE POSICION SET POS_VACIA='1' 
		WHERE POSICION_ID  NOT IN (SELECT DISTINCT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
	
		CLOSE CUR_RL_TR
		DEALLOCATE CUR_RL_TR
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

ALTER PROCEDURE [dbo].[MOB_TRANSFERENCIA_PROD]
	@CLIENTE_ID AS VARCHAR(50),
	@POSICION_O	AS VARCHAR(45),
	@POSICION_D AS VARCHAR(45),
	@Producto_id as varchar(30),
	@USUARIO 	AS VARCHAR(30),
	@Cantidad	numeric(20,5),
	@CAT_LOG_ID VARCHAR(100)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @ICANTIDAD		AS NUMERIC(20,5)
	DECLARE @DIFERENCIA		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(1,0)	
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)	
	DECLARE @vNEW_RLID		AS NUMERIC(20,0)
	declare @msg			as varchar(max)
	DECLARE @CANT_ORIG		AS numeric(20,5)	


	begin try
		SET @CANT_ORIG = @Cantidad

		EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
		IF @OUT='1'
			BEGIN
				RETURN
			END

		DECLARE CUR_RL_TR CURSOR FOR
		SELECT rl.rl_id
		FROM rl_det_doc_trans_posicion rl
			inner join	det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			inner join	det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			left join	posicion p on (rl.posicion_actual=p.posicion_id)
			left join	nave n on (rl.nave_actual=n.nave_id)
			inner join	categoria_logica cl on (rl.cat_log_id=cl.cat_log_id and cl.disp_transf='1' and cl.cliente_id =dd.cliente_id)
			left join	estado_mercaderia_rl em on (rl.est_merc_id=em.est_merc_id and em.cliente_id=dd.cliente_id and (em.disp_transf='1' or em.disp_transf is null))		
		WHERE	dd.producto_id=@Producto_id
		and (n.nave_cod=@POSICION_O or p.posicion_cod=@POSICION_O) and rl.cantidad > 0
		and rl.disponible='1'	
		AND RL.CAT_LOG_ID = @CAT_LOG_ID
		AND RL.CLIENTE_ID = @CLIENTE_ID
		and rl.doc_trans_id_egr is null and rl.nro_linea_trans_egr is null
		and rl.doc_trans_id_tr is null and rl.nro_linea_trans_tr is null
		and rl.documento_id is null and rl.nro_linea is null



			

			
			SELECT @EXISTE = COUNT(*)
			FROM  TRANSACCION T
				  INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
				  INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
				  AND RTC.CLIENTE_ID IN (SELECT CLIENTE_ID FROM CLIENTE
										 WHERE (SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
											   FROM   RL_SYS_CLIENTE_USUARIO
											   WHERE  CLIENTE_ID = RTC.CLIENTE_ID
											   AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)WHERE T.TIPO_OPERACION_ID='TR' AND RTE.ORDEN=1	
			IF @EXISTE = 0
			BEGIN
				RAISERROR('El usuario %s no posee clientes asignados',16,1,@USUARIO)
				return
			END

			--GENERO EL DOCUMENTO DE TRANSACCION .--
			EXEC CREAR_DOC_TRANSFERENCIA @USUARIO=@USUARIO

			--OBTENGO EL DOC_TRANS_ID INSERTADO.--
			SET @VDOCNEW=@@IDENTITY

			UPDATE DOCUMENTO_TRANSACCION SET TR_POS_COMPLETA= '0' WHERE DOC_TRANS_ID=@VDOCNEW

			--ABRO EL CURSOR PARA SU POSTERIOR USO	
			OPEN CUR_RL_TR

			SET @CONT_LINEA= 0
			SET @LIM_CONT=0
			SET @ICANTIDAD=@CANTIDAD

			FETCH NEXT FROM CUR_RL_TR INTO @VRLID--,@CLIENTE_ID
			WHILE (@@FETCH_STATUS=0)
			BEGIN
					SET @CONT_LINEA= @CONT_LINEA + 1 
					INSERT INTO DET_DOCUMENTO_TRANSACCION (
							DOC_TRANS_ID,     NRO_LINEA_TRANS,
							DOCUMENTO_ID,     NRO_LINEA_DOC,
							MOTIVO_ID,        EST_MERC_ID,
							CLIENTE_ID,       CAT_LOG_ID,
							ITEM_OK,          MOVIMIENTO_PENDIENTE,
							DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF)
					VALUES (
							@VDOCNEW
							,@CONT_LINEA --NRO DE LINEA DE DET_DOCUMENTO_TRANSACCION
							,NULL   ,NULL   ,NULL     ,NULL
							,@CLIENTE_ID
							,NULL ,'0' ,'0' ,NULL     ,NULL)

			
					SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@POSICION_D) AS INT)
					SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@POSICION_D) AS INT)

					
					select @VCANTIDAD=cantidad from RL_DET_DOC_TRANS_POSICION where RL_ID = @vRLID
					
					
					if @cantidad >0 
					begin
						IF @CANTIDAD = @VCANTIDAD
							BEGIN
								INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @VCANTIDAD--, CANTIDAD
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 
						             					             
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @CANTIDAD
						             
									 DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
						             
									 SET @CANTIDAD=0
						             
							END
						ELSE
							IF @CANTIDAD < @VCANTIDAD --CANTIDAD A TRANSFERIR ES MENOR A CANT RL
								BEGIN						
									
									SET @DIFERENCIA=@VCANTIDAD - @CANTIDAD
									
									INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @CANTIDAD  -- CANTIDAD TRANSFERIDA
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 
									 
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @CANTIDAD
						             
									 UPDATE RL_DET_DOC_TRANS_POSICION SET cantidad=@DIFERENCIA --CANTIDAD REMANENTE EN LA RL
									 WHERE RL_ID = @vRLID
									 
									 
									 
									 
									 
									 SET @CANTIDAD=0
								END
							ELSE
								BEGIN		--CANTIDAD CANTIDAD A TRANSFERIR MAYOR A LA RL
									SET @DIFERENCIA=@CANTIDAD - @VCANTIDAD	
									set @CANTIDAD =@CANTIDAD - @VCANTIDAD	--@CANTIDAD AHORA ES EL RESTO A TRANSFERIR
									INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @VCANTIDAD--, CANTIDAD RL
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 								 
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @VCANTIDAD
						             
						             
									 DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			
								END
					END --if @cantidad >0
					
				FETCH NEXT FROM CUR_RL_TR INTO @VRLID --,@CLIENTE_ID
				
			END
			
			CLOSE CUR_RL_TR
			DEALLOCATE CUR_RL_TR

			
			IF @CANTIDAD > 0 
				begin
					set @msg = 'Solo se pueden transferir ' + cast((@CANT_ORIG - @cantidad) as varchar) + ' .Por favor cancele la operación y comience de nuevo.'
					RAISERROR(@msg,16,1)		
				end


			INSERT INTO IMPRESION_RODC VALUES(@VDOCNEW,0,'D',0,'')
			
			UPDATE POSICION SET POS_VACIA='0' 
						WHERE POSICION_ID IN (SELECT DISTINCT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)

			UPDATE POSICION SET POS_VACIA='1' 
						WHERE POSICION_ID  NOT IN (SELECT DISTINCT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)		

			-- DEVOLUCION
			EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW

			--FINALIZA LA TRANSFERENCIA	
			EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
	end try
	begin catch
		exec usp_RethrowError
	end catch
		
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

ALTER  PROCEDURE [dbo].[Mob_UbicacionMercaderia]
@NroPallet AS VARCHAR(100)
AS
SELECT     NRO_LINEA
FROM         DET_DOCUMENTO
--WHERE     (PROP1 = @NroPallet)
WHERE     (DOCUMENTO_ID= @NroPallet)
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

ALTER  PROCEDURE [dbo].[Mob_Usuario_Correcto]
@usuario_id as nvarchar(20)
as

select  NOMBRE from SYS_USUARIO where RTRIM(LTRIM(upper(USUARIO_ID))) = upper(@usuario_id)
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

ALTER   Procedure [dbo].[Mob_Verifica_Existencia]
@Cliente_Id 	as Varchar(15),
@Producto_id	as Varchar(30),
@Solicitada		as Float,
@Documento_Id	as Numeric(20,0),
@Control		as Char(1) OUTPUT
As
Begin
	Declare @Total 			as Float
	Declare @Reservados		as Float
	Declare @Real			as Float

	--De aca saco la existencia total.	
	SELECT 
			@Total=IsNull(Sum(rl.cantidad), 0)
	FROM  	rl_det_doc_trans_posicion             rl
			inner join det_documento_transaccion  ddt 
			on ddt.doc_trans_id    = rl.doc_trans_id and ddt.nro_linea_trans = rl.nro_linea_trans
			inner join det_documento               dd 
			on dd.documento_id     = ddt.documento_id and dd.nro_linea        = ddt.nro_linea_doc
			inner join categoria_logica            cl 
			on cl.cliente_id       = rl.cliente_id	   and cl.cat_log_id       = rl.cat_log_id
			left join nave                    n on n.nave_id           = rl.nave_actual
			left join posicion                p on p.posicion_id       = rl.posicion_actual
			left join nave                   n2 on n2.nave_id          = p.nave_id
			left join calle_nave           caln on caln.calle_id       = p.calle_id
			left join columna_nave         coln on coln.columna_id     = p.columna_id
			left join estado_mercaderia_rl emrl on emrl.cliente_id     = dd.cliente_id
			and emrl.est_merc_id    = rl.est_merc_id
			,(select null as fecha_cpte,null as fecha_alta_gtw) as x
	WHERE 	dd.cliente_id = @Cliente_Id
			and dd.producto_id = @Producto_Id
			and rl.disponible = '1' and cl.disp_egreso = '1' and isnull(n.disp_egreso, isnull(n2.disp_egreso, '1')) = '1' and isnull(p.pos_lockeada, '0') = '0'
			and isnull(emrl.disp_egreso, '1') = '1'
	
	--De aca saco los reservados.
	select 
			@reservados= isnull (sum(t2.cantidad), 0)
	from (
			select 
					dd.cliente_id,
					dd.producto_id,
					sum(isnull(dd.cantidad,0)) as cantidad,
					dd.nro_serie,
					dd.nro_lote, dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,dd.costo
			from 	documento d,
					det_documento dd,
					categoria_logica cl
			where 	1 <> 0
					and d.documento_id = dd.documento_id
					and dd.cat_log_id = cl.cat_log_id
					and dd.cliente_id = cl.cliente_id
					and d.status = 'D20' and cl.categ_stock_id = 'TRAN_EGR'
					and dd.documento_id <> @Documento_Id
			group by 
					dd.cliente_id,
					dd.producto_id,
					dd.nro_serie,
					dd.nro_lote,
					dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,
					dd.costo
	
			union all
	
			select 
					dd.cliente_id,
					dd.producto_id,
					sum(isnull(dd.cantidad,0)) as cantidad,
					dd.nro_serie,
					dd.nro_lote,
					dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,
					dd.costo
			from 	det_documento dd,
					categoria_logica cl,
					det_documento_transaccion ddt,
					documento_transaccion dt
			where 	1 <> 0
					and ddt.cliente_id = cl.cliente_id
					and ddt.cat_log_id = cl.cat_log_id
					and cl.categ_stock_id = 'TRAN_EGR'
					and dd.cliente_id = cl.cliente_id
					and ddt.documento_id = dd.documento_id
					and ddt.nro_linea_doc = dd.nro_linea
					and dt.doc_trans_id = ddt.doc_trans_id
					and dt.status = 'T10'
					and dd.documento_id <> @Documento_Id
			group by 
					dd.cliente_id,
					dd.producto_id,
					dd.nro_serie,
					dd.nro_lote,
					dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,
					dd.costo
					) t2
	where 	1 <> 0 and t2.cliente_id=@Cliente_Id and t2.producto_id=@Producto_id

	--Aca saco la cantidad real que puedo usar.
	Set @Real= @Total - @Reservados


	If @Solicitada<=@Real
	Begin
		Set @Control='1' --Todo Ok hay existencias.
	End
	If @Solicitada>@Real
	Begin
		--Aviso del error.
		Set @Control='0'
		Raiserror('No hay suficientes articulos del producto %s',16,1,@Producto_id)
	End
End --Fin Procedure.
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

ALTER  Procedure 	[dbo].[Mob_Verifica_Nave_Pre] 
					@Pos_Nave_cod 	varchar (40),
					@Flag 				varchar (1) output
As
Begin
	Declare @Cant numeric (5,0)

	select @Cant = sum(x.Cantidad) from
	(
	Select	Count(*) as Cantidad
	From	Nave n
			Inner join Posicion P on (n.nave_ID = p.nave_iD)
	Where	n.pre_ingreso <> '1'
			and n.pre_egreso <> '1'
			and n.disp_transf = '1'
			and p.posicion_cod =  @Pos_Nave_cod
	
	Union all
	
	Select	Count(*) as Cantidad
	From	Nave n
	Where	n.pre_ingreso <> '1'
			and n.pre_egreso <> '1'
			and n.disp_transf = '1'
			and n.nave_cod =  @Pos_Nave_cod
	) as X
	
	If @Cant = 0 
		Begin
			Set @Flag = '0'
		End
	Else
		Begin
			Set @Flag = '1'
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

ALTER  PROCEDURE [dbo].[MOB_VERIFICA_PALLET]
@DOCUMENTO_ID	AS NUMERIC(20,0),
@NRO_LINEA		AS NUMERIC(10,0),
@NROPALLET		AS VARCHAR(100)

AS
BEGIN
	BEGIN
		IF @DOCUMENTO_ID IS NULL
			RAISERROR ('EL PARAMETRO @DOCUMENTO_ID NO PUEDE SER NULO. SQLSERVER', 16, 1)
	END
	BEGIN
		IF @NRO_LINEA IS NULL
			RAISERROR ('EL PARAMETRO @NRO_LINEA NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END
	BEGIN
		IF @NROPALLET IS NULL
			RAISERROR ('EL PARAMETRO @NROPALLET NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END

	BEGIN
		SELECT 	LI.POSICION_ID,P.POSICION_COD
		FROM 	SYS_LOCATOR_ING LI
				INNER JOIN POSICION P
				ON (LI.POSICION_ID=P.POSICION_ID)
		WHERE 	LI.DOCUMENTO_ID=@DOCUMENTO_ID 
				AND LI.NRO_LINEA=@NRO_LINEA
				AND NRO_PALLET=UPPER(LTRIM(RTRIM(@NROPALLET)))

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

ALTER Procedure [dbo].[Mob_Verifica_Posiciones_Permitidas]
@Cliente_id	as Varchar(15),
@Producto_Id	as Varchar(30),
@Posicion		as Varchar(45),
@PosOk		as Char(1) Output
As
Begin
	
	Declare @Control 	as  Numeric(10,0)

	Select @Control=Count(*)
	From Rl_Producto_Posicion_Permitida
	Where	Cliente_id=Ltrim(Rtrim(Upper(@Cliente_Id)))
				and Producto_id=Ltrim(Rtrim(Upper(@Producto_Id)))

	If @Control>0 
	Begin
		Select Distinct @Control=Count( x.Posicion)
		From (	
			Select 	Distinct
					N.Nave_cod as Posicion
			From	Rl_producto_posicion_permitida Rppp
					Inner Join Nave N
					On(Rppp.Nave_id=N.Nave_id)
			Where	Cliente_id=Ltrim(Rtrim(Upper(@Cliente_Id)))
					and Producto_id=Ltrim(Rtrim(Upper(@Producto_Id)))
			
			Union All
		
			Select 	Distinct
					P.Posicion_Cod as Posicion
			From	Rl_Producto_Posicion_Permitida Rppp
					Inner Join Posicion P
					On(Rppp.Posicion_id=P.Posicion_Id)
			Where	Cliente_id=Ltrim(Rtrim(Upper(@Cliente_Id)))
					and Producto_id=Ltrim(Rtrim(Upper(@Producto_Id)))
		) As X
		Where	x.Posicion=Ltrim(Rtrim(Upper(@Posicion)))
		
		If @Control>0
		Begin
			Set @PosOk='1'
		End
		Else
		Begin
			Set @PosOk='0'
		End
	End
	Else
	Begin
		Set @PosOk='1'
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

ALTER PROCEDURE [dbo].[Mob_Verifica_Prod_Nave] 
	@Producto_id	varchar(30),
	@Pos_Nave_cod 	varchar(40),	
	@Cliente_id		varchar(15),
	@Desc			varchar(200)OUTPUT,
	@CanDisponible	numeric(20,3)OUTPUT
AS	
begin
	declare @producto varchar(200)
	declare @Cantidad numeric(20,3)
	
	select @producto=descripcion from producto where producto_id=@producto_id and cliente_id=@Cliente_id

	if @producto is not null
		begin
			set @Desc=@producto
		end


	SELECT
	@Cantidad=sum(rl.cantidad)
	FROM rl_det_doc_trans_posicion rl
		inner join	det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
		inner join	det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
		left join	posicion p on (rl.posicion_actual=p.posicion_id)
		left join	nave n on (rl.nave_actual=n.nave_id)
		inner join	categoria_logica cl on (rl.cat_log_id=cl.cat_log_id and cl.disp_transf='1' AND RL.CLIENTE_ID = CL.CLIENTE_ID)
		left join	estado_mercaderia_rl em on (RL.CLIENTE_ID = EM.CLIENTE_ID AND rl.est_merc_id=em.est_merc_id and (em.disp_transf='1' or em.disp_transf is null))

	WHERE
	(n.nave_cod=@Pos_Nave_cod or p.posicion_cod=@Pos_Nave_cod)
	and dd.producto_id=@Producto_id
	AND RL.CLIENTE_ID = @Cliente_id
	and rl.disponible='1'	
	and rl.doc_trans_id_egr is null and rl.nro_linea_trans_egr is null
	and rl.doc_trans_id_tr is null and rl.nro_linea_trans_tr is null
	and rl.documento_id is null and rl.nro_linea is null

	GROUP By 
	dd.producto_id
	
	if @cantidad is not null
		begin
			set @CanDisponible=@Cantidad
		end
	
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

ALTER   PROCEDURE [dbo].[Mob_VerificaIntermedia]
@Pallet as varchar(100),
@Out 	as INT output
As 
Begin
	Declare @Return as int
	Declare @Q1		as int
	Declare @Q2		as int

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
	WHERE	DD.PROP1=@pallet
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
	WHERE	DD.PROP1=@pallet
			AND P.INTERMEDIA='1'

	

	If @Q1=1 Or @Q2=1
		Begin
			set @Return=1
		End
	Else
		Begin
			set @Return=0
		End

	SET @Out= @Return

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

ALTER      PROCEDURE [dbo].[MobBuscarPosicion]
@POSICION_COD AS VARCHAR(45)
AS

declare @Mensaje  VARCHAR(200)
/*
SELECT     POSICION_ID
FROM         POSICION
WHERE     (POSICION_COD = @POSICION_COD)*/
	SELECT X.*
	FROM
	(
		SELECT     POSICION_ID,'POS' AS TIPO
		FROM       POSICION
		WHERE     (POSICION_COD = @POSICION_COD)
		UNION ALL
		SELECT     NAVE_ID, 'NAVE' AS TIPO
		FROM       NAVE
		WHERE     (NAVE_COD = @POSICION_COD)
	) AS X

IF @@ROWCOUNT =0
BEGIN


	----RAISERROR ('La posición ingresada no es una ubicación válida', 16, 1)

	set @Mensaje= 'La posición:  ' + @POSICION_COD + ' no es una ubicación válida'
	RAISERROR (@Mensaje, 16, 1)
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

ALTER Procedure [dbo].[OrderByNroLinea]
@Documento_Id	Varchar(30)
As
Begin
	Declare @tCur		Cursor
	Declare @Nro_Linea	Numeric(10,0)
	Declare @vNro_Linea	Numeric(10,0)

	Set @tCur = Cursor for
		SELECT 	nro_linea 
		FROM 	det_documento_aux
		WHERE 	documento_id=@Documento_Id
		ORDER BY 
				nro_linea		

	Open 	@tCur
	Set 	@vNro_Linea=0 

	Fetch Next From @tCur Into 	@Nro_Linea
	While @@Fetch_Status=0
	Begin
		Set @vNro_Linea=@vNro_Linea +1
		
		Update Consumo_locator_Egr 	Set Nro_linea=@vNro_Linea Where	Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea
		Update det_documento_aux 	Set Nro_linea=@vNro_Linea Where	Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea

		Fetch Next From @tCur Into 	@Nro_Linea
	End

	Close @tCur
	Deallocate @tCur

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

ALTER PROCEDURE [dbo].[PARAMETROS_AUDITORIA_UPDATE]
@PARAM_ID	NUMERIC(38) OUTPUT,
@VALOR		CHAR(1) OUTPUT
AS
BEGIN
	DECLARE @CONTROL 	AS CHAR(1)
	DECLARE @USUARIO_ID AS VARCHAR(20)

	SELECT @CONTROL= AUDITABLE FROM PARAMETROS_AUDITORIA WHERE TIPO_AUDITORIA_ID=@PARAM_ID;
	
	IF @VALOR<>@CONTROL
	BEGIN
		SELECT @USUARIO_ID=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN;
	
		UPDATE PARAMETROS_AUDITORIA SET AUDITABLE=@VALOR WHERE TIPO_AUDITORIA_ID=@PARAM_ID;
	
		INSERT INTO SYS_AUDITORIA_PARAMETROS (TIPO_AUDITORIA_ID, AUDITABLE, USUARIO_ID, TERMINAL, FECHA)
		VALUES (@PARAM_ID, @VALOR, @USUARIO_ID, HOST_NAME(), GETDATE())
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

ALTER  Procedure [dbo].[PedidoMultiProducto]
@pViaje_id			as varchar(100) Output
As
Begin
declare @RsProd			as Cursor
declare @Cliente_id		as varchar(30)
declare @Doc_ext			as varchar(100)
declare @Producto_id		as varchar(100)
declare @Nave_id			as numeric(30,0)
declare @Nave_Cod			as varchar(100)
declare @QtyPick			as numeric(30,5)
declare @Nro_Linea		as numeric(20,0)
declare @QtySol			as numeric(30,5)

create table #Temp_PedMulti (
	Cliente_id			varchar(15) 	COLLATE SQL_Latin1_General_CP1_CI_AS,
	Doc_ext				varchar(100) 	COLLATE SQL_Latin1_General_CP1_CI_AS,
	Producto_id			varchar(30)		COLLATE SQL_Latin1_General_CP1_CI_AS
)

create table #Temp_PedMultiPickeados (
	Id_Temp				numeric(20,0) IDENTITY (1, 1) NOT NULL,
	Cliente_id			varchar(15)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	Doc_ext				varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,
	Producto_id			varchar(30)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	Nave_id				numeric(20,0),
	Nave_Cod				varchar(15)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	QtyPend				numeric(30,5),
	Est_Merc_id			varchar(50)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	cat_log_id			varchar(50)		COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_Bulto			varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Descripcion			varchar(500)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_Lote				varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_pallet			varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Fecha_Vto			datetime,	
	Nro_Despacho		varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_Partida			varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Unidad_id			varchar(5)		COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Documento_id		numeric(20,0)	
)
	
insert into #Temp_PedMulti
						select dd.cliente_id,dd.doc_ext,dd.producto_id
						from sys_int_documento d inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
						where
						d.codigo_viaje=@pViaje_id
						group by dd.cliente_id,dd.doc_ext,dd.producto_id
						having count(dd.producto_id)>1

insert into #Temp_PedMultiPickeados
							select dd.cliente_id,dd.doc_ext,dd.producto_id,dd.nave_id,dd.nave_cod,sum(dd.cantidad) 
									,dd.Est_Merc_id,dd.cat_log_id,dd.Nro_Bulto,dd.Descripcion,dd.Nro_Lote,dd.Nro_pallet,dd.Fecha_Vencimiento
									,dd.Nro_Despacho,dd.Nro_Partida,dd.Unidad_id,dd.Documento_id
							from sys_dev_det_documento dd inner join #Temp_PedMulti t on (dd.cliente_id=t.cliente_id and dd.doc_ext=t.doc_ext and dd.producto_id=t.producto_id)
							group by dd.cliente_id,dd.doc_ext,dd.producto_id,dd.nave_id,dd.nave_cod
										,dd.Est_Merc_id,dd.cat_log_id,dd.Nro_Bulto,dd.Descripcion,dd.Nro_Lote,dd.Nro_pallet,dd.Fecha_Vencimiento
										,dd.Nro_Despacho,dd.Nro_Partida,dd.Unidad_id,dd.Documento_id
							having sum(dd.cantidad) > 0 

Set @RsProd = Cursor For
						select dd.cliente_id,dd.doc_ext,dd.producto_id,dd.nro_linea,dd.cantidad_solicitada
						from sys_int_documento d inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
							  inner join #Temp_PedMulti t on (dd.cliente_id=t.cliente_id and dd.doc_ext=t.doc_ext and dd.producto_id=t.producto_id)
						where
						d.codigo_viaje=@pViaje_id

Open @RsProd
Fetch Next From @RsProd into @Cliente_id,@Doc_ext,@Producto_id,@Nro_Linea,@QtySol
While @@Fetch_Status=0 Begin
	delete sys_dev_det_documento where cliente_id=@Cliente_id and doc_ext=@Doc_ext and producto_id=@Producto_id	and estado_gt<>'PP'
	exec dbo.GetPutQtyMultiProd @Cliente_id,@Doc_ext,@Producto_id,@Nro_Linea,@QtySol
	Fetch Next From @RsProd into @Cliente_id,@Doc_ext,@Producto_id,@Nro_Linea,@QtySol
end --while rsProd

CLOSE @RsProd
DEALLOCATE @RsProd

drop table #Temp_PedMulti
drop table #Temp_PedMultiPickeados


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

ALTER    Procedure [dbo].[Picking_Act_Flag] 
@Viaje_Id 	as Varchar(100) Output
As
Begin
	Declare @Cantidad 	as Int
	Declare @Dif			as Int
	declare @Qty			as numeric(20,0)

	select 	@cantidad=count(picking_id)
	from	picking
	where 	LTRIM(RTRIM(UPPER(viaje_id)))=ltrim(upper(rtrim(@Viaje_Id)))


	select 	@dif=count(picking_id)
	from 	picking 
	where 	LTRIM(RTRIM(UPPER(viaje_id)))=ltrim(upper(rtrim(@Viaje_Id)))
			and cant_confirmada is not null

	if @cantidad=@dif begin

			--FO le agrego esto para que el pedido no desaparezca
			select @Qty=isnull(count(dd.producto_id),0)  	 
			from sys_int_documento d inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			where
			d.codigo_viaje=@Viaje_Id
			and dd.estado_gt is null
			if (@Qty=0) begin
				update picking set fin_picking='2' where viaje_id=@Viaje_Id
			end --if
	

	end
	Else
		Begin
			Update Picking set Fin_Picking=(	Select	Min(isnull(Fin_Picking,0)) 
											From 	Picking 
											where 	Viaje_Id=@Viaje_id) 
			Where viaje_Id=ltrim(rtrim(upper(@viaje_id)))
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
SET QUOTED_IDENTIFIER OFF
GO

ALTER   PROCEDURE [dbo].[PICKING_COMPLETADO]
@USUARIO AS VARCHAR(20),
@VIAJE_ID AS VARCHAR(30)
AS
BEGIN
	SELECT 		PRODUCTO_ID AS Cod_Producto,
				SUM(CANTIDAD)AS Cantidad,
				POSICION_COD as Posicion,
				PROP1 AS Pallet,
				PALLET_PICKING
	FROM 		PICKING
	WHERE 		--USUARIO = LTRIM(RTRIM(UPPER(@USUARIO)))
				--AND
				VIAJE_ID= RTRIM(UPPER(LTRIM(@VIAJE_ID)))
				AND FECHA_INICIO IS NOT NULL
				AND FECHA_FIN IS NOT NULL
	GROUP	BY
				PRODUCTO_ID,
				CANTIDAD,
				POSICION_COD,
				PROP1,
				PALLET_PICKING
	BEGIN
		IF @@ROWCOUNT = 0
			RAISERROR ('No hay Finalizados para mostrar', 16, 1)
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

ALTER   PROCEDURE [dbo].[PICKING_PENDIENTE]
	@USUARIO		AS VARCHAR(30),
	@TIPOPICKING	AS NUMERIC(1),
	@CLIENTE		AS VARCHAR(30)=NULL
AS
	DECLARE @VIAJEID 			AS VARCHAR(100)
	DECLARE @PRODUCTO_ID		AS VARCHAR(50)
	DECLARE @DESCRIPCION		AS VARCHAR(200)
	DECLARE @QTY				AS NUMERIC(20,5)
	DECLARE @POSICION_COD		AS VARCHAR(45)
	DECLARE @PALLET				AS VARCHAR(100)
	DECLARE @PALLET_PICKING 	AS NUMERIC(20)
	DECLARE @RUTA				AS VARCHAR(50)
	DECLARE @UNIDAD_ID			AS VARCHAR(5)
	DECLARE @VAL_COD_EGR		AS CHAR(1)
	DECLARE @HIJO 				AS NUMERIC(20,0)
	DECLARE @CLIENTE_ID			AS VARCHAR(15)
	DECLARE @NRO_LOTE			AS VARCHAR(100)
	DECLARE @PICKING_ID			AS NUMERIC(20,0)
	DECLARE @NRO_CONTENEDORA	AS VARCHAR(50)
	DECLARE @DOCUMENTO_ID		AS NUMERIC(20,0)
	DECLARE @LOTEPROVEEDOR		AS VARCHAR(100)
	DECLARE @NRO_PARTIDA		AS VARCHAR(100)
	DECLARE @NRO_SERIE			AS VARCHAR(50)
	DECLARE @NRO_SERIE_STOCK	AS VARCHAR(50)

BEGIN
		DECLARE @CONT	FLOAT

		SELECT 		TOP 1
					@VIAJEID=SP.VIAJE_ID, @PRODUCTO_ID=SP.PRODUCTO_ID,@DESCRIPCION=SP.DESCRIPCION, 
					@QTY=SUM(SP.CANTIDAD),@POSICION_COD= SP.POSICION_COD,@PALLET=SP.PROP1,@RUTA=SP.RUTA,
					@UNIDAD_ID=PROD.UNIDAD_ID, @VAL_COD_EGR=PROD.VAL_COD_EGR,@CLIENTE_ID=SP.CLIENTE_ID,
					@NRO_LOTE=CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,
					@NRO_SERIE_STOCK = SP.NRO_SERIE,
					@NRO_CONTENEDORA =DD.NRO_BULTO,@LOTEPROVEEDOR = DD.NRO_LOTE, @NRO_PARTIDA = DD.NRO_PARTIDA
					,@NRO_SERIE = DD.NRO_SERIE
										
		FROM 		PICKING SP 
					INNER JOIN PRIORIDAD_VIAJE SPV
					ON(SPV.VIAJE_ID=SP.VIAJE_ID)
					INNER JOIN PRODUCTO PROD
					ON(SP.CLIENTE_ID=PROD.CLIENTE_ID AND SP.PRODUCTO_ID=PROD.PRODUCTO_ID)
					INNER JOIN RL_SYS_CLIENTE_USUARIO SU ON(SP.CLIENTE_ID=SU.CLIENTE_ID AND SP.USUARIO=SU.USUARIO_ID)
					INNER JOIN DET_DOCUMENTO DD ON(SP.DOCUMENTO_ID=DD.DOCUMENTO_ID AND SP.NRO_LINEA=DD.NRO_LINEA)
					INNER JOIN CLIENTE C ON(SP.CLIENTE_ID=C.CLIENTE_ID)
					INNER JOIN CLIENTE_PARAMETROS CP ON(C.CLIENTE_ID=CP.CLIENTE_ID)
		WHERE 		
					SP.CANT_CONFIRMADA IS NULL
					AND (DBO.VERIFICA_PALLET_FINAL(SP.POSICION_COD,SP.VIAJE_ID,SP.RUTA, SP.PROP1)=@TIPOPICKING)
					AND SP.USUARIO=Ltrim(Rtrim(Upper(@Usuario)))
					--AND SP.FLG_PALLET_HOMBRE = SP.TRANSF_TERMINADA -- Agregado Privitera Maximiliano 06/01/2010
					AND ((@CLIENTE IS NULL) OR(SP.CLIENTE_ID=@CLIENTE))
					AND
					SP.VIAJE_ID IN (SELECT 	VIAJE_ID
									FROM  	RL_VIAJE_USUARIO
									WHERE 	VIAJE_ID=SP.VIAJE_ID AND
											USUARIO_ID =Ltrim(Rtrim(Upper(@Usuario)))	
									)
					AND SP.SALTO_PICKING = (	SELECT 	MIN(SALTO_PICKING)
												FROM 	PICKING 
												WHERE 	VIAJE_ID=SP.VIAJE_ID
														AND USUARIO=SP.USUARIO
														AND FECHA_FIN IS NULL
														AND CANT_CONFIRMADA IS NULL
												)
					AND SP.FECHA_INICIO IS NOT NULL
		GROUP BY	SP.VIAJE_ID, SP.PRODUCTO_ID,SP.DESCRIPCION, SP.RUTA,SP.POSICION_COD,SP.TIPO_CAJA,SP.PROP1,SP.PALLET_PICKING,PROD.UNIDAD_ID,PROD.VAL_COD_EGR, SP.HIJO,SP.CLIENTE_ID,
					CP.FLG_SOLICITA_LOTE, CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,SP.NRO_SERIE,DD.NRO_BULTO
					,DD.NRO_LOTE, DD.NRO_PARTIDA,DD.NRO_SERIE
		ORDER BY	CAST(SP.TIPO_CAJA AS NUMERIC(10,1)) DESC, SP.POSICION_COD ASC
			
		BEGIN
			IF @PRODUCTO_ID IS NOT NULL
			BEGIN
				SELECT 	@VIAJEID AS VIAJE_ID,@PRODUCTO_ID AS PRODUCTO_ID, 
						@DESCRIPCION AS DESCRIPCION, @QTY AS QTY, 
						@POSICION_COD AS POSICION_COD, @PALLET AS PALLET,
						@PALLET_PICKING AS PALLET_PICKING,
						@RUTA AS RUTA, @UNIDAD_ID AS UNIDAD_ID,
						@VAL_COD_EGR AS VAL_COD_EGR, @HIJO AS STRCOD,@CLIENTE_ID AS CLIENTE_ID, @NRO_LOTE AS NRO_LOTE,
						@NRO_SERIE_STOCK  AS NRO_SERIE_STOCK,
						@NRO_CONTENEDORA AS NRO_CONTENEDORA,
						@LOTEPROVEEDOR AS LOTE_PROVEEDOR, @NRO_PARTIDA AS NRO_PARTIDA, @NRO_SERIE AS NRO_SERIE
			END
			ELSE
			BEGIN
				SELECT 	@CONT=COUNT(*)
				FROM 	PICKING 
				WHERE	USUARIO=LTRIM(RTRIM(UPPER(@USUARIO)))
						AND FECHA_INICIO IS NOT NULL
						AND FECHA_FIN IS  NULL
						AND PALLET_COMPLETO<>@TIPOPICKING
				IF @CONT>0
				BEGIN
					--CON ESTO LIBERO LA TAREA TOMADA.
					UPDATE	PICKING SET FECHA_INICIO=NULL, FECHA_FIN=NULL, USUARIO=NULL, VEHICULO_ID=NULL, PALLET_COMPLETO=NULL
					WHERE	USUARIO=@USUARIO 
							AND FECHA_INICIO IS NOT NULL 
							AND FECHA_FIN IS NULL 
							AND CANT_CONFIRMADA IS NULL
							AND PALLET_COMPLETO<>@TIPOPICKING
					RETURN
				END
			END
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

ALTER PROCEDURE [dbo].[PICKING_WAVE]
@USUARIO		AS VARCHAR(50),
@NAVECALLE		AS VARCHAR(50),
@VEHICULO_ID	AS VARCHAR(50)
AS
BEGIN

	DECLARE @VERIFICACION 	AS NUMERIC(20)
	DECLARE @VIAJEID 			AS VARCHAR(30)
	DECLARE @PRODUCTO_ID		AS VARCHAR(50)
	DECLARE @DESCRIPCION		AS VARCHAR(200)
	DECLARE @QTY				AS NUMERIC(20,5)
	DECLARE @POSICION_COD	AS VARCHAR(45)
	DECLARE @PALLET			AS VARCHAR(100)
	DECLARE @RUTA				AS VARCHAR(50)
	DECLARE @UNIDAD_ID		AS VARCHAR(5)
	DECLARE @TQUERY			AS VARCHAR(1)
	DECLARE @PICKING_ID		AS INT


	SELECT @VIAJEID	

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

ALTER Procedure [dbo].[Proceso_Etiquetas]
@VIAJE_ID	VARCHAR(100) OUTPUT,
@TIPO_PICK	CHAR(1)	OUTPUT,
@VH			Varchar(20) Output
As
Begin
	/*
	CREATE TABLE #NEW_ETI(
		SUCURSAL_ID	VARCHAR(20),
		OS			VARCHAR(50),
		CODIGO_ID	VARCHAR(100),
		DESCRIPCION	VARCHAR(100),
		BULTO		VARCHAR(50),
		ID_PICK		VARCHAR(100),
		CALLE		VARCHAR(50),
		PRINTER		VARCHAR(100)	
	)*/
	truncate table #NEW_ETI

	exec DBO.PRINT_ETI_BULTOEGR @VIAJE_ID, @TIPO_PICK, @VH
	Select * From #NEW_ETI 
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

ALTER PROCEDURE [dbo].[QT_IMP_ETI]
AS
BEGIN

	SELECT 10

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

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[reAbrirProductoEnContenedora]
	-- Add the parameters for the stored procedure here
	@cliente_id		varchar(15) OUTPUT,
	@nro_remito		varchar(30) OUTPUT,
	@producto_id	varchar(30) OUTPUT,
	@cant_elegida	numeric(20,5) OUTPUT,
	@contenedora	numeric(20,0) OUTPUT,
	@check			char(1) OUTPUT

AS
BEGIN

	DECLARE @picking_id			numeric(20,0)
	DECLARE @cant_confirmada	numeric(20,5)
	DECLARE @CANT_A_LIBERAR		NUMERIC(20,5)
	DECLARE @CANT_A_CERRAR		NUMERIC(20,5)
	DECLARE @cursorFREE			cursor
	
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;


    
	IF @check = '1'
	BEGIN
		--TENGO QUE CONTROLAR SI AUMENTO O DISMINUYO LA CANTIDAD ELEJIDA A EMPACAR
		--SELECCIONO LOS PRODUCTOS DENTRO DEL EMPAQUE
		SELECT	@CANT_CONFIRMADA = SUM(CANT_CONFIRMADA)
		FROM	PICKING
		WHERE	PALLET_PICKING = @CONTENEDORA
				AND PALLET_CONTROLADO='1'
				AND PRODUCTO_ID = @PRODUCTO_ID
				AND CLIENTE_ID = @CLIENTE_ID	

		--IF @CANT_ELEGIDA > @CANT_CONFIRMADA
		--BEGIN
			--SE AUMENTO LA CANTIDAD DE UN PRODUCTO AL REABRIR EL CONTENEDOR
			--SET @CANT_A_CERRAR = @CANT_ELEGIDA - @CANT_CONFIRMADA
			--SE AGREGA LA CANTIDAD RESTANTE AL EMPAQUE
			--EXEC CerrarProductoEnContenedora @cliente_id, @nro_remito, @producto_id, @CANT_A_CERRAR, @contenedora
		--END
		IF (@CANT_ELEGIDA = @CANT_CONFIRMADA)
		BEGIN
		--SELECCIONO EL PRODUCTO COMPLETO PARA LIBERAR
			UPDATE	PICKING
			SET		PALLET_CONTROLADO = '0'
			WHERE	PALLET_PICKING = @contenedora
					and producto_id = @producto_id
		END
		ELSE
		BEGIN
			IF @CANT_ELEGIDA < @CANT_CONFIRMADA
			BEGIN
				--SE DISMINUYO LA CANTIDAD DE UN PRODUCTO AL REABRIR EL CONTENEDOR
				--SET @CANT_A_LIBERAR = @CANT_CONFIRMADA - @CANT_ELEGIDA

				SET @CANT_A_LIBERAR = @CANT_ELEGIDA
				
				SET @cursorFREE = cursor FOR
				SELECT	PICKING_ID,
						CANT_CONFIRMADA
				FROM	PICKING
				WHERE	PALLET_PICKING = @CONTENEDORA
						AND PALLET_CONTROLADO = '1'
						AND PRODUCTO_ID = @PRODUCTO_ID
						AND CLIENTE_ID = @CLIENTE_ID
				ORDER BY CANT_CONFIRMADA

				OPEN @cursorFREE
				FETCH NEXT FROM @cursorFREE INTO @picking_id, @cant_confirmada

				WHILE ((@@FETCH_STATUS = 0) AND (@CANT_A_LIBERAR - @cant_confirmada >= 0))
				BEGIN
						SET @CANT_A_LIBERAR = @CANT_A_LIBERAR - @cant_confirmada

						UPDATE	picking
						SET		pallet_picking = @contenedora,
								pallet_controlado = '0'
						WHERE	picking_id = @picking_id

					FETCH NEXT FROM @cursorFREE INTO @picking_id, @cant_confirmada
				END
			

				--en este punto si @cant_elegida_AUX < 0 entonces tenemos seleccionado el producto que hay que "partir"
					IF ((@CANT_A_LIBERAR - @cant_confirmada < 0) AND (@cant_a_liberar > 0) AND (@@fetch_status=0))
					BEGIN
						insert into picking 
						(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, PALLET_CERRADO, USUARIO_PF, TERMINAL_PF, REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS, BULTOS_NO_CONTROLADOS, FLG_PALLET_HOMBRE, TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE ) 
						select	DOCUMENTO_ID,
								NRO_LINEA,
								CLIENTE_ID,
								PRODUCTO_ID,
								VIAJE_ID,
								TIPO_CAJA,
								DESCRIPCION,
								@CANT_A_LIBERAR,--CANTIDAD,     LO COMENTADO ES LO QUE ESTABA ANTES
								NAVE_COD,
								POSICION_COD,
								RUTA,
								PROP1,
								FECHA_INICIO,
								FECHA_FIN,
								USUARIO,
								@CANT_A_LIBERAR, --CANT_CONFIRMADA
								PALLET_PICKING,
								SALTO_PICKING,
								'0', --PALLET_CONTROLADO
								USUARIO_CONTROL_PICK,
								ST_ETIQUETAS,
								ST_CAMION,
								FACTURADO,
								FIN_PICKING,
								ST_CONTROL_EXP,
								FECHA_CONTROL_PALLET,
								TERMINAL_CONTROL_PALLET,
								FECHA_CONTROL_EXP,
								USUARIO_CONTROL_EXP,
								TERMINAL_CONTROL_EXP,
								FECHA_CONTROL_FAC,
								USUARIO_CONTROL_FAC,
								TERMINAL_CONTROL_FAC,
								VEHICULO_ID,
								PALLET_COMPLETO,
								HIJO,
								QTY_CONTROLADO,
								PALLET_FINAL,
								PALLET_CERRADO,
								USUARIO_PF,
								TERMINAL_PF,
								REMITO_IMPRESO,
								NRO_REMITO_PF,
								PICKING_ID_REF,
								BULTOS_CONTROLADOS,
								BULTOS_NO_CONTROLADOS,
								FLG_PALLET_HOMBRE,
								TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE
						from picking where picking_id = @picking_id

						UPDATE PICKING SET	CANT_CONFIRMADA = CANT_CONFIRMADA - @CANT_A_LIBERAR, 
											CANTIDAD = CANT_CONFIRMADA - @CANT_A_LIBERAR --ESTA LINEA ESTA CORREGIDA, CUALQUIER COSA BORRARLA
						WHERE PICKING_ID = @PICKING_ID
					END

				CLOSE @cursorFREE
				DEALLOCATE @cursorFREE
			END
		END
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

ALTER Procedure [dbo].[Registra_Sys_Session_Login]
@pUsuario_id 	as varchar(100) output,
@pTerminal		as varchar(100) output,
@pAccion			as varchar(100) output
As

declare @vId		as numeric(30,0)
declare @vCount	as numeric(30,0)
declare @vNombre_Usuario as varchar(100)

Begin

--Borro las sessiones que detecto que no estan activas y la que corre este procedure
delete Sys_Session_Login where session_id not in (select spid from  master.dbo.sysprocesses)

select @vId=@@SPID

if (@pAccion=1) begin
		select @vNombre_Usuario=nombre from sys_usuario where usuario_id=@pUsuario_id
		select @vCount=count(*) from Sys_Session_Login where session_id=@vId
		
		if(isnull(@vCount,0)=0) begin
			insert into Sys_Session_Login values (@vId,@pUsuario_id,@vNombre_Usuario,@pTerminal,getdate())
		end 
		else begin
			update Sys_Session_Login set
													Usuario_id=@pUsuario_id,
													Nombre_Usuario=@vNombre_Usuario,
													Terminal=@pTerminal,
													fecha_login=getdate()
			where session_id=@vId
												
		end --if
end else begin

		delete Sys_Session_Login where session_id=@vId and usuario_id=@pUsuario_id
end --if
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

ALTER PROCEDURE [dbo].[RELACION_VEHICULO_POSICION]
@POSICION_COD		VARCHAR(45),
@VEHICULO_ID		VARCHAR(50)
AS
BEGIN
	DECLARE @POSICION_ID	NUMERIC(20,0)

	SELECT 	@POSICION_ID=POSICION_ID
	FROM	POSICION
	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD)))

	INSERT INTO RL_VEHICULO_POSICION (VEHICULO_ID, POSICION_ID)
	VALUES(@VEHICULO_ID, @POSICION_ID)


END--FIN PROCEDURE.
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

ALTER procedure [dbo].[renew_pos]
as
begin
	declare @cur_prod	cursor
	declare @prod		varchar(30)
	
	Set @cur_prod= cursor for
		select	distinct 
				producto_id
		from	#tmp_pos
		
	open @cur_prod
	fetch next from @cur_prod into @prod
	while @@fetch_status=0
	begin
		select @prod [debug]
		delete
		from	rl_producto_posicion_permitida
		where	cliente_id='VITALCAN'
				And producto_id=@prod
				And Posicion_id in
					(
					select	posicion_id
					from	posicion
					where	picking='1' 
							and posicion_cod not in( Select upper(posicion_cod)
													 from	#tmp_pos
													 where  producto_id=@Prod	
													)
					)

		fetch next from @cur_prod into @prod
	end --end while @cur_prod
	close @cur_prod
	deallocate @cur_prod
end-- end proc.
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