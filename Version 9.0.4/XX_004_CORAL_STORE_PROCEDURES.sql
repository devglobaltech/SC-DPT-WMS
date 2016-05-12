
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 03:36 p.m.
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
SET QUOTED_IDENTIFIER OFF
GO

ALTER        PROCEDURE [dbo].[Aux_det_doc_insert]
@documento_id			numeric(20,0)  output,
@nro_linea				numeric(10,0)  output,	
@motivo_id				varchar(15)  output,
@usuario					varchar(20)  output,
@terminal				varchar(20)  output,
@observacion			varchar(100)  output,
@documento_id_orig	numeric(20,0)  output,
@nro_linea_orig		numeric(10,0)  output	
AS
BEGIN

	Declare @QtyOriginal		as Float
	Declare @QtyNueva		as Float

	Select 	@QtyOriginal=Sum(Cant_Confirmada)
	From	vPicking
	Where	Documento_id=@documento_id_orig
			And nro_linea=@nro_linea_orig
	
	Select 	@QtyNueva=Cantidad
	from	det_documento
	where	documento_id=@documento_id
			and nro_linea=@nro_linea



	If @QtyNueva>@QtyOriginal
	Begin
		raiserror('Se esta tratando de ingresar mas de lo que egreso.',16,1)
		return
	End

     insert into aux_det_documento values(@documento_id,
					  @nro_linea,
					  @motivo_id,
					  getdate(),
					  @usuario,
					  @terminal,
					  @observacion,
					  @documento_id_orig,
					  @nro_linea_orig)
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

ALTER PROCEDURE [dbo].[BAJA_SYS_MAPEADOR]
			@CLIENTE_ID AS varchar(15) OUTPUT,
			@CARPETA AS varchar(100) OUTPUT,
			@TIPO_INTERFAZ AS varchar(100) OUTPUT,
			@TABLA_CAMPO AS varchar(100) OUTPUT,
			@LISTA_OK AS varchar(800) OUTPUT,
			@LISTA_ERROR AS varchar(800) OUTPUT,
			@TIPO_COMPROBANTE_ID AS varchar(5) OUTPUT,
			@TIPO_OPERACION_ID AS varchar(5) OUTPUT,
			@CARPETA_OK AS varchar(100) OUTPUT,
			@CARPETA_ERR AS varchar(100) OUTPUT,
			@SEPARADOR AS varchar(1) OUTPUT,
			@FTP AS varchar(1) OUTPUT,
			@USR_FTP AS varchar(100) OUTPUT,
			@PSW_FTP AS varchar(100) OUTPUT,
			@SERVIDOR_FTP AS varchar(15) OUTPUT,
			@PROCESA_PRIMERA AS varchar(1) OUTPUT,
			@PROCESA_ULTIMA AS varchar(1) OUTPUT,
			@TRANS_LINEA_O_GRUPO AS varchar(1) OUTPUT,
			@POS_ARCHIVO AS numeric OUTPUT,
			@COMPUESTO AS varchar(10) OUTPUT,
			@SYS_DATE AS varchar(1) OUTPUT,
			@DATE_OFFSET AS varchar(1) OUTPUT,	
			@FORMATO AS varchar(15) OUTPUT,
			@NUMERAR AS varchar(1) OUTPUT   
AS
BEGIN
DELETE [dbo].[SYS_MAPEADOR]
 WHERE CLIENTE_ID = @CLIENTE_ID 
		AND CARPETA = @CARPETA 
		AND TABLA_CAMPO = @TABLA_CAMPO
	
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
-- Author:		Tata Ignacio Angel
-- Create date: 17/06/2010
-- Description:	Realiza conversion de Cantidad1 a Cantidad2
-- =============================================

ALTER PROCEDURE [dbo].[BASWMS_convertir_Cantidad1_Cantidad2]
	-- Add the parameters for the stored procedure here
	@cantidad1	numeric(20,5),
	@cantidad2	numeric(20,5)	OUTPUT
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	SET @cantidad2 = 1000 * @cantidad1
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
ALTER PROCEDURE [dbo].[BASWMS_egresoMatTerceros]
	-- Add the parameters for the stored procedure here
AS
BEGIN

	DECLARE @cursor				cursor
	DECLARE @cursorDET			cursor
	DECLARE @doc_ext			varchar(100)
	DECLARE @fecha				datetime
	DECLARE @cantidad			numeric(20,5)
	DECLARE @cantidad2			numeric(20,5)
	DECLARE	@SEQ				smallint
	DECLARE @coddep				varchar(10)
	DECLARE @fechaFORM			varchar(50)
	DECLARE @nro_lote			varchar(100)
	DECLARE @producto_id		varchar(30)
	DECLARE @relac				smallint
	DECLARE @relacion			smallint
	DECLARE @prop3				varchar(100)
	DECLARE @sucursal_destino	varchar(20)
	DECLARE @partida			char(13)
	DECLARE @tipoRelacion		char(1)
	DECLARE @fechaEntrega		datetime
	DECLARE @prefijo			varchar(6)
	DECLARE @numero				varchar(20)
	DECLARE @fechaEntregaFORM	varchar(50)
	DECLARE @remitoINGR			nvarchar(MAX)
	DECLARE @remitoEGR			nvarchar(MAX)
	DECLARE @reqCompra			nvarchar(MAX)
	DECLARE @codRQ				varchar(30)
	DECLARE @observaciones		varchar(100)
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	SET @cursor = cursor for
	SELECT	fecha_cpte,
			doc_ext
	FROM	sys_dev_documento
	WHERE	tipo_documento_id = 'E04'
			AND doc_ext in (select nro_trans from BASWMS_procesados WHERE tipo_op = 'PO')

	OPEN @cursor
	FETCH NEXT FROM @cursor INTO @fecha, @doc_ext

	WHILE @@fetch_status = 0
	BEGIN

		SET @cursorDET = cursor for
		SELECT	producto_id,
				prop3,
				nro_lote
		FROM	sys_dev_det_documento
		WHERE	doc_ext = @doc_ext
				AND cat_log_id = 'DISPONIBLE'

		OPEN @cursorDET
		FETCH NEXT FROM @cursorDET INTO @producto_id, @prop3, @nro_lote
		WHILE @@fetch_status = 0
		BEGIN
			SET @fechaFORM = convert(varchar, @fecha, 103)

			SELECT @relac = relacion from BASWMS_PO where ((producto_id = @producto_id) AND (nro_trans = @doc_ext))
			SET @relacion = isnull(@relac,1)

			SET @cantidad2 = @cantidad / @relacion

			SET @partida = isnull (@nro_lote, @prop3)

			EXEC GET_VALUE_FOR_SEQUENCE 'NRO_PREFIJOEXT', @SEQ OUTPUT

			SET @remitoEGR =	'REGISTRO|' + char(13) +
								'Remito egreso' + char(13) +
								@fechaFORM + '|14|' + cast(@SEQ as varchar) + '|TED|TED||||8001|||A||||' + char(13) +
								'ITEM|' + @producto_id + '|' + cast(@cantidad as varchar) + '|' + cast(@cantidad2 as varchar) + '|N||||||||||||||||||||||||||||||' + @prop3 + '|' + @partida + '||||||||||'

			SELECT @coddep = coddep FROM BASWMS_PO where nro_trans = @doc_ext

			SELECT @sucursal_destino = ISNULL(sucursal_destino, @coddep) FROM documento where documento_id = @doc_ext
			
			
			
			if @coddep <> @sucursal_Destino
			BEGIN
				--ACTUALIZO EN BAS
				UPDATE bas_microsules.dbo.TRANSAC
				SET coddep = @sucursal_destino
				WHERE bas_microsules.dbo.TRANSAC.nroTrans = @doc_ext
			END

			SET @remitoINGR =	'REGISTRO|' + char(13) +
								'Remito ingreso' + char(13) +
								@fechaFORM + '|14|' + CAST(@SEQ as varchar) + '|TED|TED|||N|' + @sucursal_destino + '|7004||A||' + char(13) +
								'ITEM|' + @producto_id + '|' + cast(@cantidad as varchar) + '|' + cast(@cantidad2 as varchar) + '|N|||||||||||||||||||||||||||||' + @prop3 + '|' + @partida + '||||||'


			
			SELECT	@codRQ = 'K'+Substring(@producto_id,2,7),
					@cantidad = cantidad,
					@tipoRelacion = tipoRelacion,
					@relacion = relacion,
					@partida = partida,
					@prefijo = prefijo, 
					@numero = numero
			FROM	BASWMS_PO
			WHERE	secuencia3 = 0
					AND nro_trans = @doc_ext

			if @tipoRelacion = 'F'
			BEGIN
				SET @cantidad2 = @cantidad / @relacion
			END
			ELSE
			BEGIN
				SET @cantidad2 = @cantidad
			END

			SET @fechaEntrega = dateadd(day, 30, @fecha)
			
			SET @fechaEntregaFORM = convert(varchar, @fecha, 103)

			SET @observaciones = 'LOTE ' + @partida

			SET @reqCompra =	'REGISTRO|' + char(13) +
								'Requerimiento Compra' + char(13) +
								@fechaFORM + '|' + @prefijo + '|' + @numero + '|PLAN|RQT|' + @sucursal_destino + '|C||' + char(13) +
								'ITEM|' + @codRQ + '|' + cast(@cantidad as varchar) + '|' + cast(@cantidad2 as varchar) + '|||' + @fechaEntregaFORM + '|' + @observaciones + '|||||||||||'
								
			INSERT INTO  BASWMS_remitosIER VALUES (@remitoINGR, @remitoEGR, @reqCompra)
			
			FETCH NEXT FROM @cursorDET INTO @producto_id, @prop3, @nro_lote
		END

		FETCH NEXT FROM @cursor INTO @fecha, @doc_ext
	END

	CLOSE @cursor
	DEALLOCATE @cursor



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
-- Author:		Ignacio Tata
-- Create date: <Create Date,,>
-- Description:	PUNTO 10
-- =============================================
ALTER PROCEDURE [dbo].[BASWMS_egresoMuestraCalidad]
	-- Add the parameters for the stored procedure here
AS
BEGIN

	DECLARE	@cursor			cursor
	DECLARE @cursorDET		cursor
	DECLARE @doc_ext		varchar(100)
	DECLARE @cat_log_id		varchar(15)
	DECLARE @relac			numeric(20,5)
	DECLARE @cantidad		numeric(20,5)
	DECLARE	@cantidad2		numeric(20,5)
	DECLARE @remitoEGR		nvarchar(MAX)
	DECLARE @fecha			datetime	
	DECLARE @producto_id	varchar(30)
	DECLARE @relacion		numeric(20,5)
	DECLARE @coddep			varchar(10)
	DECLARE @prop3			varchar(100)
	DECLARE @nro_lote		varchar(100)
	DECLARE @fechaFORM		varchar(50)

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    SET @cursor = cursor for
	SELECT	fecha_cpte,
			doc_ext
	FROM sys_dev_documento
	WHERE tipo_documento_id = 'E10'

	OPEN @cursor
	FETCH NEXT FROM @cursor INTO @fecha, @doc_ext

	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @cursorDET = cursor FOR
		SELECT	producto_id,
				cantidad,
				prop3,
				nro_lote,
				cat_log_id
		FROM sys_dev_det_documento
		WHERE doc_ext = @doc_ext
	
		OPEN @cursorDET
		FETCH NEXT FROM @cursorDET INTO @producto_id, @cantidad, @prop3, @nro_lote, @cat_log_id

		WHILE @@fetch_status = 0
		BEGIN
			
			
			EXEC BASWMS_convertir_catLog_nDeposito @cat_log_id, @coddep OUTPUT

			SELECT @relac = relacion from bas_microsules.dbo.items where coditm = @producto_id
			SET @relacion = isnull(@relac,1)

			SET @cantidad2 = @cantidad / @relacion

			SET @fechaFORM = convert(varchar, @fecha, 103)

			SET @remitoEGR = 'REGISTRO|' + char(13) +
							'Remito egreso' + char(13) +
							@fechaFORM + '|7002||MUE|MUE||||' + @coddep + '|||A||||' + char(13) +
							'ITEM|' + @producto_id + '|' + cast(@cantidad as varchar) + '|' + cast(@cantidad2 as varchar)+ 'N||||||||||||||||||||||||||||||' + @prop3 + '|' + @nro_lote + '||||||||||'

			INSERT INTO BASWMS_remitosMuestraCalidad VALUES (@remitoEGR)

			FETCH NEXT FROM @cursorDET INTO @producto_id, @cantidad, @prop3, @nro_lote, @cat_log_id
		END
		
		CLOSE @cursorDET
		DEALLOCATE @cursorDET
		
		FETCH NEXT FROM @cursor INTO @fecha, @doc_ext
	END
	
	CLOSE @cursor
	DEALLOCATE @cursor




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
-- Author:		LRojas
-- Create date: 19/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[busca_caja_contenedora_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(30) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    SELECT P.PRODUCTO_ID [Cod Producto], ISNULL(P.NRO_LOTE,'') AS NRO_LOTE, ISNULL(P.NRO_PARTIDA,'') AS NRO_PARTIDA, ISNULL(P.NRO_SERIE,'') AS NRO_SERIE, SUM(P.CANT_CONFIRMADA) [Cantidad], PR.UNIDAD_ID [Unidad], PR.DESCRIPCION [Descripción], 'Quitar Producto' [Acción]
	  FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
	 INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
	 WHERE P.CLIENTE_ID = @CLIENTE_ID AND D.NRO_REMITO = @PEDIDO_ID AND P.PALLET_PICKING = @NRO_CONTENEDORA AND P.PALLET_CONTROLADO <> '0'
     GROUP BY P.PRODUCTO_ID,P.NRO_LOTE,P.NRO_PARTIDA,P.NRO_SERIE, PR.UNIDAD_ID, PR.DESCRIPCION
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
-- Author:		LRojas
-- Create date: 17/04/2012
-- Description:	Procedimiento para buscar productos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[busca_contenedora_empaque] 
	@CLIENTE_ID as varchar(15) OUTPUT,
    @PEDIDO_ID as varchar(30) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    SELECT DISTINCT P.PALLET_PICKING, 
           'Abrir Contenedora'      [Abrir], 
           'Ver Contenido'          [Ver], 
           'Eliminar Contenedora'   [Eliminar], 
           'Imprimir Etiqueta'      [Imprimir] 
      FROM DOCUMENTO D 
     INNER JOIN PICKING P ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
     WHERE P.CLIENTE_ID = @CLIENTE_ID
       AND D.NRO_REMITO = @PEDIDO_ID
       AND P.PALLET_CONTROLADO <> '0'
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
-- Author:		LRojas
-- Create date: 16/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[busca_pedido_empaque] 
    @PEDIDO_ID as varchar(30) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    DECLARE @CountPed           as Integer,
            @DOCUMENTO_ID       as Numeric(20),
            @CountProd          as Integer,
            @TIPO_OPERACION_ID  as Varchar(5), 
            @STATUS             as Varchar(3), 
            @NRO_REMITO         as Varchar(30), 
            @FACTURADO          as Char(1), 
            @ST_CAMION          as Char(1),
            @MSJ_ERR            as Varchar(Max)
    
    SELECT @CountPed = Count(*), @DOCUMENTO_ID = D.DOCUMENTO_ID
      FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
     INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA) 
     INNER JOIN SUCURSAL S ON(S.SUCURSAL_ID = D.SUCURSAL_DESTINO AND S.CLIENTE_ID = P.CLIENTE_ID) 
     INNER JOIN SYS_INT_DOCUMENTO ID ON (ID.CLIENTE_ID = P.CLIENTE_ID AND ID.DOC_EXT = D.NRO_REMITO) -- 
     WHERE D.TIPO_OPERACION_ID = 'EGR' 
       AND D.STATUS = 'D30' 
       AND D.NRO_REMITO IS NOT NULL 
       AND P.FACTURADO = '0' 
       AND P.ST_CAMION = '0' 
       AND D.NRO_REMITO = @PEDIDO_ID 
     GROUP BY D.DOCUMENTO_ID
    
    IF @CountPed > 0 
        BEGIN
            SELECT @CountProd = COUNT(*)
            FROM PICKING WHERE DOCUMENTO_ID = @DOCUMENTO_ID
            AND USUARIO IS NOT NULL
            AND FECHA_INICIO IS NOT NULL
            AND FECHA_FIN IS NOT NULL
            AND CANT_CONFIRMADA IS NOT NULL
            
            IF @CountPed = @CountProd
                BEGIN
                    SELECT DISTINCT 
                           P.CLIENTE_ID, D.NRO_REMITO AS [NRO PEDIDO], 
                           LTRIM(RTRIM(ISNULL(D.CPTE_PREFIJO, '') + ' ' + ISNULL(D.CPTE_NUMERO, ''))) AS [NRO REMITO], 
                           S.NOMBRE AS [SUCURSAL DESTINO] 
                      FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
                     INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA) 
                     INNER JOIN SUCURSAL S ON(S.SUCURSAL_ID = D.SUCURSAL_DESTINO AND S.CLIENTE_ID = P.CLIENTE_ID) 
                     INNER JOIN SYS_INT_DOCUMENTO ID ON (ID.CLIENTE_ID = P.CLIENTE_ID AND ID.DOC_EXT = D.NRO_REMITO) -- 
                     WHERE D.TIPO_OPERACION_ID = 'EGR' 
                       AND D.STATUS = 'D30' 
                       AND D.NRO_REMITO IS NOT NULL 
                       AND P.FACTURADO = '0' 
                       AND P.ST_CAMION = '0' 
                       AND D.NRO_REMITO = @PEDIDO_ID 
                
                    DELETE TMP_EMPAQUE_CONTENEDORA WHERE NRO_REMITO = @PEDIDO_ID 
                END
            ELSE
                RAISERROR('Todos los productos deben estar Pickeados.', 16, 1)
        END
    ELSE
        BEGIN
            IF NOT EXISTS(SELECT 1 FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
                          INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
                          WHERE D.NRO_REMITO = @PEDIDO_ID)
                BEGIN
                    RAISERROR
                        (N'El pedido %s no existe.',
                        16, -- Severity.
                        1, -- State.
                        @PEDIDO_ID, -- First substitution argument.
                        @PEDIDO_ID); -- Second substitution argument.
                END
            ELSE
                BEGIN
                    SELECT @TIPO_OPERACION_ID = D.TIPO_OPERACION_ID, 
                           @STATUS = D.STATUS, 
                           @FACTURADO = P.FACTURADO, 
                           @ST_CAMION = P.ST_CAMION
                      FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
                     INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA) 
                     INNER JOIN SUCURSAL S ON(S.SUCURSAL_ID = D.SUCURSAL_DESTINO) 
                     INNER JOIN SYS_INT_DOCUMENTO ID ON (ID.CLIENTE_ID = P.CLIENTE_ID AND ID.DOC_EXT = D.NRO_REMITO) -- 
                     WHERE D.NRO_REMITO = @PEDIDO_ID 
                    
                    SET @MSJ_ERR = ''
                    
                    IF @TIPO_OPERACION_ID <> 'EGR'
                        SET @MSJ_ERR = @MSJ_ERR + 'Documento no es Egreso. '
                    
                    IF @STATUS <> 'D30'
                        SET @MSJ_ERR = @MSJ_ERR + 'Estado no es D30. '
                    
                    IF @FACTURADO <> '0'
                        SET @MSJ_ERR = @MSJ_ERR + 'Pedido ya facturado. '
                    
                    IF @ST_CAMION <> '0'
                        SET @MSJ_ERR = @MSJ_ERR + 'Ya se encuentra en el vehiculo. '
                        
                    SET @MSJ_ERR = @PEDIDO_ID + ': ' + @MSJ_ERR
                    
                    RAISERROR
                        (N'Error pedido %s',
                        16, -- Severity.
                        1, -- State.
                        @MSJ_ERR, -- First substitution argument.
                        @MSJ_ERR); -- Second substitution argument.
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

-- =============================================
-- Author:		LRojas
-- Create date: 17/04/2012
-- Description:	Procedimiento para buscar productos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[busca_producto_empaque] 
	@CLIENTE_ID as varchar(15) OUTPUT,
    @PEDIDO_ID as varchar(30) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
SELECT	PR.PRODUCTO_ID as ProductoID
		,ISNULL(P.NRO_LOTE,'')	as LoteProveedor
		,ISNULL(P.NRO_PARTIDA,'') as NroPartida
		,ISNULL(P.NRO_SERIE,'') as NroSerie
		,SUM(P.CANT_CONFIRMADA) as CANTIDAD_PICKEADA
		,ISNULL(TMP.CANT_CONFIRMADA, 0) CANTIDAD_CONTROLADA
		,PR.UNIDAD_ID as Unidad
		,PR.DESCRIPCION as DescrProd
FROM DOCUMENTO D
INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID)
INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
LEFT JOIN	(SELECT DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, SUM(CANT_CONFIRMADA) CANT_CONFIRMADA, PALLET_CONTROLADO 
			FROM PICKING WHERE PALLET_CONTROLADO <> '0'
			GROUP BY DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, PALLET_CONTROLADO
			) TMP ON TMP.DOCUMENTO_ID = P.DOCUMENTO_ID AND TMP.PRODUCTO_ID = P.PRODUCTO_ID AND ISNULL(TMP.NRO_LOTE,'') = ISNULL(P.NRO_LOTE,'') AND ISNULL(TMP.NRO_PARTIDA,'') = ISNULL(P.NRO_PARTIDA,'') AND ISNULL(TMP.NRO_SERIE,'') = ISNULL(P.NRO_SERIE,'')
WHERE	P.CLIENTE_ID = @CLIENTE_ID
		AND D.NRO_REMITO = @PEDIDO_ID
GROUP BY	PR.PRODUCTO_ID
			,ISNULL(TMP.CANT_CONFIRMADA, 0)
			,P.NRO_LOTE
			,P.NRO_PARTIDA
			,ISNULL(P.NRO_SERIE,'')
			,PR.UNIDAD_ID
			,PR.DESCRIPCION
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
-- Author:		LRojas
-- Create date: 17/04/2012
-- Description:	Procedimiento para buscar productos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[busca_tmp_producto_empaque] 
	@CLIENTE_ID as varchar(15) OUTPUT,
    @PEDIDO_ID as varchar(30) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
SELECT PR.PRODUCTO_ID
		,ISNULL(P.NRO_LOTE,'')	as LoteProveedor
		,ISNULL(P.NRO_PARTIDA,'') as NroPartida
		,ISNULL(P.NRO_SERIE,'') as NroSerie
			,SUM(P.CANT_CONFIRMADA) CANTIDAD_PICKEADA, 
           ISNULL(TMP.CANT_CONFIRMADA, 0) CANTIDAD_CONTROLADA, 
           PR.UNIDAD_ID,
           PR.DESCRIPCION
      FROM DOCUMENTO D INNER JOIN TMP_EMPAQUE_CONTENEDORA P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
     INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
      LEFT JOIN (SELECT DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, SUM(CANT_CONFIRMADA) CANT_CONFIRMADA, PALLET_CONTROLADO 
                   FROM TMP_EMPAQUE_CONTENEDORA WHERE PALLET_CONTROLADO <> '0'
                  GROUP BY DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, PALLET_CONTROLADO
                ) TMP ON TMP.DOCUMENTO_ID = P.DOCUMENTO_ID AND TMP.PRODUCTO_ID = P.PRODUCTO_ID AND ISNULL(TMP.NRO_LOTE,'') = ISNULL(P.NRO_LOTE,'') AND ISNULL(TMP.NRO_PARTIDA,'') = ISNULL(P.NRO_PARTIDA,'') AND ISNULL(TMP.NRO_SERIE,'') = ISNULL(P.NRO_SERIE,'')
     WHERE P.CLIENTE_ID = @CLIENTE_ID
       AND D.NRO_REMITO = @PEDIDO_ID
     GROUP BY PR.PRODUCTO_ID
			,ISNULL(TMP.CANT_CONFIRMADA, 0)
			,P.NRO_LOTE
			,P.NRO_PARTIDA
			,ISNULL(P.NRO_SERIE,'')
			,PR.UNIDAD_ID
			,PR.DESCRIPCION
UNION
    SELECT PR.PRODUCTO_ID
		,ISNULL(P.NRO_LOTE,'')	as LoteProveedor
		,ISNULL(P.NRO_PARTIDA,'') as NroPartida
		,ISNULL(P.NRO_SERIE,'') as NroSerie
			,SUM(P.CANT_CONFIRMADA) CANTIDAD_PICKEADA, 
           ISNULL(TMP.CANT_CONFIRMADA, 0) CANTIDAD_CONTROLADA, 
           PR.UNIDAD_ID,
           PR.DESCRIPCION
      FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
     INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
      LEFT JOIN (SELECT DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, SUM(CANT_CONFIRMADA) CANT_CONFIRMADA, PALLET_CONTROLADO 
                   FROM PICKING WHERE PALLET_CONTROLADO <> '0'
                  GROUP BY DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, PALLET_CONTROLADO
                ) TMP ON TMP.DOCUMENTO_ID = P.DOCUMENTO_ID AND TMP.PRODUCTO_ID = P.PRODUCTO_ID AND ISNULL(TMP.NRO_LOTE,'') = ISNULL(P.NRO_LOTE,'') AND ISNULL(TMP.NRO_PARTIDA,'') = ISNULL(P.NRO_PARTIDA,'') AND ISNULL(TMP.NRO_SERIE,'') = ISNULL(P.NRO_SERIE,'')
     WHERE P.CLIENTE_ID = @CLIENTE_ID
       AND D.NRO_REMITO = @PEDIDO_ID
       AND NOT EXISTS (SELECT 1 FROM TMP_EMPAQUE_CONTENEDORA TEC WHERE TEC.DOCUMENTO_ID = P.DOCUMENTO_ID AND TEC.PRODUCTO_ID = P.PRODUCTO_ID AND ISNULL(TEC.NRO_LOTE,'') = ISNULL(P.NRO_LOTE,'') AND ISNULL(TEC.NRO_PARTIDA,'') = ISNULL(P.NRO_PARTIDA,'') AND ISNULL(TEC.NRO_SERIE,'') = ISNULL(P.NRO_SERIE,''))
     GROUP BY PR.PRODUCTO_ID
			,ISNULL(TMP.CANT_CONFIRMADA, 0)
			,P.NRO_LOTE
			,P.NRO_PARTIDA
			,ISNULL(P.NRO_SERIE,'')
			,PR.UNIDAD_ID
			,PR.DESCRIPCION

--SELECT PR.PRODUCTO_ID
--		,P.NRO_LOTE	as LoteProveedor
--		,P.NRO_PARTIDA as NroPartida
--		,ISNULL(P.NRO_SERIE,'') as NroSerie
--			,SUM(P.CANT_CONFIRMADA) CANTIDAD_PICKEADA, 
--           ISNULL(TMP.CANT_CONFIRMADA, 0) CANTIDAD_CONTROLADA, 
--           PR.UNIDAD_ID,
--           PR.DESCRIPCION
--      FROM DOCUMENTO D INNER JOIN TMP_EMPAQUE_CONTENEDORA P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
--     INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
--      LEFT JOIN (SELECT DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, SUM(CANT_CONFIRMADA) CANT_CONFIRMADA, PALLET_CONTROLADO 
--                   FROM TMP_EMPAQUE_CONTENEDORA WHERE PALLET_CONTROLADO <> '0'
--                  GROUP BY DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, PALLET_CONTROLADO
--                ) TMP ON TMP.DOCUMENTO_ID = P.DOCUMENTO_ID AND TMP.PRODUCTO_ID = P.PRODUCTO_ID AND ISNULL(TMP.NRO_LOTE,'') = ISNULL(P.NRO_LOTE,'') AND ISNULL(TMP.NRO_PARTIDA,'') = ISNULL(P.NRO_PARTIDA,'') AND ISNULL(TMP.NRO_SERIE,'') = ISNULL(P.NRO_SERIE,'')
--     WHERE P.CLIENTE_ID = @CLIENTE_ID
--       AND D.NRO_REMITO = @PEDIDO_ID
--     GROUP BY PR.PRODUCTO_ID
--			,ISNULL(TMP.CANT_CONFIRMADA, 0)
--			,P.NRO_LOTE
--			,P.NRO_PARTIDA
--			,ISNULL(P.NRO_SERIE,'')
--			,PR.UNIDAD_ID
--			,PR.DESCRIPCION
--UNION
--    SELECT PR.PRODUCTO_ID
--		,P.NRO_LOTE	as LoteProveedor
--		,P.NRO_PARTIDA as NroPartida
--		,ISNULL(P.NRO_SERIE,'') as NroSerie
--			,SUM(P.CANT_CONFIRMADA) CANTIDAD_PICKEADA, 
--           ISNULL(TMP.CANT_CONFIRMADA, 0) CANTIDAD_CONTROLADA, 
--           PR.UNIDAD_ID,
--           PR.DESCRIPCION
--      FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
--     INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
--      LEFT JOIN (SELECT DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, SUM(CANT_CONFIRMADA) CANT_CONFIRMADA, PALLET_CONTROLADO 
--                   FROM PICKING WHERE PALLET_CONTROLADO <> '0'
--                  GROUP BY DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, PALLET_CONTROLADO
--                ) TMP ON TMP.DOCUMENTO_ID = P.DOCUMENTO_ID AND TMP.PRODUCTO_ID = P.PRODUCTO_ID AND ISNULL(TMP.NRO_LOTE,'') = ISNULL(P.NRO_LOTE,'') AND ISNULL(TMP.NRO_PARTIDA,'') = ISNULL(P.NRO_PARTIDA,'') AND ISNULL(TMP.NRO_SERIE,'') = ISNULL(P.NRO_SERIE,'')
--     WHERE P.CLIENTE_ID = @CLIENTE_ID
--       AND D.NRO_REMITO = @PEDIDO_ID
--       AND NOT EXISTS (SELECT 1 FROM TMP_EMPAQUE_CONTENEDORA TEC WHERE TEC.DOCUMENTO_ID = P.DOCUMENTO_ID AND TEC.PRODUCTO_ID = P.PRODUCTO_ID AND TEC.NRO_LOTE = P.NRO_LOTE AND TEC.NRO_PARTIDA = P.NRO_PARTIDA AND TEC.NRO_SERIE = P.NRO_SERIE)
--     GROUP BY PR.PRODUCTO_ID
--			,ISNULL(TMP.CANT_CONFIRMADA, 0)
--			,P.NRO_LOTE
--			,P.NRO_PARTIDA
--			,ISNULL(P.NRO_SERIE,'')
--			,PR.UNIDAD_ID
--			,PR.DESCRIPCION

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

ALTER      PROCEDURE [dbo].[CERRAR_PALLET]
@VIAJEID 		AS VARCHAR(30),
@PRODUCTO_ID	AS VARCHAR(50),
@POSICION_COD	AS VARCHAR(45),
@PALLET			AS VARCHAR(100),
@PALLET_PICKING AS NUMERIC(20),
@USUARIO		AS VARCHAR(30),
@RUTA			AS VARCHAR(50)
AS

BEGIN
	UPDATE 	PICKING SET	PALLET_PICKING=NULL
	WHERE 	VIAJE_ID=@VIAJEID AND PRODUCTO_ID=@PRODUCTO_ID
			AND POSICION_COD=@POSICION_COD
			AND PROP1=@PALLET
			AND PALLET_PICKING=@PALLET_PICKING
			AND USUARIO=@USUARIO
			AND RUTA=@RUTA
			AND CANT_CONFIRMADA IS NULL
			AND FECHA_FIN IS NULL

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

ALTER PROCEDURE [dbo].[CERRAR_PALLETS]
@CLIENTE_ID	VARCHAR(15)	OUTPUT,
@AGENTE		VARCHAR(20) 	OUTPUT
AS
BEGIN
	DECLARE  @USUARIO		VARCHAR(20)
	DECLARE  @TERMINAL	VARCHAR(100)	

	SELECT 	@USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	SELECT 	@TERMINAL=HOST_NAME()

	UPDATE PICKING SET PALLET_CERRADO='1', USUARIO_PF=@USUARIO, TERMINAL_PF=@TERMINAL
	FROM	PICKING P INNER JOIN DET_DOCUMENTO DD	ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S						ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
	WHERE	S.CLIENTE_ID=@CLIENTE_ID
			AND S.SUCURSAL_ID=@AGENTE

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
-- Author:		LRojas
-- Create date: 18/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[cerrar_tmp_producto_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(30) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
	DECLARE @PRODUCTO_ID as varchar(30),
            @NRO_LINEA as numeric(10)
    
    DECLARE cur_productos CURSOR FOR
    SELECT DISTINCT PRODUCTO_ID, NRO_LINEA FROM TMP_EMPAQUE_CONTENEDORA WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
    
    OPEN cur_productos
    FETCH cur_productos INTO @PRODUCTO_ID, @NRO_LINEA
    
    WHILE @@FETCH_STATUS = 0
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM TMP_EMPAQUE_CONTENEDORA 
                            WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
                            AND NRO_LINEA = @NRO_LINEA
                            AND PRODUCTO_ID = @PRODUCTO_ID AND PALLET_CONTROLADO = '0')
                BEGIN
                    UPDATE PICKING
                    SET CANTIDAD = TMP.CANTIDAD,
                        CANT_CONFIRMADA = TMP.CANT_CONFIRMADA,
                        PALLET_PICKING = TMP.PALLET_PICKING,
                        PALLET_CONTROLADO = TMP.PALLET_CONTROLADO
                    FROM TMP_EMPAQUE_CONTENEDORA TMP 
                    WHERE TMP.CLIENTE_ID = @CLIENTE_ID AND TMP.NRO_REMITO = @PEDIDO_ID
                    AND TMP.PRODUCTO_ID = @PRODUCTO_ID AND TMP.PALLET_PICKING = @NRO_CONTENEDORA
                    AND TMP.NRO_LINEA = @NRO_LINEA
                    AND PICKING.PICKING_ID = TMP.PICKING_ID
                   -- AND PICKING.PALLET_CONTROLADO = '0'
                   
                   DELETE FROM PICKING WHERE PICKING_ID IN (SELECT P.PICKING_ID FROM PICKING P 
                    INNER JOIN TMP_EMPAQUE_CONTENEDORA T
					   ON P.PRODUCTO_ID = T.PRODUCTO_ID AND P.DOCUMENTO_ID = T.DOCUMENTO_ID
					  AND P.PALLET_CONTROLADO = '0' AND P.PRODUCTO_ID = @PRODUCTO_ID AND P.CLIENTE_ID = @CLIENTE_ID
					  AND T.NRO_REMITO = @PEDIDO_ID AND P.NRO_LINEA = @NRO_LINEA)
                   
                END
            ELSE
                BEGIN
                    UPDATE PICKING
                    SET CANTIDAD = TMP.CANTIDAD,
                        CANT_CONFIRMADA = TMP.CANT_CONFIRMADA
                    FROM TMP_EMPAQUE_CONTENEDORA TMP 
                    WHERE TMP.CLIENTE_ID = @CLIENTE_ID AND TMP.NRO_REMITO = @PEDIDO_ID
                    AND TMP.PRODUCTO_ID = @PRODUCTO_ID AND TMP.PALLET_CONTROLADO = '0'
                    AND TMP.NRO_LINEA = @NRO_LINEA
                    AND PICKING.PICKING_ID = TMP.PICKING_ID
                    
                    IF NOT EXISTS(SELECT 1 
                                    FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
                                    WHERE D.CLIENTE_ID = @CLIENTE_ID AND D.NRO_REMITO = @PEDIDO_ID 
                                    AND PRODUCTO_ID = @PRODUCTO_ID AND PALLET_PICKING = @NRO_CONTENEDORA
                                    AND NRO_LINEA=@NRO_LINEA)
                        INSERT INTO PICKING(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, 
                        POSICION_COD, RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
                        USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
                        TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, USUARIO_CONTROL_FAC, 
                        TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, PALLET_CERRADO, USUARIO_PF, TERMINAL_PF, 
                        REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS, BULTOS_NO_CONTROLADOS, FLG_PALLET_HOMBRE, TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE)
                        SELECT DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, 
                        RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, USUARIO_CONTROL_PICK, 
                        ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, 
                        USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, 
                        HIJO, QTY_CONTROLADO, PALLET_FINAL, PALLET_CERRADO, USUARIO_PF, TERMINAL_PF, REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS, 
                        BULTOS_NO_CONTROLADOS, FLG_PALLET_HOMBRE, TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE
                        FROM TMP_EMPAQUE_CONTENEDORA
                        WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
                        AND PRODUCTO_ID = @PRODUCTO_ID AND PALLET_PICKING = @NRO_CONTENEDORA
                        AND NRO_LINEA = @NRO_LINEA
                    ELSE
                        UPDATE PICKING
                        SET CANTIDAD = TMP.CANTIDAD,
                            CANT_CONFIRMADA = TMP.CANT_CONFIRMADA
                        FROM TMP_EMPAQUE_CONTENEDORA TMP 
                        WHERE TMP.CLIENTE_ID = @CLIENTE_ID AND TMP.NRO_REMITO = @PEDIDO_ID
                        AND TMP.PRODUCTO_ID = @PRODUCTO_ID AND TMP.PALLET_PICKING = @NRO_CONTENEDORA
                        AND TMP.NRO_LINEA = @NRO_LINEA
                        AND PICKING.PICKING_ID = TMP.PICKING_ID
                        AND PICKING.PALLET_PICKING = TMP.PALLET_PICKING
                        AND PICKING.PALLET_CONTROLADO <> '0'
                END
            FETCH cur_productos INTO @PRODUCTO_ID, @NRO_LINEA
        END
    CLOSE cur_productos
    DEALLOCATE cur_productos
    
    DELETE TMP_EMPAQUE_CONTENEDORA WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
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
ALTER PROCEDURE [dbo].[CerrarProductoEnContenedora]
	-- Add the parameters for the stored procedure here
	@cliente_id		varchar(15) OUTPUT,
	@nro_remito		varchar(30) OUTPUT,
	@producto_id	varchar(30) OUTPUT,
	@cant_elegida	numeric(20,5) OUTPUT,
	@contenedora	numeric(20,0) OUTPUT
AS
BEGIN

	DECLARE @cursorProducto		cursor
	DECLARE @picking_id			numeric(20,0)
	DECLARE @cant_confirmada	numeric(20,5)

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--AGARRO LOS PRODUCTOS LIBERADOS(SIN CONTENEDORA) DEL PEDIDO
	SET @cursorProducto = cursor FOR
	SELECT		p.picking_id,
				p.cant_confirmada
	FROM		picking p
				INNER JOIN	documento d
					on ((d.cliente_id = p.cliente_id) AND (d.documento_id = p.documento_id))
	WHERE		d.cliente_id = @cliente_id
				AND d.nro_remito = @nro_remito
				AND p.producto_id = @producto_id
				AND p.facturado = '0'
				AND p.st_camion = '0'
				AND p.pallet_controlado = '0'
				AND p.cant_confirmada is not null
				AND d.tipo_operacion_id = 'EGR'
	ORDER BY	p.cant_confirmada


	OPEN @cursorProducto
	FETCH NEXT FROM @cursorProducto INTO @picking_id, @cant_confirmada
		
	WHILE ((@@FETCH_STATUS = 0) AND (@cant_elegida - @cant_confirmada) >= 0)
	BEGIN
		
		SET @cant_elegida = @cant_elegida - @cant_confirmada
		
		-- CIERRO LA CANTIDAD DEL PRODUCTO SELECCIONADO
		UPDATE	picking
		SET		pallet_picking = @contenedora,
				pallet_controlado = '1'
		WHERE	picking_id = @picking_id
		
		FETCH NEXT FROM @cursorProducto INTO @picking_id, @cant_confirmada
	END


	--en este punto si @cant_elegida_AUX < 0 entonces tenemos seleccionado el producto que hay que "PRORRATEAR"
	IF ((@cant_elegida - @cant_confirmada < 0) AND (@cant_elegida > 0) AND (@@fetch_status=0))
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
				@cant_elegida,
				NAVE_COD,
				POSICION_COD,
				RUTA,
				PROP1,
				FECHA_INICIO,
				FECHA_FIN,
				USUARIO,
				@cant_elegida, --CANTIDAD RESTANTE ELEJIDA (cant_confirmada)
				@contenedora, --CONTENEDORA GENERADA (pallet_picking)
				SALTO_PICKING,
				'1', --PALLET_CONTROLADO
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
				@contenedora, --PALLET_FINAL
				PALLET_CERRADO,
				USUARIO_PF,
				TERMINAL_PF,
				REMITO_IMPRESO,
				NRO_REMITO_PF,
				PICKING_ID_REF,
				BULTOS_CONTROLADOS,
				BULTOS_NO_CONTROLADOS,
				FLG_PALLET_HOMBRE,
				TRANSF_TERMINADA,
				NRO_LOTE,
				NRO_PARTIDA,
				NRO_SERIE
		from picking where picking_id = @picking_id

		UPDATE PICKING SET	CANTIDAD = CANTIDAD - @CANT_ELEGIDA,
							CANT_CONFIRMADA = CANT_CONFIRMADA - @CANT_ELEGIDA
		WHERE PICKING_ID = @PICKING_ID
	END
	------------------------------------------------------------------------------------------------------

	CLOSE @cursorProducto
	DEALLOCATE @cursorProducto

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

ALTER  proc [dbo].[chObjOwner]( @usrName varchar(20), @newUsrName varchar(50))
as
-- @usrName is the current user
-- @newUsrName is the new user

set nocount on
declare @uid int                   -- UID of the user
declare @objName varchar(50)       -- Object name owned by user
declare @currObjName varchar(50)   -- Checks for existing object owned by new user 
declare @outStr varchar(256)       -- SQL command with 'sp_changeobjectowner'
set @uid = user_id(@usrName)

declare chObjOwnerCur cursor static
for
select name from sysobjects where uid = @uid

open chObjOwnerCur
if @@cursor_rows = 0
begin
  print 'Error: No objects owned by ' + @usrName
  close chObjOwnerCur
  deallocate chObjOwnerCur
  return 1
end

fetch next from chObjOwnerCur into @objName

while @@fetch_status = 0
begin
  set @currObjName = @newUsrName + "." + @objName
  if (object_id(@currObjName) > 0)
    print 'WARNING *** ' + @currObjName + ' already exists ***'
  set @outStr = "sp_changeobjectowner '" + @usrName + "." + @objName + "','" + @newUsrName + "'"
  print @outStr
  print 'go'
  fetch next from chObjOwnerCur into @objName
end

close chObjOwnerCur
deallocate chObjOwnerCur
set nocount off
return 0
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

ALTER PROCEDURE [dbo].[CLR_CONSUMO_LOCATOR_EGR]
@FECHA	AS VARCHAR(10)	OUTPUT
AS
BEGIN

	DECLARE @MyFECHA AS DATETIME	
	
	IF @FECHA IS NOT NULL
	BEGIN	
		SET @MyFECHA = CONVERT(VARCHAR,@FECHA, 103)		

		DELETE
		FROM	CONSUMO_LOCATOR_EGR	
		WHERE 	PROCESADO='S' 
				AND CONVERT(VARCHAR,FECHA,103) <= @MyFECHA
				AND DOCUMENTO_ID IN(SELECT 	DOCUMENTO_ID
									FROM	DOCUMENTO	
									WHERE	CONSUMO_LOCATOR_EGR.DOCUMENTO_ID=DOCUMENTO.DOCUMENTO_ID
											AND DOCUMENTO.STATUS IN ('D30','D40'))
	END
	
	IF @FECHA IS NULL
	BEGIN
	
		DELETE 
		FROM 	CONSUMO_LOCATOR_EGR	
		WHERE	PROCESADO='S'
				AND DOCUMENTO_ID IN(SELECT 	DOCUMENTO_ID
									FROM	DOCUMENTO	
									WHERE	CONSUMO_LOCATOR_EGR.DOCUMENTO_ID=DOCUMENTO.DOCUMENTO_ID
											AND DOCUMENTO.STATUS IN ('D30','D40'))

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

ALTER PROCEDURE [dbo].[CLR_DET_DOCUMENTO_AUX]
AS
BEGIN

	
		DELETE
		FROM 	DET_DOCUMENTO_AUX	
		WHERE	DOCUMENTO_ID IN(SELECT 	DOCUMENTO_ID
								FROM	DOCUMENTO	
								WHERE	DET_DOCUMENTO_AUX.DOCUMENTO_ID=DOCUMENTO.DOCUMENTO_ID
										AND DOCUMENTO.STATUS IN ('D30','D40'))

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

ALTER Procedure [dbo].[ConfEtibyProd]
@Cliente_Id		varchar(20),
@Producto_ID	varchar(20),
@Msg			Varchar(max) Output
As
Begin
	Declare @Flg	Char(1)
	Declare @Qty	Numeric(20,0)
	Declare @Count	SmallInt

	Select	@Count=Count(*)
	from	producto
	where	Cliente_id=@Cliente_id
			and Producto_id=@Producto_ID

	if @Count=0
	Begin
		raiserror('No se encontro el producto %s para el cliente %s',16,1,@producto_id,@cliente_id)
		return
	End
	Else
	Begin
		Select	@Flg=flg_bulto, @qty=qty_bulto
		from	producto
		where	Cliente_id=@Cliente_id
				and Producto_id=@Producto_ID

		if (@Flg is null) or (@Flg='0')
		Begin
			Set @Msg='No se generaran etiquetas para este producto.'
			return
		end
		else
		begin
			Set @Msg='Se generaran etiquetas para este producto.'
			return
		end
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

ALTER   PROCEDURE [dbo].[CONTROL_PICKING_STATUS]
	@PALLET 	AS NUMERIC(20,0),
	@USUARIO 	AS VARCHAR(30),
	@STATUS		AS CHAR(1)
AS
BEGIN

	UPDATE 	PICKING 
	SET 	PALLET_CONTROLADO=@STATUS,
			USUARIO_CONTROL_PICK=LTRIM(RTRIM(UPPER(@USUARIO))),
			FECHA_CONTROL_PALLET=GETDATE(),
			TERMINAL_CONTROL_PALLET=HOST_NAME()
	WHERE 	PALLET_PICKING=@PALLET


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

ALTER        PROCEDURE [dbo].[CONTROL_PICKING_PALLET]
	@PALLET_PIC AS NUMERIC(20,0),
	@USUARIO AS VARCHAR(30)
AS
BEGIN
	SELECT 	
			PRODUCTO_ID AS PRODUCTO_ID,DESCRIPCION AS DESCRIPCION, cast(SUM(CANT_CONFIRMADA) as int) AS CANTIDAD
	FROM 	PICKING
	WHERE	PALLET_PICKING=@PALLET_PIC AND FECHA_INICIO IS NOT NULL AND
			FECHA_FIN IS NOT NULL AND USUARIO IS NOT NULL AND
			PALLET_PICKING IS NOT NULL AND CANT_CONFIRMADA>0
	GROUP BY PRODUCTO_ID, DESCRIPCION
	
	--NO COMENTAR SE LEVANTA EN OTRO TABLE PARA SU USO POSTERIOR.
	SELECT 	CAST(SUM(CANT_CONFIRMADA) AS INT)
	FROM 	PICKING 
	WHERE 	PALLET_PICKING=@PALLET_PIC

	EXEC DBO.CONTROL_PICKING_STATUS @PALLET=@PALLET_PIC,@USUARIO=@USUARIO,@STATUS='1'
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
CREATE TABLE DBO.CONTROL_APF(
	EAN				VARCHAR(50),
	SUCURSAL		VARCHAR(20),
	CLIENTE_ID		VARCHAR(15),
	PRODUCTO_ID		VARCHAR(30),
	PALLET_INF		VARCHAR(20),
	OBS				VARCHAR(100)
)
*/
ALTER PROCEDURE [dbo].[CONTROL_PROC_APF]
		@EAN		VARCHAR(50),
		@SUCURSAL	VARCHAR(20),
		@PALLET_INF	VARCHAR(20)
AS
BEGIN
	DECLARE @PRODUCTO_ID	VARCHAR(30)
	DECLARE @CLIENTE_ID		VARCHAR(15)

	SET @CLIENTE_ID='LEADER PRICE'
	
	SELECT	@PRODUCTO_ID=PRODUCTO_ID
	FROM	RL_PRODUCTO_CODIGOS
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND CODIGO=LTRIM(RTRIM(UPPER(@EAN)))

	IF (@PRODUCTO_ID IS NULL)
	BEGIN
		INSERT INTO CONTROL_APF VALUES(@EAN, @SUCURSAL, @CLIENTE_ID, @PRODUCTO_ID, @PALLET_INF,'NO SE ENCONTRO EL PRODUCTO PARA EL EAN INFORMADO.')
	END

	IF (@PRODUCTO_ID IS NOT NULL)
	BEGIN
		INSERT INTO CONTROL_APF VALUES(@EAN, @SUCURSAL, @CLIENTE_ID, @PRODUCTO_ID, @PALLET_INF,NULL)
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

ALTER  PROCEDURE [dbo].[CONTROL_VIAJE]
	@VIAJE_ID 	AS VARCHAR(50),
	@VALUE		AS CHAR(1)
AS
BEGIN
	DECLARE @VAR VARCHAR(1)
	SET @VAR='A'
--	UPDATE PICKING SET ST_CAMION=@VALUE
--	WHERE	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))

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

--	begin transaction

-- egr_aceptar 879
/*
Commit
*/
-- rollback


ALTER    procedure [dbo].[CorrerTodo]
@DocTransId	as numeric(20,0)
As
Begin
	
	Declare @Status	as varchar(3)	
	Declare @Fi		as datetime

	Set @Fi=getdate()

	Select	@Status=Status
	from	documento_transaccion
	where	doc_trans_id=@DocTransId


	while @Status <>'T40'
	Begin

		Exec Egr_Aceptar @DocTransId

		Select	@Status=Status
		from	documento_transaccion
		where	doc_trans_id=@DocTransId
	End

	select datediff(ms,@fi,getdate())

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

ALTER PROCEDURE [dbo].[CREATE_CHILD]
	@VIAJE_ID	AS VARCHAR(100)output
AS
BEGIN
	DECLARE @TIPO_OPERACION VARCHAR(5)
	DECLARE @CANT			AS INT
	DECLARE @TCUR			CURSOR
	DECLARE @VIAJEID		VARCHAR(100)
	DECLARE @PRODUCTO_ID	VARCHAR(30)
	DECLARE @POSICION_COD	VARCHAR(50)
	DECLARE @PALLET			VARCHAR(100)
	DECLARE @RUTA			VARCHAR(100)
	DECLARE @ID				NUMERIC(20,0)	

	SET @TCUR= CURSOR FOR
		SELECT 	SP.VIAJE_ID, SP.PRODUCTO_ID, SP.POSICION_COD, PROP1, RUTA, 
				DBO.GETPICKINGID(SP.VIAJE_ID, SP.PRODUCTO_ID, SP.POSICION_COD, PROP1, RUTA)
		FROM 	PICKING SP
				INNER JOIN PRIORIDAD_VIAJE SPV
				ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
				INNER JOIN PRODUCTO PROD
				ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
				LEFT JOIN POSICION POS ON(SP.POSICION_COD=POS.POSICION_COD)
		WHERE 	SPV.PRIORIDAD = ( SELECT 	MIN(PRIORIDAD) FROM	PRIORIDAD_VIAJE	WHERE	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))								
				AND SP.VIAJE_ID=@VIAJE_ID
				AND	SP.FECHA_INICIO IS NULL
				AND	SP.FECHA_FIN IS NULL			
				AND	SP.USUARIO IS NULL
				AND	SP.CANT_CONFIRMADA IS NULL 
				AND SP.FIN_PICKING <>'2'

		GROUP BY 	
				SP.VIAJE_ID, SP.PROP1, SP.RUTA, SP.DOCUMENTO_ID ,SP.NRO_LINEA	,SP.PRODUCTO_ID,SPV.PRIORIDAD,SP.TIPO_CAJA, POS.ORDEN_PICKING, SP.POSICION_COD
		ORDER BY	SPV.PRIORIDAD ASC,CAST(SP.TIPO_CAJA AS NUMERIC(10,1)) DESC, POS.ORDEN_PICKING, SP.POSICION_COD ASC
	OPEN @TCUR
	FETCH NEXT FROM @TCUR INTO  @VIAJEID,	@PRODUCTO_ID, @POSICION_COD, @PALLET, @RUTA, @ID
	WHILE @@FETCH_STATUS=0
	BEGIN
		EXEC DBO.ACTUALIZA_RELACION_PICKING @VIAJEID, @PRODUCTO_ID, @POSICION_COD, @PALLET, @RUTA, @ID
		FETCH NEXT FROM @TCUR INTO  @VIAJEID,	@PRODUCTO_ID, @POSICION_COD, @PALLET, @RUTA, @ID
	END
	CLOSE @TCUR
	DEALLOCATE @TCUR

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

ALTER  PROCEDURE [dbo].[DeleteAllDoc]
AS


	DECLARE @DOC_TRANS_ID AS NUMERIC(20)
	DECLARE @DOCUMENTO_ID AS NUMERIC(20)

	DECLARE CUR_DET_DOC_TRANS CURSOR FOR
	SELECT DOC_TRANS_ID, DOCUMENTO_ID
	FROM DET_DOCUMENTO_TRANSACCION
        OPEN CUR_DET_DOC_TRANS
	
	FETCH NEXT FROM CUR_DET_DOC_TRANS
	INTO @DOC_TRANS_ID, @DOCUMENTO_ID
	    WHILE @@FETCH_STATUS = 0
		BEGIN

		DELETE FROM DET_DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	
		DELETE FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM HISTORICO_PRODUCTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM HISTORICO_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
		
	   FETCH NEXT FROM CUR_DET_DOC_TRANS
	   INTO @DOC_TRANS_ID, @DOCUMENTO_ID
	END
        CLOSE CUR_DET_DOC_TRANS
        DEALLOCATE CUR_DET_DOC_TRANS
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

ALTER    PROCEDURE [dbo].[DELETEDOC]
@DOCUMENTO_ID AS NUMERIC(20)
AS

DECLARE @DOC_TRANS_ID AS NUMERIC(20)
BEGIN TRANSACTION
BEGIN
	
	SELECT @DOC_TRANS_ID= DOC_TRANS_ID 
	FROM DET_DOCUMENTO_TRANSACCION
	WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DET_DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM HISTORICO_PRODUCTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM HISTORICO_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

END 
COMMIT TRANSACTION

/*
DELETEDOC
@DOCUMENTO_ID=18
*/
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

ALTER    PROCEDURE [dbo].[DELETEDOC_EGR]
@DOCUMENTO_ID	NUMERIC(20,0)
AS
BEGIN
	SET XACT_ABORT ON
	DECLARE @DOC_TRANS_ID NUMERIC(20,0)
	
	SELECT @DOC_TRANS_ID=DOC_TRANS_ID FROM DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID_EGR=@DOC_TRANS_ID
	DELETE FROM DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	DELETE FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	DELETE FROM DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
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

ALTER Procedure [dbo].[DeleteNroLinea]
@Documento_Id	Numeric(20,0)Output,
@Nro_linea		Numeric(10,0)Output
As
Begin
	
	Delete from Det_Documento_Aux 	where Documento_id=@Documento_id and Nro_Linea=@Nro_Linea
	Delete from Consumo_Locator_Egr where Documento_id=@Documento_id and Nro_Linea=@Nro_Linea

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

ALTER  PROCEDURE [dbo].[DeletePickerMan]
@viaje_id 	varchar(100) output,
@usuario_id 	varchar(100) output
AS
BEGIN
	 DELETE RL_VIAJE_USUARIO WHERE VIAJE_ID=@viaje_id AND USUARIO_ID=@usuario_id
	 
	--Limpio las tareas tomadas por el usuario
	update picking set fecha_inicio=null,usuario=null,pallet_picking=null
	where
	   usuario=@usuario_id and fecha_fin is null and cant_confirmada is null
	   and viaje_id=@viaje_id
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

ALTER    PROCEDURE [dbo].[DESK_FIN_TRANSFERENCIA]
	@Doc_trans_Id 	NUMERIC(20,0) OUTPUT

AS
BEGIN
	DECLARE @IORDEN 			AS NUMERIC(3,0)
	DECLARE @STATION 			AS VARCHAR(15)
	DECLARE @TRANSACCION_ID 	AS VARCHAR(15)
	DECLARE @STATUS			AS VARCHAR(3)
	DECLARE @FLG_FIN			AS CHAR(1)
	DECLARE @FLG_ACT_STOCK	AS CHAR(1)
	DECLARE @NEXT_STATION 	AS VARCHAR(15)
	DECLARE @NEXT_ORDEN		AS VARCHAR(15)
	DECLARE @USUARIO 			VARCHAR(20)

	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	--OBTENGO EL ORDEN DE LA ESTACION.
	SELECT 	@IORDEN=DBO.GETORDENESTACIONFORDOCTRID(@Doc_trans_Id)

	SELECT 	@STATION=ESTACION_ACTUAL,@TRANSACCION_ID=TRANSACCION_ID,
			@STATUS=STATUS
	FROM  	DOCUMENTO_TRANSACCION
	WHERE 	DOC_TRANS_ID=@Doc_trans_Id


	SELECT 	@FLG_FIN=FIN, @FLG_ACT_STOCK=ACTUALIZA_STOCK
	FROM  	RL_TRANSACCION_ESTACION
	WHERE 	TRANSACCION_ID 	=@TRANSACCION_ID
	     	AND ESTACION_ID	=@STATION
	     	AND ORDEN		=@IORDEN

	EXEC DBO.UPDATEESTACIONACTUAL_STOCK_TRANS	@DOC_TRANS_ID=@Doc_trans_Id, @USUARIO=@USUARIO

	EXEC DBO.UPDATEESTACIONACTUAL  @DOC_TRANS_ID=@Doc_trans_Id


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

ALTER         Procedure [dbo].[Det_Documento_Api#InsertRecord]
	@P_Documento_Id numeric(20,0)
	,@P_Nro_Linea numeric(10,0)
	,@P_Cliente_Id varchar(15)
	,@P_Producto_Id varchar(30)
	,@P_Cantidad numeric(20,5)
	,@P_Nro_Serie varchar(50)
	,@P_Nro_Serie_Padre varchar(50)
	,@P_Est_Merc_Id varchar(15)
	,@P_Cat_Log_Id varchar(50)
	,@P_Nro_Bulto varchar(50)
	,@P_Descripcion varchar(200)
	,@P_Nro_Lote varchar(50)
	,@P_Fecha_Vencimiento datetime
	,@P_Nro_Despacho varchar(50)
	,@P_Nro_Partida varchar(50)
	,@P_Unidad_Id varchar(5)
	,@P_Peso numeric(20,5)
	,@P_Unidad_Peso varchar(5)
	,@P_Volumen numeric(20,5)
	,@P_Unidad_Volumen varchar(5)
	,@P_Busc_Individual varchar(1)
	,@P_Tie_In varchar(1)
	,@P_Nro_Tie_In_Padre varchar(100)
	,@P_Nro_Tie_In varchar(100)
	,@P_Item_Ok varchar(1)
	,@P_Moneda_Id varchar(20)
	,@P_Costo numeric(10,3)
	,@P_Cat_Log_Id_Final varchar(50)
	,@P_Prop1 varchar(100)
	,@P_Prop2 varchar(100)
	,@P_Prop3 varchar(100)
	,@P_Largo numeric(10,3)
	,@P_Alto numeric(10,3)
	,@P_Ancho numeric(10,3)
	,@P_Volumen_Unitario varchar(1)
	,@P_Peso_Unitario varchar(1)
	,@P_Cant_Solicitada numeric(20,5)
As
Begin
	
	Declare @VTipoDoc varchar(5)
	Declare @vCATLOG varchar(15)
	Declare @vlote numeric(1,0)
	Declare @vpallet numeric(1,0)
	Declare @Secuencia varchar(30)


	Select @P_NRO_LINEA = isnull(Max(nro_linea),0) + 1
	From det_documento
	Where documento_id = @P_DOCUMENTO_ID

	IF (@P_NRO_LINEA IS NULL) SET @P_NRO_LINEA = 1

	Select @VTipoDoc = TIPO_OPERACION_ID 
	From DOCUMENTO 
	Where DOCUMENTO_ID = @P_DOCUMENTO_ID

	If @VTipoDoc is not null
		Begin
			If @VTipoDoc = 'ING'
				Begin
					Select @vlote = lote_automatico  , @vpallet = pallet_automatico 
					From producto 
					Where cliente_id = Upper(LTrim(RTrim(@P_Cliente_Id)))
					AND producto_id = Upper(LTrim(RTrim(@P_Producto_Id)))
					
					If (@vlote = 1) And ((@P_Nro_Lote is null) Or (@P_Nro_Lote = ''))
						Begin
							Set @Secuencia = 'NROLOTE_SEQ'
							exec dbo.GET_VALUE_FOR_SEQUENCE @Secuencia, @P_Nro_Lote	
							--Set @P_Nro_Lote = dbo.GET_VALUE_FOR_SEQUENCE(@Secuencia)	
						End
					If (@vpallet = 1) And ((@P_Prop1 is null) Or (@P_Prop1 = ''))
						Begin
							Set @Secuencia = 'NROPALLET_SEQ'
							exec dbo.GET_VALUE_FOR_SEQUENCE @Secuencia, @P_Prop1
							--Set @P_Prop1 = dbo.GET_VALUE_FOR_SEQUENCE(@Secuencia)		
						End
					
					if @P_Nro_Partida is null 
						begin
							Set @Secuencia = 'NRO_PARTIDA'
							Exec dbo.GET_VALUE_FOR_SEQUENCE @Secuencia, @P_Nro_Partida
						end
	               
					Select @vCatLog = ISNULL(RL.cat_log_id, P.ING_CAT_LOG_ID) 
					From PRODUCTO P 
						Inner JOIN RL_PRODUCTO_CATLOG RL
						On (P.PRODUCTO_ID=RL.PRODUCTO_ID AND P.CLIENTE_ID=RL.CLIENTE_ID),
						DOCUMENTO D
					Where RL.tipo_comprobante_id = D.tipo_comprobante_id
					AND D.DOCUMENTO_ID= @P_Documento_Id
					AND RL.PRODUCTO_ID= @P_Producto_Id 
					AND RL.CLIENTE_ID= @P_Cliente_Id 
	                
	                If @vCatLog is not null
						Begin
							Set @P_Cat_Log_Id_Final = @vCatLog
						End
				End

		Insert Into DET_DOCUMENTO ( 
							DOCUMENTO_ID
							, NRO_LINEA
							, CLIENTE_ID
							, PRODUCTO_ID
							, CANTIDAD
							, NRO_SERIE
							, NRO_SERIE_PADRE
							, EST_MERC_ID
							, CAT_LOG_ID
							, NRO_BULTO
							, DESCRIPCION
							, NRO_LOTE
							, FECHA_VENCIMIENTO
							, NRO_DESPACHO
							, NRO_PARTIDA
							, UNIDAD_ID
							, PESO
							, UNIDAD_PESO
							, VOLUMEN
							, UNIDAD_VOLUMEN
							, BUSC_INDIVIDUAL
							, TIE_IN
							, NRO_TIE_IN_PADRE
							, NRO_TIE_IN
							, ITEM_OK
							, CAT_LOG_ID_FINAL
							, MONEDA_ID
							, COSTO
							, PROP1
							, PROP2
							, PROP3
							, LARGO
							, ALTO
							, ANCHO
							, VOLUMEN_UNITARIO
							, PESO_UNITARIO
							, CANT_SOLICITADA
							, TRACE_BACK_ORDER
						   ) 
		Values (
							Upper(@P_Documento_Id) 
							, Cast(@P_Nro_Linea as varchar(10)) 
							, Upper(@P_Cliente_Id) 
							, Upper(@P_Producto_Id) 
							, Cast(@P_Cantidad as varchar(25)) 
							, Upper(@P_Nro_Serie) 
							, Upper(@P_Nro_Serie_Padre) 
							, Upper(@P_Est_Merc_Id) 
							, Upper(@P_Cat_Log_Id) 
							, Upper(@P_Nro_Bulto) 
							, ISNULL(Upper(@P_Descripcion),dbo.get_descripcion(@p_cliente_id,@p_Producto_id))
							, Upper(@P_Nro_Lote) 
							, Convert(Varchar, @P_Fecha_Vencimiento ,101) 
							, Upper(@P_Nro_Despacho) 
							, Upper(@P_Nro_Partida) 
							, isnull(Upper(@P_Unidad_Id),dbo.get_Unidad_Id(@p_Cliente_Id,@P_Producto_Id))
							, Cast(@P_Peso as varchar(25)) 
							, Upper(@P_Unidad_Peso) 
							, Cast(@P_Volumen as varchar(25)) 
							, Upper(@P_Unidad_Volumen) 
							, Upper(@P_Busc_Individual) 
							, Upper(@P_Tie_In) 
							, Upper(@P_Nro_Tie_In_Padre) 
							, Upper(@P_Nro_Tie_In) 
							, Upper(@P_Item_Ok) 
							, Upper(@P_Cat_Log_Id_Final) 
							, Upper(@P_Moneda_Id) 
							, Cast(@P_Costo as varchar(13)) 
							, Upper(@P_Prop1) 
							, Upper(@P_Prop2) 
							, Upper(@P_Prop3) 
							, Cast(@P_Largo as varchar(13)) 
							, Cast(@P_Alto as varchar(13)) 
							, Cast(@P_Ancho as varchar(13)) 
							, Upper(@P_Volumen_Unitario) 
							, Upper(@P_Peso_Unitario) 
							, Cast(@P_Cant_Solicitada as varchar(13)) 
							, Null
				)

		Update Documento 
		Set status = 'D10' 
		Where DOCUMENTO_ID = @P_DOCUMENTO_ID
	
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

ALTER   Procedure [dbo].[Det_Documento_Transaccion_Api#InsertRecord]
						@P_Doc_Trans_Id numeric(20,0)
						, @P_Nro_Linea_Trans numeric(10,0)
						, @P_Documento_Id  numeric(20,0)
						, @P_Nro_Linea_Doc numeric(10,0)
						, @P_Motivo_Id varchar(15)
						, @P_Est_Merc_Id varchar(15)
						, @P_Cliente_Id varchar(15)
						, @P_Cat_Log_Id varchar(50)
						, @P_Item_Ok varchar(1)
						, @P_Movimiento_Pendiente varchar(1)
As
Begin

	Insert Into DET_DOCUMENTO_TRANSACCION (
							DOC_TRANS_ID,
							NRO_LINEA_TRANS,
							DOCUMENTO_ID,
							NRO_LINEA_DOC,
							MOTIVO_ID,
							EST_MERC_ID,
							CLIENTE_ID,
							CAT_LOG_ID,
							ITEM_OK,
							MOVIMIENTO_PENDIENTE,
							DOC_TRANS_ID_REF,
							NRO_LINEA_TRANS_REF
							)
	Values (
							Upper(LTrim(RTrim(@P_Doc_Trans_Id))), 
							Upper(LTrim(RTrim(@P_Nro_Linea_Trans))),
							Upper(LTrim(RTrim(@P_Documento_Id))), 
							Upper(LTrim(RTrim(@P_Nro_Linea_Doc))), 
							Upper(LTrim(RTrim(@P_Motivo_Id))),  
							Upper(LTrim(RTrim(@P_Est_Merc_Id))), 
							Upper(LTrim(RTrim(@P_Cliente_Id))), 
							Upper(LTrim(RTrim(@P_Cat_Log_Id))), 
							Upper(LTrim(RTrim(@P_Item_Ok))),
							Upper(LTrim(RTrim(@P_Movimiento_Pendiente))),
							NULL,
							NULL
							)

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

ALTER  PROCEDURE [dbo].[DET_EGR_AUTOCOMPLETA_SEXISTENCIA]
@DOCUMENTO_ID	AS NUMERIC(20,0)
AS
BEGIN
	Declare @NRO_LINEA 			numeric  (10, 0)
	Declare @CLIENTE_ID 		varchar  (15)
	Declare @PRODUCTO_ID 		varchar  (30)
	Declare @CANTIDAD 			numeric  (20, 5)
	Declare @NRO_SERIE 			varchar  (50)
	Declare @NRO_SERIE_PADRE 	varchar  (50)
	Declare @EST_MERC_ID 		varchar  (15)
	Declare @CAT_LOG_ID 		varchar  (50)
	Declare @NRO_BULTO 			varchar  (50)
	Declare @DESCRIPCION 		varchar  (200)
	Declare @NRO_LOTE 			varchar  (50)
	Declare @FECHA_VENCIMIENTO 	datetime   
	Declare @NRO_DESPACHO 		varchar  (50)
	Declare @NRO_PARTIDA 		varchar  (50)
	Declare @UNIDAD_ID 			varchar  (5)
	Declare @PESO 				numeric  (20, 5)
	Declare @UNIDAD_PESO 		varchar  (5)
	Declare @VOLUMEN 			numeric  (20, 5)
	Declare @UNIDAD_VOLUMEN 	varchar  (5)
	Declare @BUSC_INDIVIDUAL 	varchar  (1)
	Declare @TIE_IN 			varchar  (1)
	Declare @NRO_TIE_IN_PADRE 	varchar  (100)
	Declare @NRO_TIE_IN 		varchar  (100)
	Declare @ITEM_OK 			varchar  (1)
	Declare @CAT_LOG_ID_FINAL 	varchar  (50)
	Declare @MONEDA_ID 			varchar  (20)
	Declare @COSTO 				numeric  (10, 3)
	Declare @PROP1 				varchar  (100)
	Declare @PROP2 				varchar  (100)
	Declare @PROP3 				varchar  (100)
	Declare @LARGO 				numeric  (10, 3)
	Declare @ALTO 				numeric  (10, 3)
	Declare @ANCHO 				numeric  (10, 3)
	Declare @VOLUMEN_UNITARIO 	varchar  (1)
	Declare @PESO_UNITARIO 		varchar  (1)
	Declare @CANT_SOLICITADA 	numeric  (20, 5)
	Declare @TRACE_BACK_ORDER 	varchar  (1)
	Declare @xCursor			Cursor
	
	Set @xCursor= Cursor for
	SELECT 	 DOCUMENTO_ID
			,CLIENTE_ID
			,PRODUCTO_ID
			,CANTIDAD
			,NRO_SERIE
			,NRO_SERIE_PADRE
			,EST_MERC_ID
			,CAT_LOG_ID
			,NRO_BULTO
			,DESCRIPCION
			,NRO_LOTE
			,FECHA_VENCIMIENTO
			,NRO_DESPACHO
			,NRO_PARTIDA
			,UNIDAD_ID
			,PESO
			,UNIDAD_PESO
			,VOLUMEN
			,UNIDAD_VOLUMEN
			,BUSC_INDIVIDUAL
			,TIE_IN
			,NRO_TIE_IN_PADRE
			,NRO_TIE_IN
			,ITEM_OK
			,CAT_LOG_ID_FINAL
			,MONEDA_ID
			,COSTO
			,PROP1
			,PROP2
			,PROP3
			,LARGO
			,ALTO
			,ANCHO
			,VOLUMEN_UNITARIO
			,PESO_UNITARIO
			,CANT_SOLICITADA
			,TRACE_BACK_ORDER
	FROM 	DET_DOCUMENTO 
	WHERE	DOCUMENTO_ID=(@DOCUMENTO_ID)
			AND PRODUCTO_ID NOT IN(
			SELECT
					dd.producto_id as producto
			FROM 	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
					inner join posicion p on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
			WHERE
					rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
					and dd.producto_id in (	select 	producto_id 
											from 	det_documento 
											where 	documento_id=@DOCUMENTO_ID
										)
			UNION 
			SELECT
					dd.producto_id as producto
			FROM 	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
					inner join nave n on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
			WHERE
					rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR'
					and dd.producto_id in (	select 	producto_id 
											from 	det_documento 
											where 	documento_id=@DOCUMENTO_ID
										)
			)
	Open @xCursor

	Fetch Next from @xCursor into 	@DOCUMENTO_ID,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD,@NRO_SERIE,@NRO_SERIE_PADRE,
									@EST_MERC_ID,@CAT_LOG_ID,@NRO_BULTO,@DESCRIPCION,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_DESPACHO,
									@NRO_PARTIDA,@UNIDAD_ID,@PESO,@UNIDAD_PESO,@VOLUMEN,@UNIDAD_VOLUMEN,@BUSC_INDIVIDUAL,@TIE_IN,
									@NRO_TIE_IN_PADRE,@NRO_TIE_IN,@ITEM_OK,@CAT_LOG_ID_FINAL,@MONEDA_ID,@COSTO,@PROP1,@PROP2,@PROP3,
									@LARGO,@ALTO,@ANCHO,@VOLUMEN_UNITARIO,@PESO_UNITARIO,@CANT_SOLICITADA,@TRACE_BACK_ORDER
	While @@Fetch_Status=0
	Begin
		Select @Nro_Linea=Max(Isnull(Nro_Linea,0))+1 From Det_Documento_Aux Where Documento_id=@Documento_id
		
		Insert Into Det_Documento_Aux 
							     Values(@DOCUMENTO_ID,@Nro_Linea,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD,@NRO_SERIE,@NRO_SERIE_PADRE,
										@EST_MERC_ID,@CAT_LOG_ID,@NRO_BULTO,@DESCRIPCION,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_DESPACHO,
										@NRO_PARTIDA,@UNIDAD_ID,@PESO,@UNIDAD_PESO,@VOLUMEN,@UNIDAD_VOLUMEN,@BUSC_INDIVIDUAL,@TIE_IN,
										@NRO_TIE_IN_PADRE,@NRO_TIE_IN,@ITEM_OK,@CAT_LOG_ID_FINAL,@MONEDA_ID,@COSTO,@PROP1,@PROP2,@PROP3,
										@LARGO,@ALTO,@ANCHO,@VOLUMEN_UNITARIO,@PESO_UNITARIO,@CANT_SOLICITADA,@TRACE_BACK_ORDER
				)

		Fetch Next from @xCursor into 	@DOCUMENTO_ID,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD,@NRO_SERIE,@NRO_SERIE_PADRE,
										@EST_MERC_ID,@CAT_LOG_ID,@NRO_BULTO,@DESCRIPCION,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_DESPACHO,
										@NRO_PARTIDA,@UNIDAD_ID,@PESO,@UNIDAD_PESO,@VOLUMEN,@UNIDAD_VOLUMEN,@BUSC_INDIVIDUAL,@TIE_IN,
										@NRO_TIE_IN_PADRE,@NRO_TIE_IN,@ITEM_OK,@CAT_LOG_ID_FINAL,@MONEDA_ID,@COSTO,@PROP1,@PROP2,@PROP3,
										@LARGO,@ALTO,@ANCHO,@VOLUMEN_UNITARIO,@PESO_UNITARIO,@CANT_SOLICITADA,@TRACE_BACK_ORDER

	End
	Close @xCursor
	Deallocate @xCursor
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

/*#14082008#*/ 
ALTER       PROCEDURE [dbo].[Det_Egr_IngresaSysInt]
@Documento_id Numeric(20,0)
As
Begin
	Declare @Doc_Ext 	as varchar(100)
	Declare @Control 	as Float
	Declare @StrInicial	as varchar(10)



	SELECT @StrInicial= Tipo_Comprobante_Id + '_'  From Documento Where Documento_id=@Documento_id

	SELECT @Doc_Ext=NRO_REMITO FROM DOCUMENTO WHERE DOCUMENTO_ID=@Documento_id
	
	SELECT @Control=count(*) FROM SYS_INT_DOCUMENTO WHERE DOC_EXT=@Doc_Ext
	
	if @Control>0
	begin
		Update sys_int_det_documento set Estado_gt='P', fecha_estado_gt=getdate(), documento_id=@Documento_Id where Doc_Ext=@Doc_Ext
		Update sys_int_documento set Estado_gt='P', fecha_estado_gt=getdate() where Doc_Ext=@Doc_Ext
	
		Return
	end

	--MANDO LA CABECERA.
	INSERT INTO SYS_INT_DOCUMENTO(
		CLIENTE_ID, TIPO_DOCUMENTO_ID, CPTE_PREFIJO, CPTE_NUMERO, FECHA_CPTE, FECHA_SOLICITUD_CPTE, AGENTE_ID,
		PESO_TOTAL, UNIDAD_PESO, VOLUMEN_TOTAL, UNIDAD_VOLUMEN, TOTAL_BULTOS, ORDEN_DE_COMPRA, OBSERVACIONES,
		NRO_REMITO, NRO_DESPACHO_IMPORTACION, DOC_EXT, CODIGO_VIAJE,INFO_ADICIONAL_1, INFO_ADICIONAL_2,
		INFO_ADICIONAL_3, TIPO_COMPROBANTE, ESTADO, FECHA_ESTADO, ESTADO_GT, FECHA_ESTADO_GT)
	SELECT 
			 CLIENTE_ID				
			,TIPO_COMPROBANTE_ID	
			,CPTE_PREFIJO
			,CPTE_NUMERO
			,FECHA_CPTE
			,FECHA_PEDIDA_ENT
			,SUCURSAL_DESTINO
			,PESO_TOTAL
			,UNIDAD_PESO
			,VOLUMEN_TOTAL
			,UNIDAD_VOLUMEN
			,TOTAL_BULTOS
			,ORDEN_DE_COMPRA
			,OBSERVACIONES
			,NRO_REMITO
			,NULL
			,@StrInicial + CAST(DOCUMENTO_ID AS VARCHAR)
			,NRO_DESPACHO_IMPORTACION
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,'P'
			,GETDATE()
	FROM 	DOCUMENTO
	WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
	
	UPDATE DOCUMENTO SET NRO_REMITO=@STRINICIAL + CAST(@DOCUMENTO_ID AS VARCHAR) WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	--MANDO EL DETALLE
	INSERT INTO SYS_INT_DET_DOCUMENTO(
		DOC_EXT, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, CANTIDAD, EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,
		NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO, NRO_PARTIDA, UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO,
		VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, DOC_BACK_ORDER, ESTADO, FECHA_ESTADO, ESTADO_GT, 
		FECHA_ESTADO_GT, DOCUMENTO_ID, NAVE_ID, NAVE_COD)
	SELECT	 @StrInicial + CAST(DOCUMENTO_ID AS VARCHAR)
			,NRO_LINEA
			,CLIENTE_ID
			,PRODUCTO_ID
			,CANTIDAD
			,NULL
			,NULL
			,NULL
			,NULL
			,DESCRIPCION
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,UNIDAD_ID
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,'P'
			,GETDATE()
			,DOCUMENTO_ID
			,NULL
			,NULL
	FROM	DET_DOCUMENTO
	WHERE	DOCUMENTO_ID=@DOCUMENTO_ID


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

ALTER     PROCEDURE [dbo].[DET_EGR_INSERT_CONSUMO_LOCATOR_EGR]
		@vDOC_ID			AS NUMERIC(20,0)	OUTPUT,
		@NRO_LINEA		AS NUMERIC(20,0)	OUTPUT,
		@vCLIENTE_ID 		AS VARCHAR(30)	OUTPUT,
		@vPRODUCTO_ID	AS VARCHAR(30)	OUTPUT,
		@vCANTIDAD		AS NUMERIC(20,5)	OUTPUT,
		@vRL_ID 			AS NUMERIC(20,0)	OUTPUT,
		@vSALDO			AS NUMERIC(20,5)	OUTPUT,
		@vTIPO				AS VARCHAR(20)	OUTPUT,
		@vPROCESADO		AS VARCHAR(1)		OUTPUT
AS
BEGIN
DECLARE @Qty_SALDO AS NUMERIC(20,5)

	IF (@vRL_ID IS NULL) OR (LTRIM(RTRIM(@vRL_ID))='') OR (@vRL_ID=0)
	BEGIN
		RAISERROR('El valor Rl no es valido',16,1)
		return
	END

	DELETE FROM CONSUMO_LOCATOR_EGR WHERE DOCUMENTO_ID = @vDOC_ID AND NRO_LINEA = @NRO_LINEA;
	DELETE FROM DET_DOCUMENTO_AUX WHERE DOCUMENTO_ID = @vDOC_ID AND NRO_LINEA = @NRO_LINEA;
	
	--Exec Dbo.Get_Qty_Stock @vCLIENTE_ID, @vPRODUCTO_ID, @Qty_SALDO Output
	
	SELECT @Qty_SALDO=CANTIDAD - @vCANTIDAD FROM RL_DET_DOC_TRANS_POSICION WHERE RL_ID=@vRL_ID

	INSERT INTO CONSUMO_LOCATOR_EGR (DOCUMENTO_ID,NRO_LINEA,CLIENTE_ID,PRODUCTO_ID, CANTIDAD, RL_ID, SALDO, TIPO, FECHA, PROCESADO)
	VALUES (@vDOC_ID, @NRO_LINEA, @vCLIENTE_ID, @vPRODUCTO_ID, @vCANTIDAD, @vRL_ID, @Qty_SALDO, @vTIPO, GETDATE(), @vPROCESADO)

	INSERT INTO DET_DOCUMENTO_AUX
	SELECT * FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@VDOC_ID AND NRO_LINEA=@NRO_LINEA
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

ALTER     PROCEDURE [dbo].[DEVO_REGISTRA_TEMP]
@VIAJE_ID 	AS VARCHAR(100) output,
@Pedido 	as varchar(100) output
AS 
Begin
	/*
	CREATE TABLE #FRONTERA_ING_EGR(
	DOCUMENTO_ID	NUMERIC(20,0),
	NRO_LINEA		NUMERIC(10,0))
	*/
	--Consigo que se cargue en la temporal los valores persistidos.
	INSERT INTO #FRONTERA_ING_EGR
	SELECT 	DISTINCT
			DD.DOCUMENTO_ID,
			DD.NRO_LINEA
	FROM	FRONTERA_ING_EGR FI (nolock)
			INNER JOIN vDET_DOCUMENTO DD (nolock)
			ON(FI.DOCUMENTO_ID=DD.DOCUMENTO_ID AND FI.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN vDOCUMENTO D (nolock)
			ON(FI.DOCUMENTO_ID=D.DOCUMENTO_ID)
	WHERE	D.NRO_DESPACHO_IMPORTACION=@VIAJE_ID
			AND D.TIPO_OPERACION_ID='EGR'
			AND ((@PEDIDO IS NULL) OR (D.NRO_REMITO LIKE '%' + @PEDIDO + '%'))
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

ALTER     Procedure [dbo].[Documento_Api#InsertRecord]
 	@P_Documento_Id numeric(20,0) OUTPUT
 	,@P_Cliente_Id varchar(15)
 	,@P_Tipo_Comprobante_Id varchar(5) 
 	,@P_Tipo_Operacion_Id varchar(5)
	,@P_Det_Tipo_Operacion_Id varchar(5)
	,@P_Cpte_Prefijo varchar(6)
	,@P_Cpte_Numero varchar(20)
	,@P_Fecha_Cpte varchar(20)
	,@P_Fecha_Pedida_Ent varchar(20)
	,@P_Sucursal_Origen varchar(20)
	,@P_Sucursal_Destino varchar(20)
	,@P_Anulado varchar(1)
	,@P_Motivo_Anulacion varchar(15)
	,@P_Peso_Total numeric(20,5)
	,@P_Unidad_Peso varchar(5)
	,@P_Volumen_Total numeric(20,5)
	,@P_Unidad_Volumen varchar(5)
	,@P_Total_Bultos numeric(10,0)
	,@P_Valor_Declarado numeric(12,2)
	,@P_Orden_De_Compra varchar(20)
	,@P_Cant_Items numeric(10,0)
	,@P_Observaciones varchar(200)
	,@P_Status varchar(3)
	,@P_NroRemito varchar(30)
	,@P_Fecha_Alta_Gtw varchar(20)
	,@P_Fecha_Fin_Gtw varchar(20)
	,@P_Personal_Id varchar(20)
	,@P_Transporte_Id varchar(20)
	,@P_Nro_Despacho_Importacion varchar(30)
	,@P_Alto numeric(20,5)
	,@P_Ancho numeric(20,5)
	,@P_Largo numeric(20,5)
	,@P_Unidad_Medida varchar(5)
	,@P_Grupo_Picking varchar(50)
	,@P_Prioridad_Picking numeric(10,0)

As
Begin
	
	Declare @StrSql nvarchar(4000)
	Declare @V_Volumen_Total numeric(20,5)
	Declare @New_Status varchar(3)

	If (((@P_Alto * @P_Ancho * @P_Largo) / 1000000) is NULL)
		Begin
			Set @V_Volumen_Total = 0	
		End
	Else
		Begin
			Set @V_Volumen_Total = ((@P_Alto * @P_Ancho * @P_Largo) / 1000000)
		End

	If (dbo.ent_documento_api#Ya_Existe_Nro_Comprobante(@P_CLIENTE_ID, @P_TIPO_COMPROBANTE_ID, @P_CPTE_PREFIJO, @P_CPTE_NUMERO, Null, @P_SUCURSAL_ORIGEN)) = 1
		Begin        
            Raiserror ('Validacion de documentos',16,1)
			Return        
		End

	If (dbo.ent_documento_api#Ya_Existe_Orden_de_Compra(@P_CLIENTE_ID, @P_TIPO_COMPROBANTE_ID, @P_TIPO_OPERACION_ID, @P_ORDEN_DE_COMPRA, Null)) = 1 
		Begin        
            Raiserror ('Validacion de documentos',16,1)
			Return        
		End
             
	Set @NEW_STATUS = 'D05'

	Insert into Documento ( Cliente_Id
							, Tipo_Comprobante_Id
							, Tipo_Operacion_Id
							, Det_Tipo_Operacion_Id
							, Cpte_Prefijo
							, Cpte_Numero
							, Fecha_Cpte 
							, Fecha_Pedida_Ent
							, Sucursal_Origen
							, Sucursal_Destino
							, Anulado
							, Motivo_Anulacion 
							, Peso_Total
							, Unidad_Peso
							, Volumen_Total
							, Unidad_Volumen
							, Total_Bultos
							, Valor_Declarado 
							, Orden_De_Compra
							, Cant_Items
							, Observaciones
							, Status
							, Nro_Remito
							, Fecha_Alta_Gtw
							, Fecha_Fin_Gtw 
							, Personal_Id
							, Transporte_Id
							, Nro_Despacho_Importacion
							, Alto
							, Ancho
							, Largo
							, Unidad_Medida
							, Grupo_Picking
							, Prioridad_Picking
						   ) 
	Values ( 
			  Upper(@P_Cliente_Id)
			, Upper(@P_Tipo_Comprobante_Id)
			, Upper(@P_Tipo_Operacion_Id) 
			, Upper(@P_Det_Tipo_Operacion_Id) 
			, Upper(@P_Cpte_Prefijo) 
			, Upper(@P_Cpte_Numero)
			, getdate() -- cast(@P_Fecha_Cpte as datetime)
			, Cast(@P_Fecha_Pedida_Ent as datetime)
			, Upper(@P_Sucursal_Origen) 
			, Upper(@P_Sucursal_Destino) 
			, Upper(@P_Anulado) 
			, Upper(@P_Motivo_Anulacion) 
			, Cast(@P_Peso_Total as varchar(25)) 
			, Upper(@P_Unidad_Peso) + char(39) 
			, Cast(@P_Volumen_Total as varchar(25)) 
			, Upper(@P_Unidad_Volumen) 
			, Cast(@P_Total_Bultos as varchar(10)) 
			, Cast(@P_Valor_Declarado as varchar(14)) 
			, Upper(@P_Orden_De_Compra) 
			, Cast(@P_Cant_Items as varchar(10)) 
			, Upper(@P_Observaciones) 
			, Upper(@New_Status) 
			, Upper(@P_NroRemito) 
			, getdate() --Cast(@P_Fecha_Alta_Gtw as Datetime)
			, Cast(@P_Fecha_Fin_Gtw as Datetime)
			, Upper(@P_Personal_Id) 
			, Upper(@P_Transporte_Id) 
			, Upper(@P_Nro_Despacho_Importacion) 
			, Cast(@P_Alto as varchar(25)) 
			, Cast(@P_Ancho as varchar(25)) 
			, Cast(@P_Largo as varchar(25)) 
			, Upper(@P_Unidad_Medida) 
			, Upper(@P_Grupo_Picking) 
			, Cast(@P_Prioridad_Picking as varchar(10)) 
		   )

	Select @P_Documento_Id = Scope_Identity()

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

ALTER  Procedure [dbo].[Documento_Transaccion_Api#InsertRecord]
						@P_Completado as varchar(1)
						, @P_Observaciones as varchar(200)
						, @P_Transaccion_Id as varchar(15)
						, @P_Estacion_Actual as varchar(15)
						, @P_Status as varchar(3)
						, @P_Est_Mov_Actual as varchar(20)
						, @P_Orden_Id as numeric(20,0)
						, @P_It_Mover as varchar(1)
						, @P_Orden_Estacion as numeric(3,0)
						, @P_Tipo_Operacion_Id as varchar(5)
						, @P_Tr_Pos_Completa as varchar(1)
						, @P_Tr_Activo as varchar(1)
						, @P_Usuario_Id as varchar(20)
                        , @P_Terminal as varchar(20)
						, @P_Fecha_Alta_Gtw as datetime
                        , @P_Tr_Activo_Id as varchar(10)
						, @P_Session_Id as varchar(60)
                        , @P_Fecha_Cambio_Tr as datetime
						, @P_Fecha_Fin_Gtw as datetime
						, @P_Doc_Trans_Id as numeric(20,0) OUTPUT

As
Begin

	Declare @Usuario_Id varchar(30)
	Declare @Terminal varchar(30)

	Select @Usuario_Id = usuario_id, @Terminal = terminal From #temp_usuario_loggin
	
	Insert Into DOCUMENTO_TRANSACCION (
		COMPLETADO,
		OBSERVACIONES,
		TRANSACCION_ID,
		ESTACION_ACTUAL,
		STATUS,
		EST_MOV_ACTUAL,
		IT_MOVER,
		ORDEN_ESTACION,
		TIPO_OPERACION_ID,
		TR_POS_COMPLETA,
		TR_ACTIVO,
		USUARIO_ID,
		TERMINAL,
		FECHA_ALTA_GTW,
		TR_ACTIVO_ID,
		SESSION_ID,
		FECHA_CAMBIO_TR,
		FECHA_FIN_GTW
		)
	Values (
		Upper(LTrim(RTrim(@P_Completado)))
		,Upper(LTrim(RTrim(@P_Observaciones)))
		,Upper(LTrim(RTrim(@P_Transaccion_Id)))
		,Upper(LTrim(RTrim(@P_Estacion_Actual)))
		,Upper(LTrim(RTrim(@P_Status)))
		,Upper(LTrim(RTrim(@P_Est_Mov_Actual)))
		,Upper(LTrim(RTrim(@P_It_Mover)))
		,Upper(LTrim(RTrim(@P_Orden_Estacion)))
		,Upper(LTrim(RTrim(@P_Tipo_Operacion_Id)))
		,Upper(LTrim(RTrim(@P_Tr_Pos_Completa)))
		,Upper(LTrim(RTrim(@P_Tr_Activo)))
		,Upper(LTrim(RTrim(@Usuario_Id)))
		,Upper(LTrim(RTrim(@Terminal)))
		,GetDate()
		,Null
		,Null
		,Null
		,Null
	)
	
	SELECT @P_Doc_Trans_Id = SCOPE_IDENTITY()
 
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

ALTER PROCEDURE [dbo].[EGR_MATCH_COD]
	@PRODUCTO_ID 	AS VARCHAR(30),
	@CODE			AS VARCHAR(50),
	@VALIDO 		AS SMALLINT OUTPUT
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	DECLARE @DUN14 		VARCHAR(50)
	DECLARE @EAN13 		VARCHAR(50)
	DECLARE @USUARIO		VARCHAR(50)
	DECLARE @CLIENTE_ID	VARCHAR(15)
	DECLARE @CONTADOR	FLOAT

	SET @VALIDO='0'

	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	SELECT DISTINCT @CLIENTE_ID= CLIENTE_ID FROM PICKING WHERE PRODUCTO_ID=UPPER(LTRIM(RTRIM(@PRODUCTO_ID))) AND USUARIO=UPPER(LTRIM(RTRIM(@USUARIO))) AND FECHA_INICIO IS NOT NULL AND FECHA_FIN IS NULL AND CANT_CONFIRMADA IS NULL
	
	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID

	IF @CONTADOR=0
	BEGIN
		RAISERROR ('El producto tiene marcado validación al egreso, pero no se definieron códigos EAN13/DUN14. Por favor, verifique el maestro de productos',16,1)
		RETURN
	END

	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID
			AND CODIGO=@CODE


	IF @CONTADOR=0
	BEGIN
		RAISERROR('El codigo ingresado no se corresponde con los cargados en el maestro de productos.',16,1)
	END
	ELSE
	BEGIN
		SET @VALIDO='1'
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

ALTER Procedure [dbo].[EliminacionUsuario]
@Usuario_id	varchar(20) Output
As
Begin
	
	delete from sys_lock_pallet where usuario_id=@Usuario_id
	delete from rl_sys_cliente_usuario where usuario_id=@Usuario_id
	delete from rl_viaje_usuario where usuario_id=@Usuario_id
	delete from sys_usu_permisos where usuario_id=@Usuario_id
	delete from rl_usuario_nave where usuario_id=@Usuario_id
	delete from sys_permisos_hh where usuario_id=@Usuario_id
	delete from sys_perfil_usuario where usuario_id=@Usuario_id
	delete from sys_usu_permisos where usuario_id=@Usuario_id
	delete from trace_documentos where usuario_id=@Usuario_id
	delete from sys_usuario where usuario_id=@Usuario_id
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

-- =============================================
-- Author:		LRojas
-- Create date: 19/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[eliminar_caja_contenedora_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(30) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    DECLARE @PRODUCTO_ID as varchar(30),
            @CANT_CONTROLADA as numeric(20,5),
			@NRO_LOTE AS VARCHAR(100),
			@NRO_PARTIDA AS VARCHAR(100),
			@NRO_SERIE AS VARCHAR(50)
	
	DECLARE cur_eliminador CURSOR FOR
    SELECT P.PRODUCTO_ID, ISNULL(P.NRO_LOTE,''), ISNULL(P.NRO_PARTIDA,''), ISNULL(P.NRO_SERIE,''), P.CANT_CONFIRMADA
    FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
    WHERE D.CLIENTE_ID = @CLIENTE_ID AND D.NRO_REMITO = @PEDIDO_ID AND P.PALLET_PICKING = @NRO_CONTENEDORA
    AND P.PALLET_CONTROLADO='1'
    
    OPEN cur_eliminador
    FETCH cur_eliminador 
    INTO @PRODUCTO_ID, @NRO_LOTE, @NRO_PARTIDA, @NRO_SERIE, @CANT_CONTROLADA
    
    WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC quitar_producto_empaque @CLIENTE_ID, @PEDIDO_ID, @NRO_LOTE, @NRO_PARTIDA, @NRO_SERIE, @PRODUCTO_ID, @NRO_CONTENEDORA, @CANT_CONTROLADA
            
            FETCH cur_eliminador INTO @PRODUCTO_ID, @NRO_LOTE, @NRO_PARTIDA, @NRO_SERIE, @CANT_CONTROLADA
        END
    CLOSE cur_eliminador
    DEALLOCATE cur_eliminador
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

ALTER    PROCEDURE [dbo].[ENVIAR_RL_A_HISTORICO]
@P_DOC_TRANS_ID AS NUMERIC(20,0)
AS
BEGIN
	DECLARE @RL_HIST_ID 		AS NUMERIC(20,0)
	DECLARE @DOC_TRANS_ID 		AS NUMERIC(20,0)	
	DECLARE @NRO_LINEA_TR		AS NUMERIC(10,0)
	DECLARE @POS_ANTERIOR		AS NUMERIC(20,0)
	DECLARE @POS_ACTUAL			AS NUMERIC(20,0)
	DECLARE @CANTIDAD			AS NUMERIC(20,5)
	DECLARE @TIPO_MOV_ID		AS VARCHAR(5)
	DECLARE @ULTIMA_EST			AS VARCHAR(5)
	DECLARE @ULTIMA_SEC			AS NUMERIC(3,0)
	DECLARE @NAVE_ANT			AS NUMERIC(20,0)
	DECLARE @NAVE_ACT			AS NUMERIC(20,0)
	DECLARE @DOC_ID				AS NUMERIC(20,0)
	DECLARE @NRO_LINEA			AS NUMERIC(10,0)
	DECLARE @DISPONIBLE			AS VARCHAR(1)
	DECLARE @DOC_TRANS_ID_EGR	AS NUMERIC(20,0)
	DECLARE @NRO_LIN_TRANS_EGR	AS NUMERIC(10,0)
	DECLARE @DOC_TRANS_ID_TR	AS NUMERIC(20,0)
	DECLARE @NRO_LIN_TRAN_ID_TR	AS NUMERIC(10,0)
	DECLARE	@CLIENTE_ID			AS VARCHAR(15)
	DECLARE @CAT_LOG_ID			AS VARCHAR(50)	
	DECLARE @CAT_LOG_ID_FINAL 	AS VARCHAR(50)
	DECLARE	@EST_MERC_ID		AS VARCHAR(15)

	--CURSOR PARA LAS INSERCIONES.	
	DECLARE PCUR2 CURSOR FOR
		SELECT * FROM RL_DET_DOC_TRANS_POSICION
		WHERE DOC_TRANS_ID_TR = @P_DOC_TRANS_ID
	
	OPEN PCUR2

	SELECT * FROM RL_DET_DOC_TRANS_POSICION
	WHERE DOC_TRANS_ID_TR = @P_DOC_TRANS_ID

	FETCH NEXT FROM PCUR2 INTO 	  @RL_HIST_ID	, @DOC_TRANS_ID	, @NRO_LINEA_TR	, @POS_ANTERIOR	, @POS_ACTUAL
								, @CANTIDAD	, @TIPO_MOV_ID	, @ULTIMA_EST	, @ULTIMA_SEC	, @NAVE_ANT
								, @NAVE_ACT	, @DOC_ID	, @NRO_LINEA	, @DISPONIBLE	, @DOC_TRANS_ID_EGR
								, @NRO_LIN_TRANS_EGR	, @DOC_TRANS_ID_TR	, @NRO_LIN_TRAN_ID_TR	, @CLIENTE_ID	
								, @CAT_LOG_ID	, @CAT_LOG_ID_FINAL 	, @EST_MERC_ID

	WHILE @@FETCH_STATUS = 0
		BEGIN

			--SELECT @RL_HIST_ID=ISNULL(MAX(RL_ID), 0)+1 AS VALOR
			--FROM RL_DET_DOC_TR_POS_HIST
			
			INSERT INTO RL_DET_DOC_TR_POS_HIST VALUES(	  @DOC_TRANS_ID	, @NRO_LINEA_TR	, @POS_ANTERIOR	, @POS_ACTUAL
														, @CANTIDAD	, @TIPO_MOV_ID	, @ULTIMA_EST	, @ULTIMA_SEC	, @NAVE_ANT
														, @NAVE_ACT	, @DOC_ID	, @NRO_LINEA	, @DISPONIBLE	, @DOC_TRANS_ID_EGR
														, @NRO_LIN_TRANS_EGR	, @DOC_TRANS_ID_TR	, @NRO_LIN_TRAN_ID_TR	, @CLIENTE_ID	
														, @CAT_LOG_ID	, @CAT_LOG_ID_FINAL 	, @EST_MERC_ID )

	

			FETCH NEXT FROM PCUR2 INTO 	  @RL_HIST_ID	, @DOC_TRANS_ID	, @NRO_LINEA_TR	, @POS_ANTERIOR	, @POS_ACTUAL
										, @CANTIDAD	, @TIPO_MOV_ID	, @ULTIMA_EST	, @ULTIMA_SEC	, @NAVE_ANT
										, @NAVE_ACT	, @DOC_ID	, @NRO_LINEA	, @DISPONIBLE	, @DOC_TRANS_ID_EGR
										, @NRO_LIN_TRANS_EGR	, @DOC_TRANS_ID_TR	, @NRO_LIN_TRAN_ID_TR	, @CLIENTE_ID	
										, @CAT_LOG_ID	, @CAT_LOG_ID_FINAL 	, @EST_MERC_ID

		END

	CLOSE PCUR2
	DEALLOCATE PCUR2
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

ALTER    PROCEDURE [dbo].[Estacion_GetProductos_Egr]
@Picking_id		Numeric(20,0) 	Output,
@Tipo			Numeric(1,0)	Output
As
Begin
	Declare @Producto_id	as varchar(30)
	Declare @Cliente_id		as varchar(15)
	Declare @Documento_id	as Numeric(20,0)
	Declare @Nro_linea		as Numeric(10,0)
	declare @Nro_Lote		as varchar(100)
	declare @Nro_Partida	as varchar(100)

	Select 	 @Producto_id	= Producto_id
			,@Cliente_id	= Cliente_id
			,@Documento_Id	= Documento_id
			,@Nro_linea		= Nro_linea
			,@Nro_Lote		= Nro_Lote
			,@Nro_Partida	= Nro_Partida
	From	Picking (nolock)
	Where 	Picking_id		= @Picking_id


	If @Tipo=0
	Begin
		
--		Select	@Nro_Partida=Nro_partida
--		From	Det_Documento (nolock)
--		Where	Documento_id=@Documento_id
--				and Nro_linea=@Nro_Linea

		SELECT
				 dd.cliente_id				As CLIENTE_ID
				,dd.producto_id 			As PRODUCTO_ID
				,dd.DESCRIPCION				As DESCRIPCION
				,rl.cantidad				AS CANTIDAD
				,dd.NRO_BULTO				AS NRO_BULTO
				,dd.NRO_LOTE				AS NRO_LOTE
				,RL.EST_MERC_ID				AS EST_MERC_ID
				,dd.NRO_DESPACHO			AS NRO_DESPACHO
				,dd.NRO_PARTIDA				AS NRO_PARTIDA
				,dd.UNIDAD_ID				AS UNIDAD_ID
				,dd.PROP1					AS NRO_PALLET
				,dd.PROP2					AS PROP2
				,dd.PROP3					AS PROP3
				,RL.CAT_LOG_ID				AS CAT_LOG_ID
				,dd.fecha_vencimiento		AS FECHA_VENCIMIENTO
				,'POS' 						AS UBICACION
				,p.posicion_cod 			AS POSICION
				,isnull(p.orden_picking,999)AS ORDEN
				,rl.rl_id					AS RL_ID
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join posicion p (nolock) on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')
	
		UNION 
		SELECT
				 dd.cliente_id
				,dd.producto_id as Producto_Id
				,dd.DESCRIPCION
				,rl.cantidad
				,dd.NRO_BULTO
				,dd.NRO_LOTE
				,RL.EST_MERC_ID
				,dd.NRO_DESPACHO
				,dd.NRO_PARTIDA
				,dd.UNIDAD_ID
				,dd.PROP1
				,dd.PROP2
				,dd.PROP3
				,RL.CAT_LOG_ID
				,dd.fecha_vencimiento
				,'NAV' as ubicacion
				,n.nave_cod as posicion
				,isnull(n.orden_locator,999) as orden
				,rl.rl_id
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join nave n (nolock) on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')

	End
	Else
	Begin
		SELECT
				 dd.cliente_id				As CLIENTE_ID
				,dd.producto_id 			As PRODUCTO_ID
				,dd.DESCRIPCION				As DESCRIPCION
				,rl.cantidad				AS CANTIDAD
				,dd.NRO_BULTO				AS NRO_BULTO
				,dd.NRO_LOTE				AS NRO_LOTE
				,RL.EST_MERC_ID				AS EST_MERC_ID
				,dd.NRO_DESPACHO			AS NRO_DESPACHO
				,dd.NRO_PARTIDA				AS NRO_PARTIDA
				,dd.UNIDAD_ID				AS UNIDAD_ID
				,dd.PROP1					AS NRO_PALLET
				,dd.PROP2					AS PROP2
				,dd.PROP3					AS PROP3
				,RL.CAT_LOG_ID				AS CAT_LOG_ID
				,dd.fecha_vencimiento		AS FECHA_VENCIMIENTO
				,'POS' 						AS UBICACION
				,p.posicion_cod 			AS POSICION
				,isnull(p.orden_picking,999)AS ORDEN
				,rl.rl_id					AS RL_ID
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join posicion p (nolock) on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')
		UNION 
		SELECT
				 dd.cliente_id
				,dd.producto_id as Producto_Id
				,dd.DESCRIPCION
				,rl.cantidad
				,dd.NRO_BULTO
				,dd.NRO_LOTE
				,RL.EST_MERC_ID
				,dd.NRO_DESPACHO
				,dd.NRO_PARTIDA
				,dd.UNIDAD_ID
				,dd.PROP1
				,dd.PROP2
				,dd.PROP3
				,RL.CAT_LOG_ID
				,dd.fecha_vencimiento
				,'NAV' as ubicacion
				,n.nave_cod as posicion
				,isnull(n.orden_locator,999) as orden
				,rl.rl_id
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join nave n (nolock) on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')

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

ALTER PROCEDURE  [dbo].[Estacion_Picking_ActNroLinea] 
@NewRl_Id		Numeric(20,0) Output,
@Picking_Id		Numeric(20,0) Output
AS
Begin
	set xact_abort on
	-----------------------------------------------------------------------------
	--Declaracion de Variables.
	-----------------------------------------------------------------------------
	Declare @OldRl_Id			as Numeric(20,0)
	Declare @QtyPicking			as Float
	Declare @QtyRl				as Float
	Declare @Documento_Id		as Numeric(20,0)
	Declare @Nro_Linea			as Numeric(10,0)
	Declare @PreEgrId			as Numeric(20,0)
	Declare @Doc_Trans_IdEgr	as Numeric(20,0)
	Declare @Nro_Linea_TransEgr	as Numeric(10,0)
	Declare @Documento_IdNew	as Numeric(20,0)
	Declare @Nro_LineaNew		as Numeric(10,0)
	Declare @Dif				as Float
	Declare @MaxLinea			as Numeric(10,0)
	Declare @Doc_Trans_Id		as Numeric(20,0)
	Declare @MaxLineaDDT		as Numeric(10,0)
	Declare @SplitRl			as Numeric(20,0)
	Declare @Producto_IdC		as Varchar(30)
	Declare @Cliente_IdC		as Varchar(15)
	Declare @Cat_log_Id_Final	as Varchar(50)
	-----------------------------------------------------------------------------
	Declare @NRO_SERIE			as varchar(50)
	Declare @NRO_SERIE_PADRE	as varchar(50)
	Declare @EST_MERC_ID		as varchar(15)
	Declare @CAT_LOG_ID			as varchar(15)
	Declare @NRO_BULTO			as varchar(50)
	Declare @DESCRIPCION		as varchar(200)
	Declare @NRO_LOTE			as varchar(50)
	Declare @FECHA_VENCIMIENTO	as datetime
	Declare @NRO_DESPACHO		as varchar(50)
	Declare @NRO_PARTIDA		as varchar(50)
	Declare @UNIDAD_ID			as varchar(5)
	Declare @PESO				as numeric(20,5)
	Declare @UNIDAD_PESO		as varchar(5)
	Declare @VOLUMEN			as numeric(20,5)
	Declare @UNIDAD_VOLUMEN		as varchar(5)
	Declare @BUSC_INDIVIDUAL	as varchar(1)
	Declare @TIE_IN				as varchar(1)
	Declare @NRO_TIE_IN			as varchar(100)
	Declare @ITEM_OK			as varchar(1)
	Declare @MONEDA_ID			as varchar(20)
	Declare @COSTO				as numeric(20,3)
	Declare @PROP1				as varchar(100)
	Declare @PROP2				as varchar(100)
	Declare @PROP3				as varchar(100)
	Declare @LARGO				as numeric(10,3)
	Declare @ALTO				as numeric(10,3)
	Declare @ANCHO				as numeric(10,3)
	Declare @VOLUMEN_UNITARIO	as varchar(1)
	Declare @PESO_UNITARIO		as varchar(1)
	Declare @CANT_SOLICITADA	as numeric(20,5)	
	-----------------------------------------------------------------------------
	Declare @PALLET_HOMBRE		AS CHAR(1)
	Declare @Transf				as char(1)

	--Obtengo las Cantidades.
	Select @QtyPicking=Cantidad from picking where picking_id=@Picking_Id
	Select @QtyRl= Cantidad From Rl_Det_Doc_Trans_Posicion Where Rl_Id=@NewRl_Id
	
	--Verifico que al momento de hacer el cambio no este tomada la tarea de picking
	If Dbo.Picking_inProcess(@Picking_Id)=1
	Begin
		Raiserror('La tarea de Picking ya fue asignada. No es posible realizar el cambio.',16,1);
		return
	End
	
	--Estos valores me van a servir mas adelante.
	Select	 @Documento_Id	=Documento_id
			,@Nro_Linea 	=Nro_linea
	From	Picking
	Where	Picking_Id		=@Picking_Id

	select	@PALLET_HOMBRE=flg_pallet_hombre
	from	cliente_parametros c inner join documento d
			on(c.cliente_id=d.cliente_id)
	where	d.documento_id=@Documento_Id

	--Saco la nave de preegreso.
	Select	@PreEgrId=Nave_Id
	From	Nave
	Where	Pre_Egreso='1'

	--Obtengo el Nuevo Documento y numero de linea para Updetear.
	Select 	 Distinct
			 @Documento_idNew	=dd.Documento_Id
			,@Nro_lineaNew		=dd.Nro_Linea
	From	Rl_Det_Doc_Trans_posicion Rl
			Inner join Det_Documento_Transaccion ddt
			On(Rl.Doc_Trans_id=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=ddt.Nro_Linea_Trans)
			Inner Join Det_Documento dd
			on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
	Where	Rl.Rl_id=@NewRl_Id
	
	If (@QtyPicking = @QtyRL)
	Begin
			--Obtengo la Rl Anterior.
			Select 	@OldRl_Id=Rl.Rl_Id
			From	Rl_Det_Doc_Trans_posicion Rl
					Inner join Det_Documento_Transaccion ddt
					On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
					Inner Join Det_Documento dd
					on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
			Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
			
			Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea
			
			--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
			Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
					,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
			From	Rl_Det_Doc_Trans_posicion
			Where	Rl_Id=@OldRl_id

			--Restauro la rl Anterior
			Update 	 Rl_Det_Doc_Trans_posicion 
			Set 	 Disponible				='1'
					,Doc_Trans_Id_Egr		=null
					,Nro_Linea_Trans_Egr	=null
					,Posicion_Actual		=Posicion_Anterior
					,Posicion_Anterior		=Null
					,Nave_Actual			=Nave_Anterior
					,Nave_Anterior			=1
					,Cat_log_id				=@Cat_log_Id_Final
			Where	Rl_Id					=@OldRl_Id
			
			--Consumo la Nueva Rl
			Update	Rl_Det_Doc_Trans_Posicion 
			Set 	 Disponible='0'
					,Posicion_Anterior=Posicion_Actual
					,Posicion_Actual=Null
					,Nave_Anterior=Nave_Actual
					,Nave_Actual=@PreEgrId
					,Doc_Trans_id_Egr=@Doc_Trans_IdEgr
					,Nro_Linea_Trans_Egr=@Nro_Linea_TransEgr
					,Cat_log_Id='TRAN_EGR'
			Where	Rl_id=@NewRl_Id

			--Saco los valores de la Nueva linea de det_documento
			Select	  @NRO_SERIE				=Nro_Serie
					, @NRO_SERIE_PADRE			=Nro_Serie_Padre
					, @EST_MERC_ID				=Est_Merc_Id
					, @CAT_LOG_ID				=Cat_log_id
					, @NRO_BULTO				=Nro_Bulto
					, @DESCRIPCION				=Descripcion
					, @NRO_LOTE					=Nro_Lote
					, @FECHA_VENCIMIENTO		=Fecha_Vencimiento
					, @NRO_DESPACHO				=Nro_Despacho
					, @NRO_PARTIDA				=Nro_Partida
					, @UNIDAD_ID				=Unidad_Id
					, @PESO						=Peso
					, @UNIDAD_PESO				=Unidad_Peso
					, @VOLUMEN					=Volumen
					, @UNIDAD_VOLUMEN			=Unidad_Volumen
					, @BUSC_INDIVIDUAL			=Busc_Individual
					, @TIE_IN					=Tie_In
					, @NRO_TIE_IN				=Nro_Tie_In
					, @ITEM_OK					=Item_Ok
					--, @CAT_LOG_ID_FINAL			=Cat_Log_Id_Final
					, @MONEDA_ID				=Moneda_id
					, @COSTO					=Costo
					, @PROP1					=Prop1
					, @PROP2					=Prop2
					, @PROP3					=Prop3
					, @LARGO					=largo
					, @ALTO						=Alto
					, @ANCHO					=Ancho
					, @VOLUMEN_UNITARIO			=Volumen_Unitario
					, @PESO_UNITARIO			=Peso_Unitario
					, @CANT_SOLICITADA			=Cant_Solicitada
			FROM 	DET_DOCUMENTO				
			Where	Documento_Id=@Documento_idNew
					And Nro_linea=@Nro_LineaNew

			--Actualizo Det_Documento
			Update Det_Documento
			Set
					  Nro_Serie			=@NRO_SERIE				
					, Nro_Serie_padre	=@NRO_SERIE_PADRE		
					, Est_Merc_Id		=@EST_MERC_ID			
					, Cat_log_id		= 'TRAN_EGR'				
					, Nro_Bulto			=@NRO_BULTO				
					, Descripcion		=@DESCRIPCION			
					, Nro_Lote			=@NRO_LOTE				
					, Fecha_Vencimiento	=@FECHA_VENCIMIENTO		
					, Nro_Despacho		=@NRO_DESPACHO			
					, nro_partida		=@NRO_PARTIDA			
					, Unidad_id			=@UNIDAD_ID				
					, Peso				=@PESO					
					, Unidad_Peso		=@UNIDAD_PESO			
					, Volumen			=@VOLUMEN				
					, Unidad_Volumen	=@UNIDAD_VOLUMEN			
					, busc_individual	=@BUSC_INDIVIDUAL		
					, tie_in			=@TIE_IN					
					, Nro_Tie_in		=@NRO_TIE_IN				
					, Item_ok			=@ITEM_OK				
					--, Cat_log_Id_Final	=@CAT_LOG_ID_FINAL		
					, Moneda_id			=@MONEDA_ID				
					, Costo				=@COSTO					
					, Prop1				=@PROP1					
					, Prop2				=@PROP2					
					, Prop3				=@PROP3					
					, Largo				=@LARGO					
					, Alto				=@ALTO					
					, Ancho				=@ANCHO					
					, Volumen_Unitario	=@VOLUMEN_UNITARIO		
					, Peso_Unitario		=@PESO_UNITARIO		
					, Cant_solicitada	=ISNULL(@CANT_SOLICITADA,CANTIDAD)
			Where	Documento_id=@Documento_id
					And Nro_Linea=@Nro_Linea

			--Elimino la Linea de Picking
			Delete From Picking Where Picking_Id=@Picking_Id

			--Inserto la Nueva linea de Picking.
select * from picking
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
					,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
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
					,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE --CAMBIAR
					,0	  AS TRANSF_TERMINANDA	--CAMBIAR
					,NULL AS NRO_LOTE
					,NULL AS NRO_PARTIDA
					,NULL AS NRO_SERIE
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
			WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
					And dd.Nro_linea=@Nro_Linea

			Select 	@Cliente_IdC= Cliente_Id,
					@Producto_idC= Producto_Id
			From	Det_Documento 
			Where	Documento_id=@Documento_id
					And Nro_Linea=@Nro_Linea

			Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QtyPicking,@NewRl_Id,0,2,GETDATE(),'S')

	
	End--Fin Picking=Rl 1er. caso

	If (@QtyPicking < @QtyRL)
	Begin	
		Set @Dif= @QtyRL - @QtyPicking

		--Obtengo la Rl Anterior.
		Select 	@OldRl_Id=Rl.Rl_Id
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
			
		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea

		--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
		Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
				,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@OldRl_id
		
		--Spliteo la Rl.
		Insert into Rl_Det_Doc_Trans_Posicion
		Select 	 Doc_Trans_id
				,Nro_Linea_Trans
				,Posicion_Anterior
				,Posicion_Actual
				,@Dif	--Cantidad
				,Tipo_movimiento_Id
				,Ultima_Estacion
				,Ultima_Secuencia
				,Nave_Anterior
				,Nave_Actual
				,Documento_id
				,Nro_Linea
				,Disponible
				,Doc_Trans_id_Egr
				,Nro_Linea_Trans_Egr
				,Doc_Trans_Id_Tr
				,Nro_Linea_Trans_Tr
				,Cliente_id
				,Cat_log_Id
				,Cat_Log_Id_Final
				,Est_Merc_Id
		From	Rl_Det_Doc_Trans_Posicion
		Where	Rl_Id=@NewRl_id

		--Consumo la Rl.
		Update	Rl_Det_Doc_Trans_Posicion 
		Set 	 Disponible='0'
				,Cantidad=@QtyPicking
				,Posicion_Anterior=Posicion_Actual
				,Posicion_Actual=Null
				,Nave_Anterior=Nave_Actual
				,Nave_Actual=@PreEgrId
				,Doc_Trans_id_Egr=@Doc_Trans_IdEgr
				,Nro_Linea_Trans_Egr=@Nro_Linea_TransEgr
				,Cat_log_Id='TRAN_EGR'
		Where	Rl_id=@NewRl_Id

		--Restauro la rl Anterior.
		Update 	 Rl_Det_Doc_Trans_posicion 
		Set 	 Disponible				='1'
				,Doc_Trans_Id_Egr		=null
				,Nro_Linea_Trans_Egr	=null
				,Posicion_Actual		=Posicion_Anterior
				,Posicion_Anterior		=Null
				,Nave_Actual			=Nave_Anterior
				,Nave_Anterior			='1'
				,Cat_log_id				=@Cat_log_Id_Final
		Where	Rl_Id					=@OldRl_Id
		
		--Saco los valores de la Nueva linea de det_documento.
		Select	  @NRO_SERIE				=Nro_Serie
				, @NRO_SERIE_PADRE			=Nro_Serie_Padre
				, @EST_MERC_ID				=Est_Merc_Id
				, @CAT_LOG_ID				=Cat_log_id
				, @NRO_BULTO				=Nro_Bulto
				, @DESCRIPCION				=Descripcion
				, @NRO_LOTE					=Nro_Lote
				, @FECHA_VENCIMIENTO		=Fecha_Vencimiento
				, @NRO_DESPACHO				=Nro_Despacho
				, @NRO_PARTIDA				=Nro_Partida
				, @UNIDAD_ID				=Unidad_Id
				, @PESO						=Peso
				, @UNIDAD_PESO				=Unidad_Peso
				, @VOLUMEN					=Volumen
				, @UNIDAD_VOLUMEN			=Unidad_Volumen
				, @BUSC_INDIVIDUAL			=Busc_Individual
				, @TIE_IN					=Tie_In
				, @NRO_TIE_IN				=Nro_Tie_In
				, @ITEM_OK					=Item_Ok
				--, @CAT_LOG_ID_FINAL			=Cat_Log_Id_Final
				, @MONEDA_ID				=Moneda_id
				, @COSTO					=Costo
				, @PROP1					=Prop1
				, @PROP2					=Prop2
				, @PROP3					=Prop3
				, @LARGO					=largo
				, @ALTO						=Alto
				, @ANCHO					=Ancho
				, @VOLUMEN_UNITARIO			=Volumen_Unitario
				, @PESO_UNITARIO			=Peso_Unitario
				, @CANT_SOLICITADA			=Cant_Solicitada
		FROM 	DET_DOCUMENTO				
		Where	Documento_Id=@Documento_idNew
				And Nro_linea=@Nro_LineaNew

		--Actualizo Det_Documento
		Update Det_Documento
		Set
				  Nro_Serie			=@NRO_SERIE				
				, Nro_Serie_padre	=@NRO_SERIE_PADRE		
				, Est_Merc_Id		=@EST_MERC_ID			
				, Cat_log_id		='TRAN_EGR'				
				, Nro_Bulto			=@NRO_BULTO				
				, Descripcion		=@DESCRIPCION			
				, Nro_Lote			=@NRO_LOTE				
				, Fecha_Vencimiento	=@FECHA_VENCIMIENTO		
				, Nro_Despacho		=@NRO_DESPACHO			
				, nro_partida		=@NRO_PARTIDA			
				, Unidad_id			=@UNIDAD_ID				
				, Peso				=@PESO					
				, Unidad_Peso		=@UNIDAD_PESO			
				, Volumen			=@VOLUMEN				
				, Unidad_Volumen	=@UNIDAD_VOLUMEN			
				, busc_individual	=@BUSC_INDIVIDUAL		
				, tie_in			=@TIE_IN					
				, Nro_Tie_in		=@NRO_TIE_IN				
				, Item_ok			=@ITEM_OK				
				--, Cat_log_Id_Final	=@CAT_LOG_ID_FINAL		
				, Moneda_id			=@MONEDA_ID				
				, Costo				=@COSTO					
				, Prop1				=@PROP1					
				, Prop2				=@PROP2					
				, Prop3				=@PROP3					
				, Largo				=@LARGO					
				, Alto				=@ALTO					
				, Ancho				=@ANCHO					
				, Volumen_Unitario	=@VOLUMEN_UNITARIO		
				, Peso_Unitario		=@PESO_UNITARIO		
				, Cant_solicitada	=ISNULL(@CANT_SOLICITADA,CANTIDAD)
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		--Elimino la Linea de Picking
		Delete From Picking Where Picking_Id=@Picking_Id

		--Inserto la Nueva linea de Picking.
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
				,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
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
				,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
				,0	  AS TRANSF_TERMINANDA
				,NULL AS NRO_LOTE
				,NULL AS NRO_PARTIDA
				,NULL AS NRO_SERIE
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
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				And dd.Nro_linea=@Nro_Linea

		Select 	@Cliente_IdC= Cliente_Id,
				@Producto_idC= Producto_Id
		From	Det_Documento 
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QtyPicking,@NewRl_Id,0,2,GETDATE(),'S')

	End --Fin @QtyPicking < @QtyRL 2do. Caso.

	If (@QtyPicking > @QtyRL)	
	Begin
		Set @Dif= @QtyPicking - @QtyRL

		--Obtengo la Rl Anterior.
		Select 	@OldRl_Id=Rl.Rl_Id
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea

		--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
		Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
				,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@OldRl_id
		
		--Actualizo la cantidad en la linea original de det_documento.	
		Update Det_Documento Set Cantidad=@Dif, Cant_Solicitada=@Dif where Documento_Id=@Documento_id And Nro_Linea=@Nro_linea

		--Ya tengo el Nuevo Nro_Linea Para el Split	
		Select @MaxLinea=Max(Nro_linea) + 1 From Det_Documento Where Documento_Id=@Documento_id

		--Hago El Split de la linea de Det_Documento.
		Insert into Det_documento
		Select	Documento_Id, @MaxLinea, Cliente_Id, Producto_Id, @QtyRL,	Nro_Serie, Nro_Serie_Padre, Est_Merc_Id, Cat_Log_Id, Nro_Bulto,
				Descripcion, Nro_Lote, Fecha_Vencimiento, Nro_Despacho, Nro_Partida, Unidad_Id, Peso, Unidad_Peso, Volumen, Unidad_Volumen,
				Busc_Individual, Tie_In, Nro_Tie_In_Padre, Nro_Tie_in, Item_Ok, Cat_log_Id_Final, Moneda_Id, Costo, Prop1, Prop2, Prop3,
				Largo, Alto, Ancho, Volumen_unitario, Peso_Unitario, Cant_Solicitada, Trace_Back_Order
		From 	Det_Documento
		Where	Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Select @MaxLineaDDT=Max(Nro_linea_doc) + 1 From Det_Documento_Transaccion Where Documento_Id=@Documento_id

		--Saco el documento de Transaccion para poder hacer la insercion de DDT
		Select @Doc_Trans_Id=Doc_Trans_id From Det_Documento_Transaccion Where Documento_id=@Documento_id and Nro_Linea_doc=@Nro_Linea

		--Inserto en Det_Documento_Transaccion.	

		Insert Into Det_Documento_Transaccion
		Select 	 Doc_Trans_Id
				,@MaxLineaDDT
				,@Documento_id
				,@MaxLinea
				,Motivo_id
				,Est_Merc_Id
				,Cliente_Id
				,Cat_Log_Id
				,Item_Ok
				,Movimiento_Pendiente
				,Doc_Trans_ID_Ref
				,Nro_Linea_Trans_Ref
		From	Det_Documento_Transaccion
		Where	Documento_Id=@Documento_id
				And Nro_linea_Doc=@Nro_linea

		Update Rl_det_doc_Trans_Posicion Set Cantidad=@QtyPicking - @QtyRL where Rl_id=@OldRl_Id
		
		--Consumo la Rl.
		Update	Rl_Det_Doc_Trans_Posicion 
		Set 	 Disponible='0'
				,Posicion_Anterior=Posicion_Actual
				,Posicion_Actual=Null
				,Nave_Anterior=Nave_Actual
				,Nave_Actual=@PreEgrId
				,Doc_Trans_id_Egr=@Doc_Trans_IdEgr
				,Nro_Linea_Trans_Egr=@MaxLineaDDT
				,Cat_log_Id='TRAN_EGR'
		Where	Rl_id=@NewRl_Id

		--Debo Hacer el Split de la Linea de Rl Anterior.
		Insert into Rl_Det_Doc_Trans_Posicion
		Select 	 Doc_Trans_id
				,Nro_Linea_Trans
				,Posicion_Anterior
				,Posicion_Actual
				,@Dif	--Cantidad
				,Tipo_movimiento_Id
				,Ultima_Estacion
				,Ultima_Secuencia
				,Nave_Anterior
				,Nave_Actual
				,Documento_id
				,Nro_Linea
				,Disponible
				,Doc_Trans_id_Egr
				,Nro_Linea_Trans_Egr
				,Doc_Trans_Id_Tr
				,Nro_Linea_Trans_Tr
				,Cliente_id
				,Cat_log_Id
				,Cat_Log_Id_Final
				,Est_Merc_Id
		From	Rl_Det_Doc_Trans_Posicion
		Where	Rl_Id=@OldRl_Id

		--Necesario para saber q rl debo liberar.
		Select @SplitRl=Scope_Identity()

		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea

		--RL NUEVA LIBERADA
		Update 	 Rl_Det_Doc_Trans_posicion 
		Set 	 Disponible				='1'
				,Cantidad				=@QtyRL
				,Doc_Trans_Id_Egr		=null
				,Nro_Linea_Trans_Egr	=null
				,Posicion_Actual		=Posicion_Anterior
				,Posicion_Anterior		=Null
				,Nave_Actual			=Nave_Anterior
				,Nave_Anterior			='1'
				,Cat_log_id				=@Cat_log_Id_Final
		Where	Rl_Id					=@SplitRl
		
		Update Picking Set Cantidad=@Dif Where Picking_id=@Picking_id

		--Inserto la Nueva linea de Picking.
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
				,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
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
				,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
				,0	  AS TRANSF_TERMINANDA
				,NULL AS NRO_LOTE
				,NULL AS NRO_PARTIDA
				,NULL AS NRO_SERIE
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
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				And dd.Nro_linea=@MaxLinea		

		Update 	Consumo_Locator_Egr 
		Set 	Cantidad= @QtyPicking - @QtyRl ,
				saldo 	= (Saldo + (@QtyPicking - @QtyRl))
		Where	Documento_id=Documento_id
				and Nro_linea=@Nro_linea

		Select 	@Cliente_IdC= Cliente_Id,
				@Producto_idC= Producto_Id
		From	Det_Documento 
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @MaxLinea, @Cliente_IdC, @Producto_idC, @QtyRl, @NewRl_Id, 0, 2, GETDATE(),'S')

	End -- Fin 	If (@QtyPicking > @QtyRL) 3er. Caso.

	If @@Error<>0
	Begin
		raiserror('Se produjo un error inesperado.',16,1)
		return
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