
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 03:49 p.m.
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