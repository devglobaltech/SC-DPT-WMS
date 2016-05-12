USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 05:50 p.m.
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

ALTER PROCEDURE ALTA_SYS_MAPEADOR
			@CLIENTE_ID AS varchar(15) OUTPUT,
			@CARPETA AS varchar(100) OUTPUT,
			@TIPO_INTERFAZ AS varchar(100) OUTPUT,
			@TABLA_CAMPO AS varchar(100) OUTPUT,
			@LISTA_OK AS varchar(800) OUTPUT,
			@LISTA_ERROR AS varchar(800) OUTPUT,
			@TIPO_COMPROBANTE_ID AS varchar(5) OUTPUT,
			@TIPO_OPERACION_ID AS varchar(5) OUTPUT,
			@CARPETA_OK AS varchar(100)OUTPUT,
			@CARPETA_ERR AS varchar(100)OUTPUT,
			@SEPARADOR AS varchar(1)OUTPUT,
			@FTP AS varchar(1)OUTPUT,
			@USR_FTP AS varchar(100)OUTPUT,
			@PSW_FTP AS varchar(100)OUTPUT,
			@SERVIDOR_FTP AS varchar(15)OUTPUT,
			@PROCESA_PRIMERA AS varchar(1)OUTPUT,
			@PROCESA_ULTIMA AS varchar(1)OUTPUT,
			@TRANS_LINEA_O_GRUPO AS varchar(1)OUTPUT,
			@POS_ARCHIVO AS numeric OUTPUT,
			@COMPUESTO AS varchar(10) OUTPUT,
			@SYS_DATE AS varchar(1) OUTPUT,
			@DATE_OFFSET AS varchar(1) OUTPUT,	
			@FORMATO AS varchar(15) OUTPUT,
			@NUMERAR AS varchar(1)  OUTPUT  
AS
BEGIN
			INSERT INTO [dbo].[SYS_MAPEADOR]
			([CLIENTE_ID],[CARPETA],[TIPO_INTERFAZ],[TABLA_CAMPO],[LISTA_OK]
           ,[LISTA_ERROR],[TIPO_COMPROBANTE_ID],[TIPO_OPERACION_ID],[CARPETA_OK]
           ,[CARPETA_ERR],[SEPARADOR],[FTP],[USR_FTP],[PSW_FTP],[SERVIDOR_FTP]
           ,[PROCESA_PRIMERA],[PROCESA_ULTIMA],[TRANS_LINEA_O_GRUPO],[POS_ARCHIVO]
           ,[COMPUESTO],[SYS_DATE],[DATE_OFFSET],[FORMATO],[NUMERAR])
			VALUES
			(@CLIENTE_ID,@CARPETA,@TIPO_INTERFAZ,@TABLA_CAMPO,@LISTA_OK,@LISTA_ERROR,
           @TIPO_COMPROBANTE_ID,@TIPO_OPERACION_ID,@CARPETA_OK,@CARPETA_ERR,@SEPARADOR,
           @FTP,@USR_FTP,@PSW_FTP,@SERVIDOR_FTP,@PROCESA_PRIMERA,@PROCESA_ULTIMA,
           @TRANS_LINEA_O_GRUPO,@POS_ARCHIVO,@COMPUESTO,@SYS_DATE,@DATE_OFFSET,
           @FORMATO,@NUMERAR)
	
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

ALTER PROCEDURE [dbo].[AltaClientes]
@SucId		Varchar(20),
@Nombre		Varchar(50),
@Calle		Varchar(50),	
@CP			varchar(10),
@Localidad	varchar(30),
@provincia	varchar(50),
@zona		varchar(5),
@obs		varchar(50)
AS
Begin
	SET XACT_ABORT ON
	Declare @Total		as numeric(20,0)
	Declare @PciaId		as varchar(20)
	Declare @ClienteId	as varchar(15)
	
	
	Set @ClienteId='VITALCAN'
	
	IF ltrim(rtrim(upper(@provincia)))=''
	begin
		set @provincia=null
		set @PciaId=null
	end
	else
	begin
		Select	@PciaId=Provincia_id
		from	provincia
		where	pais_id='AR' and ltrim(rtrim(upper(descripcion)))=ltrim(rtrim(upper(@provincia)))
	end
	if ltrim(rtrim(upper(@obs)))=''
	begin
		set @obs=null
	end
	select	@total=count(*)
	from	sucursal
	where	cliente_id=@clienteid
			and sucursal_id=@sucid
	if @total>0
	begin
		raiserror ('Duplicado %s',16,1,@SucId)
		return
	end
	insert into sucursal (Cliente_id, sucursal_id, nombre, calle, localidad, provincia_id, pais_id,observaciones, Tipo_Agente_id)
	values(
		@clienteid,
		ltrim(rtrim(upper(@sucid))),
		ltrim(rtrim(upper(@nombre))),
		ltrim(rtrim(upper(@calle))),
		ltrim(rtrim(upper(@localidad))),
		@PciaId,
		'AR',
		ltrim(rtrim(upper(@Obs))),
		'CLIENTE'
	)
End--Fin Procedure.
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

ALTER    PROCEDURE [dbo].[Estacion_GetProductos_Egr]
@Usr			varchar(20)		output,
@Rpar			varchar(1)		output,
@Tipo			Numeric(1,0)	Output
As
Begin
	Declare @Producto_id	as varchar(30)
	Declare @Cliente_id		as varchar(15)
	declare @Nro_Lote		as varchar(100)
	declare @Nro_Partida	as varchar(100)

	Select 	distinct 
			 @Producto_id	= Producto_id
			,@Cliente_id	= Cliente_id
	From	Picking (nolock)
	Where 	USUARIO =@Usr 
			and FECHA_INICIO is not null
			and FECHA_FIN is null;

	EXEC DBO.Consolidar_Existencias @Cliente_id, @Producto_id;
		
	if(@Rpar='1')begin
		------------------------------------------------------------------------------
		--De acuerdo a la definicion de C.Rivero si se desea respetar el lote/partida
		--se ejecuta esta consulta para levantar todos los datos.
		------------------------------------------------------------------------------
		Select 	 distinct 
				 @Nro_Lote		= Nro_Lote
				,@Nro_Partida	= Nro_Partida
		From	Picking (nolock)
		Where 	USUARIO =@Usr 
				and FECHA_INICIO is not null
				and FECHA_FIN is null;
		------------------------------------------------------------------------------
	end
	If @Tipo=0
	Begin
		SELECT	 dd.cliente_id				As CLIENTE_ID
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
		WHERE	rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ((@Nro_Partida IS NULL)OR(ISNULL(dd.Nro_Partida,'')=ISNULL(@Nro_Partida,'')))
				and ((@nro_lote IS NULL)OR(ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')))
	
		UNION 
		SELECT	 dd.cliente_id
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
				and ((@Nro_Partida IS NULL)OR(ISNULL(dd.Nro_Partida,'')=ISNULL(@Nro_Partida,'')))
				and ((@nro_lote IS NULL)OR(ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')))

	End
	Else
	Begin

		SELECT	 dd.cliente_id				As CLIENTE_ID
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
		WHERE	rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ((@Nro_Partida IS NULL)OR(ISNULL(dd.Nro_Partida,'')=ISNULL(@Nro_Partida,'')))
				and ((@nro_lote IS NULL)OR(ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')))
		UNION 
		SELECT	 dd.cliente_id
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
		WHERE	rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ((@Nro_Partida IS NULL)OR(ISNULL(dd.Nro_Partida,'')=ISNULL(@Nro_Partida,'')))
				and ((@nro_lote IS NULL)OR(ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')))
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