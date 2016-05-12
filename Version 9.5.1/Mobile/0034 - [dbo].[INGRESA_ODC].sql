
/****** Object:  StoredProcedure [dbo].[INGRESA_ODC]    Script Date: 09/03/2015 10:17:44 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[INGRESA_ODC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[INGRESA_ODC]
GO

CREATE          PROCEDURE [dbo].[INGRESA_ODC]
@CLIENTE_ID		VARCHAR(15),
@PRODUCTO_ID	VARCHAR(30),
@ODC			VARCHAR(100),
@QTY			FLOAT,
@LOTEPROV		VARCHAR(50),
@FECHA_VTO		VARCHAR(20),
@PALLET			VARCHAR(50),
@TIPO_ETI		CHAR(1),
@DOCUMENTO_ID	BIGINT OUTPUT,
-- LRojas 02/03/2012 TrackerID 3806: Usuario para Demonio de Impresion
@USUARIO_IMP	VARCHAR(20)

AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @DOC_ID				NUMERIC(20,0)
	DECLARE @DOC_TRANS_ID		NUMERIC(20,0)
	DECLARE @DOC_EXT			VARCHAR(100)
	DECLARE @SUCURSAL_ORIGEN	VARCHAR(20)
	DECLARE @CAT_LOG_ID			VARCHAR(50)
	DECLARE @DESCRIPCION		VARCHAR(30)
	DECLARE @UNIDAD_ID			VARCHAR(15)
	DECLARE @NRO_PARTIDA		VARCHAR(100)
	DECLARE @NRO_DESPACHO		VARCHAR(50)
	DECLARE @EST_MERC_ID		VARCHAR(15)
	DECLARE @NRO_LOTE			VARCHAR(50)
	DECLARE @LOTE_AT			VARCHAR(50)
	DECLARE @Preing				VARCHAR(45)
	DECLARE @CatLogId			Varchar(50)
	DECLARE @LineBO				Float
	DECLARE @qtyBO				Float
	DECLARE @ToleranciaMax		Float
	DECLARE @QtyIngresada		Float
	DECLARE @tmax				Float
	DECLARE @MAXP				VARCHAR(50)
	DECLARE @PALLET_AUTOMATICO  VARCHAR(1)
	DECLARE @LOTE_AUTOMATICO	VARCHAR(1)
	-- LRojas TrackerID 3851 29/03/2012: Control, si el producto genera Back Order se crea un nuevo ingreso, de lo contrario no
	DECLARE @GENERA_BO          VARCHAR(1)
	DECLARE @TIPO_DOCUMENTO VARCHAR(50)
	
	/*
	CREATE TABLE #temp_usuario_loggin (
		usuario_id            			VARCHAR(20)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		terminal              			VARCHAR(100)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		fecha_loggin       			DATETIME,
		session_id            			VARCHAR(60)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		rol_id                			VARCHAR(5)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		emplazamiento_default 	VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		deposito_default      		VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
	)
	EXEC FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN 'SGOMEZ'
	*/
	
	SELECT 	TOP 1
			@DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID,@TIPO_DOCUMENTO= SD.TIPO_DOCUMENTO_ID 
	FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
	WHERE 	ORDEN_DE_COMPRA=@ODC
			AND PRODUCTO_ID=@PRODUCTO_ID
			AND SD.CLIENTE_ID=@CLIENTE_ID
			and SDD.fecha_estado_gt is null
			and SDD.estado_gt is null

	if @doc_ext is null
	begin
		raiserror('El producto %s no se encuentra en la orden de compra %s',16,1,@producto_id, @odc)
		return
	end
	SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0) from producto where cliente_id=@cliente_id and producto_id=@producto_id

	-----------------------------------------------------------------------------------------------------------------
	--tengo que controlar el maximo en cuanto a tolerancias.
	-----------------------------------------------------------------------------------------------------------------
	Select 	@qtyBO=sum(cantidad_solicitada)
	from	sys_int_det_documento
	where	doc_ext=@doc_ext
			and fecha_estado_gt is null
			and estado_gt is null

	set @tmax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)
	
	if @qty> @tmax
	begin
		Set @maxp=ROUND(@tmax,0)
		raiserror('1- La cantidad recepcionada supera a la tolerancia maxima permitida.  Maximo permitido: %s ',16,1, @maxp)
		return
	end
	-----------------------------------------------------------------------------------------------------------------
	--Obtengo las categorias logicas antes de la transaccion para acortar el lockeo.
	-----------------------------------------------------------------------------------------------------------------
	Select 	@CAT_LOG_ID=CAT_LOG_ID
			,@EST_MERC_ID=EST_MERC_ID
			,@NRO_DESPACHO=NRO_DESPACHO
			,@NRO_PARTIDA=NRO_PARTIDA
			,@NRO_LOTE=NRO_LOTE
	from	sys_int_det_documento
	where	CLIENTE_ID=@CLIENTE_ID
			AND doc_ext=@doc_ext
			AND PRODUCTO_ID=@PRODUCTO_ID
			
	-----------------------------------------------------------------------------------------------------------------
	--Saco la descripcion y la unidad del producto
	-----------------------------------------------------------------------------------------------------------------			

	SELECT 	@PALLET_AUTOMATICO=NRO_PARTIDA_AUTOMATICO,
			@LOTE_AUTOMATICO=LOTE_AUTOMATICO,
			@DESCRIPCION=DESCRIPCION,
			@UNIDAD_ID=UNIDAD_ID
	FROM 	PRODUCTO
	WHERE  	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID			
			
	IF RTRIM(LTRIM(@NRO_DESPACHO))='' BEGIN
		SET @NRO_DESPACHO=NULL
	END
	
	IF RTRIM(LTRIM(@EST_MERC_ID))='' BEGIN
		SET @EST_MERC_ID=NULL
	END	
	
	IF RTRIM(LTRIM(@NRO_PARTIDA))='' BEGIN
		SET @NRO_PARTIDA=NULL
	END
	
	IF RTRIM(LTRIM(@NRO_LOTE))='' BEGIN
		SET @NRO_LOTE=NULL
	END
			
	IF @CAT_LOG_ID='' BEGIN SET @CAT_LOG_ID=NULL END

	IF @CAT_LOG_ID IS NULL BEGIN  
		SELECT	@CAT_LOG_ID=PC.CAT_LOG_ID
		FROM	RL_PRODUCTO_CATLOG PC
		WHERE	PC.CLIENTE_ID=@CLIENTE_ID
				AND PC.PRODUCTO_ID=@PRODUCTO_ID
				AND PC.TIPO_COMPROBANTE_ID=  ISNULL(@TIPO_DOCUMENTO,'DO')
	END
	
	If @CAT_LOG_ID Is null begin
		--entra porque no tiene categorias particulares y busca la default.
		select 	@CAT_LOG_ID=p.ing_cat_log_id
		From 	producto p 
		where  	p.cliente_id=@CLIENTE_ID
				and p.producto_id=@PRODUCTO_ID
	end 

	-----------------------------------------------------------------------------------------------------------------
	--obtengo los valores de las secuencias.
	-----------------------------------------------------------------------------------------------------------------	
	
	IF @NRO_PARTIDA IS NULL 
	BEGIN
		IF @PALLET_AUTOMATICO = '1' 
		BEGIN
			exec get_value_for_sequence  'NRO_PARTIDA', @NRO_PARTIDA Output
		END
	END
	
	IF @NRO_LOTE IS NULL 
	BEGIN
		IF @LOTE_AUTOMATICO = '1' 
		BEGIN
			exec get_value_for_sequence 'NROLOTE_SEQ', @NRO_LOTE Output
		END
	END	

	-----------------------------------------------------------------------------------------------------------------
	--Comienzo con la carga de las tablas.
	-----------------------------------------------------------------------------------------------------------------
	Begin transaction	
	--Creo Documento
	Insert into Documento (	Cliente_id	, Tipo_comprobante_id	, tipo_operacion_id	, det_tipo_operacion_id	, sucursal_origen		, fecha_cpte	, fecha_pedida_ent	, Status	, anulado	, nro_remito	, nro_despacho_importacion	,GRUPO_PICKING		, fecha_alta_gtw)
					Values(	@Cliente_Id	, @TIPO_DOCUMENTO		, 'ING'				, 'MAN'					,@SUCURSAL_ORIGEN		, GETDATE()		, GETDATE()			,'D05'		,'0'		, NULL			,@DOC_EXT					,null			, getdate())		
	--Obtengo el Documento Id recien creado.	
	Set @Doc_ID= Scope_identity()
	
	--Creo el detalle de det_documento
	insert into det_documento (documento_id, nro_linea	, cliente_id	, producto_id	, cantidad	, cat_log_id	, cat_log_id_final	, tie_in	, fecha_vencimiento	, nro_partida	, unidad_id	, descripcion		, busc_individual	, item_ok	, cant_solicitada	, prop1	, prop2		, nro_bulto	,nro_lote, NRO_DESPACHO, EST_MERC_ID)
						values(@doc_id		, 1			, @cliente_id	, @producto_id	, @qty		, null		, @cat_log_id		, '0'		, @fecha_vto		, @nro_partida	, @unidad_id	, @descripcion	, '1'				, '1'			,@qtyBO			, @pallet, UPPER(@loteProv)	, NULL		, @NRO_LOTE, @NRO_DESPACHO, @EST_MERC_ID)

	------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	--Documento a Ingreso.
	select 	@Preing=nave_id
	from	nave
	where	pre_ingreso='1'
	
	SELECT 	@catlogid=cat_log_id
	FROM 	categoria_stock cs
			INNER JOIN categoria_logica cl
			ON cl.categ_stock_id = cs.categ_stock_id
	WHERE 	cs.categ_stock_id = 'TRAN_ING'
			And cliente_id =@cliente_id

	UPDATE det_documento
	Set cat_log_id =@catlogid
	WHERE documento_id = @Doc_ID

	Update documento set status='D20' where documento_id=@doc_id
	
	Insert Into RL_DET_DOC_TRANS_POSICION (
				DOC_TRANS_ID,				NRO_LINEA_TRANS,
				POSICION_ANTERIOR,		POSICION_ACTUAL,
				CANTIDAD,					TIPO_MOVIMIENTO_ID,
				ULTIMA_ESTACION,			ULTIMA_SECUENCIA,
				NAVE_ANTERIOR,				NAVE_ACTUAL,
				DOCUMENTO_ID,				NRO_LINEA,
				DISPONIBLE,					DOC_TRANS_ID_EGR,
				NRO_LINEA_TRANS_EGR,		DOC_TRANS_ID_TR,
				NRO_LINEA_TRANS_TR,		CLIENTE_ID,
				CAT_LOG_ID,				CAT_LOG_ID_FINAL,
				EST_MERC_ID)
	Values (NULL, NULL, NULL, NULL, @qty, NULL, NULL, NULL, NULL, @PREING, @doc_id, 1, null, null, null, null, null, @cliente_id, @catlogid,@CAT_LOG_ID,@EST_MERC_ID)

	-----------------------------------------------------------------------------------------------------------------
	--ASIGNO TRATAMIENTO...
	-----------------------------------------------------------------------------------------------------------------
	exec asigna_tratamiento#asigna_tratamiento_ing @doc_id

	------------------------------------------------------------------------------------------------------------------------------------
	--Generacion del Back Order.
	-----------------------------------------------------------------------------------------------------------------
	select @lineBO=max(isnull(nro_linea,1))+1 from sys_int_det_documento WHERE 	 DOC_EXT=@doc_ext

	Select 	@qtyBO=sum(cantidad_solicitada)
	from	sys_int_det_documento
	where	doc_ext=@doc_ext
			and fecha_estado_gt is null
			and estado_gt is null

	UPDATE SYS_INT_DOCUMENTO SET ESTADO_GT='P'	,FECHA_ESTADO_GT=getdate() WHERE DOC_EXT=@doc_ext
	
	UPDATE SYS_INT_DET_DOCUMENTO SET ESTADO_GT='P', DOC_BACK_ORDER=@doc_ext,FECHA_ESTADO_GT=getdate(), DOCUMENTO_ID=@Doc_ID
	WHERE 	DOC_EXT=@doc_ext	and documento_id is null

	set @qtyBO=@qtyBO - @qty

	SELECT @GENERA_BO = 
	   CASE P.BACK_ORDER 
		WHEN '1' THEN 'S' 
		WHEN '0' THEN 'N'
	   END
	FROM PRODUCTO P INNER JOIN SYS_INT_DET_DOCUMENTO SIDD ON (P.PRODUCTO_ID = SIDD.PRODUCTO_ID)
	WHERE SIDD.DOC_EXT = @doc_ext	AND SIDD.DOCUMENTO_ID = @Doc_ID AND P.CLIENTE_ID=@cliente_id

	-- LRojas TrackerID 3851 29/03/2012: Se debe tener en cuenta la parametrizaci?n del producto.
	IF (@qtyBO > 0) AND (@GENERA_BO = 'S') --Si esta variable es mayor a 0, genero el backorder.
	begin
	
	insert into sys_int_det_documento 
		select	TOP 1 
				DOC_EXT, @lineBO ,CLIENTE_ID, PRODUCTO_ID, @qtyBO ,Cantidad , EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO, 
				NRO_PARTIDA, UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, NULL, NULL, NULL,  NULL,NULL,NULL,
				NULL,NULL,CUSTOMS_1,CUSTOMS_2,CUSTOMS_3 
		from 	sys_int_det_documento 
		WHERE 	DOC_EXT=@Doc_Ext
	end
	------------------------------------------------------------------------------------------------------------------------------------
	--Guardo en la tabla de auditoria
	-----------------------------------------------------------------------------------------------------------------
	exec dbo.AUDITORIA_HIST_INSERT_ING @doc_id
	-- LRojas 02/03/2012 TrackerID 3806: Inserto Usuario para Demonio de Impresion
	insert into IMPRESION_RODC VALUES(@Doc_id, 1, @Tipo_eti,'0', @USUARIO_IMP)
	COMMIT TRANSACTION
	Set @DOCUMENTO_ID=@doc_id
END-- FIN PROCEDIMIENTO.

GO
