/****** Object:  StoredProcedure [dbo].[SYS_INT_DET_DOC]    Script Date: 21/04/2016 04:37:02 p.m. ******/
DROP PROCEDURE [dbo].[SYS_INT_DET_DOC]
GO
/*
	========================================================================================================================
	Documentacion:
	========================================================================================================================
		21-04-2016:	Se agrega la posibilidad de crear el producto directamente con la informacion parametrizada en la tabla
					PARAMETRICA_CONFIGURACION_DEFAULT, si el campo que se envia a la funcion tiene informacion la fx devuelve
					dicha informacion, sino la tiene levanta el valor default de la tabla. Si no tiene info default retorna
					null de la fx.
	------------------------------------------------------------------------------------------------------------------------
		22-04-2016:	Se agrega la creacion de la informacion mandatoria inicial.
					Se agrega como referencia el campo CUSTOMS_1, al dar de alta el producto sera pasado a la fx para ser
					tomado como referencia, en caso de que exista mas de una configuracion posible.
	------------------------------------------------------------------------------------------------------------------------
*/
CREATE /*ALTER*/ PROCEDURE [dbo].[SYS_INT_DET_DOC]
			@DOC_EXT as varchar(100)		OUTPUT,
			@NRO_LINEA as numeric			OUTPUT,
			@CLIENTE_ID as varchar(15)		OUTPUT,
			@PRODUCTO_ID as varchar(30)		OUTPUT,
			@CANTIDAD_SOLICITADA as numeric (20,5)	OUTPUT,
			@CANTIDAD as numeric (20,5)		OUTPUT,
			@EST_MERC_ID as varchar(50)		OUTPUT,
			@CAT_LOG_ID as varchar(50)		OUTPUT,
			@NRO_BULTO as varchar(100)		OUTPUT,
			@DESCRIPCION as varchar(500)	OUTPUT,
			@NRO_LOTE as varchar(100)		OUTPUT,
			@NRO_PALLET as varchar(100)		OUTPUT,
			@FECHA_VENCIMIENTO as datetime	OUTPUT,
			@NRO_DESPACHO as varchar(100)	OUTPUT,
			@NRO_PARTIDA as varchar(100)	OUTPUT,
			@UNIDAD_ID as varchar(5)		OUTPUT,
			@UNIDAD_CONTENEDORA_ID as varchar(5)OUTPUT,
			@PESO as numeric				OUTPUT,
			@UNIDAD_PESO as varchar(5)		OUTPUT,
			@VOLUMEN as numeric				OUTPUT,
			@UNIDAD_VOLUMEN as varchar(5)	OUTPUT,
			@PROP1 as varchar(100)			OUTPUT,
			@PROP2 as varchar(100)			OUTPUT,
			@PROP3 as varchar(100)			OUTPUT,
			@LARGO as numeric				OUTPUT,
			@ALTO as numeric				OUTPUT,
			@ANCHO as numeric				OUTPUT,
			@DOC_BACK_ORDER as varchar(100)	OUTPUT,
			@ESTADO as varchar(20)			OUTPUT,
			@FECHA_ESTADO as datetime		OUTPUT,
			@ESTADO_GT as varchar(20)		OUTPUT,
			@FECHA_ESTADO_GT as datetime	OUTPUT,
			@DOCUMENTO_ID as numeric		OUTPUT,		
			@NAVE_ID as numeric				OUTPUT,	
			@NAVE_COD as varchar(15)		OUTPUT,
			@CUSTOMS_1 AS VARCHAR(4000)		OUTPUT,
			@CUSTOMS_2 AS VARCHAR(4000)		OUTPUT,
			@CUSTOMS_3 AS VARCHAR(4000)		OUTPUT

AS
BEGIN
	declare @VUNIDAD_ID							as varchar(5)
	declare @CONTROL							as smallint
	----------------------------------------------------------
	--	Para la carga de producto.
	----------------------------------------------------------
	declare @PRODUCTO_CLIENTE_ID				as varchar(15)
	declare @PRODUCTO_PRODUCTO_ID				as varchar(30)
	declare @PRODUCTO_CODIGO_PRODUCTO			as varchar(50)
	declare @PRODUCTO_SUBCODIGO_1				as varchar(50)
	declare @PRODUCTO_SUBCODIGO_2				as varchar(50)
	declare @PRODUCTO_DESCRIPCION				as varchar(200)
	declare @PRODUCTO_NOMBRE					as varchar(200)
	declare @PRODUCTO_MARCA						as varchar(60)
	declare @PRODUCTO_FRACCIONABLE				as varchar(1)
	declare @PRODUCTO_UNIDAD_FRACCION			as varchar(5)
	declare @PRODUCTO_COSTO						as numeric(10)
	declare @PRODUCTO_UNIDAD_ID					as varchar(5)
	declare @PRODUCTO_TIPO_PRODUCTO_ID			as varchar(5)
	declare @PRODUCTO_PAIS_ID					as varchar(5)
	declare @PRODUCTO_FAMILIA_ID				as varchar(30)
	declare @PRODUCTO_CRITERIO_ID				as varchar(5)
	declare @PRODUCTO_OBSERVACIONES				as varchar(400)
	declare @PRODUCTO_POSICIONES_PURAS			as varchar(1)
	declare @PRODUCTO_KIT						as varchar(1)
	declare @PRODUCTO_SERIE_EGR					as varchar(1)
	declare @PRODUCTO_MONEDA_ID					as varchar(20)
	declare @PRODUCTO_NO_AGRUPA_ITEMS			as varchar(1)
	declare @PRODUCTO_LARGO						as numeric(10)
	declare @PRODUCTO_ALTO						as numeric(10)
	declare @PRODUCTO_ANCHO						as numeric(10)
	declare @PRODUCTO_UNIDAD_VOLUMEN			as varchar(5)
	declare @PRODUCTO_VOLUMEN_UNITARIO			as varchar(1)
	declare @PRODUCTO_PESO						as numeric(20)
	declare @PRODUCTO_UNIDAD_PESO				as varchar(5)
	declare @PRODUCTO_PESO_UNITARIO				as varchar(1)
	declare @PRODUCTO_LOTE_AUTOMATICO			as varchar(1)
	declare @PRODUCTO_PALLET_AUTOMATICO			as varchar(1)
	declare @PRODUCTO_INGRESO					as varchar(15)
	declare @PRODUCTO_EGRESO					as varchar(15)
	declare @PRODUCTO_INVENTARIO				as varchar(15)
	declare @PRODUCTO_TRANSFERENCIA				as varchar(15)
	declare @PRODUCTO_TOLERANCIA_MIN			as numeric(6)
	declare @PRODUCTO_TOLERANCIA_MAX			as numeric(6)
	declare @PRODUCTO_BACK_ORDER				as varchar(1)
	declare @PRODUCTO_CLASIFICACION_COT			as varchar(100)
	declare @PRODUCTO_CODIGO_BARRA				as varchar(100)
	declare @PRODUCTO_ING_CAT_LOG_ID			as varchar(50)
	declare @PRODUCTO_EGR_CAT_LOG_ID			as varchar(50)
	declare @PRODUCTO_SUB_FAMILIA_ID			as varchar(30)
	declare @PRODUCTO_TIPO_CONTENEDORA			as varchar(100)
	declare @PRODUCTO_GRUPO_PRODUCTO			as varchar(5)
	declare @PRODUCTO_ENVASE					as varchar(1)
	declare @PRODUCTO_VAL_COD_ING				as char(1)
	declare @PRODUCTO_VAL_COD_EGR				as char(1)
	declare @PRODUCTO_ROTACION_ID				as varchar(20)
	declare @PRODUCTO_FLG_BULTO					as char(1)
	declare @PRODUCTO_QTY_BULTO					as numeric(20)	
	declare @PRODUCTO_FLG_VOLUMEN_ETI			as char(1)
	declare @PRODUCTO_QTY_VOLUMEN_ETI			as numeric(20)
	declare @PRODUCTO_FLG_CONTENEDORA			as varchar(1)
	declare @PRODUCTO_SERIE_ING					as varchar(1)
	declare @PRODUCTO_TIE_IN					as varchar(1)
	declare @PRODUCTO_ingLoteProveedor			as varchar(1)
	declare @PRODUCTO_ingPartida				as varchar(1)
	declare @PRODUCTO_NRO_PARTIDA_AUTOMATICO	as char(1)
	declare @PRODUCTO_TRANSF_PICKING			as varchar(1)
	declare @PRODUCTO_ET_TAREA_CONF				as varchar(1)

	SELECT	@CONTROL=COUNT(*)
	FROM	PRODUCTO
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID

	IF @CONTROL=0 BEGIN

		--LEVANTO LOS VALORES PARA EL PRODUCTO.
		Set @PRODUCTO_CLIENTE_ID			= @CLIENTE_ID
		Set @PRODUCTO_PRODUCTO_ID			= @PRODUCTO_ID
		Set @PRODUCTO_DESCRIPCION			= ISNULL(@DESCRIPCION,@PRODUCTO_ID)
		Set @PRODUCTO_UNIDAD_ID				= @UNIDAD_ID 
		Set @PRODUCTO_UNIDAD_ID				= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','UNIDAD_ID'			, @CUSTOMS_1,  @PRODUCTO_UNIDAD_ID),'UN')
		Set @PRODUCTO_CODIGO_PRODUCTO		= dbo.fx_get_param_conf_default('PRODUCTO','CODIGO_PRODUCTO'			, @CUSTOMS_1,  @PRODUCTO_CODIGO_PRODUCTO)
		Set @PRODUCTO_SUBCODIGO_1			= dbo.fx_get_param_conf_default('PRODUCTO','SUBCODIGO_1'				, @CUSTOMS_1,  @PRODUCTO_SUBCODIGO_1)
		Set @PRODUCTO_SUBCODIGO_2			= dbo.fx_get_param_conf_default('PRODUCTO','SUBCODIGO_2'				, @CUSTOMS_1,  @PRODUCTO_SUBCODIGO_2)
		Set @PRODUCTO_NOMBRE				= dbo.fx_get_param_conf_default('PRODUCTO','NOMBRE'						, @CUSTOMS_1,  @PRODUCTO_NOMBRE)
		Set @PRODUCTO_MARCA					= dbo.fx_get_param_conf_default('PRODUCTO','MARCA'						, @CUSTOMS_1,  @PRODUCTO_MARCA)
		Set @PRODUCTO_FRACCIONABLE			= dbo.fx_get_param_conf_default('PRODUCTO','FRACCIONABLE'				, @CUSTOMS_1,  @PRODUCTO_FRACCIONABLE)
		Set @PRODUCTO_UNIDAD_FRACCION		= dbo.fx_get_param_conf_default('PRODUCTO','UNIDAD_FRACCION'			, @CUSTOMS_1,  @PRODUCTO_UNIDAD_FRACCION)
		Set @PRODUCTO_COSTO					= dbo.fx_get_param_conf_default('PRODUCTO','COSTO'						, @CUSTOMS_1,  @PRODUCTO_COSTO)
		Set @PRODUCTO_TIPO_PRODUCTO_ID		= dbo.fx_get_param_conf_default('PRODUCTO','TIPO_PRODUCTO_ID'			, @CUSTOMS_1,  @PRODUCTO_TIPO_PRODUCTO_ID)
		Set @PRODUCTO_PAIS_ID				= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','PAIS_ID'				, @CUSTOMS_1,  @PRODUCTO_PAIS_ID),'AR')
		Set @PRODUCTO_FAMILIA_ID			= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','FAMILIA_ID'			, @CUSTOMS_1,  @PRODUCTO_FAMILIA_ID),'DF')
		Set @PRODUCTO_CRITERIO_ID			= dbo.fx_get_param_conf_default('PRODUCTO','CRITERIO_ID'				, @CUSTOMS_1,  @PRODUCTO_CRITERIO_ID)
		Set @PRODUCTO_OBSERVACIONES			= dbo.fx_get_param_conf_default('PRODUCTO','OBSERVACIONES'				, @CUSTOMS_1,  @PRODUCTO_OBSERVACIONES)
		Set @PRODUCTO_POSICIONES_PURAS		= dbo.fx_get_param_conf_default('PRODUCTO','POSICIONES_PURAS'			, @CUSTOMS_1,  @PRODUCTO_POSICIONES_PURAS)
		Set @PRODUCTO_KIT					= dbo.fx_get_param_conf_default('PRODUCTO','KIT'						, @CUSTOMS_1,  @PRODUCTO_KIT)
		Set @PRODUCTO_SERIE_EGR				= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','SERIE_EGR'			, @CUSTOMS_1,  @PRODUCTO_SERIE_EGR),'0')
		Set @PRODUCTO_MONEDA_ID				= dbo.fx_get_param_conf_default('PRODUCTO','MONEDA_ID'					, @CUSTOMS_1,  @PRODUCTO_MONEDA_ID)
		Set @PRODUCTO_NO_AGRUPA_ITEMS		= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','NO_AGRUPA_ITEMS'		, @CUSTOMS_1,  @PRODUCTO_NO_AGRUPA_ITEMS),'0')
		Set @PRODUCTO_LARGO					= dbo.fx_get_param_conf_default('PRODUCTO','LARGO'						, @CUSTOMS_1,  @PRODUCTO_LARGO)
		Set @PRODUCTO_ALTO					= dbo.fx_get_param_conf_default('PRODUCTO','ALTO'						, @CUSTOMS_1,  @PRODUCTO_ALTO)
		Set @PRODUCTO_ANCHO					= dbo.fx_get_param_conf_default('PRODUCTO','ANCHO'						, @CUSTOMS_1,  @PRODUCTO_ANCHO)
		Set @PRODUCTO_UNIDAD_VOLUMEN		= dbo.fx_get_param_conf_default('PRODUCTO','UNIDAD_VOLUMEN'				, @CUSTOMS_1,  @PRODUCTO_UNIDAD_VOLUMEN)
		Set @PRODUCTO_VOLUMEN_UNITARIO		= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','VOLUMEN_UNITARIO'	, @CUSTOMS_1,  @PRODUCTO_VOLUMEN_UNITARIO),'1')
		Set @PRODUCTO_PESO					= dbo.fx_get_param_conf_default('PRODUCTO','PESO'						, @CUSTOMS_1,  @PRODUCTO_PESO)
		Set @PRODUCTO_UNIDAD_PESO			= dbo.fx_get_param_conf_default('PRODUCTO','UNIDAD_PESO'				, @CUSTOMS_1,  @PRODUCTO_UNIDAD_PESO)
		Set @PRODUCTO_PESO_UNITARIO			= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','PESO_UNITARIO'		, @CUSTOMS_1,  @PRODUCTO_PESO_UNITARIO),'1')
		Set @PRODUCTO_LOTE_AUTOMATICO		= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','LOTE_AUTOMATICO'		, @CUSTOMS_1,  @PRODUCTO_LOTE_AUTOMATICO),'0')
		Set @PRODUCTO_PALLET_AUTOMATICO		= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','PALLET_AUTOMATICO'	, @CUSTOMS_1,  @PRODUCTO_PALLET_AUTOMATICO),'0')
		Set @PRODUCTO_INGRESO				= dbo.fx_get_param_conf_default('PRODUCTO','INGRESO'					, @CUSTOMS_1,  @PRODUCTO_INGRESO)
		Set @PRODUCTO_EGRESO				= dbo.fx_get_param_conf_default('PRODUCTO','EGRESO'						, @CUSTOMS_1,  @PRODUCTO_EGRESO)
		Set @PRODUCTO_INVENTARIO			= dbo.fx_get_param_conf_default('PRODUCTO','INVENTARIO'					, @CUSTOMS_1,  @PRODUCTO_INVENTARIO)
		Set @PRODUCTO_TRANSFERENCIA			= dbo.fx_get_param_conf_default('PRODUCTO','TRANSFERENCIA'				, @CUSTOMS_1,  @PRODUCTO_TRANSFERENCIA)
		Set @PRODUCTO_TOLERANCIA_MIN		= dbo.fx_get_param_conf_default('PRODUCTO','TOLERANCIA_MIN'				, @CUSTOMS_1,  @PRODUCTO_TOLERANCIA_MIN)
		Set @PRODUCTO_TOLERANCIA_MAX		= dbo.fx_get_param_conf_default('PRODUCTO','TOLERANCIA_MAX'				, @CUSTOMS_1,  @PRODUCTO_TOLERANCIA_MAX)
		Set @PRODUCTO_BACK_ORDER			= dbo.fx_get_param_conf_default('PRODUCTO','BACK_ORDER'					, @CUSTOMS_1,  @PRODUCTO_BACK_ORDER)
		Set @PRODUCTO_CLASIFICACION_COT		= dbo.fx_get_param_conf_default('PRODUCTO','CLASIFICACION_COT'			, @CUSTOMS_1,  @PRODUCTO_CLASIFICACION_COT)
		Set @PRODUCTO_CODIGO_BARRA			= dbo.fx_get_param_conf_default('PRODUCTO','CODIGO_BARRA'				, @CUSTOMS_1,  @PRODUCTO_CODIGO_BARRA)
		Set @PRODUCTO_ING_CAT_LOG_ID		= dbo.fx_get_param_conf_default('PRODUCTO','ING_CAT_LOG_ID'				, @CUSTOMS_1,  @PRODUCTO_ING_CAT_LOG_ID)
		Set @PRODUCTO_EGR_CAT_LOG_ID		= dbo.fx_get_param_conf_default('PRODUCTO','EGR_CAT_LOG_ID'				, @CUSTOMS_1,  @PRODUCTO_EGR_CAT_LOG_ID)
		Set @PRODUCTO_SUB_FAMILIA_ID		= dbo.fx_get_param_conf_default('PRODUCTO','SUB_FAMILIA_ID'				, @CUSTOMS_1,  @PRODUCTO_SUB_FAMILIA_ID)
		Set @PRODUCTO_TIPO_CONTENEDORA		= dbo.fx_get_param_conf_default('PRODUCTO','TIPO_CONTENEDORA'			, @CUSTOMS_1,  @PRODUCTO_TIPO_CONTENEDORA)
		Set @PRODUCTO_GRUPO_PRODUCTO		= dbo.fx_get_param_conf_default('PRODUCTO','GRUPO_PRODUCTO'				, @CUSTOMS_1,  @PRODUCTO_GRUPO_PRODUCTO)
		Set @PRODUCTO_ENVASE				= dbo.fx_get_param_conf_default('PRODUCTO','ENVASE'						, @CUSTOMS_1,  @PRODUCTO_ENVASE)
		Set @PRODUCTO_VAL_COD_ING			= dbo.fx_get_param_conf_default('PRODUCTO','VAL_COD_ING'				, @CUSTOMS_1,  @PRODUCTO_VAL_COD_ING)
		Set @PRODUCTO_VAL_COD_EGR			= dbo.fx_get_param_conf_default('PRODUCTO','VAL_COD_EGR'				, @CUSTOMS_1,  @PRODUCTO_VAL_COD_EGR)
		Set @PRODUCTO_ROTACION_ID			= dbo.fx_get_param_conf_default('PRODUCTO','ROTACION_ID'				, @CUSTOMS_1,  @PRODUCTO_ROTACION_ID)
		Set @PRODUCTO_FLG_BULTO				= dbo.fx_get_param_conf_default('PRODUCTO','FLG_BULTO'					, @CUSTOMS_1,  @PRODUCTO_FLG_BULTO)
		Set @PRODUCTO_QTY_BULTO				= dbo.fx_get_param_conf_default('PRODUCTO','QTY_BULTO'					, @CUSTOMS_1,  @PRODUCTO_QTY_BULTO)
		Set @PRODUCTO_FLG_VOLUMEN_ETI		= dbo.fx_get_param_conf_default('PRODUCTO','FLG_VOLUMEN_ETI'			, @CUSTOMS_1,  @PRODUCTO_FLG_VOLUMEN_ETI)
		Set @PRODUCTO_QTY_VOLUMEN_ETI		= dbo.fx_get_param_conf_default('PRODUCTO','QTY_VOLUMEN_ETI'			, @CUSTOMS_1,  @PRODUCTO_QTY_VOLUMEN_ETI)
		Set @PRODUCTO_FLG_CONTENEDORA		= dbo.fx_get_param_conf_default('PRODUCTO','FLG_CONTENEDORA'			, @CUSTOMS_1,  @PRODUCTO_FLG_CONTENEDORA)
		Set @PRODUCTO_SERIE_ING				= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','SERIE_ING'			, @CUSTOMS_1,  @PRODUCTO_SERIE_ING),'0')
		Set @PRODUCTO_TIE_IN				= ISNULL(dbo.fx_get_param_conf_default('PRODUCTO','TIE_IN'				, @CUSTOMS_1,  @PRODUCTO_TIE_IN),'0')
		Set @PRODUCTO_ingLoteProveedor		= dbo.fx_get_param_conf_default('PRODUCTO','ingLoteProveedor'			, @CUSTOMS_1,  @PRODUCTO_ingLoteProveedor)
		Set @PRODUCTO_ingPartida			= dbo.fx_get_param_conf_default('PRODUCTO','ingPartida'					, @CUSTOMS_1,  @PRODUCTO_ingPartida)
		Set @PRODUCTO_NRO_PARTIDA_AUTOMATICO= dbo.fx_get_param_conf_default('PRODUCTO','NRO_PARTIDA_AUTOMATICO'		, @CUSTOMS_1,  @PRODUCTO_NRO_PARTIDA_AUTOMATICO)
		Set @PRODUCTO_TRANSF_PICKING		= dbo.fx_get_param_conf_default('PRODUCTO','TRANSF_PICKING'				, @CUSTOMS_1,  @PRODUCTO_TRANSF_PICKING)
		Set @PRODUCTO_ET_TAREA_CONF			= dbo.fx_get_param_conf_default('PRODUCTO','ET_TAREA_CONF'				, @CUSTOMS_1,  @PRODUCTO_ET_TAREA_CONF)
		
		Insert into Producto (	CLIENTE_ID,					PRODUCTO_ID,				CODIGO_PRODUCTO,			SUBCODIGO_1,				SUBCODIGO_2,	
								DESCRIPCION,				NOMBRE,						MARCA,						FRACCIONABLE,				UNIDAD_FRACCION,
								COSTO,						UNIDAD_ID,					TIPO_PRODUCTO_ID,			PAIS_ID,					FAMILIA_ID,
								CRITERIO_ID,				OBSERVACIONES,				POSICIONES_PURAS,			KIT,						SERIE_EGR,
								MONEDA_ID,					NO_AGRUPA_ITEMS,			LARGO,						ALTO,						ANCHO,
								UNIDAD_VOLUMEN,				VOLUMEN_UNITARIO,			PESO,						UNIDAD_PESO,				PESO_UNITARIO,
								LOTE_AUTOMATICO,			PALLET_AUTOMATICO,			INGRESO,					EGRESO,						INVENTARIO,
								TRANSFERENCIA,				TOLERANCIA_MIN,				TOLERANCIA_MAX,				BACK_ORDER,					CLASIFICACION_COT,
								CODIGO_BARRA,				ING_CAT_LOG_ID,				EGR_CAT_LOG_ID,				SUB_FAMILIA_ID,				TIPO_CONTENEDORA,
								GRUPO_PRODUCTO,				ENVASE,						VAL_COD_ING,				VAL_COD_EGR,				ROTACION_ID,
								FLG_BULTO,					QTY_BULTO,					FLG_VOLUMEN_ETI,			QTY_VOLUMEN_ETI,			FLG_CONTENEDORA,
								SERIE_ING,					TIE_IN,						ingLoteProveedor,			ingPartida,					NRO_PARTIDA_AUTOMATICO,
								TRANSF_PICKING,				ET_TAREA_CONF
		)values(				@PRODUCTO_CLIENTE_ID,		@PRODUCTO_PRODUCTO_ID,		@PRODUCTO_CODIGO_PRODUCTO,	@PRODUCTO_SUBCODIGO_1,		@PRODUCTO_SUBCODIGO_2,
								@PRODUCTO_DESCRIPCION,		@PRODUCTO_NOMBRE,			@PRODUCTO_MARCA,			@PRODUCTO_FRACCIONABLE,		@PRODUCTO_UNIDAD_FRACCION,
								@PRODUCTO_COSTO,			@PRODUCTO_UNIDAD_ID,		@PRODUCTO_TIPO_PRODUCTO_ID,	@PRODUCTO_PAIS_ID,			@PRODUCTO_FAMILIA_ID,
								@PRODUCTO_CRITERIO_ID,		@PRODUCTO_OBSERVACIONES,	@PRODUCTO_POSICIONES_PURAS,	@PRODUCTO_KIT,				@PRODUCTO_SERIE_EGR,
								@PRODUCTO_MONEDA_ID,		@PRODUCTO_NO_AGRUPA_ITEMS,	@PRODUCTO_LARGO,			@PRODUCTO_ALTO,				@PRODUCTO_ANCHO,
								@PRODUCTO_UNIDAD_VOLUMEN,	@PRODUCTO_VOLUMEN_UNITARIO,	@PRODUCTO_PESO,				@PRODUCTO_UNIDAD_PESO,		@PRODUCTO_PESO_UNITARIO,
								@PRODUCTO_LOTE_AUTOMATICO,	@PRODUCTO_PALLET_AUTOMATICO,@PRODUCTO_INGRESO,			@PRODUCTO_EGRESO,			@PRODUCTO_INVENTARIO,
								@PRODUCTO_TRANSFERENCIA,	@PRODUCTO_TOLERANCIA_MIN,	@PRODUCTO_TOLERANCIA_MAX,	@PRODUCTO_BACK_ORDER,		@PRODUCTO_CLASIFICACION_COT,
								@PRODUCTO_CODIGO_BARRA,		@PRODUCTO_ING_CAT_LOG_ID,	@PRODUCTO_EGR_CAT_LOG_ID,	@PRODUCTO_SUB_FAMILIA_ID,	@PRODUCTO_TIPO_CONTENEDORA,
								@PRODUCTO_GRUPO_PRODUCTO,	@PRODUCTO_ENVASE,			@PRODUCTO_VAL_COD_ING,		@PRODUCTO_VAL_COD_EGR,		@PRODUCTO_ROTACION_ID,
								@PRODUCTO_FLG_BULTO,		@PRODUCTO_QTY_BULTO,		@PRODUCTO_FLG_VOLUMEN_ETI,	@PRODUCTO_QTY_VOLUMEN_ETI,	@PRODUCTO_FLG_CONTENEDORA,
								@PRODUCTO_SERIE_ING,		@PRODUCTO_TIE_IN,			@PRODUCTO_ingLoteProveedor,	@PRODUCTO_ingPartida,		@PRODUCTO_NRO_PARTIDA_AUTOMATICO,
								@PRODUCTO_TRANSF_PICKING,	@PRODUCTO_ET_TAREA_CONF)
		
		--Los mandatorios para el producto.
		insert into MANDATORIO_PRODUCTO (cliente_id, producto_id, tipo_operacion, campo)values(@PRODUCTO_CLIENTE_ID, @PRODUCTO_PRODUCTO_ID, 'ING', 'CANTIDAD')
		insert into MANDATORIO_PRODUCTO (cliente_id, producto_id, tipo_operacion, campo)values(@PRODUCTO_CLIENTE_ID, @PRODUCTO_PRODUCTO_ID, 'EGR', 'CANTIDAD')

	END
	
	IF (@UNIDAD_ID IS NULL)OR(LTRIM(RTRIM(@UNIDAD_ID))='')BEGIN
		SELECT	@VUNIDAD_ID=UNIDAD_ID
		FROM	PRODUCTO 
		WHERE	CLIENTE_ID=@CLIENTE_ID
				AND PRODUCTO_ID=@PRODUCTO_ID
	END
	ELSE
	BEGIN
		SET @VUNIDAD_ID=@UNIDAD_ID
	END
	
	INSERT INTO [dbo].[SYS_INT_DET_DOCUMENTO]
           ([DOC_EXT],[NRO_LINEA],[CLIENTE_ID],[PRODUCTO_ID],[CANTIDAD_SOLICITADA],[CANTIDAD],[EST_MERC_ID],[CAT_LOG_ID]
           ,[NRO_BULTO],[DESCRIPCION],[NRO_LOTE],[NRO_PALLET],[FECHA_VENCIMIENTO],[NRO_DESPACHO],[NRO_PARTIDA],[UNIDAD_ID]
           ,[UNIDAD_CONTENEDORA_ID],[PESO],[UNIDAD_PESO],[VOLUMEN],[UNIDAD_VOLUMEN],[PROP1],[PROP2],[PROP3],[LARGO],[ALTO]
           ,[ANCHO],[DOC_BACK_ORDER],[ESTADO],[FECHA_ESTADO],[ESTADO_GT],[FECHA_ESTADO_GT],[DOCUMENTO_ID],[NAVE_ID],[NAVE_COD]
           ,CUSTOMS_1, CUSTOMS_2, CUSTOMS_3)
     VALUES	(ltrim(rtrim(@DOC_EXT))
			,@NRO_LINEA
			,ltrim(rtrim(@CLIENTE_ID))
			,ltrim(rtrim(@PRODUCTO_ID))
			,@CANTIDAD_SOLICITADA
			,@CANTIDAD
			,ltrim(rtrim(@EST_MERC_ID))
			,ltrim(rtrim(@CAT_LOG_ID))
			,ltrim(rtrim(@NRO_BULTO))
			,ltrim(rtrim(@DESCRIPCION))
			,ltrim(rtrim(@NRO_LOTE))
			,ltrim(rtrim(@NRO_PALLET))
			,CONVERT(DATETIME,@FECHA_VENCIMIENTO,103)
			,ltrim(rtrim(@NRO_DESPACHO))
			,ltrim(rtrim(@NRO_PARTIDA))
			,ltrim(rtrim(@VUNIDAD_ID))
			,ltrim(rtrim(@UNIDAD_CONTENEDORA_ID))
			,@PESO
			,ltrim(rtrim(@UNIDAD_PESO))
			,@VOLUMEN
			,ltrim(rtrim(@UNIDAD_VOLUMEN))
			,ltrim(rtrim(@PROP1))
			,ltrim(rtrim(@PROP2))
			,ltrim(rtrim(@PROP3))
			,@LARGO
			,@ALTO
			,@ANCHO
			,@DOC_BACK_ORDER
			,@ESTADO
			,CONVERT(DATETIME,@FECHA_ESTADO,103)
			,@ESTADO_GT
			,CONVERT(DATETIME,@FECHA_ESTADO_GT,103)
			,@DOCUMENTO_ID
			,@NAVE_ID
			,@NAVE_COD
			,ltrim(rtrim(@CUSTOMS_1))
			,ltrim(rtrim(@CUSTOMS_2))
			,ltrim(rtrim(@CUSTOMS_3))
			)

END





GO


