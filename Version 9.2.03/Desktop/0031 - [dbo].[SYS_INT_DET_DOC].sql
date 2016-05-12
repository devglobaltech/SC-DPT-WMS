/****** Object:  StoredProcedure [dbo].[SYS_INT_DET_DOC]    Script Date: 10/02/2014 14:33:40 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_INT_DET_DOC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_INT_DET_DOC]
GO

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
	DECLARE @VUNIDAD_ID AS VARCHAR(5)
	
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

