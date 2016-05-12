/****** Object:  StoredProcedure [dbo].[SYS_INT_DET_DOC_ACT]    Script Date: 10/02/2014 15:14:12 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_INT_DET_DOC_ACT]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_INT_DET_DOC_ACT]
GO

/*CREATE*/ CREATE PROCEDURE [dbo].[SYS_INT_DET_DOC_ACT]
			@DOC_EXT as varchar(100)		OUTPUT,
			@NRO_LINEA as numeric			OUTPUT,
			@CLIENTE_ID as varchar(15)		OUTPUT,
			@PRODUCTO_ID as varchar(30)		OUTPUT,
			@CANTIDAD_SOLICITADA as numeric	OUTPUT,
			@CANTIDAD as numeric			OUTPUT,
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
			@CUSTOMS_1 as varchar(4000)		OUTPUT,
			@CUSTOMS_2 as varchar(4000)		OUTPUT,
			@CUSTOMS_3 as varchar(4000)		OUTPUT

AS
BEGIN

	UPDATE [dbo].[SYS_INT_DET_DOCUMENTO]
       SET  [PRODUCTO_ID]			=ltrim(rtrim(@PRODUCTO_ID))
			,[CANTIDAD_SOLICITADA]	=@CANTIDAD_SOLICITADA
			,[CANTIDAD]				=@CANTIDAD
			,[EST_MERC_ID]			=ltrim(rtrim(@EST_MERC_ID))
			,[CAT_LOG_ID]			=ltrim(rtrim(@CAT_LOG_ID))
			,[NRO_BULTO]			=ltrim(rtrim(@NRO_BULTO))
			,[DESCRIPCION]			=ltrim(rtrim(@DESCRIPCION))
			,[NRO_LOTE]				=ltrim(rtrim(@NRO_LOTE))
			,[NRO_PALLET]			=ltrim(rtrim(@NRO_PALLET))
			,[FECHA_VENCIMIENTO]	=CONVERT(DATETIME,@FECHA_VENCIMIENTO,103)
			,[NRO_DESPACHO]			=ltrim(rtrim(@NRO_DESPACHO))
			,[NRO_PARTIDA]			=ltrim(rtrim(@NRO_PARTIDA))
			,[UNIDAD_ID]			=ltrim(rtrim(@UNIDAD_ID))
			,[UNIDAD_CONTENEDORA_ID]=ltrim(rtrim(@UNIDAD_CONTENEDORA_ID))
			,[PESO]					=@PESO
			,[UNIDAD_PESO]			=ltrim(rtrim(@UNIDAD_PESO))
			,[VOLUMEN]				=@VOLUMEN
			,[UNIDAD_VOLUMEN]		=ltrim(rtrim(@UNIDAD_VOLUMEN))
			,[PROP1]				=ltrim(rtrim(@PROP1))
			,[PROP2]				=ltrim(rtrim(@PROP2))
			,[PROP3]				=ltrim(rtrim(@PROP3))
			,[LARGO]				=@LARGO
			,[ALTO]					=@ALTO
            ,[ANCHO]				=@ANCHO
			,[DOC_BACK_ORDER]		=@DOC_BACK_ORDER
			,[ESTADO]				=@ESTADO
			,[FECHA_ESTADO]			=CONVERT(DATETIME,@FECHA_ESTADO,103)
			,[ESTADO_GT]			=@ESTADO_GT
			,[FECHA_ESTADO_GT]		=CONVERT(DATETIME,@FECHA_ESTADO_GT,103)
			,[DOCUMENTO_ID]			=@DOCUMENTO_ID
			,[NAVE_ID]				=@NAVE_ID
			,[NAVE_COD]				=@NAVE_COD
			,CUSTOMS_1				=ltrim(rtrim(@CUSTOMS_1))
			,CUSTOMS_2				=ltrim(rtrim(@CUSTOMS_2))
			,CUSTOMS_3				=ltrim(rtrim(@CUSTOMS_3))
	WHERE	CLIENTE_ID = @CLIENTE_ID 
			AND DOC_EXT = @DOC_EXT 
			AND NRO_LINEA = @NRO_LINEA


	DELETE 
	FROM	SYS_INT_DET_DOCUMENTO
	WHERE	CLIENTE_ID = @CLIENTE_ID AND DOC_EXT = @DOC_EXT AND NRO_LINEA = @NRO_LINEA
			AND CANTIDAD_SOLICITADA=0


END


GO


