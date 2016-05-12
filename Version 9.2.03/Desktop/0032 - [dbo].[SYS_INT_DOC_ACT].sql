/****** Object:  StoredProcedure [dbo].[SYS_INT_DOC_ACT]    Script Date: 10/02/2014 15:08:18 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_INT_DOC_ACT]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_INT_DOC_ACT]
GO


CREATE PROCEDURE [dbo].[SYS_INT_DOC_ACT]
	@CLIENTE_ID as varchar(15)			OUTPUT,
	@TIPO_DOCUMENTO_ID as varchar(50)	OUTPUT,
	@CPTE_PREFIJO as varchar(6)			OUTPUT,
	@CPTE_NUMERO as varchar(20)			OUTPUT,
	@FECHA_CPTE as varchar(10)			OUTPUT,
	@FECHA_SOLICITUD_CPTE as varchar(10)OUTPUT,
	@AGENTE_ID as varchar(20)			OUTPUT,
	@PESO_TOTAL as numeric				OUTPUT,
	@UNIDAD_PESO as varchar(5)			OUTPUT,
	@VOLUMEN_TOTAL as numeric			OUTPUT,
	@UNIDAD_VOLUMEN as varchar(5)		OUTPUT,
	@TOTAL_BULTOS as numeric			OUTPUT,
	@ORDEN_DE_COMPRA as varchar(100)	OUTPUT,
	@OBSERVACIONES as varchar(1000)		OUTPUT,
	@NRO_REMITO as varchar(50)			OUTPUT,
	@NRO_DESPACHO_IMPORTACION as varchar(50)OUTPUT,
	@DOC_EXT as varchar(100)			OUTPUT,
	@CODIGO_VIAJE as varchar(100)		OUTPUT,
	@INFO_ADICIONAL_1 as varchar(100)	OUTPUT,
	@INFO_ADICIONAL_2 as varchar(100)	OUTPUT,
	@INFO_ADICIONAL_3 as varchar(100)	OUTPUT,
	@TIPO_COMPROBANTE as varchar(5)		OUTPUT,
	@ESTADO as varchar(20)				OUTPUT,
	@FECHA_ESTADO as varchar(10)		OUTPUT,
	@ESTADO_GT as varchar(20)			OUTPUT,
	@FECHA_ESTADO_GT as varchar(10)		OUTPUT,
	@INFO_ADICIONAL_4 as varchar(100)	OUTPUT,
	@INFO_ADICIONAL_5 as varchar(100)	OUTPUT,
	@INFO_ADICIONAL_6 as varchar(100)	OUTPUT,
	@CUSTOMS_1 as varchar(4000)			OUTPUT,
	@CUSTOMS_2 as varchar(4000)			OUTPUT,
	@CUSTOMS_3 as varchar(4000)			OUTPUT,
	@CLASE_PEDIDO as varchar(50)		OUTPUT,
	@TRANSPORTE_ID as varchar(20)		OUTPUT,
	@IMPORTE_FLETE as NUMERIC(20,5)		OUTPUT
AS
begin
		UPDATE	[dbo].[SYS_INT_DOCUMENTO] 
		SET		[TIPO_DOCUMENTO_ID]		=ltrim(rtrim(@TIPO_DOCUMENTO_ID))
				,[CPTE_PREFIJO]			=ltrim(rtrim(@CPTE_PREFIJO))
				,[CPTE_NUMERO]			=ltrim(rtrim(@CPTE_NUMERO))
				,[FECHA_CPTE]			=convert(datetime,@FECHA_CPTE,103)
				,[FECHA_SOLICITUD_CPTE]	=convert(datetime,@FECHA_SOLICITUD_CPTE,103)
				,[AGENTE_ID]			=ltrim(rtrim(@AGENTE_ID))
				,[PESO_TOTAL]			=@PESO_TOTAL
				,[UNIDAD_PESO]			=ltrim(rtrim(@UNIDAD_PESO))
				,[VOLUMEN_TOTAL]		=@VOLUMEN_TOTAL
				,[UNIDAD_VOLUMEN]		=ltrim(rtrim(@UNIDAD_VOLUMEN))
				,[TOTAL_BULTOS]			=@TOTAL_BULTOS
				,[ORDEN_DE_COMPRA]		=ltrim(rtrim(@ORDEN_DE_COMPRA))
				,[OBSERVACIONES]		=ltrim(rtrim(@OBSERVACIONES))
				,[NRO_REMITO]			=ltrim(rtrim(@NRO_REMITO))
				,[NRO_DESPACHO_IMPORTACION]=ltrim(rtrim(@NRO_DESPACHO_IMPORTACION))
				,[DOC_EXT]				=ltrim(rtrim(@DOC_EXT))
				,[CODIGO_VIAJE]			=ltrim(rtrim(@CODIGO_VIAJE))
				,[INFO_ADICIONAL_1]		=ltrim(rtrim(@INFO_ADICIONAL_1))
				,[INFO_ADICIONAL_2]		=ltrim(rtrim(@INFO_ADICIONAL_2))
				,[INFO_ADICIONAL_3]		=ltrim(rtrim(@INFO_ADICIONAL_3))
				,[TIPO_COMPROBANTE]		=ltrim(rtrim(@TIPO_COMPROBANTE))
				,[ESTADO]				=@ESTADO
				,[FECHA_ESTADO]			=convert(datetime,@FECHA_ESTADO,103)
				,[ESTADO_GT]			=@ESTADO_GT
				,[FECHA_ESTADO_GT]		=convert(datetime,@FECHA_ESTADO_GT,103)
				,INFO_ADICIONAL_4		=ltrim(rtrim(@INFO_ADICIONAL_4))
				,INFO_ADICIONAL_5		=ltrim(rtrim(@INFO_ADICIONAL_5))
				,INFO_ADICIONAL_6		=ltrim(rtrim(@INFO_ADICIONAL_6))
				,CUSTOMS_1				=ltrim(rtrim(@CUSTOMS_1))
				,CUSTOMS_2				=ltrim(rtrim(@CUSTOMS_2))
				,CUSTOMS_3				=ltrim(rtrim(@CUSTOMS_3))
				,CLASE_PEDIDO			=ltrim(rtrim(@CLASE_PEDIDO))
				,TRANSPORTE_ID			=ltrim(rtrim(@TRANSPORTE_ID))
				,IMPORTE_FLETE			=@IMPORTE_FLETE
		WHERE	CLIENTE_ID = @CLIENTE_ID AND DOC_EXT = @DOC_EXT
		

END


GO


