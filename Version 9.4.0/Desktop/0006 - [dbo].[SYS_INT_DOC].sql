
/****** Object:  StoredProcedure [dbo].[SYS_INT_DOC]    Script Date: 11/19/2014 10:07:34 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_INT_DOC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_INT_DOC]
GO


/*CREATE*/ CREATE PROCEDURE [dbo].[SYS_INT_DOC]
		   @CLIENTE_ID as varchar(15)			OUTPUT,
           @TIPO_DOCUMENTO_ID as varchar(50)	OUTPUT,
           @CPTE_PREFIJO as varchar(6)			OUTPUT,
           @CPTE_NUMERO as varchar(20)			OUTPUT,
           @FECHA_CPTE as varchar(10)			OUTPUT,
           @FECHA_SOLICITUD_CPTE as varchar(10)	OUTPUT,
           @AGENTE_ID as varchar(20)			OUTPUT,
           @PESO_TOTAL as numeric				OUTPUT,
           @UNIDAD_PESO as varchar(5)			OUTPUT,
           @VOLUMEN_TOTAL as numeric			OUTPUT,
           @UNIDAD_VOLUMEN as varchar(5)		OUTPUT,
           @TOTAL_BULTOS as numeric				OUTPUT,
           @ORDEN_DE_COMPRA as varchar(100)		OUTPUT,
           @OBSERVACIONES as varchar(1000)		OUTPUT,
           @NRO_REMITO as varchar(50)			OUTPUT,
           @NRO_DESPACHO_IMPORTACION as varchar(50)OUTPUT,
           @DOC_EXT as varchar(100)				OUTPUT,
           @CODIGO_VIAJE as varchar(100)		OUTPUT,
           @INFO_ADICIONAL_1 as varchar(100)	OUTPUT,
           @INFO_ADICIONAL_2 as varchar(100)	OUTPUT,
           @INFO_ADICIONAL_3 as varchar(100)	OUTPUT,
           @TIPO_COMPROBANTE as varchar(5)		OUTPUT,
           @ESTADO as varchar(20)				OUTPUT,
           @FECHA_ESTADO as varchar(10)			OUTPUT,
           @ESTADO_GT as varchar(20)			OUTPUT,
           @FECHA_ESTADO_GT as varchar(10)		OUTPUT,
           @INFO_ADICIONAL_4 AS VARCHAR(100)	OUTPUT,
           @INFO_ADICIONAL_5 AS VARCHAR(100)	OUTPUT,
           @INFO_ADICIONAL_6 AS VARCHAR(100)	OUTPUT,
           @CUSTOMS_1		 AS VARCHAR(4000)	OUTPUT,		
           @CUSTOMS_2		 AS VARCHAR(4000)	OUTPUT,
           @CUSTOMS_3		 AS VARCHAR(4000)	OUTPUT,
           @CLASE_PEDIDO	 AS VARCHAR(50)		OUTPUT,
           @TRANSPORTE_ID	 AS VARCHAR(20)		OUTPUT,
           @IMPORTE_FLETE	 AS NUMERIC			OUTPUT
AS
BEGIN
Declare @EXISTE		SmallInt

	SELECT	@EXISTE = COUNT(*)
			FROM	[dbo].[SYS_INT_DOCUMENTO]
			WHERE CLIENTE_ID = @CLIENTE_ID AND DOC_EXT = @DOC_EXT
			
	IF LTRIM(RTRIM(@INFO_ADICIONAL_1))='' BEGIN
		SET @INFO_ADICIONAL_1=NULL;
	END
	
	IF LTRIM(RTRIM(@INFO_ADICIONAL_2))='' BEGIN
		SET @INFO_ADICIONAL_2=NULL;
	END
	
	IF LTRIM(RTRIM(@INFO_ADICIONAL_3))='' BEGIN
		SET @INFO_ADICIONAL_3=NULL;
	END		
	
	IF @EXISTE = 0
	BEGIN
		INSERT INTO [dbo].[SYS_INT_DOCUMENTO]
           ([CLIENTE_ID],[TIPO_DOCUMENTO_ID],[CPTE_PREFIJO],[CPTE_NUMERO],[FECHA_CPTE],[FECHA_SOLICITUD_CPTE],[AGENTE_ID]
           ,[PESO_TOTAL],[UNIDAD_PESO],[VOLUMEN_TOTAL],[UNIDAD_VOLUMEN],[TOTAL_BULTOS],[ORDEN_DE_COMPRA],[OBSERVACIONES]
           ,[NRO_REMITO],[NRO_DESPACHO_IMPORTACION],[DOC_EXT],[CODIGO_VIAJE],[INFO_ADICIONAL_1],[INFO_ADICIONAL_2],[INFO_ADICIONAL_3]
           ,[TIPO_COMPROBANTE],[ESTADO],[FECHA_ESTADO],[ESTADO_GT],[FECHA_ESTADO_GT],[INFO_ADICIONAL_4],INFO_ADICIONAL_5, INFO_ADICIONAL_6
           ,CUSTOMS_1,CUSTOMS_2, CUSTOMS_3, CLASE_PEDIDO, TRANSPORTE_ID, IMPORTE_FLETE)
		VALUES
           (@CLIENTE_ID
           ,ltrim(rtrim(@TIPO_DOCUMENTO_ID))
           ,ltrim(rtrim(@CPTE_PREFIJO))
           ,ltrim(rtrim(@CPTE_NUMERO))
           ,convert(datetime,@FECHA_CPTE,103)
           ,convert(datetime,@FECHA_SOLICITUD_CPTE,103)
           ,ltrim(rtrim(@AGENTE_ID))
           ,@PESO_TOTAL
           ,ltrim(rtrim(@UNIDAD_PESO))
           ,@VOLUMEN_TOTAL
           ,ltrim(rtrim(@UNIDAD_VOLUMEN))
           ,@TOTAL_BULTOS
           ,ltrim(rtrim(@ORDEN_DE_COMPRA))
           ,ltrim(rtrim(@OBSERVACIONES))
		   ,ltrim(rtrim(@NRO_REMITO))
		   ,ltrim(rtrim(@NRO_DESPACHO_IMPORTACION))
		   ,ltrim(rtrim(@DOC_EXT))
		   ,ltrim(rtrim(@CODIGO_VIAJE))
		   ,ltrim(rtrim(@INFO_ADICIONAL_1))
		   ,ltrim(rtrim(@INFO_ADICIONAL_2))
		   ,ltrim(rtrim(@INFO_ADICIONAL_3))
		   ,ltrim(rtrim(@TIPO_COMPROBANTE))
		   ,@ESTADO
		   ,convert(datetime,@FECHA_ESTADO,103)
		   ,@ESTADO_GT
		   ,convert(datetime,@FECHA_ESTADO_GT,103)
		   ,ltrim(rtrim(@INFO_ADICIONAL_4))
		   ,ltrim(rtrim(@INFO_ADICIONAL_5))
		   ,ltrim(rtrim(@INFO_ADICIONAL_6))
		   ,ltrim(rtrim(@CUSTOMS_1))
		   ,ltrim(rtrim(@CUSTOMS_2))
		   ,ltrim(rtrim(@CUSTOMS_3))
		   ,ltrim(rtrim(@CLASE_PEDIDO))
		   ,ltrim(rtrim(@TRANSPORTE_ID))
		   ,@IMPORTE_FLETE)
	END
END



GO


