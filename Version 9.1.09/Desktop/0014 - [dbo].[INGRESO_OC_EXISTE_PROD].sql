

/****** Object:  StoredProcedure [dbo].[INGRESO_OC_EXISTE_PROD]    Script Date: 06/11/2014 15:22:39 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[INGRESO_OC_EXISTE_PROD]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[INGRESO_OC_EXISTE_PROD]
GO


/****** Object:  StoredProcedure [dbo].[INGRESO_OC_EXISTE_PROD]    Script Date: 06/11/2014 15:22:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[INGRESO_OC_EXISTE_PROD]
	@CLIENTE_ID		varchar(15),
	@PRODUCTO_ID	varchar(30),
	@ORDEN_COMPRA	varchar(100),
	@LOTE_PROVEEDOR	varchar(100),
	@PARTIDA		varchar(100)
AS
	SELECT	producto_id
	FROM	INGRESO_OC
	WHERE   CLIENTE_ID = @CLIENTE_ID
			AND PRODUCTO_ID = @PRODUCTO_ID
			AND ORDEN_COMPRA = @ORDEN_COMPRA
			AND ISNULL(PROCESADO,'0') = '0'
			AND NRO_LOTE=@LOTE_PROVEEDOR
			AND NRO_PARTIDA = @PARTIDA
			AND 1=2									--Agrego esto para saltear esta validacion

GO


