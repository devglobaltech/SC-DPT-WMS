IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CERRAR_PALLET]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[CERRAR_PALLET]
GO

CREATE      PROCEDURE [dbo].[CERRAR_PALLET]
@VIAJEID 		AS VARCHAR(30),
@PRODUCTO_ID	AS VARCHAR(50),
@POSICION_COD	AS VARCHAR(45),
@PALLET			AS VARCHAR(100),
@PALLET_PICKING AS NUMERIC(20),
@USUARIO		AS VARCHAR(30),
@RUTA			AS VARCHAR(100)
AS

BEGIN
	UPDATE 	PICKING 
	SET		PALLET_PICKING=NULL,
			FECHA_INICIO=NULL,
			FECHA_FIN=NULL,
			USUARIO=NULL
	WHERE 	VIAJE_ID=@VIAJEID AND PRODUCTO_ID=@PRODUCTO_ID
			AND POSICION_COD=@POSICION_COD
			--AND PROP1=@PALLET
			AND ((@PALLET IS NULL OR @PALLET='')OR(PROP1=@PALLET))
			AND PALLET_PICKING=@PALLET_PICKING
			AND USUARIO=@USUARIO
			AND RUTA=@RUTA
			AND CANT_CONFIRMADA IS NULL
			AND FECHA_FIN IS NULL

END

SELECT * FROM PICKING 
GO


