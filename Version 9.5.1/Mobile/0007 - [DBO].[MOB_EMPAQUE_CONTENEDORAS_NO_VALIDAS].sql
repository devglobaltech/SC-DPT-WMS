/****** Object:  StoredProcedure [DBO].[MOB_EMPAQUE_CONTENEDORAS_NO_VALIDAS]    Script Date: 12/22/2014 15:41:18 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DBO].[MOB_EMPAQUE_CONTENEDORAS_NO_VALIDAS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [DBO].[MOB_EMPAQUE_CONTENEDORAS_NO_VALIDAS]
GO

CREATE PROCEDURE [DBO].[MOB_EMPAQUE_CONTENEDORAS_NO_VALIDAS]
	@CLIENTE_ID		VARCHAR(15),
	@VIAJE_ID		VARCHAR(100)
AS
BEGIN

	SELECT	DISTINCT PALLET_PICKING 
	FROM	PICKING P
	WHERE	P.CLIENTE_ID =@CLIENTE_ID
			AND VIAJE_ID=@VIAJE_ID
			AND PALLET_CONTROLADO <>'1'
			AND NOT EXISTS (SELECT	1
							FROM	MOB_EMPAQUE_EN_PROGRESO M
							WHERE	P.PALLET_PICKING=M.CONTENEDOR
									AND M.ESTADO='VALIDADO')
									
END
	