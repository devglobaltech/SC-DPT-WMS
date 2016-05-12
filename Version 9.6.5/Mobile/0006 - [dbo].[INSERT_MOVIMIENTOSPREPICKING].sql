/****** Object:  StoredProcedure [dbo].[INSERT_MOVIMIENTOSPREPICKING]    Script Date: 02/05/2016 16:07:42 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[INSERT_MOVIMIENTOSPREPICKING]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[INSERT_MOVIMIENTOSPREPICKING]
GO


CREATE Procedure [dbo].[INSERT_MOVIMIENTOSPREPICKING]
@PALLET varchar(100),
@CONTENEDORA varchar(50),
@VIAJE_ID varchar(50),
@UBICACION_ORIGEN varchar(45),
@DOCUMENTO_ID numeric(20,0),
@NRO_LINEA numeric(10,0)

as
Begin

DECLARE @USUARIO	VARCHAR(50)
DECLARE @TERMINAL VARCHAR(100)

	
	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	--SET @USUARIO='SGOMEZ'
	SET @TERMINAL=HOST_NAME() 

	IF @CONTENEDORA IS NOT NULL BEGIN
		INSERT INTO MOVIMIENTOSPREPICKING
		(USUARIO_ID,TERMINAL,PALLET,CONTENEDORA,VIAJE_ID,UBICACION_ORIGEN,DOCUMENTO_ID,NRO_LINEA)
		VALUES
		(@USUARIO,@TERMINAL,@PALLET,@CONTENEDORA,@VIAJE_ID,@UBICACION_ORIGEN,@DOCUMENTO_ID,@NRO_LINEA)
	END ELSE BEGIN
		INSERT INTO MOVIMIENTOSPREPICKING
		(USUARIO_ID,TERMINAL,PALLET,CONTENEDORA,VIAJE_ID,UBICACION_ORIGEN,DOCUMENTO_ID,NRO_LINEA)
		SELECT	@USUARIO,@TERMINAL,@PALLET,DD.NRO_BULTO,@VIAJE_ID,@UBICACION_ORIGEN,DD.DOCUMENTO_ID,DD.NRO_LINEA
		FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
				ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
		WHERE	P.PROP1 =@PALLET
				AND P.CANT_CONFIRMADA IS NULL
				AND P.FECHA_INICIO IS NULL
	END
end

GO

