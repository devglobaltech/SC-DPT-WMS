/****** Object:  StoredProcedure [dbo].[GET_UBICACION_ANTERIOR]    Script Date: 06/06/2014 10:06:32 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GET_UBICACION_ANTERIOR]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GET_UBICACION_ANTERIOR]
GO

CREATE     PROCEDURE [dbo].[GET_UBICACION_ANTERIOR]
@POSICION_O		AS VARCHAR(45),
@CONTENEDORA	AS VARCHAR(100),
@POSICION_D		AS VARCHAR(45) OUTPUT
AS
BEGIN
	DECLARE @EXISTE 	AS INT
	DECLARE @POS_ANT	AS NUMERIC(20,0)
	DECLARE @NAV_ANT	AS NUMERIC(20,0)

	SELECT 	DISTINCT @POS_ANT=RL.POSICION_ANTERIOR, @NAV_ANT=RL.NAVE_ANTERIOR
	FROM	DET_DOCUMENTO DD 
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	((NRO_BULTO=LTRIM(RTRIM(UPPER(@CONTENEDORA)))) OR(PROP1=LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
			AND RL.DOC_TRANS_ID_EGR IS NULL
			AND RL.DISPONIBLE='1'
			AND RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID
									FROM 	NAVE
									WHERE	NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
								)
			OR RL.POSICION_ACTUAL=	(	SELECT 	TOP 1 POSICION_ID
										FROM 	POSICION
										WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
									)
			AND RL.CANTIDAD >0

	IF (@POS_ANT IS NULL)
	BEGIN
		SELECT	@POSICION_D = NAVE_COD
		FROM	NAVE
		WHERE	NAVE_ID = @NAV_ANT
				AND PRE_EGRESO<>'1'
				AND PRE_INGRESO<>'1'
	END
	ELSE
	BEGIN
		SELECT	@POSICION_D = POSICION_COD
		FROM	POSICION
		WHERE	POSICION_ID = @POS_ANT
	END

END

GO

