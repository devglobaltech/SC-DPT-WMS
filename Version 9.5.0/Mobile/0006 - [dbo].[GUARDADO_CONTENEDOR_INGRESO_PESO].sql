/****** Object:  UserDefinedFunction [dbo].[GUARDADO_CONTENEDOR_INGRESO_PESO]    Script Date: 04/21/2015 11:44:38 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GUARDADO_CONTENEDOR_INGRESO_PESO]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[GUARDADO_CONTENEDOR_INGRESO_PESO]
GO


CREATE FUNCTION [dbo].[GUARDADO_CONTENEDOR_INGRESO_PESO](
	@DOCUMENTO_ID	NUMERIC(20,0),
	@CONTENEDORA	VARCHAR(100),
	@POSICION_ID	NUMERIC(20,0)
)RETURNS VARCHAR
AS
BEGIN
	DECLARE @POS_PESO	NUMERIC(20,5)
	DECLARE @PRO_PESO	NUMERIC(20,5)
	DECLARE @PRO_CANT	NUMERIC(20,5)
	DECLARE @ING_PESO	NUMERIC(20,5)
	DECLARE @STK_PESO	NUMERIC(20,5)
	DECLARE @RETORNO	VARCHAR(1)
	-----------------------------------------------------------------------
	--Obtengo el control de peso.
	-----------------------------------------------------------------------
	SELECT	@PRO_PESO=SUM(P.PESO), @PRO_CANT=SUM(DD.CANTIDAD)
	FROM	DET_DOCUMENTO DD INNER JOIN PRODUCTO P
			ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.NRO_BULTO=@CONTENEDORA				
	
	SELECT	@POS_PESO=ISNULL(P.PESO,0)
	FROM	POSICION P
	WHERE	POSICION_ID=@POSICION_ID;
	
	IF @POS_PESO=0
	BEGIN
		RETURN '1'
	END
	
	SELECT	@STK_PESO=SUM(X.RESULTADO)
	FROM	(	SELECT	DD.CANTIDAD * ISNULL(P.PESO,0) AS RESULTADO
				FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
						ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_TRANS)
						INNER JOIN RL_DET_DOC_TRANS_POSICION RL
						ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
						INNER JOIN PRODUCTO P
						ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				WHERE	RL.POSICION_ACTUAL=@POSICION_ID
			)X
				
	SET @ING_PESO=@PRO_PESO*@PRO_CANT
	
	IF (ISNULL(@STK_PESO,0) + @ING_PESO)>@POS_PESO 
	BEGIN
		SET @RETORNO='0'--NO PUEDO UBICAR.
	END
	ELSE
	BEGIN
		SET @RETORNO='1'--SI PUEDO UBICAR.
	END
	
	RETURN @RETORNO
END

GO

