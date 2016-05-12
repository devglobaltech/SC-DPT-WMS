/****** Object:  UserDefinedFunction [dbo].[GUARDADO_PALLET_INGRESO_VOLUMEN]    Script Date: 04/20/2015 17:58:23 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GUARDADO_PALLET_INGRESO_VOLUMEN]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[GUARDADO_PALLET_INGRESO_VOLUMEN]
GO

--DROP FUNCTION DBO.GUARDADO_PALLET_INGRESO_VOLUMEN
CREATE FUNCTION [dbo].[GUARDADO_PALLET_INGRESO_VOLUMEN](
	@DOCUMENTO_ID	NUMERIC(20,0),
	@NRO_LINEA		NUMERIC(10,0),
	@NRO_PALLET		VARCHAR(100),
	@POSICION_ID	NUMERIC(20,0)
)RETURNS VARCHAR
AS
BEGIN
	DECLARE @POS_VOLUMEN	NUMERIC(20,9)
	DECLARE @POS_VOL_OCU	NUMERIC(20,9)
	DECLARE @PROD_VOLUMEN	NUMERIC(20,9)
	DECLARE @PCJ_OCUPACION	NUMERIC(20,2)
	DECLARE @OCUPACION		NUMERIC(20,9)
	DECLARE @RETORNO		VARCHAR(2)
	DECLARE @MULTIPROD		NUMERIC(20,0)
	
	SELECT	@MULTIPROD=COUNT(NRO_LINEA)
	FROM	DET_DOCUMENTO
	WHERE	PROP1=@NRO_PALLET
	
	IF @MULTIPROD >1 BEGIN
		SET @MULTIPROD=1
	END 
	----------------------------------------------------------------------------------------------------------------------------------	
	--1. OBTENGO LA VOLUMETRIA DE LA POSICION Y EL PCJ DE OCUPACION. SINO ESTA PRESENTE SALE Y SE PUEDE UBICAR.
	----------------------------------------------------------------------------------------------------------------------------------	
	SELECT	@POS_VOLUMEN	=(ISNULL(ALTO,0) * ISNULL(ANCHO,0) * ISNULL(LARGO,0))/1000000,
			@PCJ_OCUPACION	=PCJ_OCUPACION
	FROM	POSICION
	WHERE	POSICION_ID=@POSICION_ID
	
	IF @POS_VOLUMEN=0
	BEGIN
		--NO SE CARGARON LOS DATOS DE VOLUMETRIA DE LA POSICION. ASI QUE NO TIENE SENTIDO CONTINUAR CON EL ANALISIS.
		RETURN '1'
	END	
				
	----------------------------------------------------------------------------------------------------------------------------------	
	--2. OBTENGO LA VOLUMETRIA DEL PRODUCTO. SINO ESTA PRESENTE SALE Y SE PUEDE UBICAR.
	----------------------------------------------------------------------------------------------------------------------------------
	SELECT	@PROD_VOLUMEN=SUM(X.QTY)* (SUM(X.VOL_UN)/1000000)
	FROM	(	SELECT	DD.PRODUCTO_ID,SUM(RL.CANTIDAD) AS QTY, ISNULL(PR.ALTO,0) * ISNULL(PR.ANCHO,0) * ISNULL(PR.LARGO,0) AS VOL_UN
				FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
						ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
						INNER JOIN RL_DET_DOC_TRANS_POSICION RL 
						ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
						INNER JOIN PRODUCTO PR
						ON(DD.CLIENTE_ID=PR.CLIENTE_ID AND DD.PRODUCTO_ID=PR.PRODUCTO_ID)
				WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
						AND ((@MULTIPROD=1)OR(DD.NRO_LINEA=@NRO_LINEA))
						AND DD.PROP1=@NRO_PALLET
				GROUP BY
						DD.PRODUCTO_ID,  ISNULL(PR.ALTO,0), ISNULL(PR.ANCHO,0), ISNULL(PR.LARGO,0)
			)X
	IF @PROD_VOLUMEN=0
	BEGIN
		RETURN '1'
	END		
					
	----------------------------------------------------------------------------------------------------------------------------------
	--3. OBTENGO LA OCUPACION ACTUAL DE LA POSICION.
	----------------------------------------------------------------------------------------------------------------------------------	
	SELECT	@POS_VOL_OCU=SUM(X.QTY)* (SUM(X.VOL_UN)/1000000)
	FROM	(	SELECT	DD.PRODUCTO_ID,SUM(RL.CANTIDAD) AS QTY, ISNULL(PR.ALTO,0) * ISNULL(PR.ANCHO,0) * ISNULL(PR.LARGO,0) AS VOL_UN
				FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
						ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
						INNER JOIN RL_DET_DOC_TRANS_POSICION RL 
						ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
						INNER JOIN PRODUCTO PR
						ON(DD.CLIENTE_ID=PR.CLIENTE_ID AND DD.PRODUCTO_ID=PR.PRODUCTO_ID)
				WHERE	RL.POSICION_ACTUAL=@POSICION_ID	
				GROUP BY
						DD.PRODUCTO_ID,  ISNULL(PR.ALTO,0), ISNULL(PR.ANCHO,0), ISNULL(PR.LARGO,0)
			)X
			
	----------------------------------------------------------------------------------------------------------------------------------	
	--4. ANALIZO SI SE OCUPA TODO O UN PORCENTAJE DE LA UBICACION.
	----------------------------------------------------------------------------------------------------------------------------------	
	IF @PCJ_OCUPACION<>100
	BEGIN
		SET @POS_VOLUMEN=((@POS_VOLUMEN*@PCJ_OCUPACION)/100)
	END
	
	----------------------------------------------------------------------------------------------------------------------------------	
	--5. ANALIZO EFECTIVAMENTE SI ENTRA O NO EN LA POSICION EL PALLET.
	----------------------------------------------------------------------------------------------------------------------------------	
	SET @POS_VOLUMEN=ISNULL(@POS_VOLUMEN,0) - ISNULL(@POS_VOL_OCU,0)
	
	IF(@POS_VOLUMEN)>=@PROD_VOLUMEN
	BEGIN
		SET @RETORNO='1' --PUEDO UBICAR.
	END
	ELSE
	BEGIN
		SET @RETORNO='0' --NO PUEDO UBICAR.
	END
	
	RETURN @RETORNO;
END

GO


