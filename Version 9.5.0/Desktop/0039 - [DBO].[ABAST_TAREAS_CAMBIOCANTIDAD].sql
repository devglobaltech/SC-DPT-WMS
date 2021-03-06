/****** OBJECT:  STOREDPROCEDURE [DBO].[ABAST_TAREAS_CAMBIOCANTIDAD]    SCRIPT DATE: 03/30/2015 10:42:47 ******/
IF  EXISTS (SELECT * FROM SYS.OBJECTS WHERE OBJECT_ID = OBJECT_ID(N'[ABAST_TAREAS_CAMBIOCANTIDAD]') AND TYPE IN (N'P', N'PC'))
DROP PROCEDURE [DBO].[ABAST_TAREAS_CAMBIOCANTIDAD]
GO

CREATE PROCEDURE [DBO].[ABAST_TAREAS_CAMBIOCANTIDAD]
	@ABAST_ID			BIGINT			OUTPUT,
	@POSICION_COD		VARCHAR(45)		OUTPUT,
	@NRO_LOTE			VARCHAR(100)	OUTPUT,
	@NRO_PARTIDA		VARCHAR(100)	OUTPUT,
	@NRO_SERIE			VARCHAR(100)	OUTPUT,
	@NRO_BULTO			VARCHAR(100)	OUTPUT,
	@CANT_CONFIRMADA	NUMERIC(20,5)	OUTPUT
AS
BEGIN

	DECLARE @CURSOR			CURSOR
	DECLARE @CONSUMO_ID		BIGINT
	DECLARE @CANT_RL		NUMERIC(20,0)
	DECLARE @P_ACT			CHAR(1)
	DECLARE @CONT			NUMERIC(20,0)
	
	SELECT	@CONT=COUNT(*)
	FROM	DET_ABASTECIMIENTO
	WHERE	ABAST_ID=@ABAST_ID
	

	SET @CURSOR=CURSOR FOR
	SELECT	ACL.CONSUMO_ID, ACL.CANTIDAD
	FROM	ABAST_CONSUMO_LOCATOR ACL INNER JOIN RL_DET_DOC_TRANS_POSICION RL		ON(ACL.RL_ID=RL.RL_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT								ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD												ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN POSICION P													ON(RL.POSICION_ACTUAL=P.POSICION_ID)
	WHERE	ACL.ABAST_ID=@ABAST_ID
			AND ((@POSICION_COD IS NULL)OR(P.POSICION_COD=@POSICION_COD))
			AND ((@NRO_LOTE IS NULL)OR(DD.NRO_LOTE=@NRO_LOTE))
			AND ((@NRO_PARTIDA IS NULL)OR(DD.NRO_PARTIDA=@NRO_PARTIDA))
			AND ((@NRO_SERIE IS NULL)OR(DD.NRO_SERIE=@NRO_SERIE))
			AND ((@NRO_BULTO IS NULL)OR(DD.NRO_BULTO=@NRO_BULTO))
	UNION ALL			
	SELECT	ACL.CONSUMO_ID, ACL.CANTIDAD
	FROM	ABAST_CONSUMO_LOCATOR ACL INNER JOIN RL_DET_DOC_TRANS_POSICION RL		ON(ACL.RL_ID=RL.RL_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT								ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD												ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN NAVE N														ON(RL.NAVE_ACTUAL=N.NAVE_ID)
	WHERE	ACL.ABAST_ID=@ABAST_ID
			AND ((@POSICION_COD IS NULL)OR(N.NAVE_COD=@POSICION_COD))
			AND ((@NRO_LOTE IS NULL)OR(DD.NRO_LOTE=@NRO_LOTE))
			AND ((@NRO_PARTIDA IS NULL)OR(DD.NRO_PARTIDA=@NRO_PARTIDA))
			AND ((@NRO_SERIE IS NULL)OR(DD.NRO_SERIE=@NRO_SERIE))
			AND ((@NRO_BULTO IS NULL)OR(DD.NRO_BULTO=@NRO_BULTO))
	
	OPEN @CURSOR
	FETCH @CURSOR INTO @CONSUMO_ID, @CANT_RL
	WHILE @@FETCH_STATUS=0
	BEGIN
		
		SET @P_ACT='0'
		
		IF @CANT_CONFIRMADA=@CANT_RL AND @CANT_CONFIRMADA >0 BEGIN
			
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		EN_PROGRESO='1', 
					CANT_CONFIRMADA=@CANT_CONFIRMADA
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @CANT_CONFIRMADA=0
			
			SET @P_ACT='1'
		END
		
		IF @CANT_CONFIRMADA>@CANT_RL AND @CANT_CONFIRMADA >0 BEGIN
		
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		EN_PROGRESO='1', 
					CANT_CONFIRMADA=@CANT_RL
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @CANT_CONFIRMADA=@CANT_CONFIRMADA-@CANT_RL	
			
			SET @P_ACT='1'	
		END
		
		IF @CANT_CONFIRMADA<@CANT_RL AND @CANT_CONFIRMADA >0 BEGIN
		
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		EN_PROGRESO='1', 
					CANT_CONFIRMADA=@CANT_CONFIRMADA
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @CANT_CONFIRMADA=@CANT_CONFIRMADA-@CANT_RL		
			
			SET @P_ACT='1'
		END		
		
		IF @CANT_CONFIRMADA=0 AND @P_ACT='0' BEGIN
		
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		EN_PROGRESO='1', 
					CANT_CONFIRMADA=@CANT_CONFIRMADA
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @P_ACT='1'
		END
		FETCH @CURSOR INTO @CONSUMO_ID, @CANT_RL
	END--FIN WHILE
	CLOSE @CURSOR
	DEALLOCATE @CURSOR
END --FIN PROCEDURE.