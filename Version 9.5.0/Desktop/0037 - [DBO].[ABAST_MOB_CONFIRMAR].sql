/****** Object:  StoredProcedure [dbo].[ABAST_MOB_CONFIRMAR]    Script Date: 04/27/2015 15:36:21 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ABAST_MOB_CONFIRMAR]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ABAST_MOB_CONFIRMAR]
GO

CREATE PROCEDURE [dbo].[ABAST_MOB_CONFIRMAR]
	@ABAST_ID			BIGINT,
	@NRO_LOTE			VARCHAR(100),
	@NRO_PARTIDA		VARCHAR(100),
	@NRO_BULTO			VARCHAR(100),
	@CANT_CONFIRMADA	NUMERIC(20,5),
	@CONTENEDOR			NUMERIC(20,0),
	@USUARIO			VARCHAR(100)
AS
BEGIN

	DECLARE @CURSOR			CURSOR
	DECLARE @CONSUMO_ID		BIGINT
	DECLARE @CANT_RL		NUMERIC(20,0)
	DECLARE @P_ACT			CHAR(1)
	DECLARE @CONTROL		NUMERIC(20,0)
	DECLARE @CONTROL_A		NUMERIC(20,0)
	DECLARE @CONTROL_B		NUMERIC(20,0)

	SET @CURSOR=CURSOR FOR
	SELECT	ACL.CONSUMO_ID, ACL.CANTIDAD
	FROM	ABAST_CONSUMO_LOCATOR ACL INNER JOIN RL_DET_DOC_TRANS_POSICION RL		ON(ACL.RL_ID=RL.RL_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT								ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD												ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN POSICION P													ON(RL.POSICION_ACTUAL=P.POSICION_ID)
	WHERE	ACL.ABAST_ID=@ABAST_ID
			--AND ((@POSICION_COD IS NULL)OR(P.POSICION_COD=@POSICION_COD))
			AND ((@NRO_LOTE IS NULL)OR(DD.NRO_LOTE=@NRO_LOTE))
			AND ((@NRO_PARTIDA IS NULL)OR(DD.NRO_PARTIDA=@NRO_PARTIDA))
			--AND ((@NRO_SERIE IS NULL)OR(DD.NRO_SERIE=@NRO_SERIE))
			AND ((@NRO_BULTO IS NULL)OR(DD.NRO_BULTO=@NRO_BULTO))

	
	OPEN @CURSOR
	FETCH @CURSOR INTO @CONSUMO_ID, @CANT_RL
	WHILE @@FETCH_STATUS=0
	BEGIN
		
		SET @P_ACT='0'
		
		IF @CANT_CONFIRMADA=@CANT_RL AND @CANT_CONFIRMADA >0 AND @P_ACT='0'BEGIN
			
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		FINALIZADO='1'
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @CANT_CONFIRMADA=0
			
			SET @P_ACT='1'
		END
		
		IF @CANT_CONFIRMADA>@CANT_RL AND @CANT_CONFIRMADA >0 AND @P_ACT='0'BEGIN
		
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		FINALIZADO='1'
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @CANT_CONFIRMADA=@CANT_CONFIRMADA-@CANT_RL	
			
			SET @P_ACT='1'	
		END
		
		IF @CANT_CONFIRMADA<@CANT_RL AND @CANT_CONFIRMADA >0 AND @P_ACT='0' BEGIN
		
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		FINALIZADO='1'
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @CANT_CONFIRMADA=@CANT_CONFIRMADA-@CANT_RL		
			
			SET @P_ACT='1'
		END		
		
		IF @CANT_CONFIRMADA=0 AND @P_ACT='0' BEGIN
		
			UPDATE	ABAST_CONSUMO_LOCATOR
			SET		FINALIZADO='1'
			WHERE	CONSUMO_ID=@CONSUMO_ID
			
			SET @P_ACT='1'
		END
		FETCH @CURSOR INTO @CONSUMO_ID, @CANT_RL
	END--FIN WHILE
	CLOSE @CURSOR
	DEALLOCATE @CURSOR
			
	SELECT	@CONTROL=COUNT(*)
	FROM	ABAST_CONSUMO_LOCATOR
	WHERE	ABAST_ID=@ABAST_ID
			--AND ISNULL(FINALIZADO,'0')='0'
			AND CANT_CONFIRMADA=0

	IF @CONTROL>0 BEGIN
		UPDATE	RL_DET_DOC_TRANS_POSICION 
		SET		DISPONIBLE='1'
		FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN ABAST_CONSUMO_LOCATOR A
				ON(RL.RL_ID=A.RL_ID)
		WHERE	A.ABAST_ID=@ABAST_ID
				AND A.CANT_CONFIRMADA='0'
								
		DELETE				
		FROM	ABAST_CONSUMO_LOCATOR
		WHERE	ABAST_ID=@ABAST_ID
				--AND ISNULL(FINALIZADO,'0')='1'
				AND CANT_CONFIRMADA=0		
	END
	
	SELECT	@CONTROL_A=COUNT(*)
	FROM	ABAST_CONSUMO_LOCATOR
	WHERE	ABAST_ID=@ABAST_ID
			AND ISNULL(FINALIZADO,'0')='0'
	
	IF @CONTROL_A=0 BEGIN
		EXEC [DBO].[ABAST_CONFIRMAR_ID]	@ABAST_ID, @USUARIO
	END
	
END --FIN PROCEDURE.

GO


