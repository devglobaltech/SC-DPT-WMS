
/****** Object:  StoredProcedure [dbo].[EXECUTE_ALL]    Script Date: 12/17/2014 12:06:49 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EXECUTE_ALL]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EXECUTE_ALL]
GO

/* 
 ================================================================================================
 Author:		S.GOMEZ.
 Create date:	06/08/2014
 
 Descripción:

 Procedimiento que se ocupa de realizar el guardado de cada linea de detalle del documento.
 ================================================================================================
*/
CREATE PROCEDURE [dbo].[EXECUTE_ALL]
	@DOCUMENTO_ID	NUMERIC(20,0),
	@NRO_LINEA		NUMERIC(10,0),
	@POS_ID			NUMERIC(20,0) = NULL,
	@VNAVEID		NUMERIC(20,0)
AS

BEGIN

	SET XACT_ABORT ON;
	
	DECLARE @DOCTRANSID		NUMERIC(20,0)
	DECLARE @NROLINEATRANS	NUMERIC(20,0)
	DECLARE @CATLOGID		VARCHAR(50)
	DECLARE @ESTMERCID		VARCHAR(50)
	DECLARE @CATLOGIDFINAL	VARCHAR(50)
	DECLARE @CANTIDAD		NUMERIC(20,5)
	DECLARE @CLIENTE_ID		VARCHAR(15)
	DECLARE @CONTROL		NUMERIC(20,0)
	
	IF @VNAVEID=0 BEGIN
		SET @VNAVEID=NULL
	END
	------------------------------------------------------------
	-- 1. OBTENER VALORES.
	------------------------------------------------------------
    SELECT 	@DOCTRANSID		=DOC_TRANS_ID, 
			@NROLINEATRANS	=NRO_LINEA_TRANS,
			@ESTMERCID		=DDT.EST_MERC_ID,
			@CATLOGID		=DDT.CAT_LOG_ID,
			@CATLOGIDFINAL	=DD.CAT_LOG_ID_FINAL
    FROM    DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
    WHERE	DD.DOCUMENTO_ID =@DOCUMENTO_ID
			AND DD.NRO_LINEA=@NRO_LINEA 
	/*		
	--CURSOR (TODAVIA NO SE PARA QUE).
	SELECT	DISTINCT 
			RL.POSICION_ACTUAL 
	FROM	RL_DET_DOC_TRANS_POSICION RL 
	WHERE	1<>0 
			AND RL.POSICION_ACTUAL IS NOT NULL 
			AND RL.DOC_TRANS_ID =@DOCTRANSID 
			AND RL.NRO_LINEA_TRANS = @NROLINEATRANS 
			AND RL.CAT_LOG_ID_FINAL = @CATLOGIDFINAL
			AND ((@ESTMERCID IS NULL) OR(RL.EST_MERC_ID=@ESTMERCID))
			*/

	--SACO OTRO CURSOR (NO SE PARA QUE.)
	SELECT	@DOCTRANSID		=RL.DOC_TRANS_ID ,
			@NROLINEATRANS	=RL.NRO_LINEA_TRANS ,
			@CANTIDAD		=RL.CANTIDAD,
			@CLIENTE_ID		=RL.CLIENTE_ID
	FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
			ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS) 
			INNER JOIN DOCUMENTO_TRANSACCION DT 
			ON (DT.DOC_TRANS_ID = DDT.DOC_TRANS_ID) 
			INNER JOIN DET_DOCUMENTO DD 
			ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC) 
	WHERE	1<>0 
			AND RL.DOC_TRANS_ID =@DOCTRANSID 
			AND RL.NRO_LINEA_TRANS =@NROLINEATRANS 
			AND ((@CATLOGIDFINAL IS NULL) OR (RL.CAT_LOG_ID_FINAL=@CATLOGIDFINAL))
			AND ((@ESTMERCID IS NULL) OR (RL.EST_MERC_ID=@ESTMERCID))
					
	UPDATE HISTORICO_POS_OCUPADAS2 SET FECHA=Getdate()
	
	DELETE	RL_DET_DOC_TRANS_POSICION 
	WHERE	DOC_TRANS_ID =@DOCTRANSID 
			AND NRO_LINEA_TRANS =@NROLINEATRANS
			AND ((@CATLOGIDFINAL IS NULL) OR (CAT_LOG_ID_FINAL=@CATLOGIDFINAL))
			AND ((@ESTMERCID IS NULL) OR (EST_MERC_ID=@ESTMERCID))

	INSERT INTO RL_DET_DOC_TRANS_POSICION (
			DOC_TRANS_ID,NRO_LINEA_TRANS, POSICION_ANTERIOR, POSICION_ACTUAL, NAVE_ACTUAL,	CANTIDAD,	CLIENTE_ID,	CAT_LOG_ID,	CAT_LOG_ID_FINAL,	EST_MERC_ID, NAVE_ANTERIOR)
	VALUES(	@DOCTRANSID,@NROLINEATRANS,	  NULL,				 @POS_ID,		  @VNAVEID,		@CANTIDAD,	@CLIENTE_ID,'TRAN_ING',	@CATLOGIDFINAL,		@ESTMERCID,	 '1')
   
	UPDATE	DET_DOCUMENTO_TRANSACCION 
	SET		ITEM_OK = '1' , MOVIMIENTO_PENDIENTE = '1' 
	WHERE	DOC_TRANS_ID =@DOCTRANSID
			AND NRO_LINEA_TRANS =@NROLINEATRANS
			AND 0 <> (	SELECT	COUNT(RL_ID)AS PREINGRESO 
						FROM	RL_DET_DOC_TRANS_POSICION RL 
						WHERE	RL.DOC_TRANS_ID =@DOCTRANSID 
								AND RL.NRO_LINEA_TRANS =@NROLINEATRANS 
								AND NAVE_ACTUAL = (SELECT NAVE_ID FROM NAVE WHERE PRE_INGRESO = '1'));   

	UPDATE	POSICION 
	SET		POS_VACIA='0' 
	WHERE	POSICION_ID IN (SELECT	POSICION_ACTUAL 
							FROM	RL_DET_DOC_TRANS_POSICION RL, POSICION P 
							WHERE	RL.POSICION_ACTUAL = P.POSICION_ID 
									AND P.POS_VACIA = '1' 
									AND RL.DOC_TRANS_ID=@DOCTRANSID)	
									
	EXEC DBO.MOB_ELIMINAR_LOCATOR_ING @DOCUMENTO_ID, @NRO_LINEA
	 												              			
END

GO