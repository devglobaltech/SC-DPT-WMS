IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GET_OC_NUMBER]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[GET_OC_NUMBER]
GO
CREATE FUNCTION DBO.GET_OC_NUMBER(
	@DOCUMENTO_ID	NUMERIC(20,0),
	@NRO_LINEA		NUMERIC(10,0)
)RETURNS VARCHAR(100)
AS
BEGIN	
	DECLARE @RETORNO VARCHAR(100)
	SELECT	@RETORNO=SD.ORDEN_DE_COMPRA
	FROM	PICKING PIK INNER JOIN DET_DOCUMENTO DD		ON(PIK.DOCUMENTO_ID=DD.DOCUMENTO_ID AND PIK.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL		ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID_EGR AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT2	ON(RL.DOC_TRANS_ID=DDT2.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT2.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD2				ON(DDT2.DOCUMENTO_ID=DD2.DOCUMENTO_ID AND DDT2.NRO_LINEA_TRANS=DD2.NRO_LINEA)
			INNER JOIN DOCUMENTO D2						ON(DD2.DOCUMENTO_ID=D2.DOCUMENTO_ID)
			LEFT JOIN SYS_INT_DET_DOCUMENTO SDD			ON(D2.DOCUMENTO_ID=SDD.DOCUMENTO_ID)
			LEFT JOIN SYS_INT_DOCUMENTO SD				ON(SDD.CLIENTE_ID=SD.CLIENTE_ID AND SDD.DOC_EXT=SD.DOC_EXT)
	WHERE	PIK.DOCUMENTO_ID=@DOCUMENTO_ID
			AND PIK.NRO_LINEA=@NRO_LINEA
			
	RETURN @RETORNO		
END