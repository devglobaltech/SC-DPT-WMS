
ALTER    PROCEDURE [dbo].[DELETEDOC]
@DOCUMENTO_ID AS NUMERIC(20)
AS

DECLARE @DOC_TRANS_ID AS NUMERIC(20)
BEGIN TRANSACTION
BEGIN
	
	SELECT @DOC_TRANS_ID= DOC_TRANS_ID 
	FROM DET_DOCUMENTO_TRANSACCION
	WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DET_DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM HISTORICO_PRODUCTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM HISTORICO_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM AUDITORIA_HISTORICOS WHERE DOCUMENTO_ID=@DOCUMENTO_ID 

	DELETE FROM DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

END 
COMMIT TRANSACTION

/*
DELETEDOC
@DOCUMENTO_ID=18
*/
