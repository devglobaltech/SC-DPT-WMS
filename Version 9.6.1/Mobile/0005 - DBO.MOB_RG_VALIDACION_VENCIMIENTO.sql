CREATE PROCEDURE DBO.MOB_RG_VALIDACION_VENCIMIENTO
@CANT_DIAS	AS VARCHAR(100),
@FECHA		AS VARCHAR(100),
@STATUS		AS VARCHAR(1) OUTPUT
AS
BEGIN
	DECLARE @F_CALC	AS DATETIME
	DECLARE @F_COMP		AS DATETIME
	

	SET @F_CALC=DBO.TRUNC(DATEADD(DD,CAST(@CANT_DIAS AS INT),GETDATE()))
	SET @F_COMP=dbo.trunc(CAST(@FECHA AS DATETIME))
	
	--Ej.: 26/12/2015	 25/12/2015
	
	IF @F_CALC > @F_COMP BEGIN
		SET @STATUS ='0'
	END ELSE BEGIN
		SET @STATUS ='1'
	END

END