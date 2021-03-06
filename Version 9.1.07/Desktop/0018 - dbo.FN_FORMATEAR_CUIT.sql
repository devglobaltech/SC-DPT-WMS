IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[FN_FORMATEAR_CUIT]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[FN_FORMATEAR_CUIT]
GO

Create Function dbo.FN_FORMATEAR_CUIT(@PRM_CUIT VARCHAR(100)) RETURNS VARCHAR(100)
BEGIN

	DECLARE @VAR_CUIT VARCHAR(100)

	SET @VAR_CUIT = NULL
	
	IF @PRM_CUIT IS NOT NULL 
	BEGIN
		IF CHARINDEX('-',@PRM_CUIT,1) > 0 
		BEGIN
			SET @VAR_CUIT=@PRM_CUIT
		END
		ELSE
		BEGIN
		   SET @VAR_CUIT = SUBSTRING(@PRM_CUIT,1,2) + '-' +  SUBSTRING(@PRM_CUIT,3,8) + '-' + SUBSTRING(@PRM_CUIT,11,1)
		END
	END
	
	RETURN @VAR_CUIT

END
