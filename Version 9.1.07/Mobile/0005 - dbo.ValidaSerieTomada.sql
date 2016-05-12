IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ValidaSerieTomada]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ValidaSerieTomada]
GO

create procedure dbo.ValidaSerieTomada
	@IDPROCESO			NUMERIC(20,0),
	@CLIENTE_ID			VARCHAR(15),
	@NRO_CONTENEDORA	VARCHAR(100),
	@SERIE				VARCHAR(50),
	@RETORNO			VARCHAR(2) OUTPUT
as
begin
	DECLARE @CONT	AS NUMERIC
	
	select	@CONT=COUNT(*)
	from	CargaSeriesLog
	where	IDPROCESO=@IDPROCESO
			and CLIENTE_ID=@CLIENTE_ID
			and NRO_BULTO=@NRO_CONTENEDORA
			and SERIE=@SERIE
			
	IF @CONT>0 BEGIN
		SET @RETORNO ='1'
	END
	ELSE BEGIN
		SET @RETORNO ='0'
	END
end