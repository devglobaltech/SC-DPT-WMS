IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EliminarSerieTomada]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EliminarSerieTomada]
GO

create procedure dbo.EliminarSerieTomada
	@IDPROCESO			NUMERIC(20,0),
	@CLIENTE_ID			VARCHAR(15),
	@NRO_CONTENEDORA	VARCHAR(100),
	@SERIE				VARCHAR(50)
as
begin

	delete	
	from	CargaSeriesLog
	where	IDPROCESO=@IDPROCESO
			and CLIENTE_ID=@CLIENTE_ID
			and NRO_BULTO=@NRO_CONTENEDORA
			and SERIE=@SERIE
end