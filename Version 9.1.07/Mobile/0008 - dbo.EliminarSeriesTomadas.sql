IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EliminarSeriesTomadas]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EliminarSeriesTomadas]
GO

create procedure dbo.EliminarSeriesTomadas
	@IDPROCESO			NUMERIC(20,0),
	@CLIENTE_ID			VARCHAR(15),
	@NRO_CONTENEDORA	VARCHAR(100)
as
begin

	delete	
	from	CargaSeriesLog
	where	IDPROCESO=@IDPROCESO
			and CLIENTE_ID=@CLIENTE_ID
			and NRO_BULTO=@NRO_CONTENEDORA

end