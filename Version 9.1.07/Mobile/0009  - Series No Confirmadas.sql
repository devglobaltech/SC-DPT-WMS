IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SeriesNoConfirmadas]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SeriesNoConfirmadas]
GO
Create procedure dbo.SeriesNoConfirmadas
	@Usuario	as varchar(100)	
as
begin

	select	top 1 IDPROCESO,CLIENTE_ID,NRO_BULTO
	from	CargaSeriesLog
	where	CARGADA='0'
			and USUARIO=@Usuario

end