/****** Object:  StoredProcedure [dbo].[CerrarDesconsolidacion]    Script Date: 09/18/2013 18:00:33 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CerrarDesconsolidacion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[CerrarDesconsolidacion]
GO

CREATE procedure [dbo].[CerrarDesconsolidacion]
	@viaje_id varchar(50) output
as
begin
	update picking set estado = 2 
	where viaje_id = @viaje_id
end


GO


