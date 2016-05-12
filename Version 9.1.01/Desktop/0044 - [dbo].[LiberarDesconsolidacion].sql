/****** Object:  StoredProcedure [dbo].[LiberarDesconsolidacion]    Script Date: 09/19/2013 12:42:43 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LiberarDesconsolidacion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[LiberarDesconsolidacion]
GO

CREATE procedure [dbo].[LiberarDesconsolidacion]  
@Viaje_id varchar(50) output  
as  
  
begin  
	update picking set estado = 0, nro_ucdesconsolidacion = null where viaje_id = @viaje_id and estado = 1  
	exec cancelardesconsolidacion @viaje_id

end


GO


