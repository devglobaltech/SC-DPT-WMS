/****** Object:  StoredProcedure [dbo].[ValidarViaje]    Script Date: 09/18/2013 17:59:35 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ValidarViaje]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ValidarViaje]
GO

CREATE procedure [dbo].[ValidarViaje]  
@viaje_id varchar(50) output  
  
as  
begin  
 select count(picking_id)  as cantidad  
  
 from picking p  
  
 where nro_ucdesconsolidacion is null and viaje_id = @viaje_id   
	AND CANT_CONFIRMADA > 0
end


GO


