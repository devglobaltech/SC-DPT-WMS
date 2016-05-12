
/****** Object:  StoredProcedure [dbo].[Get_Estado_Proceso]    Script Date: 10/18/2013 15:29:25 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Get_Estado_Proceso]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Get_Estado_Proceso]
GO

CREATE  PROCEDURE  [dbo].[Get_Estado_Proceso]         
	@Estado as varchar(1) output                
AS         
begin    
	if exists(select id from prioridades_pickeadores)begin    
		select	top 1 
				@Estado = isnull(proceso_activo,'0')    
		from	prioridades_pickeadores    
		order by id desc    
	end    
	else begin    
		set @Estado = '0'     
	end    
	select @Estado
	
end
GO