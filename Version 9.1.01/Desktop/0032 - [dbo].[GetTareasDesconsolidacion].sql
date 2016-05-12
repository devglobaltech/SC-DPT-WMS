/****** Object:  StoredProcedure [dbo].[GetTareasDesconsolidacion]    Script Date: 09/18/2013 16:24:34 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetTareasDesconsolidacion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetTareasDesconsolidacion]
GO

CREATE procedure [dbo].[GetTareasDesconsolidacion]  
as  
begin  
	select	distinct   
			'0' as checkbox,  
			viaje_id,   
			usuario_desconsolidacion,  
			terminal_desconsolidacion,  
			max(fecha_desconsolidacion ) as fecha_desconsolidacion
	from	picking   
	where	estado = '1'  
	group by viaje_id,usuario_desconsolidacion,terminal_desconsolidacion
end


GO


