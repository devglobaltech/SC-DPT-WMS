/****** Object:  StoredProcedure [dbo].[reservarviaje]    Script Date: 09/18/2013 11:29:41 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[reservarviaje]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[reservarviaje]
GO

create procedure [dbo].[reservarviaje]     
	@viaje_id varchar(50) output,    
	@resultado varchar(1) output  
as    
begin    
	declare @cant as numeric(1,0)  
	declare @usuario as varchar(100)  
	declare @terminal as varchar(100)  
  
	SELECT  @usuario = Su.nombre, @terminal = tul.Terminal    
	FROM    #TEMP_USUARIO_LOGGIN TUL    
			INNER JOIN SYS_USUARIO SU    
			ON (TUL.USUARIO_ID = SU.USUARIO_ID) 
  
	 select  distinct @cant = isnull(estado,'0') from picking where viaje_id = @viaje_id  
	 set @resultado = 1  
  
	 if @cant = 0  
	  begin    
	   update picking set estado = 1,
		 usuario_desconsolidacion = @usuario,
		 terminal_desconsolidacion = @terminal,	 
		 fecha_desconsolidacion = getdate()
		 where viaje_id = @viaje_id      
	  set @resultado = 1  
	 end    
	 else    
	  begin    
	   set @resultado = 0  
	  end;    
	select @resultado as resultado  
end



GO


