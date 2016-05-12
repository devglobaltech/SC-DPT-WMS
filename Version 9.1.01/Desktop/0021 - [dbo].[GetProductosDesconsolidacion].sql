/****** Object:  UserDefinedFunction [dbo].[GetProductosDesconsolidacion]    Script Date: 09/18/2013 11:06:49 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetProductosDesconsolidacion]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[GetProductosDesconsolidacion]
GO

CREATE function [dbo].[GetProductosDesconsolidacion]  
(@viaje_id varchar(50),@nrocarro varchar(20)) returns varchar(30)  
as  
begin  
	declare @Cant as numeric(5,0)  
	
	select	@cant = round(sum(cant_confirmada),0)
	from	picking p  
	where	viaje_id = @viaje_id  
			and nro_ucdesconsolidacion = @nrocarro  
  
	return @cant  
  
end