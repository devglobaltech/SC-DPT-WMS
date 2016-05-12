/****** Object:  UserDefinedFunction [dbo].[fx_TomaInicial_ValNavePosicion]    Script Date: 10/30/2014 10:23:29 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[fx_TomaInicial_ValNavePosicion]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[fx_TomaInicial_ValNavePosicion]
GO

Create function [dbo].[fx_TomaInicial_ValNavePosicion](@NavePosicion varchar(45))
returns Numeric
as
Begin
	declare @ret as numeric;
	
	select	@ret =count(*)
	from	posicion
	where	posicion_cod =@NavePosicion;
	
	if @ret=0 begin
		select	@ret =count(*)
		from	nave
		where	nave_cod =@NavePosicion
				and nave_tiene_layout='0';
	end

	return @ret;
End
GO
