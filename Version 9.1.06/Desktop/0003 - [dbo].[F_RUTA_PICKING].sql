IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[F_RUTA_PICKING]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[F_RUTA_PICKING]
GO

CREATE FUNCTION [dbo].[F_RUTA_PICKING](
	@Cliente_id	as varchar(100),
	@Doc_ext	as varchar(100)
)returns  varchar(100)
begin
	declare @Ruta	varchar(100)
	
	select	@Ruta=isnull(info_adicional_1,'1')
	from	sys_int_documento
	where	cliente_id=@Cliente_id
			and doc_ext=@Doc_ext

	if (ltrim(rtrim(@ruta))='')begin
		Set @ruta='1'
	end
	return @Ruta	
end



