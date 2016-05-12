/****** Object:  UserDefinedFunction [dbo].[CLIENTE_VIAJERUTA]    Script Date: 07/03/2014 16:15:17 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CLIENTE_VIAJERUTA]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[CLIENTE_VIAJERUTA]
GO

CREATE     FUNCTION [dbo].[CLIENTE_VIAJERUTA](
@pViaje_id			as varchar(100), 
@pRuta	 			as varchar(30)
) RETURNS VARCHAR (300)
AS
BEGIN
	Declare @sString as varchar (200)
	Declare @rString as varchar (200)

	Set @rString = ''
	
	DECLARE dcursor CURSOR FOR 	
		SELECT	s.nombre
		FROM	picking p (nolock)
				inner join documento d (nolock) on (p.documento_id = d.documento_id)	
				inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.sucursal_destino = s.sucursal_id)
		WHERE 	viaje_id = @pViaje_id
				AND ruta = @pRuta
		Group by 
				s.sucursal_id, s.nombre
		Order by 
				s.sucursal_id
	
	open dcursor
	fetch next from dcursor into @sString
	WHILE @@FETCH_STATUS = 0
	BEGIN
     		If @rString = ''
			Begin
				set @rString = @sString
			End
		Else
			Begin
				set @rString = @rString + ' - ' + @sString
			End

     		fetch next from dcursor into @sString
	END

CLOSE dcursor
DEALLOCATE dcursor

RETURN @rSTRING

END

GO


