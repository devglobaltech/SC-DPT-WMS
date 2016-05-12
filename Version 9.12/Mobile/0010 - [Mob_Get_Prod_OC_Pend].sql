/****** Object:  StoredProcedure [dbo].[Mob_Get_Prod_OC_Pend]    Script Date: 07/16/2013 12:49:57 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Get_Prod_OC_Pend]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Get_Prod_OC_Pend]
GO
CREATE PROCEDURE [dbo].[Mob_Get_Prod_OC_Pend]
	@PROVEEDOR_ID	VARCHAR(20),
	@Id				varchar(20)
AS
begin
	declare @USUARIO varchar(20)
	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
		
	SELECT     PRODUCTO_ID as codigo, Descripcion as producto, SUM(cantidad) as can
	FROM         tmp_Producto
	where proveedor_id =@PROVEEDOR_ID and usuario=@usuario
	and id=@id
	GROUP BY PRODUCTO_ID , Descripcion
end


