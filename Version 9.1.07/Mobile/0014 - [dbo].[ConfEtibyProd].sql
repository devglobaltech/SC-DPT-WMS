
/****** Object:  StoredProcedure [dbo].[ConfEtibyProd]    Script Date: 05/06/2014 14:14:36 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ConfEtibyProd]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ConfEtibyProd]
GO

CREATE Procedure [dbo].[ConfEtibyProd]
@Cliente_Id		varchar(20),
@Producto_ID	varchar(30),
@Msg			Varchar(max) Output
As
Begin
	Declare @Flg	Char(1)
	Declare @Qty	Numeric(20,0)
	Declare @Count	SmallInt

	Select	@Count=Count(*)
	from	producto
	where	Cliente_id=@Cliente_id
			and Producto_id=@Producto_ID

	if @Count=0
	Begin
		raiserror('No se encontro el producto %s para el cliente %s',16,1,@producto_id,@cliente_id)
		return
	End
	Else
	Begin
		Select	@Flg=flg_bulto, @qty=qty_bulto
		from	producto
		where	Cliente_id=@Cliente_id
				and Producto_id=@Producto_ID

		if (@Flg is null) or (@Flg='0')
		Begin
			Set @Msg='No se generaran etiquetas para este producto.'
			return
		end
		else
		begin
			Set @Msg='Se generaran etiquetas para este producto.'
			return
		end
	end
End

GO


