/****** Object:  StoredProcedure [dbo].[TomaInicial_ValProducto]    Script Date: 10/30/2014 10:25:10 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TomaInicial_ValProducto]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[TomaInicial_ValProducto]
GO

Create Procedure [dbo].[TomaInicial_ValProducto](	@Cliente_ID		varchar(15),
												@Codigo			varchar(50),
												@Producto_ID	varchar(30)		output,
												@Ret			numeric(20,0)	output
											)
as
Begin

	Select	@Ret =COUNT(*)
	From	RL_PRODUCTO_CODIGOS 
	Where	CLIENTE_ID =@Cliente_ID 
			and CODIGO=@Codigo
			
	if @Ret=0 Begin
		
		select	@ret =count(*)
		from	producto
		where	cliente_id=@cliente_id
				and producto_id=@Codigo
				
		if @Ret=1 begin
		
			Select	@Producto_ID =PRODUCTO_ID
			From	producto 
			Where	CLIENTE_ID =@Cliente_ID 
					and PRODUCTO_ID =@Codigo	
										
		end
	end
	else
	begin
		Select	@Producto_ID =PRODUCTO_ID
		From	RL_PRODUCTO_CODIGOS 
		Where	CLIENTE_ID =@Cliente_ID 
				and CODIGO=@Codigo	
	end
End
GO