/****** Object:  StoredProcedure [dbo].[TomaInicial_LotePartida]    Script Date: 10/30/2014 10:26:02 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TomaInicial_LotePartida]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[TomaInicial_LotePartida]
GO

Create Procedure [dbo].[TomaInicial_LotePartida](	@Cliente_ID		varchar(15),
												@Producto_ID	varchar(30),
												@Lote			varchar(1)	OUTPUT,
												@Partida		varchar(1)	OUTPUT
											)
as
Begin
	select	@Lote =isnull(ingLoteProveedor,'0'), @Partida=isnull(ingPartida,'0')
	from	producto
	where	cliente_id=@cliente_id
			and producto_id=@producto_id;
	
End

GO