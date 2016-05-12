/****** Object:  StoredProcedure [dbo].[verificaOc]    Script Date: 07/16/2013 12:48:39 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[verificaOc]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[VERIFICAOC]
GO
CREATE  PROCEDURE [dbo].[VERIFICAOC] 
	@ID				AS VARCHAR(20),
	@PROVEEDOR_ID	AS VARCHAR(20)
--	@CORRECTO		AS VARCHAR(1) OUTPUT
AS
BEGIN
	declare @CANT_OC		as int
	declare @CANT_REMITO	as int
	declare @CANT_PRODUCTO	as int
	
	SET @CANT_OC =0	
	SET @CANT_REMITO =0
	SET @CANT_PRODUCTO =0
	
	select @CANT_REMITO=count(*) from tmp_remito where id_remito = @ID AND IDPROVEEDOR = @PROVEEDOR_ID and procesado = 0
	
	select @CANT_PRODUCTO=count(*) from tmp_producto where id =@ID and proveedor_id=@PROVEEDOR_ID and procesado=0
	
	IF @CANT_REMITO =0
	BEGIN
		RAISERROR ('Debe ingresar al menos un remito',16,1)
		Return
	END
	
	if @CANT_PRODUCTO =0
	BEGIN
		RAISERROR ('Debe ingresar al menos un producto',16,1)
		Return
	END	
END
