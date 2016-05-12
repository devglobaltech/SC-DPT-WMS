/****** Object:  StoredProcedure [dbo].[MOB_ALTA_PROD_TMP]    Script Date: 07/16/2013 12:48:18 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MOB_ALTA_PROD_TMP]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[MOB_ALTA_PROD_TMP]
GO
CREATE PROCEDURE [dbo].[MOB_ALTA_PROD_TMP] 
	@ID				VARCHAR(20),
	@PROVEEDOR_ID	VARCHAR(20),
	@PRODUCTO_ID	VARCHAR(30),	
	@CANTIDAD		numeric(20, 5)
AS
BEGIN
	declare @DESCRIPCION	VARCHAR(200)
	DECLARE @USUARIO		VARCHAR(20)
	
	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	
	set @DESCRIPCION =''	
	
	select  distinct @DESCRIPCION= p.descripcion 
	from sys_int_documento sd
		inner join sys_int_det_documento sdd on(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		inner join producto p on (p.producto_id=sdd.producto_id)
	where agente_id =@PROVEEDOR_ID AND SDD.PRODUCTO_ID =@PRODUCTO_ID
  
	if @DESCRIPCION =''
		begin
			raiserror('No Existe el producto.',16,1)
			Return
		end
	else
		begin
		INSERT INTO tmp_Producto
							  (Producto_Id, Descripcion, cantidad, PROVEEDOR_ID, ID, USUARIO, PROCESADO)
		VALUES     (@PRODUCTO_ID,@DESCRIPCION,@CANTIDAD,@PROVEEDOR_ID,@ID,@USUARIO,'0')
		end

END