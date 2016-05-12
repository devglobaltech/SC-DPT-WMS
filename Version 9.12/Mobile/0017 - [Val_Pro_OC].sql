/****** Object:  StoredProcedure [dbo].[Val_Pro_OC]    Script Date: 07/16/2013 12:47:29 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Val_Pro_OC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Val_Pro_OC]
GO
CREATE PROCEDURE [dbo].[Val_Pro_OC]
	@CLIENTE_ID		VARCHAR(15),
	@PROVEEDOR_ID	VARCHAR(20), 
	@PRODUCTO_ID	varchar(30),
	@PRODUCTO		varchar(30) output
AS
BEGIN
	DECLARE @EXISTE 			AS INTEGER

	set		@EXISTE = (select count(*)--sdd.producto_id, p.descripcion 
	from	sys_int_documento sd
			inner join sys_int_det_documento sdd on(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
			inner join producto p on (P.CLIENTE_ID = SD.CLIENTE_ID AND p.producto_id=sdd.producto_id)
	where	agente_id =@PROVEEDOR_ID AND SDD.PRODUCTO_ID =@PRODUCTO_ID AND SD.CLIENTE_ID = @CLIENTE_ID
			AND SDD.ESTADO_GT IS NULL)
	
	select	distinct @PRODUCTO=p.descripcion 
	from	sys_int_documento sd
			inner join sys_int_det_documento sdd on(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
			inner join producto p on (P.CLIENTE_ID = SD.CLIENTE_ID AND p.producto_id=sdd.producto_id)
	where	agente_id =@PROVEEDOR_ID AND SDD.PRODUCTO_ID =@PRODUCTO_ID AND SD.CLIENTE_ID = @CLIENTE_ID

	IF @EXISTE =0		
		BEGIN
			SET @PRODUCTO = NULL
		END

END