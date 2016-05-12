/****** Object:  StoredProcedure [dbo].[Mob_Get_Producto_OC]    Script Date: 07/16/2013 12:50:05 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Get_Producto_OC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Get_Producto_OC]
GO
CREATE PROCEDURE [dbo].[Mob_Get_Producto_OC]
	@PROVEEDOR_ID	VARCHAR(20)
	
AS
begin
	DECLARE @USUARIO VARCHAR(20)
	
	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	select CODPRODUCTO, PRODUCTO, SUM(CANT) as Cant FROM
	(
		select sdd.producto_id as CODPRODUCTO, p.descripcion AS PRODUCTO, 
		sd.orden_de_compra as OC, SUM(sdd.cantidad_solicitada) as CANT
		from sys_int_documento sd
		inner join sys_int_det_documento sdd on(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		inner join producto p on (p.producto_id=sdd.producto_id)
		where agente_id =@PROVEEDOR_ID and sdd.estado_gt is null
		AND EXISTS(SELECT * FROM TMP_OC WHERE sd.orden_de_compra = oc AND PROCESADO = 0 and usuario=@usuario) 
		GROUP BY sdd.producto_id , p.descripcion, sd.orden_de_compra 
		UNION ALL
		SELECT     PRODUCTO_ID as codigo, Descripcion as producto,NULL,cantidad * -1 as CANT
		FROM         tmp_Producto
		where proveedor_id =@PROVEEDOR_ID and usuario=@usuario
	) X
	GROUP BY CODPRODUCTO, PRODUCTO
	HAVING SUM(CANT) >0
	
end
