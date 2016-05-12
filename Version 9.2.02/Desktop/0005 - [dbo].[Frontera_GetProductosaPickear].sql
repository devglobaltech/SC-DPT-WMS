/****** Object:  StoredProcedure [dbo].[Frontera_GetProductosaPickear]    Script Date: 07/10/2014 15:47:43 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_GetProductosaPickear]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_GetProductosaPickear]
GO

CREATE  PROCEDURE [dbo].[Frontera_GetProductosaPickear]
@doc_ext varchar(100) output
AS
BEGIN
	select   dd.cliente_id
			,dd.producto_id
			,sum(dd.cantidad_solicitada) as cantidad_solicitada
			,p.descripcion producto_descripcion
			,p.unidad_id as producto_unidad
			,dd.nro_lote as nro_lote
			,dd.nro_partida as nro_partida
			,dd.prop3 as nro_serie
			,dd.CAT_LOG_ID as cat_log_id
			,dd.EST_MERC_ID as est_merc_id
	from	sys_int_det_documento dd
			inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext) 
			inner join producto p on (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
			inner join #temp_gproductos_viajes tgp on (d.codigo_viaje=tgp.viaje_id and p.grupo_producto=tgp.grupo_producto_id)
	WHERE	dd.DOC_EXT=@doc_ext
			and dd.estado_gt is null
	GROUP BY
			dd.cliente_id, dd.producto_id, p.descripcion, p.unidad_id, 
			dd.nro_lote, dd.nro_partida,dd.prop3,dd.CAT_LOG_ID,dd.EST_MERC_ID

END

GO


