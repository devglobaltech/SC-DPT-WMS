/****** Object:  StoredProcedure [dbo].[Frontera_IngresoxEgreso]    Script Date: 12/11/2014 14:32:31 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_IngresoxEgreso]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_IngresoxEgreso]
GO

CREATE                PROCEDURE [dbo].[Frontera_IngresoxEgreso]
	@VIAJEID  	AS VARCHAR(200) 	output,
	@PEDIDO		AS VARCHAR(100)	output
AS
BEGIN
	SELECT 	dd.producto_id,
			dd.descripcion,
			sum(pic.cant_confirmada)as cantidad,
			'' as cant,
			'' as motivo,
			'' as observacion,
			dd.unidad_id,
			s.nombre,
			d.nro_remito,
			d.sucursal_destino,
			dd.nro_bulto,
			pic.nro_lote,
			pic.nro_partida,
			pic.nro_serie,
			dd.prop1,
			dd.prop2,
			dd.prop3,
			CONVERT(VARCHAR(23),dd.fecha_vencimiento,103) as fecha_vencimiento,
			dd.documento_id,	
			dd.nro_linea,
			'' AS motivo_id,
			p.Fraccionable,
			'' as NRO_CMR
	FROM	vdocumento d (nolock)
			inner join vdet_documento dd (nolock) on (d.documento_id=dd.documento_id) 
			left join sucursal s (nolock) on (s.cliente_id=d.cliente_id and s.sucursal_id = d.sucursal_destino)
			inner join producto p (nolock) on(p.cliente_id=dd.cliente_id and p.producto_id=dd.producto_id)
			inner join vpicking pic(nolock) on(dd.documento_id=pic.documento_id and dd.nro_linea=pic.nro_linea)
	WHERE 	d.nro_despacho_importacion= @VIAJEID
			and ((@pedido is null) or (d.nro_remito like '%'+ @pedido + '%'))
			and d.tipo_operacion_id='EGR'
	      	and STR( dd.nro_linea)+STR(dd.documento_id) NOT IN (	select 	STR(f.nro_linea)+STR(f.documento_id) 
		  														  	from 	#frontera_ing_egr f (nolock) 
																	WHERE 	f.nro_linea = dd.nro_linea  
																			AND f.documento_id=dd.documento_id)	
			and p.envase='0'

	GROUP BY 
			d.documento_id,
			dd.nro_linea,
			dd.producto_id,
			dd.descripcion,
			dd.unidad_id,
			s.nombre,
			d.nro_remito,
			d.sucursal_destino,
			dd.nro_bulto,
			pic.nro_lote,
			pic.nro_serie,
			pic.nro_partida,
			dd.prop1,
			dd.prop2,
			dd.prop3,
			dd.fecha_vencimiento,
			dd.documento_id,	
			dd.nro_linea,
			p.Fraccionable
	HAVING	sum(pic.cant_confirmada)>0
	order by
			DD.producto_id, pic.nro_lote, pic.nro_partida,pic.nro_serie
END

GO


