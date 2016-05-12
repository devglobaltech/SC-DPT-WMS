
Create view dbo.view_web_ingreso_resumido
as
SELECT	d.CLIENTE_ID			as cliente_id,
		d.FECHA_ALTA_GTW		as anio,
		d.fecha_alta_gtw		as mes,
		d.TIPO_COMPROBANTE_ID	as tipo_de_ingreso,
		d.ORDEN_DE_COMPRA			as orden_compra,
		dd.PRODUCTO_ID			as articulo,
		dd.DESCRIPCION			as descripcion,
		sum(dd.CANT_SOLICITADA)	as cantidad_solicitada,
		sum(dd.CANTIDAD)		as cantidad_ingresada,
		s.NOMBRE				as proveedor
FROM	DOCUMENTO D inner join DET_DOCUMENTO dd
		on(d.DOCUMENTO_ID=dd.DOCUMENTO_ID)
		inner join SUCURSAL s
		on(d.CLIENTE_ID=s.CLIENTE_ID and d.SUCURSAL_ORIGEN=s.SUCURSAL_ID)
WHERE	D.TIPO_OPERACION_ID='ING'
group by
		d.CLIENTE_ID,		d.FECHA_ALTA_GTW,		d.TIPO_COMPROBANTE_ID,
		d.ORDEN_DE_COMPRA,	dd.DESCRIPCION,			s.NOMBRE,
		dd.PRODUCTO_ID