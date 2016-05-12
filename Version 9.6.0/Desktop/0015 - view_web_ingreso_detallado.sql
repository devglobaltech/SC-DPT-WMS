create view dbo.view_web_ingreso_detallado
as
SELECT	d.CLIENTE_ID						as cliente_id,
		d.FECHA_ALTA_GTW					as anio,
		d.fecha_alta_gtw					as mes,
		d.TIPO_COMPROBANTE_ID				as tipo_de_ingreso,
		d.ORDEN_DE_COMPRA					as orden_compra,
		D.DOCUMENTO_ID						as documento_id,
		s.NOMBRE							as proveedor,		
		dd.PRODUCTO_ID						as articulo,
		dd.DESCRIPCION						as descripcion,
		sum(dd.CANTIDAD)					as cantidad_ingresada,
		(sum(dd.CANTIDAD)*isnull(p.PESO,0))	as peso,
		(	sum(dd.cantidad)*
			(isnull(p.alto,0)*
			 isnull(p.ancho,0)*
			 isnull(p.largo,0))/1000000)	as volumen,
		f.DESCRIPCION						as familia,
		sf.DESCRIPCION						as subfamilia,
		su.NOMBRE							as usuario_ingreso,
		su2.nombre							as usuario_guardado
FROM	DOCUMENTO D inner join DET_DOCUMENTO dd
		on(d.DOCUMENTO_ID=dd.DOCUMENTO_ID)
		inner join SUCURSAL s
		on(d.CLIENTE_ID=s.CLIENTE_ID and d.SUCURSAL_ORIGEN=s.SUCURSAL_ID)
		inner join PRODUCTO p
		on(dd.CLIENTE_ID=p.CLIENTE_ID and dd.PRODUCTO_ID=p.PRODUCTO_ID)
		inner join FAMILIA_PRODUCTO f
		on(p.FAMILIA_ID=f.FAMILIA_ID)
		inner join SUB_FAMILIA sf
		on(p.SUB_FAMILIA_ID=sf.SUB_FAMILIA_ID)
		left join (	select	DOCUMENTO_ID, NRO_LINEA_DOC,USUARIO_ID
					from	AUDITORIA_HISTORICOS a
					where	TIPO_AUDITORIA_ID='4') ahi
		on(dd.DOCUMENTO_ID=ahi.DOCUMENTO_ID and dd.NRO_LINEA=ahi.NRO_LINEA_DOC)		
		left join SYS_USUARIO su
		on(ahi.USUARIO_ID=su.USUARIO_ID)	
		left join (	select	DISTINCT 
							DOCUMENTO_ID, NRO_LINEA_DOC,USUARIO_ID
					from	AUDITORIA_HISTORICOS a
					where	TIPO_AUDITORIA_ID='1'
							AND CANTIDAD>0) AHG
		on(dd.DOCUMENTO_ID=AHG.DOCUMENTO_ID and dd.NRO_LINEA=AHG.NRO_LINEA_DOC)		
		left JOIN SYS_USUARIO SU2
		ON(AHG.USUARIO_ID=SU2.USUARIO_ID)
WHERE	D.TIPO_OPERACION_ID='ING'
group by
		d.CLIENTE_ID,		d.FECHA_ALTA_GTW,		d.TIPO_COMPROBANTE_ID,
		d.ORDEN_DE_COMPRA,	dd.DESCRIPCION,			s.NOMBRE,
		dd.PRODUCTO_ID,		D.DOCUMENTO_ID,			p.PESO,
		p.ALTO,				p.ANCHO,				p.LARGO,
		f.DESCRIPCION,		sf.DESCRIPCION,			su.NOMBRE,
		SU2.NOMBRE
		
/*
select	pa.DESCRIPCION, a.*
from	AUDITORIA_HISTORICOS a inner join parametros_auditoria pa
		on(a.TIPO_AUDITORIA_ID=pa.TIPO_AUDITORIA_ID)
where	1=1 AND a.DOCUMENTO_ID='6'		
order by
		a.AUDITORIA_ID
		*/
		
---SELECT * FROM SYS_USUARIO WHERE USUARIO_ID='WMS'

