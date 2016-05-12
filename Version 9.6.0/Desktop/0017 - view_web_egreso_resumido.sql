create view dbo.view_web_egreso_resumido
as
SELECT	distinct
		 d.CLIENTE_ID							as cliente_id
		,d.FECHA_FIN_GTW						as anio
		,d.FECHA_FIN_GTW						as mes
		,sd.FECHA_CPTE							as fecha_pedido
		,d.TIPO_COMPROBANTE_ID					as tipo_egreso
		,d.NRO_DESPACHO_IMPORTACION				as pedido
		,sd.CODIGO_VIAJE						as ola_viaje
		,s.NOMBRE								as destinatario
		,d.FECHA_ALTA_GTW						as fecha_procesado
		,pik.fin_picking						as fecha_fin_picking
		,desconsolidacion.f_desconsolidacion	as fecha_desconsolidacion
		,emp.f_empaquetado						as fecha_fin_empaquetado
		,d.FECHA_FIN_GTW						as fecha_egresado
		,cont.ctn_cont							as cant_unidades_contenedora
FROM	dbo.vsys_int_documento sd INNER join VSYS_INT_DET_DOCUMENTO sdd	on(sd.CLIENTE_ID=sdd.CLIENTE_ID and sd.DOC_EXT=sdd.DOC_EXT)
		inner join DOCUMENTO d											on(sd.CLIENTE_ID=d.CLIENTE_ID and sd.DOC_EXT=d.NRO_DESPACHO_IMPORTACION)
		inner join CLIENTE c											on(d.CLIENTE_ID=c.CLIENTE_ID)
		left join SUCURSAL s											on(sd.CLIENTE_ID=s.CLIENTE_ID and sd.AGENTE_ID=s.SUCURSAL_ID)
		left join (	select	DOCUMENTO_ID,
							MAX(FECHA_FIN)as fin_picking
					from	PICKING
					group by
							DOCUMENTO_ID)pik							on(d.DOCUMENTO_ID=pik.DOCUMENTO_ID)
		left join (	select	documento_id,
							MAX(fecha_desconsolidacion)f_desconsolidacion
					from	picking
					group by
							DOCUMENTO_ID)desconsolidacion				on(d.DOCUMENTO_ID=desconsolidacion.DOCUMENTO_ID)
		left join ( select	documento_id, 
							MAX(FECHA_UCEMPAQUETADO)as f_empaquetado
					from	PICKING
					group by
							documento_id)emp							on(d.DOCUMENTO_ID=emp.DOCUMENTO_ID)
		left join (	select	documento_id,
							COUNT(distinct pallet_picking) as ctn_cont
					from	PICKING
					group by
							documento_id) cont							on(d.DOCUMENTO_ID=cont.DOCUMENTO_ID)
where	D.TIPO_OPERACION_ID='EGR'
