/****** Object:  View [dbo].[view_web_egreso_detallado]    Script Date: 20/04/2016 05:33:14 p.m. ******/
DROP VIEW [dbo].[view_web_egreso_detallado]
GO

CREATE view [dbo].[view_web_egreso_detallado]
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
		,p.PRODUCTO_ID							as articulo
		,p.DESCRIPCION							as descripcion
		,p.CANT_CONFIRMADA						as cantidad_unidades
		,(p.CANT_CONFIRMADA*(ISNULL(pr.peso,0)))as peso
		,(P.CANT_CONFIRMADA*((ISNULL(PR.ALTO,0)*ISNULL(PR.ANCHO,0)*ISNULL(PR.LARGO,0))/1000000))
												as volumen
		,usr_pik.NOMBRE							as usuario_picking
		,ipik.f_inicio							as fecha_inicio_picking
		,pik.fin_picking						as fecha_fin_picking
		,isnull(usr_desc.NOMBRE,p.USUARIO_DESCONSOLIDACION)
												as usuario_desconsolidacion
		,desconsolidacion.f_desconsolidacion	as fecha_desconsolidacion
		,isnull(usr_emp.NOMBRE,p.USUARIO_PF)	as usuario_empaque
		,p.FECHA_UCEMPAQUETADO					as fecha_empaque
		,p.NRO_UCEMPAQUETADO					as nro_contenedora_empaque
		,usr_ctrl_exp.NOMBRE					as usuario_control_expedicion
		,p.FECHA_CONTROL_EXP					as fecha_control_expedicion
		,d.FECHA_FIN_GTW						as fecha_egreso
FROM	dbo.vsys_int_documento sd INNER join VSYS_INT_DET_DOCUMENTO sdd	on(sd.CLIENTE_ID=sdd.CLIENTE_ID and sd.DOC_EXT=sdd.DOC_EXT)
		inner join DOCUMENTO d											on(sd.CLIENTE_ID=d.CLIENTE_ID and sd.DOC_EXT=ISNULL(d.nro_remito,d.nro_despacho_importacion))
		inner join CLIENTE c											on(d.CLIENTE_ID=c.CLIENTE_ID)
		left join SUCURSAL s											on(sd.CLIENTE_ID=s.CLIENTE_ID and sd.AGENTE_ID=s.SUCURSAL_ID)
		left join (select	DOCUMENTO_ID,
							min(FECHA_INICIO)as f_inicio
					from	PICKING
					group by
							DOCUMENTO_ID)ipik							on(d.DOCUMENTO_ID=ipik.DOCUMENTO_ID)
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
		inner join DET_DOCUMENTO dd										on(d.DOCUMENTO_ID=dd.DOCUMENTO_ID)
		inner join PICKING p											on(dd.DOCUMENTO_ID=p.DOCUMENTO_ID and dd.NRO_LINEA=p.NRO_LINEA)
		inner join PRODUCTO pr											on(dd.CLIENTE_ID=pr.CLIENTE_ID and dd.PRODUCTO_ID=pr.PRODUCTO_ID)
		left join SYS_USUARIO usr_pik									on(p.USUARIO=usr_pik.USUARIO_ID)
		left join SYS_USUARIO usr_desc									on(p.USUARIO_DESCONSOLIDACION=usr_desc.USUARIO_ID)
		left join SYS_USUARIO usr_emp									on(p.USUARIO_PF=usr_emp.USUARIO_ID)
		left join SYS_USUARIO usr_ctrl_exp								on(p.USUARIO_CONTROL_EXP=usr_ctrl_exp.USUARIO_ID)
where	D.TIPO_OPERACION_ID='EGR'


GO


