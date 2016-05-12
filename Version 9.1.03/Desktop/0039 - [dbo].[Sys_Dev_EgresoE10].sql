IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_Dev_EgresoE10]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Sys_Dev_EgresoE10]
GO

CREATE  Procedure [dbo].[Sys_Dev_EgresoE10]
	@pviaje AS varchar(100) output
As
Begin
	Declare @Usuario	as Varchar(30)

	insert into sys_dev_documento
	select	distinct 
			sid.CLIENTE_ID, 
			CASE WHEN sid.tipo_documento_id='E04' THEN 'E05' WHEN sid.tipo_documento_id='E08' THEN 'E09' ELSE sid.tipo_documento_id END, 
			sid.CPTE_PREFIJO, 
			sid.CPTE_NUMERO, 
			getdate(), --FECHA_CPTE, 
			sid.FECHA_SOLICITUD_CPTE, 
			sid.AGENTE_ID, 
			sid.PESO_TOTAL, 
			sid.UNIDAD_PESO, 
			sid.VOLUMEN_TOTAL, 
			sid.UNIDAD_VOLUMEN, 
			sid.TOTAL_BULTOS, 
			sid.ORDEN_DE_COMPRA, 
			sid.OBSERVACIONES, 
			cast(d.cpte_prefijo as varchar(20)) + cast(d.cpte_numero  as varchar(20)), 
			sid.NRO_DESPACHO_IMPORTACION, 
			sid.DOC_EXT, 
			sid.CODIGO_VIAJE, 
			sid.INFO_ADICIONAL_1, 
			sid.INFO_ADICIONAL_2, 
			sid.INFO_ADICIONAL_3, 
			d.TIPO_COMPROBANTE_id, 	
			NULL, 
			NULL, 
			'P', 
			GETDATE(),
			Null, --Flg_Movimiento
			sid.CUSTOMS_1,
			sid.CUSTOMS_2,
			sid.CUSTOMS_3,
			null as nro_guia,
			null as importe_flete,
			null as transportista_id,
			sid.INFO_ADICIONAL_4,
			sid.INFO_ADICIONAL_5,
			sid.INFO_ADICIONAL_6
	from	sys_int_documento sid
			left join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
	where	sid.codigo_viaje=@pViaje


	insert into sys_dev_det_documento
	select	 d.nro_remito as doc_ext
			,(p.picking_id) as nro_linea
			,dd.cliente_id
			,dd.producto_id
			,dd.cant_solicitada
			,p.cant_confirmada
			,dd.est_merc_id
			,dd.cat_log_id_final
			,null as nro_bulto
			,dd.descripcion
			,dd.nro_lote
			,dd.prop1 as nro_pallet
			,dd.fecha_vencimiento
			,null as nro_despacho
			,dd.nro_partida
			,unidad_id
			,null as unidad_contenedora_id
			,null as peso
			,null as unidad_peso
			,null as volumen
			,null as unidad_volumen
			,Case  When D.Tipo_Comprobante_ID='E10'
				then  	DBO.GetValuesSysIntIA1(d.Documento_id, dd.Nro_linea) 
				Else 	dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1)
			 End As prop1
			,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,2) as prop2
			,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,3) as prop3
			,null as largo
			,null as alto
			,dd.nro_linea as ancho --nro de linea
			,null as doc_back_order
			,null as estado
			,null as fecha_estado
			,'P' as estado_gt
			,getdate() as fecha_estado_gt
			,p.documento_id
			,dbo.Aj_NaveCod_to_Nave_id(p.nave_cod) as nave_id
			,p.nave_cod	
			,Null --Flg_movimiento
			,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'1')
			,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'2')
			,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'3')
			,null as nro_cmr
	from 	det_documento dd
			inner join documento d on (dd.documento_id=d.documento_id)
			inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
	where
			p.Viaje_id=@pViaje

	Select @Usuario=Usuario_id from #Temp_Usuario_Loggin
	
	update 	picking 
		set 	facturado='1',
			fecha_control_Fac=Getdate(),
			Usuario_Control_fac=@Usuario,
			Terminal_Control_Fac=Host_Name()
	where 	viaje_id=@pViaje

End --Fin Procedure

GO


