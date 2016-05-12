IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_DEV_EGRESO_MOSTRADOR]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_DEV_EGRESO_MOSTRADOR]
GO

CREATE                         PROCEDURE [dbo].[SYS_DEV_EGRESO_MOSTRADOR]
 @pviaje AS varchar(100) output,
 @DOC_EXT AS varchar(100) output
AS
	declare @Qty as numeric(10,0)
	declare @ErrorSave int
	declare @AuxNroLinea bigint
	declare @ControlExpedicion char(1)
	declare @TipoComp	as varchar(5)
	declare @Usuario 	as varchar(20)
	declare @count		as smallint
	declare @controla	as char(1)
BEGIN
begin try
	--Controlo que el viaje no este cerrado
	select @Qty=count(picking_id) from picking where viaje_id=@pViaje and facturado='1'

	--Controlo que el viaje tenga todos los picking's cerrados
	set @Qty=0
	select @Qty=count(picking_id) from picking where (fin_picking in ('0','1') or fin_picking is null) and viaje_id=@pViaje

	select	@Controla=isnull(flg_control_picking,'0')
	from	cliente_parametros 
	where	cliente_id=(select distinct cliente_id from picking(nolock) where viaje_id=@pviaje)

	---------------------------------------------------------------------------------------------------------------------
	--Controlo que el viaje este en el camion
	---------------------------------------------------------------------------------------------------------------------
	SELECT @TipoComp=TIPO_DOCUMENTO_ID FROM SYS_INT_DOCUMENTO WHERE CODIGO_VIAJE=@pviaje
	if @TipoComP ='E04'
	Begin

		select 	distinct 
				@ControlExpedicion=isnull(control_expedicion,'0')
		from 	documento d inner join tipo_comprobante tc
				on(d.tipo_comprobante_id=tc.tipo_comprobante_id)
		where	nro_despacho_importacion=ltrim(rtrim(Upper(@pViaje)))

	End
	Else
	Begin
		select @ControlExpedicion=control_expedicion from tipo_comprobante where tipo_comprobante_id=@TipoComp
	End


	set @Qty=0

	--Controlo que no queden en sys_int_det_documento productos pendientes 
	set @Qty=0
	select @Qty=count(dd.doc_ext) 
	from sys_int_det_documento dd 
		inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext)		
		inner join producto prod on (dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id)
	where dd.estado_gt is null and d.codigo_viaje=@pViaje

	If Dbo.GetTipoDocumento(@pViaje)='E10'
	Begin
		Exec Dbo.Sys_Dev_EgresoE10 @pViaje
		Return
	End
		INSERT INTO SYS_DEV_DOCUMENTO
		SELECT	DISTINCT 
				SID.CLIENTE_ID, 
				CASE WHEN SID.TIPO_DOCUMENTO_ID='E04' THEN 'E05' WHEN SID.TIPO_DOCUMENTO_ID='E08' THEN 'E09' ELSE SID.TIPO_DOCUMENTO_ID END, 
				SID.CPTE_PREFIJO, 
				SID.CPTE_NUMERO, 
				GETDATE(), --FECHA_CPTE, 
				SID.FECHA_SOLICITUD_CPTE, 
				SID.AGENTE_ID, 
				SID.PESO_TOTAL, 
				SID.UNIDAD_PESO, 
				SID.VOLUMEN_TOTAL, 
				SID.UNIDAD_VOLUMEN, 
				SID.TOTAL_BULTOS, 
				SID.ORDEN_DE_COMPRA, 
				SID.OBSERVACIONES, 
				CAST(D.CPTE_PREFIJO AS VARCHAR(20)) + CAST(D.CPTE_NUMERO  AS VARCHAR(20)), 
				SID.NRO_DESPACHO_IMPORTACION, 
				SID.DOC_EXT, 
				SID.CODIGO_VIAJE, 
				SID.INFO_ADICIONAL_1, 
				SID.INFO_ADICIONAL_2, 
				SID.INFO_ADICIONAL_3, 
   				D.TIPO_COMPROBANTE_ID, 	
				NULL, 
				NULL, 
				'P', 
				GETDATE(),
				NULL, --FLG_MOVIMIENTO 
				SID.CUSTOMS_1,
				SID.CUSTOMS_2,
				SID.CUSTOMS_3,
				null as nro_guia,
				null as importe_flete,
				null as transportista_id,
				SID.INFO_ADICIONAL_4,
				SID.INFO_ADICIONAL_5,
				SID.INFO_ADICIONAL_6
		FROM	SYS_INT_DOCUMENTO SID
				INNER JOIN DOCUMENTOS_E07 E ON(SID.DOC_EXT=E.DOC_EXT)
				LEFT JOIN DOCUMENTO D ON (SID.CLIENTE_ID=D.CLIENTE_ID AND SID.DOC_EXT=D.NRO_REMITO)
		WHERE	SID.DOC_EXT=@DOC_EXT
		
		if @@rowcount = 0 begin
			RAISERROR('Error al Insertar en Sys_Dev_Documento, No se inserto la cabecera',16,1)
		end
		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error al Insertar en Sys_Dev_Documento, Codigo_Error: %s',16,1,@ErrorSave)
		END

		insert into sys_dev_det_documento
		select	 d.nro_remito as doc_ext
				,(p.picking_id) as nro_linea
				,dd.cliente_id
				,dd.producto_id
				,dd.cant_solicitada
				,p.cant_confirmada
				,dd.est_merc_id
				,dd.cat_log_id_final
				,dd.nro_bulto as nro_bulto
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
				,ISNULL(dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1),NULL) as prop1
				,ISNULL(dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,2),NULL) as prop2
				,dd.nro_serie
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
				,Null		--Flg_Movimiento
				,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'1')
				,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'2')
				,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'3')
				,null as nro_cmr
		from 	det_documento dd
				inner join documento d on (dd.documento_id=d.documento_id)
				inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
		where	D.NRO_REMITO=@DOC_EXT

		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error al Insertar en Sys_Dev_Det_Documento, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END
	  
		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error al Insertar en Sys_Dev_Det_Documento de los Productos Sin Stock, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END
	 	
		exec DBO.PedidoMultiProducto @pViaje

		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error Al Ejecutar la devolucion Pedido MultiProducto, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END

		
		--Si fue todo bien y no salto por error hago el update en facturado
		Select @Usuario=Usuario_id from #Temp_Usuario_Loggin
		
		update 	picking 
			set 	facturado='1',
				fecha_control_Fac=Getdate(),
				Usuario_Control_fac=@Usuario,
				Terminal_Control_Fac=Host_Name()
		where 	viaje_id=@pViaje


		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error Al Realizar la Actualizacion en Cierre de Picking, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END
end try
begin catch
	exec usp_RethrowError
end catch

END

GO


