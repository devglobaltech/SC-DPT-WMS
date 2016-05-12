IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_DEV_EGRESO_EMPAQUE]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_DEV_EGRESO_EMPAQUE]
GO

set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go


CREATE PROCEDURE [dbo].[SYS_DEV_EGRESO_EMPAQUE]
 @pviaje AS varchar(100) output
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

		IF EXISTS (SELECT 1 FROM SYS_DEV_DOCUMENTO WHERE CODIGO_VIAJE = @pviaje)
			RETURN
		--Controlo que el viaje no este cerrado
		select @Qty=count(picking_id) from picking where viaje_id=@pViaje and facturado='1'
		if (@Qty>0) begin
			RAISERROR('El Picking/Viaje ya fue Cerrado!!!!',16,1)
			RETURN 
		end --if

		--Controlo que el viaje tenga todos los picking's cerrados
		set @Qty=0
		select @Qty=count(picking_id) from picking where (fin_picking in ('0','1') or fin_picking is null) and viaje_id=@pViaje
		if (@Qty>0) begin
			RAISERROR('Aun quedan Productos Pendientes por Pickear!!!!',16,1)
			RETURN 	
		end --if

		--Controlo que no queden en sys_int_det_documento productos pendientes 
		set @Qty=0
		select @Qty=count(dd.doc_ext) 
		from sys_int_det_documento dd 
			inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext)		
			inner join producto prod on (dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id)
		where dd.estado_gt is null and d.codigo_viaje=@pViaje
		if (@Qty>0) begin
			RAISERROR('El Picking/Viaje aun Tiene Productos Pendientes por Procesar!!!!',16,1)
			RETURN 	
		end --if

		If Dbo.GetTipoDocumento(@pViaje)='E10'
		Begin
			Exec Dbo.Sys_Dev_EgresoE10 @pViaje
			Return
		End
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
					sid.CUSTOMS_3 
			from	sys_int_documento sid
					left join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
			where	sid.codigo_viaje=@pViaje
					and not exists (select	1 
									from	sys_dev_documento sd 
									where	sd.cliente_id=sid.cliente_id
											and sd.doc_ext=sid.doc_ext)
			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Documento, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END

			insert into sys_dev_det_documento
			select	 DISTINCT
					 d.nro_remito as doc_ext
					,(p.picking_id) as nro_linea
					,dd.cliente_id
					,dd.producto_id
					,isnull(dd.cant_solicitada,sidd.CANTIDAD_SOLICITADA)
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
					,dd.unidad_id
					,null as unidad_contenedora_id
					,null as peso
					,null as unidad_peso
					,null as volumen
					,null as unidad_volumen
					,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1) as prop1
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
					,Null		--Flg_Movimiento
					,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'1')
					,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'2')
					,DBO.GET_SIDD_CUSTOMS(dd.cliente_id,d.nro_remito,dd.producto_id,'3')
			from 	det_documento dd
					inner join documento d on (dd.documento_id=d.documento_id)
					inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
					--JOIN AGREGADO PORQUE TREA MAS REGISTROS DADO QUE EXISTE OTRO DOCUMENTO EN LA TABLA DOCUMENTO
					--QUE TIENE EL MISMO 
					INNER JOIN sys_int_documento sid ON (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
					INNER JOIN SYS_INT_DET_DOCUMENTO sidd on(sidd.DOCUMENTO_ID=dd.DOCUMENTO_ID and sidd.PRODUCTO_ID=dd.PRODUCTO_ID)
			where	p.Viaje_id=@pViaje
					and ((d.tipo_operacion_id is null)or(d.tipo_operacion_id='EGR'))
					and not exists (select 1 from sys_dev_det_documento where sys_dev_det_documento.cliente_id = dd.cliente_id and sys_dev_det_documento.doc_Ext = d.nro_remito and sys_dev_det_documento.nro_linea = p.picking_id)

			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Det_Documento, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END
		  
			--Insert los productos que no ingresaron en el documento por falta de Stock
			insert into sys_dev_det_documento
			select	DISTINCT
					 dd.doc_ext
					,dd.nro_linea
					,dd.cliente_id
					,dd.producto_id
					,dd.cantidad_solicitada
					,0
					,dd.est_merc_id
					,dd.cat_log_id
					,dd.nro_bulto
					,dd.descripcion
					,dd.nro_lote
					,dd.nro_pallet
					,dd.fecha_vencimiento
					,dd.nro_despacho
					,dd.nro_partida
					,dd.unidad_id
					,dd.unidad_contenedora_id
					,dd.peso
					,dd.unidad_peso
					,dd.volumen
					,dd.unidad_volumen
					,dd.prop1
					,dd.prop2
					,dd.prop3
					,dd.largo
					,dd.alto
					,dd.ancho
					,dd.doc_back_order
					,null
					,null
					,dd.estado_gt
					,getdate()
					,dd.documento_id
					,dd.nave_id
					,dd.nave_cod
					,Null --Flg_Movimiento
					,dd.CUSTOMS_1
					,dd.CUSTOMS_2
					,dd.CUSTOMS_3 
			from	sys_int_det_documento dd
					inner join sys_int_documento d on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			where	cast(dd.doc_ext + dd.producto_id as varchar(400))  not in 
					(select cast(doc_ext + producto_id as varchar(400)) from sys_dev_det_documento)
					and d.codigo_viaje=@pViaje
					and not exists (select 1 from sys_dev_det_documento where sys_dev_det_documento.cliente_id = dd.cliente_id and sys_dev_det_documento.doc_Ext = dd.doc_ext and sys_dev_det_documento.nro_linea = dd.nro_linea)

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




