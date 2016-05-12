/****** Object:  StoredProcedure [dbo].[Frontera_GetTareasPickingTomadas]    Script Date: 11/27/2014 11:18:12 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_GetTareasPickingTomadas]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_GetTareasPickingTomadas]
GO


CREATE     PROCEDURE [dbo].[Frontera_GetTareasPickingTomadas]
@viaje_id 		varchar(100) output,
@tipo			int	     output	
AS
BEGIN
	--Tareas en Curso
	if @tipo=1 begin	 
		select	'0' as [check]
				,p.usuario
				,su.nombre
				,p.producto_id
				,p.nro_lote
				,ISNULL(p.nro_partida,dd.NRO_PARTIDA)AS nro_partida
				,p.nro_serie
				,p.descripcion
				,p.cantidad as qty_pick
				,p.posicion_cod
				,p.fecha_inicio
				,p.pallet_picking
				,p.tipo_caja
				,p.ruta
				,p.prop1 as pallet_pick
				,salto_picking
				,picking_id
				,dd.prop2
				,dd.nro_bulto
		from	picking p (nolock) inner join det_documento dd (nolock)
				on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
				inner join sys_usuario su (nolock) on (p.usuario=su.usuario_id)
		where 	p.usuario is not null
				and p.fecha_fin is null
				and p.viaje_id=@viaje_id
				and flg_pallet_hombre=transf_terminada
	end --if

	--Tareas en Pendientes
	if @tipo=2 begin	 
		select	'0' as [CHECK] 
				,p.producto_id
				,p.nro_lote
				,ISNULL(p.nro_partida,dd.NRO_PARTIDA)AS nro_partida
				,p.nro_serie
				,p.descripcion
				,p.cantidad as qty_pick
				,p.posicion_cod
				,p.tipo_caja
				,p.ruta
				,p.prop1 as pallet_pick
				,p.salto_picking
				,p.picking_id
				,dd.prop2
				,dd.nro_bulto
		from	picking p(nolock) inner join det_documento dd (nolock)
				on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
		where 	usuario is null
				and fecha_inicio is null
				and fecha_fin is null
				and viaje_id=@viaje_id
				and flg_pallet_hombre=transf_terminada
	end --if

	--Tareas Finalizadas
	if @tipo=3 begin	 
		select 	'0' as [CHECK] 
				,p.usuario
				,su.nombre	
				,p.producto_id
				,p.nro_lote
				,ISNULL(p.nro_partida,dd.NRO_PARTIDA)AS nro_partida
				,p.nro_serie
				,p.descripcion
				,p.posicion_cod	
				,p.cantidad
				,p.cant_confirmada
				,p.cant_confirmada-p.cantidad as dif
				,p.fecha_inicio
				,p.fecha_fin
				,p.pallet_picking
				,p.tipo_caja
				,p.ruta
				,p.prop1 as pallet_pick
				,p.salto_picking
				,picking_id
				,dd.prop2
				,dd.nro_bulto
		from	picking p (nolock)inner join det_documento dd
				on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
				inner join sys_usuario su (nolock) on (p.usuario=su.usuario_id)	
		where 	p.usuario is not null
				and p.fecha_inicio is not null
				and p.fecha_fin is not null
				and p.viaje_id=@viaje_id
				--and p.facturado='0'
				and flg_pallet_hombre=transf_terminada
	end --if
	if @tipo=4 begin	 
		select	'0' as [CHECK] 
				,p.producto_id
				,p.descripcion
				,p.cantidad as qty_pick
				,p.posicion_cod
				,p.tipo_caja
				,p.ruta
				,p.prop1 as pallet_pick
				,p.salto_picking
				,p.picking_id
				,dd.prop2
				,dd.Nro_bulto
				,p.nro_lote
				,p.nro_partida
				,p.nro_serie				
		from	picking p(nolock) inner join det_documento dd (nolock)
				on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
		where 	usuario is null
				and fecha_inicio is null
				and fecha_fin is null
				and viaje_id=@viaje_id
				--Catalina Castillo.Tracker 4707. Se agrega PICK para que no aparezca como tarea pendiente de transferencia
				and (flg_pallet_hombre= '1' AND P.POSICION_COD<>'PICK')
				and transf_terminada = '0'
	end --if
END



GO


