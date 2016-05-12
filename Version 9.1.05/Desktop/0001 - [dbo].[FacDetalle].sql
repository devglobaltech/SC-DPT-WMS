
/****** Object:  StoredProcedure [dbo].[FacDetalle]    Script Date: 03/05/2014 11:45:08 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[FacDetalle]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[FacDetalle]
GO

/****** Object:  StoredProcedure [dbo].[FacDetalle]    Script Date: 03/05/2014 11:45:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create Procedure [dbo].[FacDetalle]
@fDesde		as varchar(20)	output,
@fHasta		as varchar(20)	output,
@Pedido		as varchar(100) output,
@viaje		as varchar(100) output,
@cliente	as varchar(30)	output
As
Begin

	Select	 c.razon_social								[Cod. Cliente]
			,s.sucursal_id								[Cod. Sucursal Destinatario]
			,s.nombre									[Razon Social Destinatario]
			,p.viaje_id									[Cod.Viaje]
			,d.nro_remito								[Pedido]
			,p.producto_id								[Cod. Producto]
			,isnull(dd.prop2,'')						[Lote proveedor]
			,p.descripcion								[Desc. Producto]
			,p.cant_confirmada							[Cant. Confirmada]
			,p.posicion_cod								[Posicion]
			,convert(varchar, p.fecha_inicio,103)+' '+
			convert(varchar, p.fecha_inicio,108)		[Fecha Inicio Pick.]
			,convert(varchar, p.fecha_fin, 103)+ ' '+			
			convert(varchar, p.fecha_fin,108)			[Fecha Fin Pick.]
			,su.nombre									[Pickeador]
			,p.pallet_picking							[Pallet Picking]
			,isnull(su2.nombre,'')						[Usuario control picking]
			,convert(varchar,p.fecha_control_exp,103) + ' ' +	
			 dbo.FxTimebyDetime(p.fecha_fin)			[Fecha Control Expedicion]
			,su3.nombre									[Usuario Control Expedicion]
	from	documento d inner join det_documento dd
			on(d.documento_id=dd.documento_id)
			inner join cliente c
			on(c.cliente_id=d.cliente_id)
			inner join sucursal s 
			on(s.cliente_id=d.cliente_id and s.sucursal_id=d.sucursal_destino)
			inner join picking p
			on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			inner join sys_usuario su
			on(su.usuario_id=p.usuario)
			left join sys_usuario su2
			on(su2.usuario_id=p.usuario_control_pick)
			left join sys_usuario su3
			on(su3.usuario_id=p.usuario_control_exp)
	where	((@cliente is null) or(d.cliente_id=@cliente))
			and ((@Pedido is null) or (d.nro_remito=@Pedido))
			and ((@viaje is null) or(p.viaje_id=@viaje))
			and ((@fDesde is null) or(p.fecha_inicio between @fDesde and dateadd(d,1,@fHasta)))
	order by
			d.nro_remito, p.fecha_inicio
End--End Procedure.

GO


