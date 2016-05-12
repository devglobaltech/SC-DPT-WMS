IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[BuscarPedidosHistoricos]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[BuscarPedidosHistoricos]
GO

CREATE         PROCEDURE [dbo].[BuscarPedidosHistoricos]
@FECHA_DESDE AS VARCHAR(100) output,
@FECHA_HASTA AS VARCHAR(100) output,
@PEDIDO AS VARCHAR(100) output,
@VIAJE_ID AS VARCHAR(100) output
AS
BEGIN
	Declare @RolID		as varchar(5)
	Declare @UsuarioId	as varchar(30)
	Select @RolId=rol_id, @UsuarioId=usuario_id from #temp_usuario_loggin

	select	distinct 
			p.viaje_id
			,0 as [Check]
			,CASE WHEN dbo.STATUS_EXPEDICION(p.viaje_id)='1' THEN 'SI' ELSE 'NO' END as camion
			,dbo.date_picking(p.viaje_id,'1') as f_inicio_picking
			,dbo.date_picking(p.viaje_id,'2') as f_final_picking
			,ROUND(((SUM(p.CANT_CONFIRMADA)*100)/SUM(p.cantidad)),2) as Nivel_Cumplimiento_Picking
			,round(((sum(p.cant_confirmada)*100)/x.cantidad_solicitada),2) as Nivel_Cumplimiento_Pedido
	From	picking p (NoLock)
			Inner Join det_documento dd (NoLock) on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
			inner join DOCUMENTO d (nolock) on(dd.DOCUMENTO_ID=d.DOCUMENTO_ID)
			inner join rl_sys_cliente_usuario su on(p.cliente_id=su.cliente_id)
			left join
			(	select	sum(isnull(cantidad_solicitada,0))cantidad_solicitada, codigo_viaje,s.DOC_EXT
				from	sys_int_det_documento ss inner join sys_int_documento s 
						on(s.cliente_id=ss.cliente_id and s.doc_ext=ss.doc_ext)
				where	s.tipo_documento_id in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R (NoLock) /*where r.rol_id=@RolId*/)
				group by
						codigo_viaje,s.DOC_EXT
			)x on(x.DOC_EXT=d.NRO_REMITO)
	Where	p.fin_picking='2'
			and p.facturado='1' 
			and su.usuario_id=@usuarioid
			and p.viaje_id like @VIAJE_ID
			AND d.NRO_REMITO like @PEDIDO
			and (p.Fecha_inicio between @FECHA_DESDE and @FECHA_HASTA OR (@FECHA_DESDE IS NULL AND @FECHA_HASTA IS NULL))
	group by 
			p.viaje_id, x.cantidad_solicitada
	Having  Dbo.Fx_Procesados(p.viaje_id)=0
END




GO


