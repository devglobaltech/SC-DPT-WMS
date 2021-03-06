/****** Object:  StoredProcedure [dbo].[Frontera_PickingEmpaquetado]    Script Date: 10/16/2013 15:28:47 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_PickingEmpaquetado]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_PickingEmpaquetado]
GO

CREATE      PROCEDURE [dbo].[Frontera_PickingEmpaquetado]
AS

BEGIN
	Declare @RolID		as varchar(5)
	Declare @UsuarioId	as varchar(30)
	Select @RolId=rol_id, @UsuarioId=usuario_id from #temp_usuario_loggin

	select 	 p.viaje_id as [PICKING/VIAJE]
			,D.NRO_REMITO AS [NRO.PEDIDO]
			,round((sum(isnull(p.cant_confirmada,0))/sum(p.cantidad)*100),2) as POR_COMPLETO
			,sum(isnull(p.cantidad,0)) as QTY_BULTOS_A_PICKEAR
			,sum(isnull(p.cant_confirmada,0)) as QTY_BULTOS_PICKEADOS
			,cast(pv.prioridad as VARCHAR(20)) as PRIORIDAD_VIAJE
			,dbo.GetPickerMans(p.viaje_id) AS PICKEADORES
			,CASE p.FLG_PALLET_HOMBRE  WHEN '1' THEN 'SI' WHEN '0' THEN 'NO' END AS FLG_PALLET_HOMBRE
			,C.RAZON_SOCIAL
	From	documento d (nolock)
			inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
			inner join picking p (nolock) on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			left join prioridad_viaje pv (nolock) on (p.viaje_id=pv.viaje_id)
			inner join rl_sys_cliente_usuario su on(d.cliente_id=su.cliente_id)
			inner join cliente c on(d.cliente_id=c.cliente_id)
			inner join CLIENTE_PARAMETROS cp on(c.CLIENTE_ID=cp.CLIENTE_ID)
	Where	p.fin_picking in ('2')
			and ((ISNULL(cp.FLG_DESCONSOLIDACION,'0')='0')or(isnull(p.ESTADO,'0') in('2')))
			and p.FACTURADO='0'
			and su.usuario_id=@usuarioid
			and p.NRO_UCEMPAQUETADO is null
			and p.CANT_CONFIRMADA>0
			and dbo.Get_Tipo_Documento_id(d.cliente_id,d.nro_remito) in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R where r.rol_id=@RolId)
	GROUP BY 
			p.viaje_id,pv.prioridad,p.FLG_PALLET_HOMBRE,C.RAZON_SOCIAL,D.NRO_REMITO

END


GO
