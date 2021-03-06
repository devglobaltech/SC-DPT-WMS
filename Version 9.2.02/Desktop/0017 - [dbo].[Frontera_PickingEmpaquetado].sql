
/****** Object:  StoredProcedure [dbo].[Frontera_PickingEmpaquetado]    Script Date: 06/03/2014 15:51:39 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_PickingEmpaquetado]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_PickingEmpaquetado]
GO

/****** Object:  StoredProcedure [dbo].[Frontera_PickingEmpaquetado]    Script Date: 06/03/2014 15:51:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE      PROCEDURE [dbo].[Frontera_PickingEmpaquetado]
AS

BEGIN
	Declare @RolID		as varchar(5)
	Declare @UsuarioId	as varchar(30)
	Select @RolId=rol_id, @UsuarioId=usuario_id from #temp_usuario_loggin

	SELECT 	 P.VIAJE_ID									AS [PICKING/VIAJE]
			,D.NRO_REMITO								AS [NRO.PEDIDO]
			,SUM(ISNULL(P.CANT_CONFIRMADA,0))			AS [QTY_BULTOS_PICKEADOS]
			,COUNT(DISTINCT P.NRO_UCDESCONSOLIDACION)	AS [CANT. CONTENEDORAS]
			,S.NOMBRE									AS [SUCURSAL]
			,C.RAZON_SOCIAL								AS [CLIENTE]
	From	documento d (nolock)
			inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
			inner join picking p (nolock) on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			inner join rl_sys_cliente_usuario su on(d.cliente_id=su.cliente_id)
			inner join cliente c on(d.cliente_id=c.cliente_id)
			inner join CLIENTE_PARAMETROS cp on(c.CLIENTE_ID=cp.CLIENTE_ID)
	Where	p.fin_picking in ('2')
			and ((ISNULL(cp.FLG_DESCONSOLIDACION,'0')='0')or(isnull(p.ESTADO,'0') in('2')))
			AND ISNULL(CP.FLG_EMPAQUETADO,'0')='1'
			and p.FACTURADO='0'
			and su.usuario_id=@usuarioid
			and p.NRO_UCEMPAQUETADO is null
			and p.CANT_CONFIRMADA>0
			and dbo.Get_Tipo_Documento_id(d.cliente_id,d.nro_remito) in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R where r.rol_id=@RolId)
			AND D.STATUS <> 'D40'
	GROUP BY 
			p.viaje_id,S.NOMBRE,C.RAZON_SOCIAL,D.NRO_REMITO

END

GO


