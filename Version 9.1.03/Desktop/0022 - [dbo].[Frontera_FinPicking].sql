IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_FinPicking]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_FinPicking]
GO

CREATE   PROCEDURE [dbo].[Frontera_FinPicking]
AS
BEGIN
	Declare @RolID		as varchar(5)
	Declare @UsuarioId	as varchar(30)
	Select @RolId=rol_id, @UsuarioId=usuario_id from #temp_usuario_loggin

	select	p.viaje_id
			,0 as [Check]
			,CASE WHEN dbo.STATUS_EXPEDICION(p.viaje_id)='1' THEN 'SI' ELSE 'NO' END as camion
			,dbo.date_picking(p.viaje_id,'1') as f_inicio_picking
			,dbo.date_picking(p.viaje_id,'2') as f_final_picking
			,ROUND(((SUM(p.CANT_CONFIRMADA)*100)/SUM(p.cantidad)),2) as Nivel_Cumplimiento_Picking
			,round(((sum(p.cant_confirmada)*100)/x.cantidad_solicitada),2) as Nivel_Cumplimiento_Pedido
			,CASE WHEN Y.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END AS razon_social
	From	picking p (NoLock)
			Inner Join det_documento dd (NoLock) on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
			inner join rl_sys_cliente_usuario su on(p.cliente_id=su.cliente_id)
			inner join documento d(nolock) on(dd.documento_id=d.documento_id)
			inner join cliente c (nolock) on(d.cliente_id=c.cliente_id)
			left join
			(	select	sum(isnull(cantidad_solicitada,0))cantidad_solicitada, codigo_viaje
				from	sys_int_det_documento ss inner join sys_int_documento s 
						on(s.cliente_id=ss.cliente_id and s.doc_ext=ss.doc_ext)
				where	s.tipo_documento_id in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R (NoLock) where r.rol_id=@RolId)
				group by
						codigo_viaje
			)x on(x.codigo_viaje=p.viaje_id)
			inner join(	SELECT	CODIGO_VIAJE,COUNT(distinct SD.CLIENTE_ID) AS CANT_CLIENTE    
						FROM	sys_int_documento SD    
						---WHERE	SD.ESTADO_GT IS NULL  
						GROUP BY 
								SD.CODIGO_VIAJE    
			) as Y on (Y.CODIGO_VIAJE=D.NRO_DESPACHO_IMPORTACION)			
			inner join CLIENTE_PARAMETROS cp on(c.cliente_id=cp.cliente_id)
	Where	p.fin_picking='2'
			and p.facturado='0' 
			and su.usuario_id=@usuarioid
			--para poder determinar si va por desconsolidacion obligatoria.
			and ((ISNULL(cp.FLG_DESCONSOLIDACION,'0')='0')or(isnull(p.ESTADO,'0') in('2')))
			--para poder determinar si va por empaquetado obligatorio
			and ((ISNULL(cp.FLG_EMPAQUETADO,'0')='0')or(P.NRO_UCEMPAQUETADO IS NOT NULL))
			--para distribucion.
			and ((ISNULL(cp.flg_distribucion,'0')='0')or(isnull(p.ST_CONTROL_EXP,'0') IN('1','2')))

	group by 
			p.viaje_id, x.cantidad_solicitada,CASE WHEN Y.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END
	Having  Dbo.Fx_Procesados(p.viaje_id)=0
END
























GO


