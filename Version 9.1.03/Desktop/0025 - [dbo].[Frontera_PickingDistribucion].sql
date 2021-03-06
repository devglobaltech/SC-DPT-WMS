IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_PickingDistribucion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_PickingDistribucion]
GO

CREATE      PROCEDURE [dbo].[Frontera_PickingDistribucion]
AS

BEGIN
	Declare @RolID		as varchar(5)
	Declare @UsuarioId	as varchar(30)
	
	Select	@RolId=rol_id, 
			@UsuarioId=usuario_id 
	from	#temp_usuario_loggin

	select 	p.viaje_id as [PICKING/VIAJE]
			,round((sum(isnull(p.cant_confirmada,0))/sum(p.cantidad)*100),2) as POR_COMPLETO
			,sum(isnull(p.cantidad,0)) as QTY_BULTOS_A_PICKEAR
			,sum(isnull(p.cant_confirmada,0)) as QTY_BULTOS_PICKEADOS
			,cast(pv.prioridad as VARCHAR(20)) as PRIORIDAD_VIAJE
			,dbo.GetPickerMans(p.viaje_id) AS PICKEADORES
			,CASE p.FLG_PALLET_HOMBRE  WHEN '1' THEN 'SI' WHEN '0' THEN 'NO' END AS FLG_PALLET_HOMBRE
			,CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END AS RAZON_SOCIAL
	From	documento d (nolock)
			inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
			inner join picking p (nolock) on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			left join prioridad_viaje pv (nolock) on (p.viaje_id=pv.viaje_id)
			inner join rl_sys_cliente_usuario su on(d.cliente_id=su.cliente_id)
			inner join cliente c on(d.cliente_id=c.cliente_id)
			inner join CLIENTE_PARAMETROS cp on(c.CLIENTE_ID=cp.CLIENTE_ID)
			inner join(	SELECT	CODIGO_VIAJE,COUNT(distinct SD.CLIENTE_ID) AS CANT_CLIENTE    
						FROM	sys_int_documento SD    
						--WHERE	SD.ESTADO_GT IS NULL  
						GROUP BY 
								SD.CODIGO_VIAJE    
			) as X on (X.CODIGO_VIAJE=d.NRO_DESPACHO_IMPORTACION)				
	Where	p.fin_picking in ('2')
			and p.FACTURADO='0'
			and su.usuario_id=@usuarioid
			and p.ST_CAMION='0'
			and p.CANT_CONFIRMADA>0
			and dbo.Get_Tipo_Documento_id(d.cliente_id,d.nro_remito) in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R where r.rol_id=@RolId)
			--para poder determinar si va por desconsolidacion obligatoria.
			and ((ISNULL(cp.FLG_DESCONSOLIDACION,'0')='0')or(isnull(p.ESTADO,'0') in('2')))
			--para poder determinar si va por empaquetado obligatorio
			and ((ISNULL(cp.FLG_EMPAQUETADO,'0')='0')or(P.NRO_UCEMPAQUETADO IS NOT NULL))	
			AND ISNULL(CP.FLG_DISTRIBUCION,'0')='1'		
	GROUP BY 
			p.viaje_id,pv.prioridad,p.FLG_PALLET_HOMBRE,CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END

END



GO


