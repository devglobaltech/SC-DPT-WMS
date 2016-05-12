IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_GetDocumentosEgresos]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_GetDocumentosEgresos]
GO

CREATE            PROCEDURE [dbo].[Frontera_GetDocumentosEgresos]
AS

BEGIN
	Declare @RolID		as varchar(5)
	Declare @Usuario_id as varchar(30)
	
	Select @RolId=rol_id,@usuario_id=usuario_id from #temp_usuario_loggin

	select	d.CODIGO_VIAJE as [PICKING/VIAJE],
			count(distinct d.doc_ext) AS QTY_DOC,
			count(distinct dd.producto_id) AS QTY_PROD,
			sum(dd.cantidad_solicitada) as QTY_CAJAS ,
			cast(pv.prioridad as VARCHAR(20)) as PRIORIDAD_VIAJE ,
			dbo.GetPickerMans(d.CODIGO_VIAJE) AS PICKEADORES,
			CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END AS razon_social,
			CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.cliente_id END AS cliente_id    
	from	sys_int_documento d (nolock)
			inner join sys_int_det_documento dd WITH(nolock, index (IDX_SIDD_ESTADOGT)) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			inner join producto p (nolock) on (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.agente_id=s.sucursal_id)
			left  join prioridad_viaje pv (nolock) on (d.codigo_viaje=pv.viaje_id)
			inner join rl_sys_cliente_usuario su (nolock) on(d.cliente_id=su.cliente_id)
			inner join RL_ROL_INT_TIPO_DOCUMENTO rd (nolock) on(d.tipo_documento_id=rd.tipo_documento_id)--agregado SG.
			inner join cliente c on(d.cliente_id=c.cliente_id)
			inner join(	SELECT	CODIGO_VIAJE,COUNT(distinct SD.CLIENTE_ID) AS CANT_CLIENTE    
						FROM	sys_int_documento SD    
						WHERE	SD.ESTADO_GT IS NULL  
						GROUP BY 
								SD.CODIGO_VIAJE    
			) as X on (X.CODIGO_VIAJE=d.CODIGO_VIAJE)
	where 
			d.tipo_documento_id in ('E01','E02','E03','E04','E06','E08')
			and dd.estado_gt is null
			and su.usuario_id=@usuario_id
			and rd.rol_id=@RolId --Agregado SG.
			and d.tipo_documento_id in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R (nolock) where r.rol_id=@RolId)
	GROUP BY 
			d.CODIGO_VIAJE
			,pv.prioridad
			,CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END,
			CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.cliente_id END 
	ORDER BY 
			ISNULL(pv.prioridad,9999999999),d.CODIGO_VIAJE

END

GO


