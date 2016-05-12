
/****** Object:  StoredProcedure [dbo].[Frontera_PickingDesconsolidacion]    Script Date: 03/03/2015 15:59:32 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_PickingDesconsolidacion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_PickingDesconsolidacion]
GO

CREATE      PROCEDURE [dbo].[Frontera_PickingDesconsolidacion]
AS

BEGIN
	Declare @RolID		as varchar(5)
	Declare @UsuarioId	as varchar(30)
	Select @RolId=rol_id, @UsuarioId=usuario_id from #temp_usuario_loggin

	SELECT 	P.VIAJE_ID							AS [PICKING/VIAJE]
			,COUNT(DISTINCT P.DOCUMENTO_ID)		AS [CANT. DOCUMENTOS]
			,SUM(ISNULL(P.CANTIDAD,0))			AS QTY_BULTOS_A_PICKEAR
			,SUM(ISNULL(P.CANT_CONFIRMADA,0))	AS QTY_BULTOS_PICKEADOS
			,ROUND((SUM(ISNULL(P.CANT_CONFIRMADA,0))/SUM(P.CANTIDAD)*100),2) AS [CUMPLIMIENTO PICKING]
			,COUNT(DISTINCT P.PALLET_PICKING)	AS [CANT. CONTENEDORAS]
			,DBO.GETPICKERMANS(P.VIAJE_ID)		AS PICKEADORES
			,CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END AS RAZON_SOCIAL
	From	documento d (nolock)
			inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
			inner join picking p (nolock) on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			inner join rl_sys_cliente_usuario su on(d.cliente_id=su.cliente_id)
			inner join cliente c on(d.cliente_id=c.cliente_id)
			inner join CLIENTE_PARAMETROS cp on(c.CLIENTE_ID=cp.CLIENTE_ID and ISNULL(CP.FLG_DESCONSOLIDACION,'0')='1')
			inner join(	SELECT	CODIGO_VIAJE,COUNT(distinct SD.CLIENTE_ID) AS CANT_CLIENTE    
						FROM	sys_int_documento SD    
						--WHERE	SD.ESTADO_GT IS NULL  
						GROUP BY 
								SD.CODIGO_VIAJE    
			) as X on (X.CODIGO_VIAJE=d.NRO_DESPACHO_IMPORTACION)					
	Where	p.fin_picking in ('2')
			and ISNULL(p.ESTADO,'0')in('0')
			and p.FACTURADO='0'
			AND P.CANT_CONFIRMADA > 0
			and su.usuario_id=@usuarioid
			and dbo.Get_Tipo_Documento_id(d.cliente_id,d.nro_remito) in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R where r.rol_id=@RolId)
	GROUP BY 
			p.viaje_id,p.FLG_PALLET_HOMBRE,CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END

END




GO


