
/****** Object:  StoredProcedure [dbo].[Frontera_PickingProceso]    Script Date: 09/11/2014 14:38:09 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_PickingProceso]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_PickingProceso]
GO

CREATE      PROCEDURE [dbo].[Frontera_PickingProceso]
AS

BEGIN
	Declare @RolID		as varchar(5)
	Declare @UsuarioId	as varchar(30)
	Select @RolId=rol_id, @UsuarioId=usuario_id from #temp_usuario_loggin

	select 	p.viaje_id as [PICKING/VIAJE]
			,round((sum(isnull(p.cant_confirmada,0))/sum(p.cantidad)*100),2) as POR_COMPLETO
			,sum(isnull(p.cantidad,0)) as QTY_BULTOS_A_PICKEAR
			,sum(isnull(p.cant_confirmada,0)) as QTY_BULTOS_PICKEADOS
			,cast(pv.prioridad as VARCHAR(20)) as PRIORIDAD_VIAJE
			,dbo.GetPickerMans(p.viaje_id) AS PICKEADORES
			--,CASE p.FLG_PALLET_HOMBRE  WHEN '1' THEN 'SI' WHEN '0' THEN 'NO' END AS FLG_PALLET_HOMBRE
			,isnull(z.FLG_PALLET_HOMBRE,'NO') as FLG_PALLET_HOMBRE
			--,C.RAZON_SOCIAL
			,CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END AS razon_social
	From	documento d (nolock)
			inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
			inner join picking p (nolock) on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			left join prioridad_viaje pv (nolock) on (p.viaje_id=pv.viaje_id)
			inner join rl_sys_cliente_usuario su on(d.cliente_id=su.cliente_id)
			inner join cliente c on(d.cliente_id=c.cliente_id)
			inner join(	SELECT	CODIGO_VIAJE,COUNT(distinct SD.CLIENTE_ID) AS CANT_CLIENTE    
						FROM	sys_int_documento (nolock) SD
						INNER JOIN SYS_INT_DET_DOCUMENTO (nolock) SDD ON (SD.CLIENTE_ID = SDD.CLIENTE_ID AND SD.DOC_EXT = SDD.DOC_EXT)    
						WHERE SDD.DOCUMENTO_ID IS NOT NULL    
						--WHERE	SD.ESTADO_GT IS NULL  
						GROUP BY 
								SD.CODIGO_VIAJE    
			) as X on (X.CODIGO_VIAJE=d.NRO_DESPACHO_IMPORTACION)	
			left join (
				select	distinct cliente_id, viaje_id, CASE p.FLG_PALLET_HOMBRE  WHEN '1' THEN 'SI' WHEN '0' THEN 'NO' END AS FLG_PALLET_HOMBRE
				from	PICKING p
				where	p.FLG_PALLET_HOMBRE='1'
			)as z on(p.CLIENTE_ID=z.CLIENTE_ID and p.VIAJE_ID=z.VIAJE_ID )
	Where	p.fin_picking in (0,1) 
			and su.usuario_id=@usuarioid
			and dbo.Get_Tipo_Documento_id(d.cliente_id,d.nro_remito) in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R where r.rol_id=@RolId)
	GROUP BY 
			p.viaje_id,pv.prioridad,isnull(z.FLG_PALLET_HOMBRE,'NO') ,
			CASE WHEN X.CANT_CLIENTE > 1 THEN 'MULTICLIENTE' ELSE C.RAZON_SOCIAL END

END






GO


