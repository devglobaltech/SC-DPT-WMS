
/****** Object:  StoredProcedure [dbo].[GetEgresosxDocumento]    Script Date: 10/28/2014 13:20:34 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetEgresosxDocumento]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetEgresosxDocumento]
GO

CREATE PROCEDURE [dbo].[GetEgresosxDocumento]        
as        
begin        
	--usuario y permisos        
	Declare @RolID  as varchar(5)        
	Declare @Usuario_id as varchar(30)        
	Select @RolId=rol_id,@usuario_id=usuario_id from #temp_usuario_loggin        

	select   0										as Seleccionar      
			,c.razon_social							as ID_Cliente  
			,d.codigo_viaje							as Codigo_Viaje       
			,D.TIPO_DOCUMENTO_ID					as tipo_documento_id
			,D.CPTE_PREFIJO							as cpte_prefijo
			,D.CPTE_NUMERO							as cpte_numero
			,d.fecha_cpte							as Fecha				
			,D.FECHA_SOLICITUD_CPTE					as fecha_solicitud_cpte
			,d.agente_id							as Cod_Agente         
			,S.NOMBRE								as Agente      
			,D.PESO_TOTAL							as peso_total
			,D.UNIDAD_PESO							as unidad_peso
			,D.VOLUMEN_TOTAL						as volumen_total
			,D.UNIDAD_VOLUMEN						as unidad_volumen
			,D.TOTAL_BULTOS							as total_bultos
			,D.ORDEN_DE_COMPRA						as orden_de_compra
			,D.OBSERVACIONES						as observaciones
			,D.NRO_REMITO							as nro_remito
			,D.NRO_DESPACHO_IMPORTACION				as nro_despacho_importacion
			,d.doc_ext								as Documento_Externo 
			,D.INFO_ADICIONAL_1						as info_adicional_1
			,D.INFO_ADICIONAL_2						as info_adicional_2
			,D.INFO_ADICIONAL_3						as info_adicional_3
			,D.INFO_ADICIONAL_4						as info_adicional_4
			,D.INFO_ADICIONAL_5						as info_adicional_5
			,D.INFO_ADICIONAL_6						as info_adicional_6
			,D.TIPO_COMPROBANTE						as tipo_comprobante
			,D.TRANSPORTE_ID						as transporte_id
			,isnull(T.NOMBRE,'DATOS NO DISPONIBLES')as nombre
			,D.IMPORTE_FLETE						as importe_flete
			,D.CLASE_PEDIDO							as clase_pedido
			,PV.DESCRIPCION							as descripcion
			,s.localidad							as localidad
			,s.CALLE								as calle
			,s.NUMERO								as numero
			,s.CLIENTE_ID							as cliente_id
	from    sys_int_documento d (nolock)
			inner join sys_int_det_documento dd	(nolock)		on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)        
			inner join sucursal s (nolock)						on (d.cliente_id=s.cliente_id and d.agente_id=s.sucursal_id)        
			--Perfiles        
			inner join RL_ROL_INT_TIPO_DOCUMENTO rd (nolock)    on(d.tipo_documento_id=rd.tipo_documento_id)        
			inner join rl_sys_cliente_usuario su (nolock)       on(d.cliente_id=su.cliente_id)        
			inner join cliente c(nolock)						on(d.cliente_id = c.cliente_id)       
			LEFT JOIN PROVINCIA PV (nolock)						on(s.PROVINCIA_ID = PV.PROVINCIA_ID AND s.PAIS_ID = PV.PAIS_ID)
			LEFT JOIN TRANSPORTE T (nolock)						on(T.TRANSPORTE_ID = D.TRANSPORTE_ID)
			INNER JOIN TIPO_COMPROBANTE TC (nolock)				on(TC.TIPO_COMPROBANTE_ID = RD.TIPO_DOCUMENTO_ID)
	where   d.estado_gt is null        
			AND DD.ESTADO_GT IS NULL
			and su.usuario_id=@usuario_id        
			and rd.rol_id=@RolId --Agregado SG.        
			and d.tipo_documento_id in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R (nolock) where r.rol_id=@RolId)        
			AND TC.TIPO_OPERACION_ID = 'EGR'
			AND D.TIPO_DOCUMENTO_ID NOT IN ('E07')
	GROUP BY
			c.razon_social					,d.codigo_viaje   
			,D.TIPO_DOCUMENTO_ID			,D.CPTE_PREFIJO
			,D.CPTE_NUMERO					,d.fecha_cpte			
			,D.FECHA_SOLICITUD_CPTE			,d.agente_id       
			,S.NOMBRE						,D.PESO_TOTAL
			,D.UNIDAD_PESO					,D.VOLUMEN_TOTAL
			,D.UNIDAD_VOLUMEN				,D.TOTAL_BULTOS
			,D.ORDEN_DE_COMPRA				,D.OBSERVACIONES
			,D.NRO_REMITO					,D.NRO_DESPACHO_IMPORTACION
			,d.doc_ext						,D.INFO_ADICIONAL_1
			,D.INFO_ADICIONAL_2				,D.INFO_ADICIONAL_3
			,D.INFO_ADICIONAL_4				,D.INFO_ADICIONAL_5
			,D.INFO_ADICIONAL_6				,D.TIPO_COMPROBANTE
			,D.TRANSPORTE_ID				,isnull(T.NOMBRE,'DATOS NO DISPONIBLES')
			,D.IMPORTE_FLETE				,D.CLASE_PEDIDO
			,PV.DESCRIPCION					,s.localidad
			,s.CALLE						,s.NUMERO
			,s.CLIENTE_ID
	
	order by 
			d.CODIGO_VIAJE        
        
END



GO


