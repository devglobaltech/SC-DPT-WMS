ALTER     PROCEDURE [dbo].[SYS_DEV_I01_BULTOS]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
BEGIN

	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
begin 
	
	if @qty=0
      	   BEGIN 	
		insert into sys_dev_documento
		select	top 1
				sid.CLIENTE_ID, 
				'I02', 
				sid.CPTE_PREFIJO, 
				sid.CPTE_NUMERO, 
				d.FECHA_CPTE, 
				sid.FECHA_SOLICITUD_CPTE, 
				sid.AGENTE_ID, 
				sid.PESO_TOTAL, 
				sid.UNIDAD_PESO, 
				sid.VOLUMEN_TOTAL, 
				sid.UNIDAD_VOLUMEN, 
				sid.TOTAL_BULTOS, 
				sid.ORDEN_DE_COMPRA, 
				sid.OBSERVACIONES, 
				d.NRO_REMITO, 
				sid.NRO_DESPACHO_IMPORTACION, 
				sid.DOC_EXT, 
				sid.CODIGO_VIAJE, 
				sid.INFO_ADICIONAL_1, 
				sid.INFO_ADICIONAL_2, 
				sid.INFO_ADICIONAL_3, 
				d.TIPO_COMPROBANTE_id, 
				NULL, 
				NULL, 
				'P', 
				GETDATE(),
				NULL	--flg_movimiento	
		from	sys_int_documento sid
				inner join documento d on (sid.cliente_id=d.cliente_id)
				inner join det_documento dd on(d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2)
		where	sid.doc_ext=@doc_ext
	END
	
	insert into sys_dev_det_documento
	select	distinct
			sidd.DOC_EXT, 
			CAST(DD.DOCUMENTO_ID AS VARCHAR) + CAST(DD.NRO_LINEA AS VARCHAR), 
			sidd.CLIENTE_ID, 
			sidd.PRODUCTO_ID, 
			MAX(SIDD.CANTIDAD_SOLICITADA), 
			dd.CANTIDAD, 
			dd.EST_MERC_ID, 
			dd.CAT_LOG_ID_FINAL, 
			dd.NRO_BULTO, 
			dd.DESCRIPCION, 
			dd.NRO_LOTE, 
			dd.PROP1 AS NRO_PALLET, --NRO_PALLET 
			dd.FECHA_VENCIMIENTO, 
			dd.NRO_DESPACHO, 
			dd.NRO_PARTIDA, 
			sidd.UNIDAD_ID, 
			sidd.UNIDAD_CONTENEDORA_ID, 
			sidd.PESO, 
			sidd.UNIDAD_PESO, 
			sidd.VOLUMEN, 
			sidd.UNIDAD_VOLUMEN, 
			sidd.PROP1, 
			dd.PROP2, --NRO_LOTE 
			Isnull(sidd.PROP3,dd.nro_serie), 
			sidd.LARGO, 
			sidd.ALTO, 
			sidd.ANCHO, 
			sidd.DOC_BACK_ORDER, 
			NULL, 
			NULL, 
			'P', 
			GETDATE(), 
			DD.DOCUMENTO_ID, 
			dbo.get_nave_id(dd.documento_id,dd.nro_linea),
			dbo.get_nave_cod(dd.documento_id,dd.nro_linea), 	
			NULL	--flg_movimiento	
	from	sys_int_documento sid
			inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)
			inner join documento d on (sid.cliente_id=d.cliente_id and sidd.documento_id=d.documento_id)
			inner join det_documento dd on (d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2 and sidd.producto_id = dd.producto_id)
	where	sid.doc_ext=@doc_ext 
			and sidd.estado_gt is not null 
			and dd.documento_id=@documento_id
			and sidd.documento_id=@documento_id
	group by
			SIDD.DOC_EXT					, CAST(DD.DOCUMENTO_ID AS VARCHAR) + CAST(DD.NRO_LINEA AS VARCHAR)	, SIDD.CLIENTE_ID
			, SIDD.PRODUCTO_ID				, DD.CANTIDAD						, DD.EST_MERC_ID				, DD.CAT_LOG_ID_FINAL
			, DD.NRO_BULTO					, DD.DESCRIPCION					, DD.NRO_LOTE					, DD.PROP1
			, DD.FECHA_VENCIMIENTO			, DD.NRO_DESPACHO					, DD.NRO_PARTIDA				, SIDD.UNIDAD_ID
			, SIDD.UNIDAD_CONTENEDORA_ID	, SIDD.PESO							, SIDD.UNIDAD_PESO				, SIDD.VOLUMEN
			, SIDD.UNIDAD_VOLUMEN			, SIDD.PROP1						, DD.PROP2						, ISNULL(SIDD.PROP3,DBO.FX_GETNROREMITODO(DD.DOCUMENTO_ID))			
			, SIDD.LARGO					, SIDD.ALTO							, SIDD.ANCHO					, SIDD.DOC_BACK_ORDER
			, DD.DOCUMENTO_ID				, DBO.GET_NAVE_ID(DD.DOCUMENTO_ID,DD.NRO_LINEA)						, DBO.GET_NAVE_COD(DD.DOCUMENTO_ID,DD.NRO_LINEA)
			, SIDD.PROP3					, DD.NRO_SERIE		
end
END
