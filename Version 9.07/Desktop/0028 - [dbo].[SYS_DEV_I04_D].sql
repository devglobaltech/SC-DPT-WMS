
ALTER   PROCEDURE [dbo].[SYS_DEV_I04_D]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(5,0)
BEGIN

	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
begin 
	
	if @qty=0
      	   BEGIN 	
		insert into sys_dev_documento
		select	sid.CLIENTE_ID, 
				'I06', 
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
				null,	--Flg_Movimiento
				SID.CUSTOMS_1,
				sid.CUSTOMS_2,
				sid.CUSTOMS_3 
		from	sys_int_documento sid
				inner join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_despacho_importacion)
		where	sid.doc_ext=@doc_ext
	END --IF
	
	insert into sys_dev_det_documento
	select	sidd.DOC_EXT, 
			isnull(@nro_lin,0) + dd.NRO_LINEA, 
			sidd.CLIENTE_ID, 
			sidd.PRODUCTO_ID, 
			sidd.CANTIDAD_SOLICITADA, 
			null, 
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
			sidd.PROP3, 
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
			null, --flg_Movimiento.
			sid.CUSTOMS_1,
			sid.CUSTOMS_2,
			sid.CUSTOMS_3 
	from	sys_int_documento sid
			inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)
			inner join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_despacho_importacion)
			inner join det_documento dd on (d.documento_id=dd.documento_id and sidd.producto_id = dd.producto_id)
	where	sid.doc_ext=@doc_ext 
			and sidd.estado_gt is not null 
			and dd.documento_id=@documento_id
			and sidd.documento_id=@documento_id
	
	exec dbo.deletedoc @documento_id=@documento_id 
		
end
END
