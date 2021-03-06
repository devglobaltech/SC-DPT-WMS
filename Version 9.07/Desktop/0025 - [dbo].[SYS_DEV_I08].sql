

ALTER         PROCEDURE [dbo].[SYS_DEV_I08]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
set xact_abort on
BEGIN

	select @qty=count(*) from sys_dev_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext

	BEGIN 
	
		IF @qty=0
			BEGIN 	
				INSERT INTO sys_dev_documento
				SELECT	 d.CLIENTE_ID
						,'I08'
						,d.CPTE_PREFIJO 
						,d.CPTE_NUMERO 
						,d.FECHA_CPTE
						,GetDate()
						,NULL
						,d.PESO_TOTAL
						,d.UNIDAD_PESO
						,d.VOLUMEN_TOTAL
						,d.UNIDAD_VOLUMEN
						,d.TOTAL_BULTOS
						,d.ORDEN_DE_COMPRA
						,d.OBSERVACIONES
						,d.NRO_REMITO
						,d.NRO_DESPACHO_IMPORTACION
						,'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))
						,NULL
						,NULL
						,NULL
						,NULL
						,d.TIPO_COMPROBANTE_ID
						,NULL
						,NULL
						,'P' --ESTADO_GT
						,GETDATE() 
						,null	--Flg_movimiento
						,NULL
						,NULL
						,NULL
				FROM	documento d (nolock)
				WHERE	d.documento_id = @documento_id
			END
	
			INSERT INTO sys_dev_det_documento
			SELECT	 'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))
					,isnull(@nro_lin,0) + dd.NRO_LINEA
					,d.CLIENTE_ID 
					,dd.PRODUCTO_ID
					,dd.CANTIDAD					--sidd.CANTIDAD_SOLICITADA
					,dd.CANTIDAD
					,dd.EST_MERC_ID
					,dd.CAT_LOG_ID_FINAL
					,dbo.Get_data_I08(dd.documento_id,dd.nro_linea,'3')
					,dd.DESCRIPCION
					,dd.NRO_LOTE
					,dd.PROP1 AS NRO_PALLET --NRO_PALLET 
					,dd.FECHA_VENCIMIENTO
					,dd.NRO_DESPACHO
					,dd.NRO_PARTIDA
					,dd.UNIDAD_ID 			--sidd.UNIDAD_ID
					,NULL					--sidd.UNIDAD_CONTENEDORA_ID
					,dd.PESO				--sidd.PESO
					,dd.UNIDAD_PESO 		--sidd.UNIDAD_PESO
					,dd.VOLUMEN				--sidd.VOLUMEN
					,dd.UNIDAD_VOLUMEN		--sidd.UNIDAD_VOLUMEN
					,dd.PROP1				--sidd.PROP1
					,dd.prop2
					,dd.prop3
					,dd.LARGO				--sidd.LARGO
					,dd.ALTO 				--sidd.ALTO 
					,dd.ANCHO				--sidd.ANCHO
					,dd.TRACE_BACK_ORDER	--sidd.DOC_BACK_ORDER
					,NULL
					,NULL
					,'P'
					,GETDATE()
					,DD.DOCUMENTO_ID
					,dbo.get_nave_id(dd.documento_id,dd.nro_linea)
					,dbo.get_nave_cod(dd.documento_id,dd.nro_linea)
					,null 	--Flg_movimiento 	
					,NULL
					,NULL
					,NULL
			FROM	documento d (nolock)
					inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			WHERE	dd.documento_id=@documento_id

	END
END


