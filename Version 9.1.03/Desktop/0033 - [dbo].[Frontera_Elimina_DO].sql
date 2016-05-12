IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_Elimina_DO]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_Elimina_DO]
GO

CREATE     Procedure [dbo].[Frontera_Elimina_DO]
@Doc_Ext		as Varchar(100) Output,
@Cliente_id		as Varchar(15) 	Output
As
Begin
	
	INSERT INTO SYS_DEV_DOCUMENTO
		SELECT  
				CLIENTE_ID
				,'I99'
				,CPTE_PREFIJO
				,CPTE_NUMERO
				,FECHA_CPTE
				,FECHA_SOLICITUD_CPTE
				,AGENTE_ID
				,PESO_TOTAL
				,UNIDAD_PESO
				,VOLUMEN_TOTAL
				,UNIDAD_VOLUMEN
				,TOTAL_BULTOS
				,ORDEN_DE_COMPRA
				,OBSERVACIONES
				,NRO_REMITO
				,NRO_DESPACHO_IMPORTACION
				,DOC_EXT + space(30-len(doc_ext)) + 'I99' 
--				,DOC_EXT
				,CODIGO_VIAJE
				,INFO_ADICIONAL_1
				,INFO_ADICIONAL_2
				,INFO_ADICIONAL_3
				,TIPO_COMPROBANTE
				,null
				,null
				,'E'
				,getdate()
				,Null	 --Flg_Movimiento
				,CUSTOMS_1
				,CUSTOMS_2
				,CUSTOMS_3
				,null as nro_guia
				,null as importe_flete
				,null as transporte_id
				,info_adicional_4
				,info_adicional_5
				,info_adicional_6
		FROM 	SYS_INT_DOCUMENTO 
		WHERE	CLIENTE_ID=@Cliente_id
				AND DOC_EXT=@Doc_Ext

	If @@Error<>0
	Begin
		Return 
	End
	
	INSERT INTO SYS_DEV_DET_DOCUMENTO
		SELECT
				 DOC_EXT + space(30-len(doc_ext)) + 'I99' 
				,NRO_LINEA
				,CLIENTE_ID
				,PRODUCTO_ID
				,CANTIDAD_SOLICITADA
				,CANTIDAD
				,EST_MERC_ID
				,CAT_LOG_ID
				,NRO_BULTO
				,DESCRIPCION
				,NRO_LOTE
				,NRO_PALLET
				,FECHA_VENCIMIENTO
				,NRO_DESPACHO
				,NRO_PARTIDA
				,UNIDAD_ID
				,UNIDAD_CONTENEDORA_ID
				,PESO
				,UNIDAD_PESO
				,VOLUMEN
				,UNIDAD_VOLUMEN
				,PROP1
				,PROP2
				,PROP3
				,LARGO
				,ALTO
				,ANCHO
				,DOC_BACK_ORDER
				,null
				,null
				,'E'
				, getdate()
				,DOCUMENTO_ID
				,NAVE_ID
				,NAVE_COD
				,Null --Flg_Movimiento
				,CUSTOMS_1
				,CUSTOMS_2
				,CUSTOMS_3
				,null as nro_cmr
		FROM 	SYS_INT_DET_DOCUMENTO 
		WHERE	CLIENTE_ID=@Cliente_Id
				AND DOC_EXT=@Doc_Ext
				AND ESTADO_GT IS NULL

	If @@Error<>0
	Begin
		Return
	End            

	UPDATE 	SYS_INT_DOCUMENTO SET
			ESTADO_GT='E', FECHA_ESTADO_GT=GETDATE()
	WHERE	CLIENTE_ID=@Cliente_Id
			AND DOC_EXT=@Doc_Ext

	If @@Error<>0
	Begin
		Return
	End            

	UPDATE 	SYS_INT_DET_DOCUMENTO SET
			ESTADO_GT='E', FECHA_ESTADO_GT=GETDATE()
	WHERE	CLIENTE_ID=@Cliente_Id
			AND DOC_EXT=@DOC_EXT
        
	If @@Error<>0
	Begin
		Return
	End            

End	--Fin Procedure

GO


