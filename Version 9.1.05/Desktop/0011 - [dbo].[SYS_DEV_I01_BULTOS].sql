
/****** Object:  StoredProcedure [dbo].[SYS_DEV_I01_BULTOS]    Script Date: 03/11/2014 12:24:54 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_DEV_I01_BULTOS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_DEV_I01_BULTOS]
GO

/****** Object:  StoredProcedure [dbo].[SYS_DEV_I01_BULTOS]    Script Date: 03/11/2014 12:24:54 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SYS_DEV_I01_BULTOS]  
 @doc_ext AS varchar(100)  
,@estado as numeric(2,0)  
,@documento_id numeric(20,0)  
AS    
	DECLARE @qty AS numeric(3,0)  
	DECLARE @nro_lin AS numeric(20,0)  
	DECLARE @NroRemito AS varchar(100)  
	DECLARE @NrosRemitos AS varchar(4000)  
	--PARA REVERSION DE OC
	DECLARE @V_CLIENTE_ID VARCHAR(100)
	DECLARE @V_DOC_EXT  varchar(100)
	DECLARE @V_DOCUMENTO_ID numeric(20,0)
	DECLARE @V_PRODUCTO_ID VARCHAR(100)
	DECLARE @V_CANTIDAD_REV NUMERIC(20,6)
BEGIN    
	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext  
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext  
	select @NroRemito=NRO_REMITO from documento where DOCUMENTO_ID=@documento_id 
	select @NrosRemitos = DBO.FX_REMITOS(@NroRemito)
BEGIN TRY 
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
				DBO.FX_REMITOS(d.NRO_REMITO),   
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
				NULL, --flg_movimiento   
				SID.CUSTOMS_1,
				SID.CUSTOMS_2,
				SID.CUSTOMS_3,
				NULL, --NRO_GUIA
				NULL, --IMPORTE_FLETE
				NULL,  --TRANSPORTE_ID
				sid.INFO_ADICIONAL_4,
				sid.INFO_ADICIONAL_5,
				sid.INFO_ADICIONAL_6
		from	sys_int_documento sid  
				inner join documento d on (sid.cliente_id=d.cliente_id)  
				inner join det_documento dd on(d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2)  
		where	sid.doc_ext=@doc_ext  
	end else begin
		update sys_dev_documento set NRO_REMITO=left( NRO_REMITO  + DBO.FX_REMITOS(@NroRemito),4000) where DOC_EXT=@doc_ext
	END  
 
 
 
 
	 select @qty = count(*) from sys_dev_documento d
	 inner join SYS_DEV_DET_DOCUMENTO sd on (sd.CLIENTE_ID = d.CLIENTE_ID AND sd.DOC_EXT = d.DOC_EXT)
	 where d.doc_ext=@doc_ext  and sd.documento_id = @documento_id
	 
	if @qty > 0 BEGIN
		DECLARE @MSG VARCHAR(4000)
		Declare @Usuario_Id		Varchar(30)
		DECLARE @Terminal VARCHAR(100)
		
		Select @Usuario_Id=Usuario_Id from #Temp_Usuario_loggin
		Set @Terminal=Host_Name()	

		INSERT INTO AUDITORIA_SYS_DEV VALUES (@documento_id,@doc_ext,NULL,@Usuario_Id,@Terminal,GETDATE())

		SET @MSG = 'EL DOCUMENTO NRO. ' + CAST(@documento_id AS VARCHAR) + ' YA ESTÁ INFORMADO AL ERP, POR FAVOR NOTIFIQUE A SISTEMAS'
		raiserror(@MSG,15,1)
	END
    
 
	insert into sys_dev_det_documento  
	select	sidd.DOC_EXT,   
			isnull(@nro_lin,0) + dd.NRO_LINEA,   
			--dd.documento_id + dd.nro_linea,
			sidd.CLIENTE_ID,   
			sidd.PRODUCTO_ID,   
			sidd.CANTIDAD_SOLICITADA,   
			--dd.CANTIDAD,   
			sum(rl.cantidad),
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
			SIDD.PROP2, --NRO_LOTE   
			@NrosRemitos,  --prop3, nros de remitos recepcionados
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
			NULL, --flg_movimiento  
			SIDD.CUSTOMS_1,
			SIDD.CUSTOMS_2,
			SIDD.CUSTOMS_3,
			NULL	 --NRO_CMR
	from	sys_int_documento sid  
			inner join (select	DOC_EXT,CLIENTE_ID,PRODUCTO_ID,SUM(CANTIDAD_SOLICITADA) AS CANTIDAD_SOLICITADA,EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,NRO_LOTE,NRO_PALLET,
								FECHA_VENCIMIENTO,NRO_DESPACHO,NRO_PARTIDA,UNIDAD_ID,UNIDAD_CONTENEDORA_ID,PESO,UNIDAD_PESO,VOLUMEN,UNIDAD_VOLUMEN,PROP1,
								PROP2,PROP3,LARGO,ALTO,ANCHO,DOC_BACK_ORDER,ESTADO,FECHA_ESTADO,ESTADO_GT,DOCUMENTO_ID,NAVE_ID,NAVE_COD,CUSTOMS_1,CUSTOMS_2,CUSTOMS_3
						from	sys_int_det_documento
						GROUP BY 
								DOC_EXT,CLIENTE_ID,PRODUCTO_ID,EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,NRO_LOTE,NRO_PALLET,
								FECHA_VENCIMIENTO,NRO_DESPACHO,NRO_PARTIDA,UNIDAD_ID,UNIDAD_CONTENEDORA_ID,PESO,UNIDAD_PESO,VOLUMEN,UNIDAD_VOLUMEN,PROP1,
								PROP2,PROP3,LARGO,ALTO,ANCHO,DOC_BACK_ORDER,ESTADO,FECHA_ESTADO,ESTADO_GT,DOCUMENTO_ID,NAVE_ID,NAVE_COD,CUSTOMS_1,CUSTOMS_2,CUSTOMS_3) 
			sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext) 
			inner join documento d on (sid.cliente_id=d.cliente_id and sidd.documento_id=d.documento_id)  
			inner join det_documento dd on (d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2)  
			inner join det_documento_transaccion ddt on (ddt.documento_id = dd.documento_id and ddt.nro_linea_doc = dd.nro_linea)
			inner join rl_det_doc_trans_posicion rl on (rl.doc_trans_id = ddt.doc_trans_id and rl.nro_linea_trans = ddt.nro_linea_trans)
	where	sid.doc_ext=@doc_ext   
			and sidd.estado_gt is not null   
			and dd.documento_id=@documento_id 
			and sidd.documento_id=@documento_id  
			and not exists (select 1 from nave n where n.nave_id=rl.nave_actual and n.en_error = '1')
	group by 
			sidd.DOC_EXT,   
			dd.NRO_LINEA,   
			--dd.documento_id + dd.nro_linea,
			sidd.CLIENTE_ID,   
			sidd.PRODUCTO_ID,   
			sidd.CANTIDAD_SOLICITADA,   
			dd.EST_MERC_ID,   
			dd.CAT_LOG_ID_FINAL,   
			dd.NRO_BULTO,   
			dd.DESCRIPCION,   
			dd.NRO_LOTE,   
			dd.PROP1,
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
			SIDD.PROP2, --NRO_LOTE   
			sidd.LARGO,   
			sidd.ALTO,   
			sidd.ANCHO,   
			sidd.DOC_BACK_ORDER,   
			DD.DOCUMENTO_ID,   
			dbo.get_nave_id(dd.documento_id,dd.nro_linea),  
			dbo.get_nave_cod(dd.documento_id,dd.nro_linea),
			sidd.CUSTOMS_1,
			sidd.CUSTOMS_2,
			sidd.CUSTOMS_3


	--HAGO REVERSION DE OC SI CORRESPONDE



	DECLARE VCUR CURSOR FOR
	 select sidd.CLIENTE_ID, sidd.DOC_EXT, DD.DOCUMENTO_ID, sidd.PRODUCTO_ID, sum(rl.cantidad)
	 from	sys_int_documento sid  
			--inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)  
			inner join (select	distinct DOC_EXT,CLIENTE_ID,PRODUCTO_ID,EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,NRO_LOTE,NRO_PALLET,
								FECHA_VENCIMIENTO,NRO_DESPACHO,NRO_PARTIDA,UNIDAD_ID,UNIDAD_CONTENEDORA_ID,PESO,UNIDAD_PESO,VOLUMEN,UNIDAD_VOLUMEN,PROP1,
								PROP2,PROP3,LARGO,ALTO,ANCHO,DOC_BACK_ORDER,ESTADO,FECHA_ESTADO,ESTADO_GT,DOCUMENTO_ID,NAVE_ID,NAVE_COD
						from	sys_int_det_documento) 
			sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)  
			inner join documento d on (sid.cliente_id=d.cliente_id and sidd.documento_id=d.documento_id)  
			inner join det_documento dd on (d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2)  
			inner join det_documento_transaccion ddt on (ddt.documento_id = dd.documento_id and ddt.nro_linea_doc = dd.nro_linea)
			inner join rl_det_doc_trans_posicion rl on (rl.doc_trans_id = ddt.doc_trans_id and rl.nro_linea_trans = ddt.nro_linea_trans)
	 where	sid.doc_ext=@doc_ext   
			and sidd.estado_gt is not null   
			and dd.documento_id=@documento_id  
			and sidd.documento_id=@documento_id  
			and exists (select 1 from nave n where n.nave_id=rl.nave_actual and n.en_error = '1')
	group by 
			sidd.CLIENTE_ID, sidd.DOC_EXT, DD.DOCUMENTO_ID, sidd.PRODUCTO_ID

	OPEN VCUR
	FETCH NEXT FROM VCUR INTO @V_CLIENTE_ID, @V_DOC_EXT, @V_DOCUMENTO_ID, @V_PRODUCTO_ID, @V_CANTIDAD_REV
	WHILE (@@FETCH_STATUS =0) 
	BEGIN
		
		EXEC REVERSION_CONSUMO_OC @V_CLIENTE_ID, @V_DOC_EXT, @V_DOCUMENTO_ID, @V_PRODUCTO_ID, @V_CANTIDAD_REV

		FETCH NEXT FROM VCUR INTO @V_CLIENTE_ID, @V_DOC_EXT, @V_DOCUMENTO_ID, @V_PRODUCTO_ID, @V_CANTIDAD_REV
	END
	CLOSE VCUR
	DEALLOCATE VCUR

end  
END TRY
 BEGIN CATCH  
   EXEC usp_RethrowError  
 END CATCH  
END


GO


