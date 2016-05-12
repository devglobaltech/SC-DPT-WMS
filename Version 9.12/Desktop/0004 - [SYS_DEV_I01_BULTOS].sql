/****** Object:  StoredProcedure [dbo].[SYS_DEV_I01_BULTOS]    Script Date: 07/16/2013 15:29:59 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_DEV_I01_BULTOS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_DEV_I01_BULTOS]
GO

CREATE     PROCEDURE [dbo].[SYS_DEV_I01_BULTOS]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
--PARA REVERSION DE OC
DECLARE @V_CLIENTE_ID VARCHAR(100)
DECLARE @V_DOC_EXT  varchar(100)
DECLARE @V_DOCUMENTO_ID numeric(20,0)
DECLARE @V_PRODUCTO_ID VARCHAR(100)
DECLARE @V_CANTIDAD_REV NUMERIC(20,6)

BEGIN

	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
	begin 
		
		if @qty=0	BEGIN 	
      		SELECT * FROM SYS_DEV_DOCUMENTO
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
					NULL,	--flg_movimiento
					DBO.GET_SID_CUSTOMS(SID.CLIENTE_ID,SID.DOC_EXT,'1'),
					DBO.GET_SID_CUSTOMS(SID.CLIENTE_ID,SID.DOC_EXT,'2'),
					DBO.GET_SID_CUSTOMS(SID.CLIENTE_ID,SID.DOC_EXT,'3')
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
				SUM(RL.CANTIDAD), 
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
				isnull(n2.nave_id,n.nave_id),
				isnull(n2.nave_cod,n.nave_cod), 
				NULL,	--flg_movimiento	
				DBO.GET_SIDD_CUSTOMS(SIDD.CLIENTE_ID,SIDD.DOC_EXT,SIDD.PRODUCTO_ID,'1'),
				DBO.GET_SIDD_CUSTOMS(SIDD.CLIENTE_ID,SIDD.DOC_EXT,SIDD.PRODUCTO_ID,'2'),
				DBO.GET_SIDD_CUSTOMS(SIDD.CLIENTE_ID,SIDD.DOC_EXT,SIDD.PRODUCTO_ID,'3')
		from	sys_int_documento sid
				inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)
				inner join documento d on (sid.cliente_id=d.cliente_id and sidd.documento_id=d.documento_id)
				inner join det_documento dd on (d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2 and sidd.producto_id = dd.producto_id)
				inner join det_documento_transaccion ddt on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
				inner join rl_det_doc_trans_posicion rl on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				left join nave n on(rl.nave_actual=n.nave_id)
				left join posicion p on(rl.posicion_actual=p.posicion_id)
				left join nave n2 on(p.nave_id=n2.nave_id)
		where	sid.doc_ext=@doc_ext 
				and sidd.estado_gt is not null 
				and dd.documento_id=@documento_id
				and sidd.documento_id=@documento_id
				and ((isnull(n.en_error,'0')<>'1') and (isnull(n2.en_error,'0')<>'1'))
		group by
				SIDD.DOC_EXT					, CAST(DD.DOCUMENTO_ID AS VARCHAR) + CAST(DD.NRO_LINEA AS VARCHAR)	, SIDD.CLIENTE_ID
				, SIDD.PRODUCTO_ID				--, RL.CANTIDAD						
				, DD.EST_MERC_ID				, DD.CAT_LOG_ID_FINAL
				, DD.NRO_BULTO					, DD.DESCRIPCION					, DD.NRO_LOTE					, DD.PROP1
				, DD.FECHA_VENCIMIENTO			, DD.NRO_DESPACHO					, DD.NRO_PARTIDA				, SIDD.UNIDAD_ID
				, SIDD.UNIDAD_CONTENEDORA_ID	, SIDD.PESO							, SIDD.UNIDAD_PESO				, SIDD.VOLUMEN
				, SIDD.UNIDAD_VOLUMEN			, SIDD.PROP1						, DD.PROP2						, ISNULL(SIDD.PROP3,DBO.FX_GETNROREMITODO(DD.DOCUMENTO_ID))			
				, SIDD.LARGO					, SIDD.ALTO							, SIDD.ANCHO					, SIDD.DOC_BACK_ORDER
				, DD.DOCUMENTO_ID				, DBO.GET_NAVE_ID(DD.DOCUMENTO_ID,DD.NRO_LINEA)						, DBO.GET_NAVE_COD(DD.DOCUMENTO_ID,DD.NRO_LINEA)
				, SIDD.PROP3					, DD.NRO_SERIE						,sidd.CLIENTE_ID				, sidd.DOC_EXT 
				, n2.nave_id					, n.nave_id							, n2.nave_cod					, n.nave_cod
	end
	
	DECLARE VCUR CURSOR FOR
		select	sidd.CLIENTE_ID, sidd.DOC_EXT, DD.DOCUMENTO_ID, sidd.PRODUCTO_ID, sum(rl.cantidad)
		from	sys_int_documento sid  
				inner join (select	distinct DOC_EXT,CLIENTE_ID,PRODUCTO_ID,EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,NRO_LOTE,NRO_PALLET,
									FECHA_VENCIMIENTO,NRO_DESPACHO,NRO_PARTIDA,UNIDAD_ID,UNIDAD_CONTENEDORA_ID,PESO,UNIDAD_PESO,VOLUMEN,UNIDAD_VOLUMEN,PROP1,
									PROP2,PROP3,LARGO,ALTO,ANCHO,DOC_BACK_ORDER,ESTADO,FECHA_ESTADO,ESTADO_GT,DOCUMENTO_ID,NAVE_ID,NAVE_COD
							from sys_int_det_documento) 
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
		group by sidd.CLIENTE_ID, sidd.DOC_EXT, DD.DOCUMENTO_ID, sidd.PRODUCTO_ID;

	OPEN VCUR
	FETCH NEXT FROM VCUR INTO @V_CLIENTE_ID, @V_DOC_EXT, @V_DOCUMENTO_ID, @V_PRODUCTO_ID, @V_CANTIDAD_REV
	WHILE (@@FETCH_STATUS =0) 
	BEGIN
		
		EXEC REVERSION_CONSUMO_OC @V_CLIENTE_ID, @V_DOC_EXT, @V_DOCUMENTO_ID, @V_PRODUCTO_ID, @V_CANTIDAD_REV

		FETCH NEXT FROM VCUR INTO @V_CLIENTE_ID, @V_DOC_EXT, @V_DOCUMENTO_ID, @V_PRODUCTO_ID, @V_CANTIDAD_REV
	END
	CLOSE VCUR
	DEALLOCATE VCUR	
END


