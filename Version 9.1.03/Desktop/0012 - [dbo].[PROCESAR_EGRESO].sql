IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PROCESAR_EGRESO]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[PROCESAR_EGRESO]
GO

CREATE PROCEDURE [dbo].[PROCESAR_EGRESO]    
@CLIENTE_ID AS VARCHAR(15),    
@DOC_EXT AS VARCHAR(100),    
@COD_VIAJE AS VARCHAR(100)    
    
AS    
BEGIN    
	DECLARE @MSG AS VARCHAR(MAX)          
	DECLARE @DOCUMENTO_ID AS NUMERIC(13) 
	DECLARE @CONTROL AS NUMERIC(20)   
    
	IF OBJECT_ID('tempdb.dbo.#temp_usuario_loggin','U') IS NULL
	BEGIN
		CREATE TABLE #temp_usuario_loggin (    
			usuario_id              VARCHAR(20)		COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,    
			terminal                VARCHAR(100)	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,    
			fecha_loggin			DATETIME     ,    
			session_id				VARCHAR(60)		COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,    
			rol_id                  VARCHAR(5)		COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,    
			emplazamiento_default	VARCHAR(15)		COLLATE SQL_Latin1_General_CP1_CI_AS NULL,    
			deposito_default        VARCHAR(15)		COLLATE SQL_Latin1_General_CP1_CI_AS NULL     
		)    
		exec FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN 'USER'    
    END
  
 --================================================================    
   
	SELECT USUARIO_ID FROM #temp_usuario_loggin  
    
    
   BEGIN TRY    
   BEGIN TRANSACTION          
	--MARCO COMO PROCESADOS 
	update sys_int_documento set estado_gt = 'P', FECHA_ESTADO_GT = getdate() where doc_ext=@Doc_Ext      
	update sys_int_det_documento set estado_gt ='P',FECHA_ESTADO_GT = getdate() where doc_ext=@Doc_Ext      
      
	--1) Creacion del Documento.      
	insert into documento(cliente_id, tipo_comprobante_id, sucursal_destino, fecha_cpte, fecha_pedida_ent, status, anulado, nro_remito, nro_despacho_importacion, prioridad_picking,tipo_operacion_id,det_tipo_operacion_id,GRUPO_PICKING)        
	select	cliente_id, 'E04', agente_id, getdate(), getdate(), 'D05', '0', DOC_EXT, CODIGO_VIAJE,'1','EGR','MAN',INFO_ADICIONAL_1      
	from	sys_int_documento      
	where	codigo_viaje=@COD_VIAJE AND DOC_EXT = @Doc_Ext AND CLIENTE_ID = @CLIENTE_ID     
           
	--Obtengo el documento_id      
	set @Documento_id= Scope_identity()      
   
	UPDATE SYS_INT_DET_DOCUMENTO SET DOCUMENTO_ID=@DOCUMENTO_ID WHERE CLIENTE_ID=@CLIENTE_ID AND DOC_EXT=@DOC_EXT;
          
	--2) Creacion de los detalles del documento.      
	insert into det_documento (documento_id, nro_linea, cliente_id, producto_id, cantidad, tie_in, unidad_id, descripcion, busc_individual, item_ok, cant_solicitada)      
	select	@documento_id, ROW_NUMBER() OVER (ORDER BY SD.NRO_LINEA), sd.cliente_id, sd.producto_id, sd.cantidad_solicitada,'0',      
			p.unidad_id, p.descripcion, null, null,sd.cantidad_solicitada      
	from	sys_int_det_documento sd inner join producto p	on(sd.cliente_id=p.cliente_id and sd.producto_id=p.producto_id)      
			inner join sys_int_documento s					on(sd.cliente_id=s.cliente_id and sd.doc_ext=s.doc_ext)      
	where	s.codigo_viaje=@COD_VIAJE 
			AND SD.DOC_EXT = @Doc_Ext     
          
          
	--3) Asigno       
	SELECT @DOCUMENTO_ID, @COD_VIAJE    
	      
	--Exec LocatorEgreso @Documento_id, @COD_VIAJE   
	EXEC dbo.LocatorEgreso_OLA @Documento_id,@CLIENTE_ID,@COD_VIAJE
	
	      
	--Exec Dbo.Ingresa_Picking @Documento_id 
	EXEC dbo.INGRESA_PICKING_OLA @CLIENTE_ID, @COD_VIAJE 

	--Cuando no tiene stock el viaje o la ola no manda nada a dev.
	--Con esto controlo que sea el ultimo item de la ola.
	PRINT('COMIENZO CONTROL.')
	
	SELECT	@CONTROL=COUNT(*)
	FROM	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD
			ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
	WHERE	SD.CODIGO_VIAJE=@COD_VIAJE
			AND SDD.DOCUMENTO_ID IS NULL

	IF @CONTROL=0 BEGIN
		PRINT('ENTRO PORQUE ES EL ULTIMO DE LA COLA')
		--es el ultimo item de la ola. Ahora reviso si al menos un pedido tuvo stock.
		SELECT	@CONTROL=COUNT(*)
		FROM	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD
				ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		WHERE	SD.CODIGO_VIAJE=@COD_VIAJE
				AND EXISTS(	SELECT	1
							FROM	DET_DOCUMENTO DD
							WHERE	SDD.DOCUMENTO_ID=DD.DOCUMENTO_ID)

		IF @CONTROL=0 BEGIN	
			PRINT('NO HAY EXISTENCIAS... DEBE DEVOLVER TODO')	
								
			--NO TUVO EXISTENCIAS LA OLA ASI Q LO DEVUELVO.
			insert into sys_dev_documento
			select	distinct 
					sid.CLIENTE_ID, CASE WHEN sid.tipo_documento_id='E04' THEN 'E05' WHEN sid.tipo_documento_id='E08' THEN 'E09' ELSE sid.tipo_documento_id END, 
					sid.CPTE_PREFIJO, sid.CPTE_NUMERO, getdate(), sid.FECHA_SOLICITUD_CPTE, sid.AGENTE_ID, sid.PESO_TOTAL, 
					sid.UNIDAD_PESO, sid.VOLUMEN_TOTAL, sid.UNIDAD_VOLUMEN, sid.TOTAL_BULTOS, sid.ORDEN_DE_COMPRA, 
					sid.OBSERVACIONES, cast(d.cpte_prefijo as varchar(20)) + cast(d.cpte_numero  as varchar(20)), 
					sid.NRO_DESPACHO_IMPORTACION, sid.DOC_EXT, sid.CODIGO_VIAJE, sid.INFO_ADICIONAL_1, sid.INFO_ADICIONAL_2, 
					sid.INFO_ADICIONAL_3, d.TIPO_COMPROBANTE_id, 	NULL, NULL, 'P', GETDATE(),Null, --Flg_Movimiento 
					sid.CUSTOMS_1,sid.CUSTOMS_2,sid.CUSTOMS_3,null as nro_guia, null as importe_flete,null as transporte_id
			from	sys_int_documento sid
					left join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
			where	sid.CODIGO_VIAJE=@COD_VIAJE
					AND ((D.TIPO_OPERACION_ID IS NULL) OR(D.TIPO_OPERACION_ID='EGR'))
					and not exists (select	1 
									from	sys_dev_documento sd 
									where	sd.cliente_id=sid.cliente_id
											and sd.doc_ext=sid.doc_ext)
			--DEVUELVO TODO EL DETALLE.
			insert into sys_dev_det_documento
			select	dd.doc_ext ,dd.nro_linea ,dd.cliente_id ,dd.producto_id ,dd.cantidad_solicitada
					,0 ,dd.est_merc_id ,dd.cat_log_id ,dd.nro_bulto ,dd.descripcion ,dd.nro_lote
					,dd.nro_pallet,dd.fecha_vencimiento ,dd.nro_despacho,dd.nro_partida
					,dd.unidad_id,dd.unidad_contenedora_id,dd.peso,dd.unidad_peso,dd.volumen
					,dd.unidad_volumen,dd.prop1,dd.prop2,dd.prop3,dd.largo,dd.alto,dd.ancho
					,dd.doc_back_order,null,null,dd.estado_gt,getdate(),dd.documento_id
					,dd.nave_id,dd.nave_cod,Null,dd.CUSTOMS_1,dd.CUSTOMS_2,dd.CUSTOMS_3,null as nro_cmr
			from	sys_int_det_documento dd
					inner join sys_int_documento d on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			where	cast(dd.doc_ext + dd.producto_id as varchar(400))  not in 
					(select cast(doc_ext + producto_id as varchar(400)) from sys_dev_det_documento)
					and d.codigo_viaje=@COD_VIAJE
					and not exists (select 1 from sys_dev_det_documento where sys_dev_det_documento.cliente_id = dd.cliente_id and sys_dev_det_documento.doc_Ext = dd.doc_ext and sys_dev_det_documento.nro_linea = dd.nro_linea)
														
		END
    END   
	commit transaction      
       
   END TRY      
   BEGIN CATCH      
   IF @@TRANCOUNT > 0     
   BEGIN     
    ROLLBACK     
    SELECT 'ERROR'  
    EXEC usp_RethrowError  
   END    
   END CATCH      
END


GO


