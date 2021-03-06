
ALTER PROCEDURE [dbo].[SYS_DEV_COMPROBANTES_ING]
@doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS
  DECLARE @qty AS numeric(3,0)
  DECLARE @nro_lin AS numeric(20,0)
  DECLARE @E_SP     AS VARCHAR(300)
  declare @Cliente_Id as varchar(15)  
  DECLARE @TempDocumento_id AS VARCHAR(100) 
  DECLARE @TempTipoComp		AS VARCHAR(10)
  
  set xact_abort on
BEGIN
	
  select @qty=count(*) from sys_dev_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext
  select @nro_lin=max(nro_linea) from sys_dev_det_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext
  --select @Cliente_Id =cliente_id from documento where DOCUMENTO_ID=@documento_id
  
    BEGIN 

        IF @qty=0 BEGIN 	
			
            INSERT INTO sys_dev_documento
            SELECT	d.CLIENTE_ID      ,d.TIPO_COMPROBANTE_ID    ,d.CPTE_PREFIJO     ,d.CPTE_NUMERO    ,d.FECHA_CPTE
                  ,GetDate()         ,d.SUCURSAL_ORIGEN		     ,d.PESO_TOTAL       ,d.UNIDAD_PESO    ,d.VOLUMEN_TOTAL
                  ,d.UNIDAD_VOLUMEN  ,d.TOTAL_BULTOS               ,d.ORDEN_DE_COMPRA,d.OBSERVACIONES
                  ,d.NRO_REMITO      ,d.NRO_DESPACHO_IMPORTACION   ,'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))
                  ,NULL              ,NULL                         ,NULL             ,NULL
                  ,d.TIPO_COMPROBANTE_ID                           ,NULL             ,NULL
                  ,'P'               ,GETDATE()                    ,null	         ,NULL --customs_1
                  ,NULL              ,NULL                         ,null             ,null --importe_flete
                  ,null				 ,NULL							,NULL			,NULL 
            FROM	  documento d (nolock)
            WHERE	  d.documento_id = @documento_id
        END
        
        INSERT INTO sys_dev_det_documento
        SELECT  'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))    
                ,isnull(@nro_lin,0) + dd.NRO_LINEA    
                ,d.CLIENTE_ID     
                ,dd.PRODUCTO_ID    
                ,dd.CANTIDAD     --sidd.CANTIDAD_SOLICITADA    
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
                ,dd.UNIDAD_ID    --sidd.UNIDAD_ID    
                ,NULL     --sidd.UNIDAD_CONTENEDORA_ID    
                ,dd.PESO    --sidd.PESO    
                ,dd.UNIDAD_PESO   --sidd.UNIDAD_PESO    
                ,dd.VOLUMEN    --sidd.VOLUMEN    
                ,dd.UNIDAD_VOLUMEN  --sidd.UNIDAD_VOLUMEN    
                ,dd.PROP1    --sidd.PROP1    
                ,dbo.Get_data_I08(dd.documento_id,dd.nro_linea,'1')    
                ,dbo.Get_data_I08(dd.documento_id,dd.nro_linea,'2')    
                ,dd.LARGO    --sidd.LARGO    
                ,dd.ALTO     --sidd.ALTO     
                ,dd.ANCHO    --sidd.ANCHO    
                ,dd.TRACE_BACK_ORDER --sidd.DOC_BACK_ORDER    
                ,NULL    
                ,NULL    
                ,'P'    
                ,GETDATE()    
                ,DD.DOCUMENTO_ID    
                ,dbo.get_nave_id(dd.documento_id,dd.nro_linea)    
                ,dbo.get_nave_cod(dd.documento_id,dd.nro_linea)    
                ,null  --Flg_movimiento      
                ,NULL --Customs_1
                ,NULL --Customs_2
                ,NULL --Customs_3
                ,cmr.nro_cmr  --nro cmr
        FROM	  documento d (nolock)
                inner join det_documento dd (nolock)	on (d.documento_id=dd.documento_id)  
      		      LEFT JOIN NROCMR_POR_DOCUMENTO CMR (NOLOCK)	ON (CMR.CLIENTE_ID = D.CLIENTE_ID AND CMR.DOCUMENTO_ID = D.DOCUMENTO_ID	AND CMR.NRO_LINEA = DD.NRO_LINEA)
        WHERE	  dd.documento_id=@documento_id;
        
        SELECT  @E_SP=ISNULL(VALOR,'0')
        FROM    SYS_PARAMETRO_PROCESO
        WHERE   PROCESO_ID='DEV'
                AND SUBPROCESO_ID='EJECUTA_SP'
                AND PARAMETRO_ID='E_SP';
                
		SELECT	@cliente_id=CLIENTE_ID,
				@TempDocumento_id=	CASE WHEN TIPO_COMPROBANTE_ID ='DE' 
									THEN 'DEV'
									ELSE TIPO_COMPROBANTE_ID + '-' END 
									+ CAST(DOCUMENTO_ID AS VARCHAR(100)),
				@TempTipoComp = TIPO_COMPROBANTE_ID
		FROM	DOCUMENTO 
		WHERE	DOCUMENTO_ID=@DOCUMENTO_ID			                
                
        if (@e_sp='1') AND (@TempTipoComp = 'DE') begin 
			
			BEGIN TRY
			
				if @TempTipoComp = 'DE'
					exec dbo.ERP_CARGAR_DATOS @cliente_id, @TempDocumento_id
				else	
					exec dbo.ERP_INGRESOS @cliente_id, @TempDocumento_id 			
				
				INSERT INTO AUDITORIA_ERP_INGRESOS VALUES(GETDATE(),@CLIENTE_ID,@TempDocumento_id,'OK','ADMIN',HOST_NAME(),'DESKTOP',NULL,@DOCUMENTO_ID);
				
			END TRY
			BEGIN CATCH
			
				INSERT INTO AUDITORIA_ERP_INGRESOS VALUES(GETDATE(),@CLIENTE_ID,@TempDocumento_id,'ERROR','ADMIN',HOST_NAME(),'DESKTOP',ERROR_MESSAGE(),@DOCUMENTO_ID);						
				
			END CATCH
        end;
    END
END --fin procedure

