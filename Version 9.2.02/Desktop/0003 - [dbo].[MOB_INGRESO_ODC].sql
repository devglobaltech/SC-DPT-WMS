/****** Object:  StoredProcedure [dbo].[MOB_INGRESO_ODC]    Script Date: 07/10/2014 13:19:00 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MOB_INGRESO_ODC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[MOB_INGRESO_ODC]
GO

CREATE PROCEDURE [dbo].[MOB_INGRESO_ODC]  
 @ID			VARCHAR(20),  
 @CLIENTE_ID	VARCHAR(15),  
 @PROVEEDOR_ID	VARCHAR(20)   
AS   
BEGIN  
 DECLARE @DOC_ID    NUMERIC(20,0)  
 DECLARE @NROLINEA   Float  
 DECLARE @USUARIO   VARCHAR(20)  
 DECLARE @OC     VARCHAR(100)  
 DECLARE @PRODUCTO_ID  VARCHAR(30)  
 DECLARE @DESCRIPCION  VARCHAR(200)  
 DECLARE @CANTIDAD   NUMERIC(20,5)  
 DECLARE @DOC_EXT   VARCHAR(100)  
 DECLARE @cant_sol   NUMERIC(20,5)  
 DECLARE @cant_oc    NUMERIC(20,5)  
 DECLARE @cant_pro    NUMERIC(20,5) --Cantidad que tiene el producto  
 DECLARE @cant_ins    NUMERIC(20,5) --Cantidad que se inserta  
 DECLARE @NRO_PARTIDA  NUMERIC(38)  
 DECLARE @PALLET_AUTOMATICO VARCHAR(1)  
 DECLARE @lote    VARCHAR(1)  
 DECLARE @NRO_PALLET   VARCHAR(100)  
 DECLARE @LOTE_AT   VARCHAR(50)  
 DECLARE @CAT_LOG_ID   VARCHAR(50)  
 DECLARE @UNIDAD_ID   VARCHAR(15)  
 DECLARE @tmax    Float  
 DECLARE @ToleranciaMax  Float  
 DECLARE @qtyBO    Float  
 DECLARE @MAXP    VARCHAR(50)  
 DECLARE @Preing    VARCHAR(45)  
 DECLARE @CatLogId   Varchar(50)   
 DECLARE @LineBO    Float
 declare @Nro_Partida_at char(1)  
   

 --SELECT DISTINCT @CLIENTE_ID = CLIENTE_ID FROM SUCURSAL WHERE SUCURSAL_ID = @PROVEEDOR_ID
  
 SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN  
 --set @usuario='ADMIN'  
 
 BEGIN TRANSACTION  
     
   --Creo Documento  
   Insert into Documento ( Cliente_id , Tipo_comprobante_id , tipo_operacion_id , det_tipo_operacion_id , sucursal_origen  , fecha_cpte , fecha_pedida_ent , Status , anulado , nro_remito ,orden_de_compra, nro_despacho_importacion ,GRUPO_PICKING  , fecha_alta_gtw)  
       Values( @Cliente_Id , 'DO'     , 'ING'    , 'MAN'     ,upper(@PROVEEDOR_ID)  , GETDATE()  , GETDATE()   ,'D05'  ,'0'  , @ID  ,null   ,@DOC_EXT     ,null   , getdate())    
  
   --Obtengo el Documento Id recien creado.   
   Set @Doc_ID= Scope_identity()  
     
   set @Nrolinea=0  
  
   --Cursor de Productos  
   DECLARE CURSOR_PRODUCTO CURSOR FOR  
   SELECT PRODUCTO_ID, sum(CANTIDAD) as cantidad   
   from tmp_producto where PROCESADO ='0' AND PROVEEDOR_ID = @PROVEEDOR_ID and id =@ID AND USUARIO=@USUARIO  
   GROUP BY PRODUCTO_ID  
  
   OPEN CURSOR_PRODUCTO   
   FETCH NEXT FROM CURSOR_PRODUCTO INTO @PRODUCTO_ID, @CANTIDAD  
   WHILE @@FETCH_STATUS=0  
   BEGIN  
    set @cant_pro=@CANTIDAD  
    DECLARE CURSOR_OC CURSOR FOR  
    SELECT  SD.DOC_EXT, sdd.cantidad_solicitada, sdd.cantidad, SD.ORDEN_DE_COMPRA  
    FROM  SYS_INT_DOCUMENTO SD   
    INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
         WHERE  SDD.PRODUCTO_ID=@PRODUCTO_ID  
           AND SD.CLIENTE_ID=@CLIENTE_ID  
           and SDD.fecha_estado_gt is null  
           and SDD.estado_gt is null  
           AND AGENTE_ID=@PROVEEDOR_ID  
    order by sd.FECHA_CPTE        
  
    OPEN CURSOR_OC  
    FETCH NEXT FROM CURSOR_OC INTO @DOC_EXT,@cant_sol, @cant_oc,@oc  
    
    WHILE @@FETCH_STATUS=0  
    BEGIN  
     IF @DOC_EXT <>'' and @cant_pro >0  
     BEGIN  
      if @doc_ext is null  
      begin  
       raiserror('El producto %s no se encuentra en la orden de compra %s',16,1,@producto_id, @oc)  
       return  
      end  
	  
      
      set @Nro_Partida_at=Null
      SET @PALLET_AUTOMATICO=NULL  
      set @lote=null  
  
      IF @cant_pro >= @cant_sol  
      BEGIN  
       set @cant_ins=@cant_sol  
      END  
      else  
      if @cant_pro < @cant_sol  
      begin  
       set @cant_ins=@cant_pro  
      end  
  
      SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0) from producto where cliente_id=@cliente_id and producto_id=@producto_id  
       
      -----------------------------------------------------------------------------------------------------------------  
      --tengo que controlar el maximo en cuanto a tolerancias.  
      -----------------------------------------------------------------------------------------------------------------  
      Select  @qtyBO=sum(cantidad_solicitada)  
      from sys_int_det_documento  
      where doc_ext=@doc_ext  
        and fecha_estado_gt is null  
        and estado_gt is null  
          
      set @tmax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)  
       
      if @cant_ins > @tmax --@CANTIDAD > @tmax  
      begin  
       Set @maxp=ROUND(@tmax,0)  
       raiserror('1- La cantidad recepcionada supera a la tolerancia maxima permitida.  Maximo permitido: %s ',16,1, @maxp)  
       return  
      end  
  
      -----------------------------------------------------------------------------------------------------------------  
      --Obtengo las categorias logicas antes de la transaccion para acortar el lockeo.  
      -----------------------------------------------------------------------------------------------------------------  
      SELECT	DISTINCT @CAT_LOG_ID=CAT_LOG_ID
      FROM		SYS_INT_DET_DOCUMENTO
      WHERE		CLIENTE_ID=@CLIENTE_ID
				AND DOC_EXT=@doc_ext
				AND PRODUCTO_ID=@PRODUCTO_ID
      
      IF(@CAT_LOG_ID IS NULL) BEGIN
		SELECT  @CAT_LOG_ID=PC.CAT_LOG_ID  
		FROM	RL_PRODUCTO_CATLOG PC   
		WHERE	PC.CLIENTE_ID=@CLIENTE_ID  
				AND PC.PRODUCTO_ID=@PRODUCTO_ID  
				AND PC.TIPO_COMPROBANTE_ID='DO'  
      END    
      
      If @CAT_LOG_ID Is null   
      begin  
      --entra porque no tiene categorias particulares y busca la default.  
       select	@CAT_LOG_ID=p.ing_cat_log_id,  
				@PALLET_AUTOMATICO=PALLET_AUTOMATICO,  
				@lote=lote_automatico,
				@Nro_Partida_at =nro_partida_automatico         
       From		producto p   
       where	p.cliente_id=@CLIENTE_ID  
				and p.producto_id=@PRODUCTO_ID  
      end  
      IF @PALLET_AUTOMATICO = '1'  
      BEGIN  
       --obtengo la secuencia para el numero de partida.  
       exec get_value_for_sequence  'NROPALLET_SEQ', @nro_pallet Output  
      END  
      if @lote='1'  
      begin    
       --obtengo la secuencia para el numero de Lote.  
       exec get_value_for_sequence 'NROLOTE_SEQ', @Lote_At Output     
      end  
      if @nro_partida_at='1' begin
		exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output  
      end
      
      select @descripcion=descripcion, @unidad_id=unidad_id from producto where cliente_id=@cliente_id and producto_id=@producto_id  
      
      --set @nro_pallet=9999 --seteo el nro de pallet en 9999 porque este cliente ubica x bulto
        
      set @Nrolinea= @Nrolinea + 1  
      insert into det_documento (documento_id, nro_linea , cliente_id , producto_id , cantidad , cat_log_id , cat_log_id_final , tie_in , fecha_vencimiento , nro_partida , unidad_id  , descripcion , busc_individual , item_ok , cant_solicitada , prop1     
, prop2   , prop3, nro_bulto ,nro_lote)  
           values(@doc_id,      @Nrolinea , @cliente_id , @producto_id , @cant_ins , null   , @cat_log_id  , '0'  , null       , @nro_partida , @unidad_id , @descripcion , '1'    , '1'  ,@qtyBO     , @nro_pallet ,@DOC_EXT     , @oc  ,null  , @lote_at)  
  
      --Documento a Ingreso.  
      select  @Preing=nave_id  
      from nave  
      where pre_ingreso='1'  
        
      SELECT  @catlogid=cat_log_id  
      FROM  categoria_stock cs  
        INNER JOIN categoria_logica cl ON cl.categ_stock_id = cs.categ_stock_id  
      WHERE  cs.categ_stock_id = 'TRAN_ING'  
        And cliente_id =@cliente_id  
          
      UPDATE det_documento  
      Set cat_log_id =@catlogid  
      WHERE documento_id = @Doc_ID  
        
      Update documento set status='D20' where documento_id=@doc_id  
  
      Insert Into RL_DET_DOC_TRANS_POSICION (  
       DOC_TRANS_ID,    NRO_LINEA_TRANS,  
       POSICION_ANTERIOR,   POSICION_ACTUAL,  
       CANTIDAD,     TIPO_MOVIMIENTO_ID,  
       ULTIMA_ESTACION,   ULTIMA_SECUENCIA,  
       NAVE_ANTERIOR,    NAVE_ACTUAL,  
       DOCUMENTO_ID,    NRO_LINEA,  
       DISPONIBLE,     DOC_TRANS_ID_EGR,  
       NRO_LINEA_TRANS_EGR,  DOC_TRANS_ID_TR,  
       NRO_LINEA_TRANS_TR,   CLIENTE_ID,  
       CAT_LOG_ID,     CAT_LOG_ID_FINAL,  
       EST_MERC_ID)  
      Values (NULL, NULL, NULL, NULL, @cant_ins, NULL, NULL, NULL, NULL, @PREING, @doc_id, @Nrolinea, null, null, null, null, null, @cliente_id, @catlogid,@CAT_LOG_ID,null)  
  
      ------------------------------------------------------------------------------------------------------------------------------------  
      --Generacion del Back Order.  
      -----------------------------------------------------------------------------------------------------------------  
      select @lineBO=max(isnull(nro_linea,1))+1 from sys_int_det_documento WHERE   DOC_EXT=@doc_ext  
  
      Select  @qtyBO=sum(cantidad_solicitada)  
      from sys_int_det_documento  
      where doc_ext=@doc_ext AND producto_id=@producto_id   
        and fecha_estado_gt is null  
        and estado_gt is null  
  
      UPDATE SYS_INT_DET_DOCUMENTO   
      SET ESTADO_GT='P',   
       DOC_BACK_ORDER=@doc_ext,  
       FECHA_ESTADO_GT=getdate(),   
       cantidad=@cant_ins,  
       DOCUMENTO_ID=@Doc_ID  
      WHERE  DOC_EXT=@doc_ext and documento_id is null AND producto_id=@producto_id  
  
      set @qtyBO=@qtyBO - @cant_ins --@cantidad  
        
      IF @qtyBO>0 --Si esta variable es mayor a 0, genero el backorder.  
      begin  
       insert into sys_int_det_documento   
       select TOP 1   
				DOC_EXT, @lineBO ,CLIENTE_ID, PRODUCTO_ID, @qtyBO ,0 , EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO, NRO_PARTIDA, UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, NULL, NULL, NULL,  NULL,NULL,NULL,NULL,NULL,CUSTOMS_1,CUSTOMS_2,CUSTOMS_3
       from		sys_int_det_documento   
       WHERE  DOC_EXT=@Doc_Ext AND producto_id=@producto_id  
      end  
      if NOT (select count(*) from sys_int_det_documento WHERE estado_gt is null and DOC_EXT=@Doc_Ext) >0  
        begin  
       UPDATE SYS_INT_DOCUMENTO   
       SET ESTADO_GT='P' ,  
        FECHA_ESTADO_GT=getdate()   
       WHERE DOC_EXT=@doc_ext  
        end  
  
      set @cant_pro = @cant_pro - @cant_ins  
      if @cant_pro =0  
      begin  
       update tmp_producto  
       set procesado='1'  
       where ID=@ID and PRODUCTO_ID=@PRODUCTO_ID and usuario=@usuario  
      end        
     END  
     FETCH NEXT FROM CURSOR_OC INTO @DOC_EXT,@cant_sol, @cant_oc,@oc  
    END  
    CLOSE CURSOR_OC  
    DEALLOCATE CURSOR_OC  
      
    FETCH NEXT FROM CURSOR_PRODUCTO INTO @PRODUCTO_ID, @CANTIDAD  
   END  
   CLOSE CURSOR_PRODUCTO  
   DEALLOCATE CURSOR_PRODUCTO  
   INSERT INTO IMPRESION_RODC VALUES(@Doc_ID,0,'D',0,NULL)  
   
   -----------------------------------------------------------------------------------------------------------------  
   --ASIGNO TRATAMIENTO...  
   -----------------------------------------------------------------------------------------------------------------  
   exec asigna_tratamiento#asigna_tratamiento_ing @doc_id   
   exec dbo.AUDITORIA_HIST_INSERT_ING @doc_id  
     
   update tmp_remito  
   set procesado='1'  
   where id_remito=@ID and usuario=@usuario     
        
   delete tmp_producto  
   where id=@ID and usuario=@usuario  
     
  
 COMMIT TRANSACTION  
END




GO


