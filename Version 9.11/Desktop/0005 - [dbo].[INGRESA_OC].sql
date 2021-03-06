set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go





ALTER PROCEDURE [dbo].[INGRESA_OC]  
@CLIENTE_ID    VARCHAR(15),  
@OC          VARCHAR(100),  
@Remito       varchar(30),  
@DOCUMENTO_ID   NUMERIC(20,0) OUTPUT,  
@USUARIO_IMP   VARCHAR(20)  
  
AS  
BEGIN  
 SET XACT_ABORT ON  
 SET NOCOUNT ON  
  
 DECLARE @DOC_ID				NUMERIC(20,0)  
 DECLARE @DOC_TRANS_ID			NUMERIC(20,0)  
 DECLARE @DOC_EXT				VARCHAR(100)  
 DECLARE @SUCURSAL_ORIGEN		VARCHAR(20)  
 DECLARE @CAT_LOG_ID			VARCHAR(50)  
 DECLARE @DESCRIPCION			VARCHAR(30)  
 DECLARE @UNIDAD_ID				VARCHAR(15)  
 DECLARE @NRO_PARTIDA			VARCHAR(100)  
 DECLARE @LOTE_AT				VARCHAR(50)  
 DECLARE @Preing				VARCHAR(45)  
 DECLARE @CatLogId				Varchar(50)  
 DECLARE @LineBO				Float  
 DECLARE @qtyBO					Float  
 DECLARE @ToleranciaMax			Float  
 DECLARE @QtyIngresada			Float  
 DECLARE @tmax					Float  
 DECLARE @MAXP					VARCHAR(50)  
 DECLARE @NROLINEA				INTEGER  
 DECLARE @cantidad				numeric(20,5)  
 DECLARE @fecha					datetime   
 DECLARE @PRODUCTO_ID			VARCHAR(30)  
 DECLARE @PALLET_AUTOMATICO		VARCHAR(1)  
 DECLARE @LOTE					VARCHAR(1)  
 DECLARE @NRO_PALLET			VARCHAR(100)  
 -- Catalina Castillo.25/01/2012.Se agrega variable para saber si tiene registros de contenedoras, el producto   
 DECLARE @NRO_REG_CONTENEDORAS	INTEGER  
 DECLARE @NROBULTO				INTEGER  
 DECLARE @NRO_LINEA_CONT		INTEGER  
 DECLARE @CPTE_PREFIJO			VARCHAR(10)  
 DECLARE @CPTE_NUMERO			VARCHAR(20)  
 -- LRojas TrackerID 3851 29/03/2012: Control, si el producto genera Back Order se crea un nuevo ingreso, de lo contrario no  
 DECLARE @GENERA_BO				VARCHAR(1)  
 DECLARE @NRO_LOTE				VARCHAR(100)  
 DECLARE @INGLOTEPROVEEDOR		VARCHAR(1)  
 DECLARE @VLOTE_DOC				VARCHAR(100)
 DECLARE @VPARTIDA_DOC			VARCHAR(100)
 DECLARE @ST					VARCHAR(1)
 -----------------------------------------------------------------------------------------------------------------  
 --obtengo los valores de las secuencias.  
 -----------------------------------------------------------------------------------------------------------------   
 --obtengo la secuencia para el numero de partida.  
 -- exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output  
 SET @NROBULTO = 0  
 SET @NRO_LINEA_CONT = 0  
  SELECT  TOP 1  
    @DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID, @cpte_prefijo=sd.CPTE_PREFIJO , @cpte_numero=sd.CPTE_NUMERO  
  FROM  SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
  WHERE  ORDEN_DE_COMPRA=@OC  
    AND SD.CLIENTE_ID=@CLIENTE_ID  
    and SDD.fecha_estado_gt is null  
    and SDD.estado_gt is null  
       
 -----------------------------------------------------------------------------------------------------------------  
 --Comienzo con la carga de las tablas.  
 -----------------------------------------------------------------------------------------------------------------  
 Begin transaction   
 --Creo Documento  
 Insert into Documento ( Cliente_id , Tipo_comprobante_id , tipo_operacion_id , det_tipo_operacion_id , sucursal_origen  , fecha_cpte , fecha_pedida_ent , Status , anulado , nro_remito ,orden_de_compra, nro_despacho_importacion ,GRUPO_PICKING  , fecha_alta_gtw, CPTE_PREFIJO , CPTE_NUMERO)  
     Values( @Cliente_Id , 'DO'     , 'ING'    , 'MAN'     ,@SUCURSAL_ORIGEN  , GETDATE()  , GETDATE()   ,'D05'  ,'0'  , @Remito  ,@oc   ,@DOC_EXT     ,null   , getdate(),@cpte_prefijo, @cpte_numero)    
 --Obtengo el Documento Id recien creado.   
 Set @Doc_ID= Scope_identity()  
 
 IF (CURSOR_STATUS('variable','Ingreso_Cursor')>=-1)
 BEGIN
	 DEALLOCATE Ingreso_Cursor;
 END

 declare Ingreso_Cursor CURSOR FOR  
 select doc_ext,producto_id, cantidad, fecha, CASE WHEN nro_partida = '' THEN NULL ELSE nro_partida END, CASE WHEN nro_lote = '' THEN NULL ELSE nro_lote END from ingreso_oc WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PROCESADO = 0)order
 by CANT_CONTENEDORAS   
  
 set @Nrolinea=0  
 open Ingreso_Cursor  
 fetch next from Ingreso_Cursor INTO @doc_ext,@producto_id, @cantidad, @fecha, @nro_partida, @nro_lote  
   
 WHILE @@FETCH_STATUS = 0  
 BEGIN   
  
  IF @NRO_LOTE = ''  
   SET @NRO_LOTE = NULL  
    
  IF @NRO_PARTIDA = ''  
   SET @NRO_PARTIDA = NULL  
  
  --exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output  
  SET @PALLET_AUTOMATICO=NULL  
  set @lote=null  
  set @Nrolinea= @Nrolinea + 1  
    
    select @SUCURSAL_ORIGEN=agente_id from sys_int_documento where doc_ext = @DOC_EXT and cliente_id = @CLIENTE_ID  
  /*SELECT  TOP 1  
    @DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID  
  FROM  SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
  WHERE  ORDEN_DE_COMPRA=@OC  
    AND PRODUCTO_ID=@PRODUCTO_ID  
    AND SD.CLIENTE_ID=@CLIENTE_ID  
        AND ISNULL(SDD.NRO_LOTE,'') = @nro_lote  
        AND ISNULL(SDD.NRO_PARTIDA,'')=@nro_partida  
    and SDD.fecha_estado_gt is null  
    and SDD.estado_gt is null  
          
    PRINT 'DOC_EXT EN BSUQUEDA = ' + ISNULL(@DOC_EXT,'') + ', PRODUCTO_ID = ' + @PRODUCTO_ID  
          
    IF ISNULL(@DOC_EXT,'')=''  
    BEGIN  
    SELECT  TOP 1  
      @DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID  
    FROM  SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
    WHERE  ORDEN_DE_COMPRA=@OC  
      AND PRODUCTO_ID=@PRODUCTO_ID  
      AND SD.CLIENTE_ID=@CLIENTE_ID  
      and SDD.fecha_estado_gt is null  
      and SDD.estado_gt is null  
    END*/  
      
    --PRINT 'DOC_EXT EN BSUQUEDA = ' + ISNULL(@DOC_EXT,'') + ', PRODUCTO_ID = ' + @PRODUCTO_ID  
      
  if @doc_ext is null  
  begin  
   raiserror('El producto %s no se encuentra en la orden de compra %s',16,1,@producto_id, @oc)  
   return  
  end  
  SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0) from producto where cliente_id=@cliente_id and producto_id=@producto_id  
  
  -----------------------------------------------------------------------------------------------------------------  
  --tengo que controlar el maximo en cuanto a tolerancias.  
  -----------------------------------------------------------------------------------------------------------------  
  --Cambio esta linea x la de abajo ya que el control lo tengo que hacer por OC y producto_id y no por @doc_ext  
  Select  @qtyBO=sum(cantidad_solicitada)  
  from sys_int_det_documento  
  where doc_ext=@doc_ext  
    and fecha_estado_gt is null  
    and estado_gt is null  
    
  
  set @tmax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)  
    
  if @cantidad > @tmax  
  begin  
   Set @maxp=ROUND(@tmax,0)  
   raiserror('1- La cantidad recepcionada supera a la tolerancia maxima permitida.  Maximo permitido: %s ',16,1, @maxp)  
   return  
  end  
  -----------------------------------------------------------------------------------------------------------------  
  --Obtengo las categorias logicas antes de la transaccion para acortar el lockeo.  
  -----------------------------------------------------------------------------------------------------------------  
  SELECT  @CAT_LOG_ID=PC.CAT_LOG_ID  
  FROM  RL_PRODUCTO_CATLOG PC   
  WHERE  PC.CLIENTE_ID=@CLIENTE_ID  
    AND PC.PRODUCTO_ID=@PRODUCTO_ID  
    AND PC.TIPO_COMPROBANTE_ID='DO'  
  
  If @CAT_LOG_ID Is null begin  
   --entra porque no tiene categorias particulares y busca la default.  
   select  @CAT_LOG_ID=p.ing_cat_log_id,  
     @PALLET_AUTOMATICO=PALLET_AUTOMATICO,  
     @lote=lote_automatico,  
          @INGLOTEPROVEEDOR=isnull(ingloteproveedor,'0')  
   From  producto p   
   where   p.cliente_id=@CLIENTE_ID  
     and p.producto_id=@PRODUCTO_ID  
  end   
  IF @PALLET_AUTOMATICO = '1'  
   BEGIN  
    --obtengo la secuencia para el numero de partida.  
      exec get_value_for_sequence  'NROPALLET_SEQ', @nro_pallet Output  
   END  
     
  if @lote='1' AND @INGLOTEPROVEEDOR='0'  
   begin    
    --obtengo la secuencia para el numero de Lote.  
    exec get_value_for_sequence 'NROLOTE_SEQ', @NRO_LOTE Output     
   end  
  select @descripcion=descripcion, @unidad_id=unidad_id from producto where cliente_id=@cliente_id and producto_id=@producto_id  
  
  -- Esto se usa para los clientes que no usan pallet caso contrario comentarlo  
  --set @nro_pallet = '99999'   
    
  --Catalina Castillo.25/01/2012.Se verifica que existan registros en la tabal configuracion_contenedoras  
   SELECT @NRO_REG_CONTENEDORAS=COUNT(*) from CONFIGURACION_CONTENEDORAS   
   WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
	AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
	AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

   SET @NRO_LINEA_CONT = @NroLinea  
   IF @NRO_REG_CONTENEDORAS>0  
    BEGIN  
     DECLARE Contenedoras_Cursor CURSOR FOR  
     SELECT Nro_Contenedora, Cantidad FROM CONFIGURACION_CONTENEDORAS   
      WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id)
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

       
       
     OPEN Contenedoras_Cursor  
     FETCH NEXT FROM Contenedoras_Cursor INTO @NROBULTO, @cantidad  
       
     WHILE @@FETCH_STATUS = 0  
     BEGIN   
  
     -- INSERTANDO EL DETALLE  
      INSERT INTO det_documento (documento_id, nro_linea , cliente_id , producto_id , cantidad , cat_log_id , cat_log_id_final , tie_in , fecha_vencimiento , nro_partida , unidad_id  , descripcion , busc_individual , item_ok , cant_solicitada , prop1 , prop2   , nro_bulto ,nro_lote)  
           VALUES(@doc_id, @Nrolinea , @cliente_id , @producto_id , @cantidad , null   , @cat_log_id  , '0'  , null   , @NRO_PARTIDA , @unidad_id , @descripcion , '1'    , '1'  ,@cantidad   , @nro_pallet ,@DOC_EXT , @NROBULTO  , @NRO_LOTE)  
  
     SET @Nrolinea=@Nrolinea+1  
     FETCH NEXT FROM Contenedoras_Cursor INTO @NROBULTO, @cantidad  
     END   
     --COMMIT TRANSACTION  
     CLOSE Contenedoras_Cursor  
     DEALLOCATE Contenedoras_Cursor  
      SET @NroLinea = @NRO_LINEA_CONT   
    END  
  ELSE  
   BEGIN  
  
  -- INSERTANDO EL DETALLE  
  insert into det_documento (documento_id, nro_linea , cliente_id , producto_id , cantidad , cat_log_id , cat_log_id_final , tie_in , fecha_vencimiento , nro_partida , unidad_id  , descripcion , busc_individual , item_ok , cant_solicitada , prop1 , prop2 
  , nro_bulto ,nro_lote)  
        values(@doc_id, @Nrolinea , @cliente_id , @producto_id , @cantidad , null   , @cat_log_id  , '0'  , null   , @nro_partida , @unidad_id , @descripcion , '1'    , '1'  ,@qtyBO   , @nro_pallet ,@DOC_EXT , null  , @NRO_LOTE)  
   END  
  --Documento a Ingreso.  
  select  @Preing=nave_id  
  from nave  
  where pre_ingreso='1'  
    
  SELECT  @catlogid=cat_log_id  
  FROM  categoria_stock cs  
    INNER JOIN categoria_logica cl  
    ON cl.categ_stock_id = cs.categ_stock_id  
  WHERE  cs.categ_stock_id = 'TRAN_ING'  
    And cliente_id =@cliente_id  
  
  UPDATE det_documento  
  Set cat_log_id =@catlogid  
  WHERE documento_id = @Doc_ID  
  
  Update documento set status='D20' where documento_id=@doc_id  
  
  
  --Catalina Castillo.25/01/2012.Se verifica que existan registros en la tabal configuracion_contenedoras  
   SELECT @NRO_REG_CONTENEDORAS= COUNT(*) from CONFIGURACION_CONTENEDORAS   
   WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
	AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
	AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

  
   IF @NRO_REG_CONTENEDORAS>0  
    BEGIN  
     DECLARE Contenedoras_RL_Cursor CURSOR FOR  
     SELECT Cantidad FROM CONFIGURACION_CONTENEDORAS   
      WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

       
     OPEN Contenedoras_RL_Cursor  
     FETCH NEXT FROM Contenedoras_RL_Cursor INTO @cantidad  
       
     WHILE @@FETCH_STATUS = 0  
     BEGIN   
  
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
      Values (NULL, NULL, NULL, NULL, @cantidad, NULL, NULL, NULL, NULL, @PREING, @doc_id, @Nrolinea, null, null, null, null, null, @cliente_id, @catlogid,@CAT_LOG_ID,null)  
       
     SET @Nrolinea=@Nrolinea+1  
     FETCH NEXT FROM Contenedoras_RL_Cursor INTO @cantidad  
     END   
     --COMMIT TRANSACTION  
     CLOSE Contenedoras_RL_Cursor  
     DEALLOCATE Contenedoras_RL_Cursor  
    --Sumo el total de la cantidad para setear y que no genere un backorder  
     SELECT @cantidad = SUM(CANTIDAD) FROM CONFIGURACION_CONTENEDORAS  
      WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

    --Elimino los registros que cumplan los filtros de la tabla CONFIGURACION_CONTENEDORAS  
     DELETE FROM CONFIGURACION_CONTENEDORAS WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

     SET @Nrolinea=@Nrolinea-1  
    END  
  ELSE  
   BEGIN    
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
  Values (NULL, NULL, NULL, NULL, @cantidad, NULL, NULL, NULL, NULL, @PREING, @doc_id, @Nrolinea, null, null, null, null, null, @cliente_id, @catlogid,@CAT_LOG_ID,null)  
  END  
  ------------------------------------------------------------------------------------------------------------------------------------  
  --Generacion del Back Order.  
  -----------------------------------------------------------------------------------------------------------------  
  select @lineBO=max(isnull(nro_linea,1))+1 from sys_int_det_documento WHERE   DOC_EXT=@doc_ext  
      
    --PRINT 'DOC_EXT= ' + @DOC_EXT + ', NRO_LINEA = ' + CAST(@LINEBO AS VARCHAR)  
      
  Select  @qtyBO=sum(cantidad_solicitada)  
  from sys_int_det_documento  
  where doc_ext=@doc_ext  
    and fecha_estado_gt is null  
    and estado_gt is null  
  
  --PRINT 'DOC_EXT= ' + @DOC_EXT + ', QTY_BO = ' + CAST(@qtyBO AS VARCHAR)  

  SELECT	@VLOTE_DOC=NRO_LOTE, @VPARTIDA_DOC=NRO_PARTIDA
  FROM		SYS_INT_DET_DOCUMENTO
  WHERE		DOC_EXT=@doc_ext
           
  UPDATE	SYS_INT_DET_DOCUMENTO 
  SET		ESTADO_GT='P', 
			DOC_BACK_ORDER=@doc_ext,
			FECHA_ESTADO_GT=getdate(), 
			DOCUMENTO_ID=@Doc_ID 
			--NRO_PARTIDA	=CASE(ISNULL(NRO_PARTIDA,'#'))  WHEN '#' THEN NULL ELSE @NRO_PARTIDA END,
			--NRO_LOTE	=CASE(ISNULL(NRO_LOTE,'#'))		WHEN '#' THEN NULL ELSE @NRO_LOTE END			
  WHERE		DOC_EXT=@doc_ext and documento_id is null  
  
  set @qtyBO=@qtyBO - @cantidad  
          
  SELECT @GENERA_BO =   
     CASE P.BACK_ORDER   
   WHEN '1' THEN 'S'   
   WHEN '0' THEN 'N'  
     END  
  FROM PRODUCTO P INNER JOIN SYS_INT_DET_DOCUMENTO SIDD ON (P.PRODUCTO_ID = SIDD.PRODUCTO_ID)  
  WHERE SIDD.DOC_EXT = @doc_ext AND SIDD.DOCUMENTO_ID = @Doc_ID AND P.CLIENTE_ID=@CLIENTE_ID  
       
  -- LRojas TrackerID 3851 29/03/2012: Se debe tener en cuenta la parametrización del producto.  
  IF (@qtyBO > 0) AND (@GENERA_BO = 'S') --Si esta variable es mayor a 0, genero el backorder.  
  begin  
	insert into sys_int_det_documento   
	SELECT	TOP 1   
			DOC_EXT, @LINEBO ,CLIENTE_ID, PRODUCTO_ID, @QTYBO ,CANTIDAD , EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO, 
			NRO_PARTIDA,UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, NULL, NULL, NULL,NULL,NULL,NULL,
			NULL,NULL,NULL,NULL,NULL
	FROM	SYS_INT_DET_DOCUMENTO   
	WHERE	DOC_EXT=@DOC_EXT   
  end  
  ------------------------------------------------------------------------------------------------------------------------------------  
  --Guardo en la tabla de auditoria  
  -----------------------------------------------------------------------------------------------------------------  
  exec dbo.AUDITORIA_HIST_INSERT_ING @doc_id  
  --insert into IMPRESION_RODC VALUES(@Doc_id, 1, @Tipo_eti,'0')  
  --COMMIT TRANSACTION  
  Set @DOCUMENTO_ID=@doc_id  
  
  update ingreso_oc  
  set procesado = 1  
  WHERE     (CLIENTE_ID = @CLIENTE_ID) AND (PRODUCTO_ID = @producto_id) AND (ORDEN_COMPRA = @oc)   
   AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)  
   AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)  
  
    
    SET @DOC_EXT = NULL  
  fetch next from Ingreso_Cursor INTO @doc_ext,@producto_id, @cantidad, @fecha, @nro_partida, @nro_lote  
 END   
 --COMMIT TRANSACTION  
 CLOSE Ingreso_Cursor  
 DEALLOCATE Ingreso_Cursor  
   
 -- LRojas 02/03/2012 TrackerID 3806: Inserto Usuario para Demonio de Impresion  
 INSERT INTO IMPRESION_RODC VALUES(@Doc_ID,0,'D',0, @USUARIO_IMP)  
 -----------------------------------------------------------------------------------------------------------------  
 --ASIGNO TRATAMIENTO...  
 -----------------------------------------------------------------------------------------------------------------  
 exec asigna_tratamiento#asigna_tratamiento_ing @doc_id   
 exec dbo.AUDITORIA_HIST_INSERT_ING @doc_id  
 if @@error<>0  
 begin  
  rollback transaction  
  raiserror('No se pudo completar la transaccion',16,1)  
 end  
 else  
 begin  
  commit transaction  
 end   
END  
  



                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     