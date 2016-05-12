
/****** Object:  StoredProcedure [dbo].[INGRESA_OC]    Script Date: 07/16/2015 15:53:41 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[INGRESA_OC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[INGRESA_OC]
GO

CREATE PROCEDURE [dbo].[INGRESA_OC]
 @CLIENTE_ID  VARCHAR(15),
 @OC    VARCHAR(100),
 @Remito   VARCHAR(30),
 @DOCUMENTO_ID   NUMERIC(20,0) OUTPUT,
 @USUARIO_IMP VARCHAR(20)
AS
BEGIN
 SET XACT_ABORT ON
 SET NOCOUNT ON

 DECLARE @DOC_ID    NUMERIC(20,0)
 DECLARE @DOC_TRANS_ID   NUMERIC(20,0)
 DECLARE @DOC_EXT    VARCHAR(100)
 DECLARE @SUCURSAL_ORIGEN  VARCHAR(20)
 DECLARE @CAT_LOG_ID   VARCHAR(50)
 DECLARE @DESCRIPCION   VARCHAR(30)
 DECLARE @UNIDAD_ID    VARCHAR(15)
 DECLARE @NRO_PARTIDA   VARCHAR(100)
 DECLARE @NRO_DESPACHO   VARCHAR(50)
 DECLARE @LOTE_AT    VARCHAR(50)
 DECLARE @Preing    VARCHAR(45)
 DECLARE @CatLogId    VARCHAR(50)
 DECLARE @LineBO    FLOAT
 DECLARE @qtyBO     FLOAT
 DECLARE @ToleranciaMax   FLOAT
 DECLARE @QtyIngresada   FLOAT
 DECLARE @tmax     FLOAT
 DECLARE @MAXP     VARCHAR(50)
 DECLARE @NROLINEA    INTEGER
 DECLARE @cantidad    NUMERIC(20,5)
 DECLARE @fecha     DATETIME
 DECLARE @PRODUCTO_ID   VARCHAR(30)
 DECLARE @PALLET_AUTOMATICO  VARCHAR(1)
 DECLARE @LOTE     VARCHAR(1)
 DECLARE @NRO_PALLET   VARCHAR(100)
 -- Catalina Castillo.25/01/2012.Se agrega variable para saber si tiene registros de contenedoras, el producto
 DECLARE @NRO_REG_CONTENEDORAS INTEGER
 DECLARE @NROBULTO    INTEGER
 DECLARE @NRO_LINEA_CONT  INTEGER
 DECLARE @TIPO_DOCUMENTO VARCHAR(50)
 DECLARE @CPTE_PREFIJO   VARCHAR(10)
 DECLARE @CPTE_NUMERO   VARCHAR(20)
 -- LRojas TrackerID 3851 29/03/2012: Control, si el producto genera Back Order se crea un nuevo ingreso, de lo contrario no
 DECLARE @GENERA_BO    VARCHAR(1)
 DECLARE @NRO_LOTE    VARCHAR(100)
 DECLARE @INGLOTEPROVEEDOR  VARCHAR(1)
 DECLARE @VLOTE_DOC    VARCHAR(100)
 DECLARE @VPARTIDA_DOC   VARCHAR(100)
 DECLARE @ST     VARCHAR(1)
 DECLARE @DEXT    VARCHAR(100)
 DECLARE @VCANT   NUMERIC(20,5)
 Declare @CntDD   Numeric(20,5)
 Declare @CntIng  Numeric(20,5)
 Declare @Arg	  varchar(4000)
 Declare @OC_Ant  varchar(100)
 Declare @Prod_ant varchar(30)
 DECLARE @NRO_PART_AT	CHAR(1)
 DECLARE @SEQ			NUMERIC(30,0)
 DECLARE @VPARTIDA	VARCHAR(100)
 DECLARE @USUARIO   VARCHAR(20)
 DECLARE @EST_MERC_ID		VARCHAR(15)
 DECLARE @FECHAVTO     DATETIME
 -----------------------------------------------------------------------------------------------------------------
 --obtengo los valores de las secuencias.
 -----------------------------------------------------------------------------------------------------------------
 --obtengo la secuencia para el numero de partida.
 -- exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output
 SET @NROBULTO = 0
 SET @NRO_LINEA_CONT = 0
 
 SELECT TOP 1
		@DOC_EXT=SD.DOC_EXT, @TIPO_DOCUMENTO= SD.TIPO_DOCUMENTO_ID 
		,@SUCURSAL_ORIGEN=AGENTE_ID, @cpte_prefijo=sd.CPTE_PREFIJO , @cpte_numero=sd.CPTE_NUMERO
 FROM	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
 WHERE  ORDEN_DE_COMPRA=@OC
		AND SD.CLIENTE_ID=@CLIENTE_ID
		AND SDD.fecha_estado_gt IS NULL
		AND SDD.estado_gt IS NULL

 SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN  
 
 -----------------------------------------------------------------------------------------------------------------
 --Comienzo con la carga de las tablas.
 -----------------------------------------------------------------------------------------------------------------
 BEGIN TRANSACTION
 --Creo Documento
 INSERT INTO Documento ( Cliente_id , Tipo_comprobante_id , tipo_operacion_id , det_tipo_operacion_id , sucursal_origen  , fecha_cpte , fecha_pedida_ent , Status , anulado , nro_remito ,orden_de_compra, nro_despacho_importacion ,GRUPO_PICKING  , fecha_alta_gtw, CPTE_PREFIJO , CPTE_NUMERO)
     VALUES( @Cliente_Id , @TIPO_DOCUMENTO , 'ING'    , 'MAN'     ,@SUCURSAL_ORIGEN  , GETDATE()  , GETDATE()   ,'D05'  ,'0'  , @Remito  ,@oc   ,@DOC_EXT     ,NULL   , getdate(),@cpte_prefijo, @cpte_numero)
 --Obtengo el Documento Id recien creado.
 SET @Doc_ID= Scope_identity()

 IF (CURSOR_STATUS('variable','Ingreso_Cursor')>=-1)
 BEGIN
  DEALLOCATE Ingreso_Cursor;
 END

 DECLARE Ingreso_Cursor CURSOR FOR
 SELECT	doc_ext,
		producto_id, 
		cantidad, 
		fecha, 
		CASE WHEN nro_partida = '' THEN NULL ELSE nro_partida END, 
		CASE WHEN nro_lote = '' THEN NULL ELSE nro_lote END,
		CASE WHEN FECHA_VTO = '' THEN NULL ELSE FECHA_VTO END
 FROM	ingreso_oc 
 WHERE (CLIENTE_ID = @CLIENTE_ID) 
		AND (ORDEN_COMPRA = @oc) 
		AND (PROCESADO = 0) 
		AND USUARIO=@USUARIO  
ORDER BY CANT_CONTENEDORAS

SET @Nrolinea=0
OPEN Ingreso_Cursor

IF @@FETCH_STATUS = -1
BEGIN
	CLOSE Ingreso_Cursor
	DEALLOCATE Ingreso_Cursor
	RAISERROR('El usuario %s no tiene productos pendientes de recepción',16,1,@USUARIO)
	ROLLBACK TRANSACTION
	RETURN
END

FETCH NEXT FROM Ingreso_Cursor INTO @doc_ext,@producto_id, @cantidad, @fecha, @nro_partida, @nro_lote, @FECHAVTO

 WHILE @@FETCH_STATUS = 0
 BEGIN

  IF @NRO_LOTE = ''
   SET @NRO_LOTE = NULL

  IF @NRO_PARTIDA = ''
   SET @NRO_PARTIDA = NULL
   
  IF @FECHAVTO = ''
   SET @FECHAVTO = NULL      

  --exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output
  SET @PALLET_AUTOMATICO=NULL
  SET @lote=NULL
  SET @Nrolinea= @Nrolinea + 1

    SELECT @SUCURSAL_ORIGEN=agente_id FROM sys_int_documento WHERE doc_ext = @DOC_EXT AND cliente_id = @CLIENTE_ID


  IF @doc_ext IS NULL
  BEGIN
   RAISERROR('El producto %s no se encuentra en la orden de compra %s',16,1,@producto_id, @oc)
   RETURN
  END
  SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0) FROM producto WHERE cliente_id=@cliente_id AND producto_id=@producto_id

  -----------------------------------------------------------------------------------------------------------------
  --tengo que controlar el maximo en cuanto a tolerancias.
  -----------------------------------------------------------------------------------------------------------------
  --Cambio esta linea x la de abajo ya que el control lo tengo que hacer por OC y producto_id y no por @doc_ext
  SELECT @qtyBO=sum(cantidad_solicitada)
  FROM  sys_int_det_documento
  WHERE  doc_ext=@doc_ext
   AND fecha_estado_gt IS NULL
   AND estado_gt IS NULL


  SET @tmax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)

  IF @cantidad > @tmax
  BEGIN
   SET @maxp=ROUND(@tmax,0)
   CLOSE Ingreso_Cursor
   DEALLOCATE Ingreso_Cursor
   RAISERROR('1- La cantidad recepcionada supera a la tolerancia maxima permitida.  Maximo permitido: %s ',16,1, @maxp)
   ROLLBACK TRANSACTION
   RETURN
  END
  -----------------------------------------------------------------------------------------------------------------
  --Obtengo las categorias logicas antes de la transaccion para acortar el lockeo.
  -----------------------------------------------------------------------------------------------------------------
  SELECT	@CAT_LOG_ID=SDD.CAT_LOG_ID
			,@NRO_DESPACHO=SDD.NRO_DESPACHO
			,@EST_MERC_ID=SDD.EST_MERC_ID
  FROM		SYS_INT_DET_DOCUMENTO SDD
  WHERE		SDD.CLIENTE_ID=@CLIENTE_ID
			AND SDD.PRODUCTO_ID=@PRODUCTO_ID
			AND SDD.DOC_EXT=@doc_ext

  IF RTRIM(LTRIM(@NRO_DESPACHO))=''BEGIN
	SET @NRO_DESPACHO=NULL
  END
  
  IF RTRIM(LTRIM(@EST_MERC_ID))=''BEGIN
	SET @EST_MERC_ID=NULL
  END  
			
  IF RTRIM(LTRIM(@CAT_LOG_ID))=''BEGIN
	SET @CAT_LOG_ID=NULL
  END

  IF @CAT_LOG_ID IS NULL BEGIN  
	SELECT	@CAT_LOG_ID=PC.CAT_LOG_ID
	FROM	RL_PRODUCTO_CATLOG PC
	WHERE	PC.CLIENTE_ID=@CLIENTE_ID
			AND PC.PRODUCTO_ID=@PRODUCTO_ID
			AND PC.TIPO_COMPROBANTE_ID=  ISNULL(@TIPO_DOCUMENTO,'DO')
  END

  IF @CAT_LOG_ID IS NULL BEGIN
	--entra porque no tiene categorias particulares y busca la default.
	SELECT	@CAT_LOG_ID=p.ing_cat_log_id,
			@PALLET_AUTOMATICO=PALLET_AUTOMATICO,
			@lote=lote_automatico,
			@INGLOTEPROVEEDOR=isnull(ingloteproveedor,'0')
	FROM	producto p
	WHERE   p.cliente_id=@CLIENTE_ID
			AND p.producto_id=@PRODUCTO_ID
  END
  ELSE
  BEGIN
	--ESTE SEGMENTO ES PARA QUE CUANDO INGRESE CON UN ESTADO LOGICO DESDE LA INTERFAZ
	--RECUPERE LOS DATOS DE CONFIGURACION DEL PRODUCTO.
	SELECT	@PALLET_AUTOMATICO=PALLET_AUTOMATICO,
			@lote=lote_automatico,
			@INGLOTEPROVEEDOR=isnull(ingloteproveedor,'0')
	FROM	producto p
	WHERE   p.cliente_id=@CLIENTE_ID
			AND p.producto_id=@PRODUCTO_ID  
  END
  
  IF @PALLET_AUTOMATICO = '1' AND @nro_pallet IS NULL
  BEGIN
	--obtengo la secuencia para el numero de partida.
	EXEC get_value_for_sequence  'NROPALLET_SEQ', @nro_pallet OUTPUT
  END

  IF @lote='1' AND @INGLOTEPROVEEDOR='0'
  BEGIN
    --obtengo la secuencia para el numero de Lote.
    EXEC get_value_for_sequence 'NROLOTE_SEQ', @NRO_LOTE OUTPUT
  END
  
  SELECT @descripcion=descripcion, @unidad_id=unidad_id FROM producto WHERE cliente_id=@cliente_id AND producto_id=@producto_id

  -- Esto se usa para los clientes que no usan pallet caso contrario comentarlo
  --set @nro_pallet = '99999'

  --Catalina Castillo.25/01/2012.Se verifica que existan registros en la tabal configuracion_contenedoras
  SELECT  @NRO_REG_CONTENEDORAS=COUNT(*) 
  FROM    CONFIGURACION_CONTENEDORAS
  WHERE   (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id)
          AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
          AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)
		AND USUARIO = @USUARIO
  SET @NRO_LINEA_CONT = @NroLinea
  IF @NRO_REG_CONTENEDORAS>0
    BEGIN
      DECLARE Contenedoras_Cursor CURSOR FOR
      SELECT  Nro_Contenedora, Cantidad FROM CONFIGURACION_CONTENEDORAS
      WHERE   (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id)
              AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
              AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)
			AND USUARIO = @USUARIO
     OPEN Contenedoras_Cursor
     FETCH NEXT FROM Contenedoras_Cursor INTO @NROBULTO, @cantidad
     WHILE @@FETCH_STATUS = 0
     BEGIN
      declare @cantDif  numeric(20,5)
      declare @CantDD numeric(20,5)
      
      declare pcur_cont_oc cursor for
      SELECT	SDD.DOC_EXT,I.CANTIDAD
			FROM	  SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD
					    ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
					    INNER JOIN INGRESO_OC I
					    ON(SD.CLIENTE_ID=I.CLIENTE_ID AND SD.DOC_EXT=I.DOC_EXT)
			WHERE	  SD.ORDEN_DE_COMPRA=@OC
					    AND SD.CLIENTE_ID=@CLIENTE_ID
					    AND SDD.PRODUCTO_ID=@PRODUCTO_ID;
      
      open pcur_cont_oc;
      FETCH NEXT from pcur_cont_oc into @dext, @vcant;
      while @@FETCH_STATUS=0
      begin
        
        select  @CantDD=isnull(sum(cantidad),0) 
        from    det_documento
        where   documento_id=@doc_id
                and producto_id=@producto_id
                and PROP2=@dext
        Set @cantDif=@CantDD + @cantidad;
       
        if @CantDD=0 begin
          break;
        end 
        if @cantDif<@vcant begin
          print('Valor de Documento externo evaluado: ' + @dext)        
          print('Cantidad en DD: ' + cast(@CantDD as varchar))        
          print('Cantidad en i.Cantidad: ' + cast(@vcant as varchar))        
          print('Diferencia: ' + cast(@cantDif as varchar))           
          print('@CantDif>@vcant ' + @dext)        
          break;
        end
      
        FETCH NEXT from pcur_cont_oc into @dext, @vcant;
      end
      close pcur_cont_oc
      deallocate pcur_cont_oc;
      
      if ISNULL(@Prod_ant,'') <> @PRODUCTO_ID begin
		set @prod_ant=@producto_id;
		SELECT	@NRO_PART_AT = ISNULL(NRO_PARTIDA_AUTOMATICO,'0') FROM	PRODUCTO WHERE	CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID= @PRODUCTO_ID;		
		IF (@NRO_PARTIDA IS NULL) OR (LTRIM(RTRIM(@NRO_PARTIDA))='')BEGIN
			IF @NRO_PART_AT ='1' BEGIN
				EXEC dbo.GET_VALUE_FOR_SEQUENCE 'NRO_PARTIDA', @SEQ OUTPUT
				SET @VPARTIDA =@SEQ;
			END
			ELSE
			BEGIN
				SET @VPARTIDA=@NRO_PARTIDA;
			END
		END	
		ELSE BEGIN
			SET @VPARTIDA=UPPER(@NRO_PARTIDA);
		END	
      end
      
      INSERT INTO det_documento (documento_id, nro_linea , cliente_id , producto_id , cantidad , cat_log_id , cat_log_id_final , tie_in , fecha_vencimiento , nro_partida , unidad_id  , descripcion , busc_individual , item_ok , cant_solicitada , prop1 , prop2   , nro_bulto ,nro_lote, NRO_DESPACHO, EST_MERC_ID)
      VALUES(@doc_id, @Nrolinea , @cliente_id , @producto_id , @cantidad , NULL   , @cat_log_id  , '0'  , @FECHAVTO   , @VPARTIDA , @unidad_id , @descripcion , '1'    , '1'  ,@cantidad   , @nro_pallet ,@dext , @NROBULTO  , @NRO_LOTE, @NRO_DESPACHO, @EST_MERC_ID)

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
    --Declare @CntDD   Numeric(20,5)
    --Declare @CntIng  Numeric(20,5)
    
    select  @Cnting=isnull(sum(cantidad),0)
    from    ingreso_oc
    where   cliente_id=@cliente_id
            and PRODUCTO_ID=@producto_id
            and ORDEN_COMPRA=@OC;

    select  @CntDD= isnull(sum(dd.cantidad),0)
    from    det_documento dd
    where   dd.documento_id=@doc_id
            and dd.producto_id=@producto_id;
    
    if @cntdd<>@CntIng begin 
		SELECT	@NRO_PART_AT = ISNULL(NRO_PARTIDA_AUTOMATICO,'0') FROM	PRODUCTO WHERE	CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID= @PRODUCTO_ID;		
		IF (@NRO_PARTIDA IS NULL) OR (LTRIM(RTRIM(@NRO_PARTIDA))='')BEGIN
			IF @NRO_PART_AT ='1' BEGIN
				EXEC dbo.GET_VALUE_FOR_SEQUENCE 'NRO_PARTIDA', @SEQ OUTPUT
				SET @VPARTIDA =@SEQ;
			END
		END		
		ELSE
		BEGIN
			SET @VPARTIDA=@NRO_PARTIDA;
		END
      
      INSERT INTO det_documento (documento_id, nro_linea , cliente_id , producto_id , cantidad , cat_log_id , cat_log_id_final , tie_in , fecha_vencimiento , nro_partida , unidad_id  , descripcion , busc_individual , item_ok , cant_solicitada , prop1 , prop2, nro_bulto ,nro_lote, NRO_DESPACHO, EST_MERC_ID)
      VALUES(@doc_id, @Nrolinea , @cliente_id , @producto_id , @cantidad , NULL   , @cat_log_id  , '0'  , @FECHAVTO   , @VPARTIDA , @unidad_id , @descripcion , '1'    , '1'  ,@qtyBO   , @nro_pallet ,@DOC_EXT , NULL  , @NRO_LOTE, @NRO_DESPACHO, @EST_MERC_ID)

    end 
   END
  --Documento a Ingreso.
  SELECT  @Preing=nave_id
  FROM nave
  WHERE pre_ingreso='1'

  SELECT  @catlogid=cat_log_id
  FROM  categoria_stock cs
    INNER JOIN categoria_logica cl
    ON cl.categ_stock_id = cs.categ_stock_id
  WHERE  cs.categ_stock_id = 'TRAN_ING'
    AND cliente_id =@cliente_id

  UPDATE det_documento
  SET cat_log_id =@catlogid
  WHERE documento_id = @Doc_ID

  UPDATE documento SET status='D20' WHERE documento_id=@doc_id

  --Catalina Castillo.25/01/2012.Se verifica que existan registros en la tabal configuracion_contenedoras
   SELECT @NRO_REG_CONTENEDORAS= COUNT(*) FROM CONFIGURACION_CONTENEDORAS
   WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id)
 AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
 AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)


   IF @NRO_REG_CONTENEDORAS>0
    BEGIN
     DECLARE Contenedoras_RL_Cursor CURSOR FOR
  SELECT Cantidad
  FROM CONFIGURACION_CONTENEDORAS
  WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id)
    AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
    AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

     OPEN Contenedoras_RL_Cursor
     FETCH NEXT FROM Contenedoras_RL_Cursor INTO @cantidad

     WHILE @@FETCH_STATUS = 0
     BEGIN

      INSERT INTO RL_DET_DOC_TRANS_POSICION ( DOC_TRANS_ID,  NRO_LINEA_TRANS, POSICION_ANTERIOR, POSICION_ACTUAL, CANTIDAD,  TIPO_MOVIMIENTO_ID,
            ULTIMA_ESTACION, ULTIMA_SECUENCIA, NAVE_ANTERIOR,  NAVE_ACTUAL,  DOCUMENTO_ID,   NRO_LINEA,
            DISPONIBLE,   DOC_TRANS_ID_EGR, NRO_LINEA_TRANS_EGR,DOC_TRANS_ID_TR, NRO_LINEA_TRANS_TR,   CLIENTE_ID,
            CAT_LOG_ID,   CAT_LOG_ID_FINAL, EST_MERC_ID)
      --VALUES (NULL, NULL, NULL, NULL, @cantidad, NULL, NULL, NULL, NULL, @PREING, @doc_id, @Nrolinea, NULL, NULL, NULL, NULL, NULL, @cliente_id, @catlogid,@CAT_LOG_ID,NULL)
	  Select	NULL, NULL, NULL, NULL, dd.cantidad, NULL, NULL, NULL, NULL, @PREING, dd.documento_id, dd.nro_linea, NULL, NULL, NULL, NULL, NULL, dd.cliente_id, @catlogid,@CAT_LOG_ID,@EST_MERC_ID
	  from		det_documento dd
	  where		dd.documento_id=@doc_id
				and not exists (select	1
								from	RL_DET_DOC_TRANS_POSICION rl
								where	rl.documento_id=dd.documento_id
										and rl.nro_linea=dd.nro_linea);      

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
  DELETE FROM CONFIGURACION_CONTENEDORAS
  WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id)
    AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
    AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

  SET @Nrolinea=@Nrolinea-1
    END
  ELSE
  BEGIN
    select  @Cnting=isnull(sum(cantidad),0)
    from    ingreso_oc
    where   cliente_id=@cliente_id
            and PRODUCTO_ID=@producto_id
            and ORDEN_COMPRA=@OC;

    select  @CntDD= isnull(sum(dd.cantidad),0)
    from    det_documento dd inner join rl_det_doc_trans_posicion rl
			on(dd.documento_id=rl.documento_id and dd.nro_linea=rl.nro_linea)
    where   dd.documento_id=@doc_id
            and dd.producto_id=@producto_id;
    
    if @cntdd<>@CntIng begin 
    
      INSERT INTO RL_DET_DOC_TRANS_POSICION(DOC_TRANS_ID,  NRO_LINEA_TRANS, POSICION_ANTERIOR,  POSICION_ACTUAL, CANTIDAD,   TIPO_MOVIMIENTO_ID,
              ULTIMA_ESTACION, ULTIMA_SECUENCIA, NAVE_ANTERIOR,   NAVE_ACTUAL,  DOCUMENTO_ID,  NRO_LINEA,
              DISPONIBLE,   DOC_TRANS_ID_EGR, NRO_LINEA_TRANS_EGR, DOC_TRANS_ID_TR, NRO_LINEA_TRANS_TR, CLIENTE_ID,
              CAT_LOG_ID,   CAT_LOG_ID_FINAL, EST_MERC_ID)
              
      Select	NULL, NULL, NULL, NULL, dd.cantidad, NULL, NULL, NULL, NULL, @PREING, dd.documento_id, dd.nro_linea, NULL, NULL, NULL, NULL, NULL, dd.cliente_id, @catlogid,@CAT_LOG_ID,@EST_MERC_ID
      from		det_documento dd
      where		dd.documento_id=@doc_id
				and not exists (select	1
								from	RL_DET_DOC_TRANS_POSICION rl
								where	rl.documento_id=dd.documento_id
										and rl.nro_linea=dd.nro_linea);
    end
  END
  ------------------------------------------------------------------------------------------------------------------------------------
  --Generacion del Back Order.
  -----------------------------------------------------------------------------------------------------------------
  SELECT @lineBO=max(isnull(nro_linea,1))+1 FROM sys_int_det_documento WHERE   DOC_EXT=@doc_ext

    --PRINT 'DOC_EXT= ' + @DOC_EXT + ', NRO_LINEA = ' + CAST(@LINEBO AS VARCHAR)

  SELECT  @qtyBO=sum(cantidad_solicitada)
  FROM sys_int_det_documento
  WHERE doc_ext=@doc_ext
    AND fecha_estado_gt IS NULL
    AND estado_gt IS NULL

  --PRINT 'DOC_EXT= ' + @DOC_EXT + ', QTY_BO = ' + CAST(@qtyBO AS VARCHAR)

  SELECT @VLOTE_DOC=NRO_LOTE, @VPARTIDA_DOC=NRO_PARTIDA
  FROM  SYS_INT_DET_DOCUMENTO
  WHERE  DOC_EXT=@doc_ext

  UPDATE SYS_INT_DET_DOCUMENTO
  SET  ESTADO_GT='P',
   DOC_BACK_ORDER=@doc_ext,
   FECHA_ESTADO_GT=getdate(),
   DOCUMENTO_ID=@Doc_ID
   --NRO_PARTIDA	=CASE(ISNULL(NRO_PARTIDA,'#'))  WHEN '#' THEN NULL ELSE @NRO_PARTIDA END,
  --NRO_LOTE	=CASE(ISNULL(NRO_LOTE,'#'))		WHEN '#' THEN NULL ELSE @NRO_LOTE END
  WHERE  DOC_EXT=@doc_ext AND documento_id IS NULL

  SET @qtyBO=@qtyBO - @cantidad

  SELECT @GENERA_BO =
     CASE P.BACK_ORDER
   WHEN '1' THEN 'S'
   WHEN '0' THEN 'N'
     END
  FROM PRODUCTO P INNER JOIN SYS_INT_DET_DOCUMENTO SIDD ON (P.PRODUCTO_ID = SIDD.PRODUCTO_ID)
  WHERE SIDD.DOC_EXT = @doc_ext AND SIDD.DOCUMENTO_ID = @Doc_ID AND P.CLIENTE_ID=@CLIENTE_ID

  -- LRojas TrackerID 3851 29/03/2012: Se debe tener en cuenta la parametrizaci?n del producto.
  IF (@qtyBO > 0) AND (@GENERA_BO = 'S') --Si esta variable es mayor a 0, genero el backorder.
  BEGIN
 INSERT INTO sys_int_det_documento
 SELECT TOP 1
   DOC_EXT, @LINEBO ,CLIENTE_ID, PRODUCTO_ID, @QTYBO ,CANTIDAD , EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO,
   NRO_PARTIDA,UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, NULL, NULL, NULL,NULL,NULL,NULL,
   NULL,NULL,CUSTOMS_1,CUSTOMS_2,CUSTOMS_3
 FROM SYS_INT_DET_DOCUMENTO
 WHERE DOC_EXT=@DOC_EXT
  END
  ------------------------------------------------------------------------------------------------------------------------------------
  --Guardo en la tabla de auditoria
  -----------------------------------------------------------------------------------------------------------------
  EXEC dbo.AUDITORIA_HIST_INSERT_ING @doc_id
  --insert into IMPRESION_RODC VALUES(@Doc_id, 1, @Tipo_eti,'0')
  --COMMIT TRANSACTION
  SET @DOCUMENTO_ID=@doc_id

	UPDATE  ingreso_oc
	SET     procesado = 1
	WHERE   (CLIENTE_ID = @CLIENTE_ID) AND (PRODUCTO_ID = @producto_id) AND (ORDEN_COMPRA = @oc)
			AND USUARIO=@USUARIO  
			AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE OR (@LOTE = 1 AND (NRO_LOTE IS NULL OR NRO_LOTE = '')))
			AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)


	SET @DOC_EXT = NULL
	FETCH NEXT FROM Ingreso_Cursor INTO @doc_ext,@producto_id, @cantidad, @fecha, @nro_partida, @nro_lote, @FECHAVTO
END

--COMMIT TRANSACTION
CLOSE Ingreso_Cursor
DEALLOCATE Ingreso_Cursor

 -- LRojas 02/03/2012 TrackerID 3806: Inserto Usuario para Demonio de Impresion
 INSERT INTO IMPRESION_RODC VALUES(@Doc_ID,0,'D',0, @USUARIO_IMP)
 -----------------------------------------------------------------------------------------------------------------
 --ASIGNO TRATAMIENTO...
 -----------------------------------------------------------------------------------------------------------------
 update rl_det_doc_trans_posicion set cantidad=dd.cantidad
 from	rl_det_doc_trans_posicion rl inner join det_documento dd on(dd.documento_id=rl.documento_id and dd.nro_linea=rl.nro_linea)
 where	rl.documento_id=@doc_id
 
 EXEC asigna_tratamiento#asigna_tratamiento_ing @doc_id
 EXEC dbo.AUDITORIA_HIST_INSERT_ING @doc_id
 IF @@error<>0
 BEGIN
  ROLLBACK TRANSACTION
  RAISERROR('No se pudo completar la transaccion',16,1)
 END
 ELSE
 BEGIN
  COMMIT TRANSACTION
 END
END

GO