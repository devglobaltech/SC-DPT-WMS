IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[UpdateNroDespacho]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[UpdateNroDespacho]
GO

create PROCEDURE [dbo].[UpdateNroDespacho]
	@Cliente       VARCHAR(100),  
	@Deposito      VARCHAR(100), 
	@OC            VARCHAR(20),
	@PRODUCTO_ID   VARCHAR(20),
	@QTY           numeric(20,5),
	@NRO_DESPACHO  VARCHAR(20) 
AS
BEGIN
	declare @total					numeric(20,5)
	declare @Usuario				AS VARCHAR (30)
	declare @Terminal				AS VARCHAR (100)
	declare @cantidad_RL			numeric(20,5)
	declare @Msg					AS VARCHAR (400)
	declare @documento_id			bigint
	declare @nro_linea				numeric(10)
	DECLARE @RL_ID					AS BIGINT
	declare @CantPendiente			numeric(20,5)
	DECLARE @RL_OLD_CAT_LOG			AS VARCHAR(100)
	declare @nro_linea_ins			numeric(10)
	declare @cantidad_LIN_DD		numeric(20,5)
	declare @CantNewLine			numeric(20,5)
	declare @rl_id_new				as bigint
	declare @vDocNroLineaOld		AS VARCHAR(100)
	declare @vDocNroLineaNew		AS VARCHAR(100)
	declare @vCantConsumNroLineaDD	AS numeric(20,5)
	declare @vCantRemLinDDActual    AS numeric(20,5)
	declare @vCantVerifDD    		AS numeric(20,5)
	declare @vCantVerifRL    		AS numeric(20,5)
	declare @argumentos				as varchar(4000)
	declare @MSG_ERR				as varchar(4000)
	SET XACT_ABORT ON
	-----------------------------------------------------------------------------------------
	--ARMO LA LLAMADA AL SP CON LOS ARGUMENTOS PARA PODER GUARDAR LA AUDITORIA.
	-----------------------------------------------------------------------------------------
	set @argumentos = 'EXEC DBO.UPDATENRODESPACHO '
	IF @CLIENTE IS NOT NULL BEGIN
		set @argumentos = @argumentos + ' @CLIENTE=' + CHAR(39) + @CLIENTE + CHAR(39) + ','
	END ELSE BEGIN
		set @argumentos = @argumentos + ' @CLIENTE= NULL,'
	END
	
	if @DEPOSITO IS NOT NULL BEGIN
		set @argumentos = @argumentos + ' @DEPOSITO=' + CHAR(39) + @DEPOSITO + CHAR(39) + ','
	END ELSE BEGIN
		set @argumentos = @argumentos + ' @DEPOSITO= NULL ,'
	END
	
	IF @OC IS NOT NULL BEGIN
		set @argumentos = @argumentos + ' @OC=' + CHAR(39) + @OC + CHAR(39) + ','
	END ELSE BEGIN
		set @argumentos = @argumentos + ' @OC= NULL,'
	END
	
	IF @PRODUCTO_ID IS NOT NULL BEGIN
		set @argumentos = @argumentos + ' @PRODUCTO_ID=' + CHAR(39) + @PRODUCTO_ID + CHAR(39) + ','
	END ELSE BEGIN
		set @argumentos = @argumentos + ' @PRODUCTO_ID= NULL,'
	END
	
	IF @QTY IS NOT NULL BEGIN
		set @argumentos = @argumentos + ' @QTY=' + CAST(@QTY AS VARCHAR) + ','
	END ELSE BEGIN
		set @argumentos = @argumentos + ' @QTY= NULL,'
	END
	
	IF @NRO_DESPACHO IS NOT NULL BEGIN
		set @argumentos = @argumentos + ' @NRO_DESPACHO=' + CHAR(39) + @NRO_DESPACHO + CHAR(39) + ';'
	END ELSE BEGIN
		set @argumentos = @argumentos + ' @NRO_DESPACHO= NULL;'
	END
	-----------------------------------------------------------------------------------------
	BEGIN TRY
		
	set @vCantVerifDD = 0
	set @vCantVerifRL = 0
	
	BEGIN TRANSACTION
  
  
	SELECT 	@total=isnull(sum(RL.CANTIDAD),0)
	FROM	DOCUMENTO D
			INNER JOIN DET_DOCUMENTO DD				 ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL  ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.PRODUCTO_ID=@PRODUCTO_ID AND dd.prop3=@OC --D.ORDEN_DE_COMPRA IN(SELECT ID_OC FROM TMP_OC WHERE OC=@OC) 
			and dd.nro_despacho is null and D.CLIENTE_ID = @Cliente AND RL.CAT_LOG_ID = 'PENDDESP'
			and not exists (select 1 from nave n where n.nave_id=rl.nave_actual and n.en_error = '1')
			AND RL.DISPONIBLE = '1'

	if @QTY > @total begin
		set @Msg='La Cantidad a Procesar es Mayor a la Cantidad Disponible Sin Nro de Despacho, Cantidad Disponible:' + Cast(@total as varchar)
		raiserror(@Msg,16,1)
		return
	end
      
	---Registro el Usuario si no Esta Activo
	SELECT 	@Usuario='ADMIN'--Usuario_id FROM #Temp_Usuario_Loggin
	SELECT  @Terminal=Host_Name()
          
	IF OBJECT_ID ('tempdb.dbo.#temp_usuario_loggin','U') is  null
	begin
		CREATE TABLE #temp_usuario_loggin (
			usuario_id            		VARCHAR(20)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			terminal              		VARCHAR(100)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			fecha_loggin          		DATETIME     ,
			session_id            		VARCHAR(60)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			rol_id                		VARCHAR(5)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			emplazamiento_default 		VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
			deposito_default      		VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
		)

		exec FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN @Usuario
	end
    
    
	declare CURSOR_OC INSENSITIVE CURSOR FOR
		SELECT 	RL.CANTIDAD, DD.DOCUMENTO_ID, DD.NRO_LINEA, RL.RL_ID, RL.CAT_LOG_ID, DD.CANTIDAD
		FROM	DOCUMENTO D
				INNER JOIN DET_DOCUMENTO DD				 ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL  ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	DD.PRODUCTO_ID=@PRODUCTO_ID AND dd.prop3=@OC --D.ORDEN_DE_COMPRA IN(SELECT ID_OC FROM TMP_OC WHERE OC=@OC) 
				and RL.DISPONIBLE =1
				and not exists (select 1 from nave n where n.nave_id=rl.nave_actual and n.en_error = '1')
				and dd.nro_despacho is null	and D.CLIENTE_ID = @Cliente AND RL.CAT_LOG_ID = 'PENDDESP'
		ORDER BY 
				DD.DOCUMENTO_ID, DD.NRO_LINEA
    
    open CURSOR_OC
    fetch next from CURSOR_OC INTO @cantidad_RL, @documento_id, @nro_linea, @RL_ID, @RL_OLD_CAT_LOG, @cantidad_LIN_DD
    set @vCantConsumNroLineaDD=0
    set @CantPendiente=@QTY
    WHILE ((@@FETCH_STATUS = 0) and (@CantPendiente>0))BEGIN
    set @vDocNroLineaOld=CAST(@documento_id as varchar) + '-' + CAST(@nro_linea as varchar)
        
     if (@cantidad_RL <= @CantPendiente) 
     begin
		if (@cantidad_RL = (@cantidad_LIN_DD-@vCantConsumNroLineaDD))
		begin
			--Hago Update en Det_Documento y Rl, descuento la cantidad pendiente y fin
			update det_documento set nro_despacho = @NRO_DESPACHO where documento_id =@documento_id and nro_linea = @nro_linea 
			set @vCantVerifDD = @vCantVerifDD + @cantidad_RL --p/verif final
			update RL_DET_DOC_TRANS_POSICION set cat_log_id='DISPONIBLE' WHERE  RL_ID = @RL_ID
			set @vCantVerifRL = @vCantVerifRL + @cantidad_RL --p/verif final
			set @vCantConsumNroLineaDD=@vCantConsumNroLineaDD+@cantidad_RL
			set @CantPendiente=@CantPendiente-@cantidad_RL
	        
			EXEC [AUDITORIA_HIST_INSERT_CATLOG] @RL_ID, @RL_OLD_CAT_LOG, 'DISPONIBLE', @cantidad_RL
		end
		else --(@cantidad_RL < (@cantidad_LIN_DD-@vCantConsumNroLineaDD))
		begin
			--split det_documento
		    --Obtenga el Nro de Linea del Doc + 1, Inserto el documento y actualizo la rl
			SELECT @NRO_LINEA_INS=MAX(NRO_LINEA + 1) FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
			set @CantNewLine = @cantidad_RL
			set @CantPendiente=@CantPendiente-@cantidad_RL
			set @vCantRemLinDDActual = (@cantidad_LIN_DD-@vCantConsumNroLineaDD)-@cantidad_RL
			SET @vCantConsumNroLineaDD = @vCantConsumNroLineaDD + @cantidad_RL
			
			INSERT INTO DET_DOCUMENTO
			  SELECT DOCUMENTO_ID, @NRO_LINEA_INS,CLIENTE_ID, PRODUCTO_ID, @CantNewLine,NRO_SERIE, NRO_SERIE_PADRE, EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, FECHA_VENCIMIENTO, @NRO_DESPACHO, 
			  NRO_PARTIDA, UNIDAD_ID,PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, BUSC_INDIVIDUAL, TIE_IN, NRO_TIE_IN_PADRE, NRO_TIE_IN, 
			  ITEM_OK, CAT_LOG_ID_FINAL, MONEDA_ID, COSTO, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, VOLUMEN_UNITARIO, PESO_UNITARIO, CANT_SOLICITADA,TRACE_BACK_ORDER
			  FROM  DET_DOCUMENTO
			  WHERE DOCUMENTO_ID =@DOCUMENTO_ID AND NRO_LINEA =@NRO_LINEA

	    
			INSERT INTO det_documento_transaccion
			  SELECT DOC_TRANS_ID, @NRO_LINEA_INS,DOCUMENTO_ID,@NRO_LINEA_INS,MOTIVO_ID, EST_MERC_ID, CLIENTE_ID, CAT_LOG_ID, 
			  ITEM_OK,MOVIMIENTO_PENDIENTE, DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF
			  FROM  DET_DOCUMENTO_TRANSACCION
			  WHERE DOCUMENTO_ID =@DOCUMENTO_ID AND NRO_LINEA_DOC=@NRO_LINEA	
			  
			update RL_DET_DOC_TRANS_POSICION set cat_log_id='DISPONIBLE', NRO_LINEA_TRANS = @NRO_LINEA_INS WHERE  RL_ID = @RL_ID					
			update DET_DOCUMENTO set CANTIDAD=@vCantRemLinDDActual where DOCUMENTO_ID=@DOCUMENTO_ID and NRO_LINEA=@nro_linea
			
			set @vCantVerifDD = @vCantVerifDD + @CantNewLine --p/verif final
			set @vCantVerifRL = @vCantVerifRL + @cantidad_RL --p/verif final
	        
			EXEC [AUDITORIA_HIST_INSERT_CATLOG] @RL_ID, @RL_OLD_CAT_LOG, 'DISPONIBLE', @CantNewLine
				
		end
      
      end  --if (@cantidad_RL <= @CantPendiente) 
      
      else if (@cantidad_RL > @CantPendiente) begin		
        --Obtenga el Nro de Linea del Doc + 1, Inserto el documento y creo la rl
        SELECT @NRO_LINEA_INS=MAX(NRO_LINEA + 1) FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
        --set @CantNewLine = (@cantidad_LIN_DD-@vCantConsumNroLineaDD)-@CantPendiente
        set @CantNewLine = @CantPendiente
        set @vCantRemLinDDActual = (@cantidad_LIN_DD-@vCantConsumNroLineaDD)-@CantPendiente
		SET @vCantConsumNroLineaDD = @vCantConsumNroLineaDD + @CantPendiente
			
        
        INSERT INTO DET_DOCUMENTO
          SELECT DOCUMENTO_ID, @NRO_LINEA_INS,CLIENTE_ID, PRODUCTO_ID, @CantNewLine,NRO_SERIE, NRO_SERIE_PADRE, EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, FECHA_VENCIMIENTO, @NRO_DESPACHO, 
          NRO_PARTIDA, UNIDAD_ID,PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, BUSC_INDIVIDUAL, TIE_IN, NRO_TIE_IN_PADRE, NRO_TIE_IN, 
          ITEM_OK, CAT_LOG_ID_FINAL, MONEDA_ID, COSTO, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, VOLUMEN_UNITARIO, PESO_UNITARIO, CANT_SOLICITADA,TRACE_BACK_ORDER
          FROM  DET_DOCUMENTO
          WHERE DOCUMENTO_ID =@DOCUMENTO_ID AND NRO_LINEA =@NRO_LINEA
    
        INSERT INTO det_documento_transaccion
          SELECT DOC_TRANS_ID, @NRO_LINEA_INS,DOCUMENTO_ID,@NRO_LINEA_INS,MOTIVO_ID, EST_MERC_ID, CLIENTE_ID, CAT_LOG_ID, 
          ITEM_OK,MOVIMIENTO_PENDIENTE, DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF
          FROM  DET_DOCUMENTO_TRANSACCION
          WHERE DOCUMENTO_ID =@DOCUMENTO_ID AND NRO_LINEA_DOC=@NRO_LINEA						
        
        INSERT INTO RL_DET_DOC_TRANS_POSICION
          SELECT DOC_TRANS_ID, @NRO_LINEA_INS, POSICION_ANTERIOR, POSICION_ACTUAL, @CantNewLine, TIPO_MOVIMIENTO_ID, ULTIMA_ESTACION, ULTIMA_SECUENCIA, NAVE_ANTERIOR, NAVE_ACTUAL, DOCUMENTO_ID, NRO_LINEA, DISPONIBLE, DOC_TRANS_ID_EGR,NRO_LINEA_TRANS_EGR,DOC_TRANS_ID_TR, NRO_LINEA_TRANS_TR, CLIENTE_ID, 
          'DISPONIBLE', CAT_LOG_ID_FINAL, EST_MERC_ID
          FROM RL_DET_DOC_TRANS_POSICION
          WHERE  RL_ID = @RL_ID
    
        set @rl_id_new = scope_identity()
      
        EXEC [AUDITORIA_HIST_INSERT_CATLOG] @rl_id_new, @RL_OLD_CAT_LOG, 'DISPONIBLE', @CantNewLine
        
        update RL_DET_DOC_TRANS_POSICION set CANTIDAD=(@cantidad_RL - @CantNewLine) WHERE  RL_ID = @RL_ID					
		update DET_DOCUMENTO set CANTIDAD=@vCantRemLinDDActual where DOCUMENTO_ID=@DOCUMENTO_ID and NRO_LINEA=@nro_linea
		
		set @vCantVerifDD = @vCantVerifDD + @CantNewLine --p/verif final
		set @vCantVerifRL = @vCantVerifRL + @CantNewLine --p/verif final
      
        set @CantPendiente=0
      end
    
      fetch next from CURSOR_OC INTO @cantidad_RL, @documento_id, @nro_linea, @RL_ID, @RL_OLD_CAT_LOG, @cantidad_LIN_DD
      set @vDocNroLineaNew=CAST(@documento_id as varchar) + '-' + CAST(@nro_linea as varchar)
      if (@vDocNroLineaNew<>@vDocNroLineaOld) begin
        set @vCantConsumNroLineaDD=0
        set @vDocNroLineaOld=CAST(@documento_id as varchar) + '-' + CAST(@nro_linea as varchar)
      end --if
    END
    
    CLOSE CURSOR_OC
    DEALLOCATE CURSOR_OC
    
    if ((@CantPendiente=0 ) AND ((@vCantVerifDD <> @QTY) or (@vCantVerifRL <> @QTY)))BEGIN
		RAISERROR('No se pudo completar la actualización, no cumple verificación de acumuladores al final del SP',15,1)
    END

	EXEC DBO.INS_AUDITORIA_CAMBIO_DESPACHOS @PARAMETROS		=@argumentos,
											@ERROR			='0',
											@ERR_TECNICO	=NULL,
											@OBSERVACIONES	=NULL
	    
   	COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
		IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
		
		set @MSG_ERR=ERROR_MESSAGE();
		
		EXEC DBO.INS_AUDITORIA_CAMBIO_DESPACHOS @PARAMETROS		=@argumentos,
												@ERROR			='1',
												@ERR_TECNICO	=@MSG_ERR,
												@OBSERVACIONES	=NULL		

		EXEC usp_RethrowError
	END CATCH
    
END