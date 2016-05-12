IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AJUSTAR_MASIVAMENTE_INGRESOS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[AJUSTAR_MASIVAMENTE_INGRESOS]
GO

CREATE procedure DBO.AJUSTAR_MASIVAMENTE_INGRESOS
  @ajuste_id    bigint,
  @Err          char          output,
  @Err_Msg      varchar(2000) output
as
begin
	  if OBJECT_ID('tempdb.#temp_usuario_loggin','U') IS NULL
		Begin
			--================================================================
			CREATE TABLE #temp_usuario_loggin (
				usuario_id            	VARCHAR(20)   COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				terminal              	VARCHAR(100)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				fecha_loggin          	DATETIME     ,
				session_id            	VARCHAR(60)   COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				rol_id                	VARCHAR(5)    COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				emplazamiento_default 	VARCHAR(15)   COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
				deposito_default      	VARCHAR(15)   COLLATE SQL_Latin1_General_CP1_CI_AS NULL);
        
			Exec FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN 'ADMIN';
			--================================================================
		End; 
    -----------------------------------------------
    --  Documento de Ingreso.
    -----------------------------------------------
    DECLARE @P_Documento_Id             numeric
    DECLARE @P_Cliente_Id               varchar(15)
    DECLARE @P_Tipo_Comprobante_Id      varchar(5)
    DECLARE @P_Tipo_Operacion_Id        varchar(5)
    DECLARE @P_Det_Tipo_Operacion_Id    varchar(5)
    DECLARE @P_Cpte_Prefijo             varchar(6)
    DECLARE @P_Cpte_Numero              varchar(20)
    DECLARE @P_Fecha_Cpte               varchar(20)
    DECLARE @P_Fecha_Pedida_Ent         varchar(20)
    DECLARE @P_Sucursal_Origen          varchar(20)
    DECLARE @P_Sucursal_Destino         varchar(20)
    DECLARE @P_Anulado                  varchar(1)
    DECLARE @P_Motivo_Anulacion         varchar(15)
    DECLARE @P_Peso_Total               numeric
    DECLARE @P_Unidad_Peso              varchar(5)
    DECLARE @P_Volumen_Total            numeric
    DECLARE @P_Unidad_Volumen           varchar(5)
    DECLARE @P_Total_Bultos             numeric
    DECLARE @P_Valor_Declarado          numeric
    DECLARE @P_Orden_De_Compra          varchar(20)
    DECLARE @P_Cant_Items               numeric
    DECLARE @P_Observaciones            varchar(200)
    DECLARE @P_Status                   varchar(3)
    DECLARE @P_NroRemito                varchar(30)
    DECLARE @P_Fecha_Alta_Gtw           varchar(20)
    DECLARE @P_Fecha_Fin_Gtw            varchar(20)
    DECLARE @P_Personal_Id              varchar(20)
    DECLARE @P_Transporte_Id            varchar(20)
    DECLARE @P_Nro_Despacho_Importacion varchar(30)
    DECLARE @P_Alto                     numeric
    DECLARE @P_Ancho                    numeric
    DECLARE @P_Largo                    numeric
    DECLARE @P_Unidad_Medida            varchar(5)
    DECLARE @P_Grupo_Picking            varchar(50)
    DECLARE @P_Prioridad_Picking        numeric
    -----------------------------------------------
    --  Documento Transaccion
    -----------------------------------------------
		DECLARE @P_Completado               varchar(1)
		DECLARE @P_Transaccion_Id           varchar(15)
		DECLARE @P_Estacion_Actual          varchar(15)
		DECLARE @P_Est_Mov_Actual           varchar(20)
		DECLARE @P_Orden_Id                 numeric
		DECLARE @P_It_Mover                 varchar(1)
		DECLARE @P_Orden_Estacion           numeric
		DECLARE @P_Tr_Pos_Completa          varchar(1)
		DECLARE @P_Tr_Activo                varchar(1)
		DECLARE @P_Usuario_Id               varchar(20)
		DECLARE @P_Terminal                 varchar(20)
		DECLARE @P_Tr_Activo_Id             varchar(10)
		DECLARE @P_Session_Id               varchar(60)
		DECLARE @P_Fecha_Cambio_Tr          datetime
		DECLARE @P_Doc_Trans_Id             numeric    
    -----------------------------------------------
    -- Det. Documento.
    -----------------------------------------------
		DECLARE @P_Nro_Linea                numeric
		DECLARE @P_Cantidad                 numeric
		DECLARE @P_Nro_Serie                varchar(50)
		DECLARE @P_Nro_Serie_Padre          varchar(50)
		DECLARE @P_Est_Merc_Id              varchar(15)
		DECLARE @P_Cat_Log_Id               varchar(50)
		DECLARE @P_Nro_Bulto                varchar(50)
		DECLARE @P_Descripcion              varchar(200)
		DECLARE @P_Nro_Lote                 varchar(50)
		DECLARE @P_Fecha_Vencimiento        datetime
		DECLARE @P_Nro_Despacho             varchar(50)
		DECLARE @P_Nro_Partida              varchar(50)
		DECLARE @P_Unidad_Id                varchar(5)
		DECLARE @P_Peso                     numeric
		DECLARE @P_Volumen                  numeric
		DECLARE @P_Busc_Individual          varchar(1)
		DECLARE @P_Tie_In                   varchar(1)
		DECLARE @P_Nro_Tie_In_Padre         varchar(100)
		DECLARE @P_Nro_Tie_In               varchar(100)
		DECLARE @P_Item_Ok                  varchar(1)
		DECLARE @P_Moneda_Id                varchar(20)
		DECLARE @P_Costo                    numeric
		DECLARE @P_Cat_Log_Id_Final         varchar(50)
		DECLARE @P_Prop1                    varchar(100)
		DECLARE @P_Prop2                    varchar(100)
		DECLARE @P_Prop3                    varchar(100)
		DECLARE @P_Volumen_Unitario         varchar(1)
		DECLARE @P_Peso_Unitario            varchar(1)
		DECLARE @P_Cant_Solicitada          numeric
		DECLARE @P_Nro_Linea_Trans          numeric
		DECLARE @P_Nro_Linea_Doc            numeric
		DECLARE @P_Motivo_Id                varchar(15)
		DECLARE @P_Movimiento_Pendiente     varchar(1)
		declare @RL_ID                      numeric(20)
		DECLARE @FL_CONTENEDORA             VARCHAR(1)
		DECLARE @SEC_CONTENEDORA            int  
    Declare @FProcesamiento             varchar(100)
    -----------------------------------------------
    --Ajustes Masivos
    -----------------------------------------------
    Declare @ACliente_Id                varchar(15)
    Declare @AProducto_id               varchar(30)
    Declare @ANro_Serie                 varchar(50)
    Declare @AEst_Merc_ID               varchar(15)
    Declare @ACat_Log_ID                varchar(50)
    Declare @ANro_Bulto                 varchar(50)
    Declare @ANro_Lote                  varchar(50)
    Declare @AFecha_Vencimiento         datetime
    Declare @ANro_Despacho              varchar(50)
    Declare @ANro_Partida               varchar(50)
    Declare @Aprop1                     varchar(100)
    Declare @Aprop2                     varchar(100)
    Declare @Aprop3                     varchar(100)
    Declare @APosicion_ID               numeric(20,0)
    Declare @ANave_ID                   numeric(20,0)
    Declare @ACantidad_Ajuste           Numeric(20,5)
    
    Set @Err='0'
    
    Select  @ACliente_Id          =cliente_id,
            @AProducto_id         =producto_id,
            @ANro_Serie           =nro_serie,
            @AEst_Merc_ID         =est_merc_id,
            @ACat_Log_ID          =cat_log_id,
            @ANro_Bulto           =nro_bulto,
            @ANro_Lote            =nro_lote,
            @AFecha_Vencimiento   =fecha_vencimiento,
            @ANro_Despacho        =nro_despacho,
            @ANro_Partida         =nro_partida,
            @Aprop1               =prop1,
            @Aprop2               =prop2,
            @Aprop3               =prop3,
            @APosicion_ID         =posicion_id,
            @ANave_ID             =nave_id,
            @ACantidad_Ajuste     =cantidad_ajuste
    from    ajustes_masivos
    where   ajuste_id=@ajuste_id;

    --Armo la fecha y la hora en varchar para mandarlo a las observaciones del documento.
    Set @FProcesamiento=convert(varchar,getdate(),103) + ' ' + cast(DATEPART(HH,GETDATE()) as varchar) + ':'+ cast(DATEPART(MI,GETDATE()) as varchar) + ':'+ cast(DATEPART(SS,GETDATE()) as varchar);
    
    SET @P_Cliente_Id = @ACliente_Id
    SET @P_Tipo_Comprobante_Id = 'IM'
    SET @P_Tipo_Operacion_Id = 'ING'
    SET @P_Det_Tipo_Operacion_Id = 'MAN'
    
    SET @P_Fecha_Cpte   =CONVERT(datetime,CONVERT(VARCHAR,GETDATE(),101),101)--PARA DEVOLVER FECHA SIN HORAS Y MINUTOS
    SET @P_Observaciones='AJUSTE MASIVO ' + @FProcesamiento + ', terminal de proceso: ' + host_name();
    SET @P_Status       = 'D40'
    SET @P_Unidad_Peso  = NULL

    begin try
    
      SET @P_Completado         ='0'
			SET @P_Transaccion_Id     ='ING_ABAST_F'
			SET @P_Status             ='T40'
			SET @P_Tipo_Operacion_Id  ='ING'
			SET @P_Tr_Activo          ='0' 
      Set @P_Nro_Linea          =1;
      Set @P_Nro_Linea_Trans    =1;
      Set @P_Tie_In             ='0';

      --Inserta la cabecera del documento.
      EXEC [dbo].[Documento_Api#InsertRecord]    @P_Documento_Id OUTPUT        ,@P_Cliente_Id        ,@P_Tipo_Comprobante_Id     ,@P_Tipo_Operacion_Id
                                                ,@P_Det_Tipo_Operacion_Id      ,@P_Cpte_Prefijo      ,@P_Cpte_Numero             ,@P_Fecha_Cpte
                                                ,@P_Fecha_Pedida_Ent           ,@P_Sucursal_Origen   ,@P_Sucursal_Destino        ,@P_Anulado
                                                ,@P_Motivo_Anulacion           ,@P_Peso_Total        ,@P_Unidad_Peso             ,@P_Volumen_Total
                                                ,@P_Unidad_Volumen             ,@P_Total_Bultos      ,@P_Valor_Declarado         ,@P_Orden_De_Compra
                                                ,@P_Cant_Items                 ,@P_Observaciones     ,@P_Status                  ,@P_NroRemito
                                                ,@P_Fecha_Alta_Gtw             ,@P_Fecha_Fin_Gtw     ,@P_Personal_Id             ,@P_Transporte_Id
                                                ,@P_Nro_Despacho_Importacion   ,@P_Alto              ,@P_Ancho                   ,@P_Largo
                                                ,@P_Unidad_Medida              ,@P_Grupo_Picking     ,@P_Prioridad_Picking;
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end
      
      EXEC [dbo].[Documento_Transaccion_Api#InsertRecord]    @P_Completado      ,@P_Observaciones     ,@P_Transaccion_Id        ,@P_Estacion_Actual
                                                            ,@P_Status          ,@P_Est_Mov_Actual    ,@P_Orden_Id              ,@P_It_Mover
                                                            ,@P_Orden_Estacion  ,@P_Tipo_Operacion_Id ,@P_Tr_Pos_Completa       ,@P_Tr_Activo
                                                            ,@P_Usuario_Id      ,@P_Terminal          ,@P_Fecha_Alta_Gtw        ,@P_Tr_Activo_Id
                                                            ,@P_Session_Id      ,@P_Fecha_Cambio_Tr   ,@P_Fecha_Fin_Gtw         ,@P_Doc_Trans_Id OUTPUT;
      
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end
      
      select  @P_Descripcion = descripcion, @P_Unidad_Id = unidad_id, @P_Unidad_Peso = unidad_peso, @P_Unidad_Volumen = unidad_volumen 
			from    producto
			where   cliente_id=@P_Cliente_Id 
              and producto_id =@AProducto_id;
              
      EXEC [dbo].[Det_Documento_Api#InsertRecord]   @P_Documento_Id     ,@P_Nro_Linea       ,@P_Cliente_Id        ,@AProducto_id  ,@ACantidad_Ajuste
			                                              ,@ANro_Serie        ,@P_Nro_Serie_Padre ,@AEst_Merc_Id        ,@ACat_Log_Id   ,@ANro_Bulto
			                                              ,@P_Descripcion     ,@ANro_Lote         ,@AFecha_Vencimiento  ,@ANro_Despacho ,@ANro_Partida
			                                              ,@P_Unidad_Id       ,@P_Peso            ,@P_Unidad_Peso       ,@P_Volumen     ,@P_Unidad_Volumen
			                                              ,@P_Busc_Individual ,@P_Tie_In          ,@P_Nro_Tie_In_Padre  ,@P_Nro_Tie_In  ,@P_Item_Ok
			                                              ,@P_Moneda_Id       ,@P_Costo           ,@ACat_Log_Id         ,@AProp1        ,@AProp2
                                                    ,@AProp3            ,@P_Largo           ,@P_Alto              ,@P_Ancho       ,@P_Volumen_Unitario
                                                    ,@P_Peso_Unitario   ,@P_Cant_Solicitada;

      Exec [Det_Documento_Transaccion_Api#InsertRecord]  @P_Doc_Trans_Id    ,@P_Nro_Linea_Trans   ,@P_Documento_Id
				                                                ,@P_Nro_Linea       ,@P_Motivo_Id         ,@AEst_Merc_Id
				                                                ,@P_Cliente_Id      ,@P_Cat_Log_Id        ,@P_Item_Ok
				                                                ,@P_Movimiento_Pendiente
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end
			Insert Into RL_DET_DOC_TRANS_POSICION ( DOC_TRANS_ID,				NRO_LINEA_TRANS,    POSICION_ANTERIOR,		POSICION_ACTUAL,
                                              CANTIDAD,					  TIPO_MOVIMIENTO_ID, ULTIMA_ESTACION,			ULTIMA_SECUENCIA,
                                              NAVE_ANTERIOR,			NAVE_ACTUAL,        DOCUMENTO_ID,				  NRO_LINEA,
                                              DISPONIBLE,					DOC_TRANS_ID_EGR,   NRO_LINEA_TRANS_EGR,  DOC_TRANS_ID_TR,
                                              NRO_LINEA_TRANS_TR,	CLIENTE_ID,         CAT_LOG_ID,				    CAT_LOG_ID_FINAL,
                                              EST_MERC_ID)
			Values (@P_Doc_Trans_Id, @P_Nro_Linea_Trans, NULL, @APosicion_ID, @ACantidad_Ajuste, NULL, NULL, NULL, null,@ANave_ID, @P_Documento_Id, @P_Nro_Linea, '1', null, null, null, null, @P_Cliente_Id, @ACat_Log_Id,@ACat_Log_Id,@AEst_Merc_Id);
      
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end
      set @RL_ID = scope_identity()
      
      EXEC Funciones_Historicos_api#Actualizar_Historicos_X_Mov @RL_ID
      
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end
      
      UPDATE DOCUMENTO SET STATUS = 'D20' WHERE DOCUMENTO_ID = @P_Documento_Id

      Exec Am_Funciones_Estacion_Api#UpdateStatusDoc @P_Documento_Id, 'D30';
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end      
      
      Exec Am_Funciones_Estacion_Api#DocID_A_DocTrID @P_Documento_Id;
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end      
      
      exec dbo.AUDITORIA_HIST_INSERT_ING_AJU_INV @P_Documento_Id;
      if @@error<>0 begin
        Set @Err='1';
        Set @Err_Msg=ERROR_MESSAGE();
        return;
      end      
      
      UPDATE DOCUMENTO SET STATUS = 'D40' WHERE DOCUMENTO_ID = @P_Documento_Id;

    end Try
    Begin Catch
      Set @Err='1';
      Set @Err_Msg=ERROR_MESSAGE();
    End Catch;
End--Fin procedure Insert.