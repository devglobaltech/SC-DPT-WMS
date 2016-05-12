IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AJUSTAR_MASIVAMENTE]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[AJUSTAR_MASIVAMENTE]
GO
CREATE PROCEDURE DBO.AJUSTAR_MASIVAMENTE
  @UsuarioID  varchar(20)
AS
BEGIN
    --Segmento para declaracion de variables.
    Declare @CurAju             cursor;
    Declare @CurEx              Cursor;
    Declare @RlID               numeric(20,0)
    Declare @QtyEx              numeric(20,5)
    Declare @PESO				        numeric(20,5)
    Declare @VOLUMEN			      numeric(20,5)
    Declare @UNIDAD_ID			    varchar(5)		
    Declare @UNIDAD_PESO		    varchar(5)		
    Declare @UNIDAD_VOLUMEN	    varchar(5)   
    Declare @MONEDA_ID			    varchar(20)  
    Declare @DESCRIPCION        varchar(100)
    Declare @COSTO				      numeric(10,3)    
    Declare @ajuste_id          bigint
    Declare @Cliente_id         varchar(15)
    Declare @PRODUCTO_ID			  varchar(30)
    Declare @NRO_SERIE			    varchar(50)
    Declare @EST_MERC_ID			  varchar(15)
    Declare @CAT_LOG_ID			    varchar(50)
    Declare @NRO_BULTO			    varchar(50)
    Declare @NRO_LOTE			      varchar(50)
    Declare @FECHA_VENCIMIENTO	datetime
    Declare @NRO_DESPACHO		    varchar(50)
    Declare @NRO_PARTIDA			  varchar(50)
    Declare @PROP1				      varchar(100)
    Declare @PROP2				      varchar(100)
    Declare @PROP3				      varchar(100)
    Declare @POSICION_ID			  numeric(20,0)
    Declare @NAVE_ID				    numeric(20,0)
    Declare @NAVE_COD           varchar(15)
    Declare @CANTIDAD_AJUSTE		numeric(20,5)
    Declare @Signo              varchar(10)
    Declare @Seq                numeric(38)
    Declare @CantCur            numeric(20)
    Declare @Paso               char(1);
    Declare @FProceso           datetime;
    Declare @Error              char(1);
    Declare @ErrorMsg           varchar(2000)
    
    --Inicializacion de variables.
    Set @Paso='0';
    Set @FProceso=Getdate();
    
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
        
			Exec FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN @UsuarioID;
			--================================================================
		End;    
    
    if (CURSOR_STATUS('variable','CurAju')>=-1) Begin
        Deallocate @CurAju;
    End;
    
    Set @CurAju=Cursor for
      Select  a.Ajuste_id, a.cliente_id, a.producto_id, a.nro_serie, a.est_merc_id, a.cat_log_id, a.nro_bulto, a.nro_lote, a.fecha_vencimiento, a.nro_despacho, a.nro_partida,
              a.prop1, a.prop2, a.prop3, a.posicion_id, a.nave_id, a.cantidad_ajuste
      from    AJUSTES_MASIVOS a
      where   isnull(procesado,'0')='0';
    
    open @CurAju
    fetch next from @CurAju into  @Ajuste_id, @Cliente_id, @Producto_id, @Nro_Serie, @Est_Merc_id, @Cat_Log_id, @Nro_Bulto, @Nro_lote, @Fecha_Vencimiento, @Nro_Despacho,
                                  @Nro_Partida, @prop1, @prop2, @prop3, @posicion_id, @nave_id, @cantidad_ajuste;
    While @@FETCH_STATUS=0
    Begin
        if (CURSOR_STATUS('variable','CurEx')>=-1) Begin
          Deallocate @CurEx;
        End;

        if @Cantidad_Ajuste>=0 Begin
            Set @Signo='+';
        end
        else
        Begin
            Set @Signo='-';
        End;
            
        Set @paso='0'
        --Comienza la logica del programa.
        Set @CurEx=cursor for
          SELECT  rl.rl_id
					FROM    rl_det_doc_trans_posicion rl (NoLock)
						      LEFT JOIN nave n (NoLock)            on rl.nave_actual = n.nave_id 
						      LEFT JOIN posicion p  (NoLock)       on rl.posicion_actual = p.posicion_id 
						      LEFT JOIN nave n2   (NoLock)         on p.nave_id = n2.nave_id 
						      LEFT JOIN calle_nave caln (NoLock)   on p.calle_id = caln.calle_id 
						      LEFT JOIN columna_nave coln (NoLock) on p.columna_id = coln.columna_id
						      LEFT JOIN nivel_nave nn  (NoLock)    on p.nivel_id = nn.nivel_id
						      ,det_documento dd (NoLock) 
						      inner join documento d (NoLock)     on(dd.documento_id=d.documento_id) 
						      left join sucursal s                on(s.sucursal_id=d.sucursal_origen and s.cliente_id=d.cliente_id)
						      ,det_documento_transaccion ddt (NoLock),cliente c (NoLock),producto prod (NoLock)
						      ,categoria_logica cl (NoLock),documento_transaccion dt (NoLock)
					WHERE   rl.doc_trans_id = ddt.doc_trans_id      AND rl.nro_linea_trans = ddt.nro_linea_trans 
                  and ddt.documento_id = dd.documento_id  and ddt.doc_trans_id = dt.doc_trans_id 
                  AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA    AND DD.CLIENTE_ID = C.CLIENTE_ID 
                  AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID   AND DD.CLIENTE_ID = PROD.CLIENTE_ID 
                  AND RL.CAT_LOG_ID = CL.CAT_LOG_ID       AND RL.CLIENTE_ID = CL.CLIENTE_ID 
                  AND RL.DISPONIBLE= '1'                  AND ISNULL(p.pos_lockeada,'0')='0'
                  AND DD.CLIENTE_ID   =@Cliente_id
                  AND DD.PRODUCTO_ID  =@Producto_id
                  AND RL.cat_log_id   =@Cat_log_id
                  and (@EST_MERC_ID       is null or rl.est_merc_id=@EST_MERC_ID)
                  and (@NRO_SERIE         is null or dd.nro_serie=@nro_serie)
                  and (@NRO_BULTO         is null or dd.nro_bulto=@nro_bulto)
                  and (@FECHA_VENCIMIENTO is null or dd.fecha_vencimiento=@FECHA_VENCIMIENTO)
                  and (@NRO_DESPACHO      is null or dd.nro_despacho=@NRO_DESPACHO)
                  and (@NRO_PARTIDA       is null or dd.nro_partida=@NRO_PARTIDA)
                  and (@PROP1             IS NULL OR dd.prop1=@PROP1)
                  and (@PROP2             IS NULL OR dd.prop1=@PROP2)
                  and (@PROP3             IS NULL OR dd.prop1=@PROP3)
                  AND (@nave_id           IS NULL OR RL.NAVE_ACTUAL = @nave_id)
                  AND (@posicion_id       IS NULL OR RL.POSICION_ACTUAL = @posicion_id);

          open @CurEx;
         
          Fetch next from @CurEx into @RlID;
          select @CantCur=@@Cursor_rows;
          While @@FETCH_STATUS=0
          begin
          
            Set @Paso='1';
            
            select  @volumen=dd.volumen, @unidad_id=dd.unidad_id, @unidad_peso=dd.unidad_peso, @UNIDAD_VOLUMEN=dd.unidad_volumen, @MONEDA_ID=dd.moneda_id, @COSTO=dd.costo,
                    @descripcion=dd.DESCRIPCION
            from    det_documento dd inner join det_documento_transaccion ddt on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
                    inner join rl_det_doc_trans_posicion rl                   on(ddt.doc_trans_id=rl.doc_trans_id and ddt.nro_linea_trans=rl.nro_linea_trans)
            where   rl.rl_id=@RlID;
          

            Set @cantidad_ajuste=abs(@cantidad_ajuste);
            
            Begin Try
            
              if @posicion_id is not null begin
      
                select  @nave_id=n.nave_id, @nave_cod=n.nave_cod
                from    posicion p inner join nave n  on(p.nave_id=n.nave_id)
                where   posicion_id=@posicion_id;
                
              end else begin
              
                select  @nave_cod=nave_cod
                from    nave
                where   nave_id=@NAVE_ID;
              end;    
              
              Begin Transaction
              --Aqui se realizan los ajustes de la mercaderia.
              Exec DBO.FUNCIONES_INVENTARIO_API#REALIZAR_AJUSTE    @CLIENTE_ID      ,@PRODUCTO_ID     ,@FECHA_VENCIMIENTO
                                                                  ,@NRO_LOTE        ,@NRO_PARTIDA     ,@NRO_DESPACHO
                                                                  ,@NRO_BULTO       ,@NRO_SERIE       ,@NAVE_ID
                                                                  ,@POSICION_ID     ,@CAT_LOG_ID      ,@EST_MERC_ID
                                                                  ,@PROP1           ,@PROP2           ,@PROP3
                                                                  ,@PESO            ,@VOLUMEN         ,@UNIDAD_ID
                                                                  ,@UNIDAD_PESO     ,@UNIDAD_VOLUMEN  ,@MONEDA_ID
                                                                  ,@COSTO           ,@cantidad_ajuste ,@Signo
              
              --En este segmento se realizan los envios a las tablas de devolucion.  
              Exec dbo.GET_VALUE_FOR_SEQUENCE 'AJUSTE_QTY', @Seq Output;
              
              EXEC DBO.Ajuste_Qty   @pCliente_id		    =@cliente_id,
                                    @pTipo_Documento	  ='ST02',
                                    @pDoc_ext		        =@Seq,
                                    @pproducto_id		    =@producto_id,
                                    @pcantidad		      =@Cantidad_ajuste,
                                    @pest_merc_id		    =@est_merc_id,
                                    @pcat_log_id		    =@cat_log_id,
                                    @pdescripcion		    =@descripcion,
                                    @pnro_lote		      =@nro_lote,
                                    @pnro_pallet		    =@prop1,
                                    @pfecha_vencimiento	=@fecha_vencimiento,
                                    @pnro_despacho		  =@nro_despacho,
                                    @pnro_partida		    =@nro_partida,
                                    @punidad_id		      =@unidad_id,
                                    @pnave_id		        =@nave_id,
                                    @pnave_cod		      =@nave_cod,
                                    @pSigno			        =@signo;
              
              Update Ajustes_Masivos Set PROCESADO='S',F_PROCESADO=@FProceso, obs_ajuste='El ajuste se realizo correctamente.' where ajuste_id=@Ajuste_Id
              commit Transaction;
              break;
            end try
            Begin Catch
              Set @ErrorMsg=ERROR_MESSAGE();
              rollback transaction;
              Update Ajustes_Masivos Set PROCESADO='E',F_PROCESADO=@FProceso, obs_ajuste='Ocurrio un error al realizar el ajuste. Error: ' + @ErrorMsg where ajuste_id=@Ajuste_Id
            End Catch; 
            Fetch next from @CurEx into @RlID;
          End--Fin Cursor existencias.
          
          if (@Paso='0' and @signo='-') begin --Si paso=0 y signo='-', quiere decir que no habia existencias (con las caracteristicas indicadas) por lo cual no se puede dar de baja del stock.
            Update Ajustes_Masivos Set PROCESADO='E',F_PROCESADO=@FProceso, obs_ajuste='No se encontro el producto con las caracteristicas indicadas.' where ajuste_id=@Ajuste_Id
          end else begin
            if @Paso='0' AND @Signo='+' begin
              --Para reducir la complejidad de este Sp envio el alta de nuevos productos a otro sp de forma tal que este se ocupe de generar los documentos de ingreso
              Set @Error='0';
              Begin Transaction
              EXEC DBO.AJUSTAR_MASIVAMENTE_INGRESOS @Ajuste_id, @Error Output, @ErrorMsg Output;
              if @Error='1' Begin
                Rollback Transaction
                Update Ajustes_Masivos Set PROCESADO='E',F_PROCESADO=@FProceso, obs_ajuste='No fue posible dar el alta del producto. Error: ' + @ErrorMsg where ajuste_id=@Ajuste_Id;
              End Else Begin
                Commit Transaction;
                Update Ajustes_Masivos Set PROCESADO='S',F_PROCESADO=@FProceso, obs_ajuste='Se Proceso correctamente el registro' where ajuste_id=@Ajuste_Id;
              End;
            end
          end          
          close @CurEx;
          Deallocate @CurEx;
      fetch next from @CurAju into  @Ajuste_id, @Cliente_id, @Producto_id, @Nro_Serie, @Est_Merc_id, @Cat_Log_id, @Nro_Bulto, @Nro_lote, @Fecha_Vencimiento, @Nro_Despacho,
                                    @Nro_Partida, @prop1, @prop2, @prop3, @posicion_id, @nave_id, @cantidad_ajuste;
    end--Fin @@FETCH_STATUS.
    Close @CurAju;
    Deallocate @CurAju;
    
    delete from RL_DET_DOC_TRANS_POSICION where CANTIDAD=0;
    
    --------------------------------------------------------------------------------------------------
    --Al final del proceso, saco una consulta donde se pueda apreciar el procesamiento de los ajustes.
    --------------------------------------------------------------------------------------------------
    SELECT	AJUSTE_ID,
            CLIENTE_ID			  [COD. CLIENTE],
            PRODUCTO_ID			  [COD. PRODUCTO],
            NRO_SERIE			    [NRO. SERIE],
            EST_MERC_ID			  [COD. EST. MERCADERIA],
            CAT_LOG_ID			  [COD. CATEGORIA LOGICA],
            NRO_BULTO			    [NRO. BULTO],
            NRO_LOTE			    [NRO. LOTE],	
            FECHA_VENCIMIENTO	[F. VENCIMIENTO],
            NRO_DESPACHO		  [NRO. DESPACHO],
            NRO_PARTIDA			  [NRO. PARTIDA],
            PROP1				      [PROP1],
            PROP2				      [PROP2],
            PROP3				      [PROP3],
            P.POSICION_COD			  [POSICION],
            N.NAVE_COD				    [NAVE],
            CANTIDAD_AJUSTE		[CANT. A AJUSTAR],
            OBS_AJUSTE		  	[OBSERVACIONES],
            Case PROCESADO 
            WHEN 'S' THEN 'PROCESADO'
            WHEN 'E' THEN 'CON ERROR'
            WHEN '0' THEN 'NO PROCESADO'
            END					      [PROCESADO],
            F_PROCESADO			  [FECHA PROCESO]
    FROM	  AJUSTES_MASIVOS LEFT JOIN POSICION P	ON(AJUSTES_MASIVOS.POSICION_ID=P.POSICION_ID)
			  LEFT JOIN NAVE N						on(AJUSTES_MASIVOS.NAVE_ID=P.NAVE_ID)	
    WHERE	  F_PROCESADO=@FProceso;
    --------------------------------------------------------------------------------------------------
END --FIN PROCEDIMIENTO ALMACENADO.