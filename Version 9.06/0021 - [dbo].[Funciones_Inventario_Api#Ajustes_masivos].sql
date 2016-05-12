ALTER        Procedure [dbo].[Funciones_Inventario_Api#Ajustes_masivos]
@P_INVENTARIO_ID 		as Numeric(20,0) OUTPUT,
@P_RESULTADO			AS NUMERIC(20,0) OUTPUT
As
Begin

declare @cur cursor
declare @CUR_INT cursor
DECLARE @V_CLIENTE_ID VARCHAR(100)
DECLARE @V_PRODUCTO_ID VARCHAR(100)
DECLARE @V_CANTIDAD NUMERIC(20,5)
DECLARE @V_NAVE_ID NUMERIC(20)
DECLARE @V_POSICION_ID NUMERIC(20)
DECLARE @V_MARBETE NUMERIC(20)
DECLARE @V_NRO_LOTE AS VARCHAR(100)
DECLARE @V_NRO_PARTIDA AS VARCHAR(100)
--PARA DBO.FUNCIONES_INVENTARIO_API#REALIZAR_AJUSTE
DECLARE @V2_CAT_LOG_ID VARCHAR(50)
DECLARE @V2_FEC_VTO VARCHAR(50)
DECLARE @V2_NRO_LOTE VARCHAR(50)
DECLARE @V2_NRO_PARTIDA VARCHAR(50) 		
DECLARE @V2_NRO_DESPACHO VARCHAR(50) 		
DECLARE @V2_NRO_BULTO VARCHAR(50) 		
DECLARE @V2_NRO_SERIE VARCHAR(50) 		
DECLARE @V2_EST_MERC_ID VARCHAR(50)		
DECLARE @V2_PROP1 VARCHAR(100)		
DECLARE @V2_PROP2 VARCHAR(100) 	
DECLARE @V2_PROP3 VARCHAR(100)		
DECLARE @V2_PESO NUMERIC(20,5)	
DECLARE @V2_VOLUMEN NUMERIC(20,5)	
DECLARE @V2_UNIDAD_ID VARCHAR(5)		
DECLARE @V2_UNIDAD_PESO VARCHAR(5)		
DECLARE @V2_UNIDAD_VOLUMEN VARCHAR(5) 		
DECLARE @V2_MONEDA_ID VARCHAR(20)		
DECLARE @V2_COSTO NUMERIC(10,3)	
DECLARE @V2_CANTIDAD NUMERIC(20,5)	
DECLARE @V2_SIGNO VARCHAR(3)
DECLARE @V2_CANT_AJU_ACT NUMERIC(20,4)
--FIN PARA DBO.FUNCIONES_INVENTARIO_API#REALIZAR_AJUSTE		
DECLARE @V_CANT_AUX NUMERIC(20,4)




BEGIN TRY
	SET XACT_ABORT ON
	------primero hago los ajustes
	
	SELECT *  INTO #INV_T FROM DET_INVENTARIO WHERE INVENTARIO_ID=@P_INVENTARIO_ID;
	
	UPDATE	DET_INVENTARIO SET MODO_INGRESO=CASE WHEN P.SERIE_ING=1 THEN 'M' ELSE D.MODO_INGRESO END
	FROM	DET_INVENTARIO D INNER JOIN PRODUCTO P
			ON(D.CLIENTE_ID=P.CLIENTE_ID AND D.PRODUCTO_ID=P.PRODUCTO_ID)
	WHERE	INVENTARIO_ID=@P_INVENTARIO_ID;
	
	--BEGIN TRAN EXTERNA
	Set @cur = Cursor For
		Select	A.CLIENTE_ID, A.PRODUCTO_ID, A.MARBETE, A.CANT_AJU, A.NAVE_ID, A.POSICION_ID, I.NRO_LOTE, I.NRO_PARTIDA  
		FROM	DET_INVENTARIO_AJU A
				INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
		WHERE	(A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
				AND I.MODO_INGRESO = 'S'
				AND A.INVENTARIO_ID = @P_INVENTARIO_ID AND A.CANT_AJU <> 0 
		ORDER BY 
				A.CLIENTE_ID, A.POSICION_ID, I.NRO_LOTE DESC, I.NRO_PARTIDA DESC FOR UPDATE
			
	Open @cur
	Fetch Next From @cur into @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA


	IF OBJECT_ID('tempdb.dbo.#temp_usuario_loggin','U') IS NULL
		BEGIN
			--================================================================
			CREATE TABLE #temp_usuario_loggin (
				usuario_id            			VARCHAR(20)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				terminal              			VARCHAR(100)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				fecha_loggin          		DATETIME     ,
				session_id            			VARCHAR(60)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				rol_id                			VARCHAR(5)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				emplazamiento_default 	VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
				deposito_default      		VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
			)
			exec FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN 'USER'
			--================================================================
		END


	While @@Fetch_Status=0
	Begin
				
		--PRINT '----------------------------------------------------------'
		--print @V_MARBETE

		IF @V_CANTIDAD < 0 
			BEGIN
				SET @V2_SIGNO = '-' 
				SET @V_CANT_AUX = @V_CANTIDAD * (-1)
			END 
		ELSE 
			BEGIN
				SET @V2_SIGNO = '+'
				SET @V_CANT_AUX = @V_CANTIDAD
			END

		

		Set @CUR_INT = Cursor For SELECT rl.cat_log_id as CategLogID 
										  ,sum(ISNULL(rl.cantidad,0)) AS Cantidad 
										  ,dd.nro_serie 
										  ,dd.Nro_lote 
										  ,dd.Fecha_vencimiento 
										  ,dd.Nro_Despacho 
										  ,dd.Nro_bulto 
										  ,dd.Nro_Partida 
										  ,rl.est_merc_id 
										  ,dd.prop1 
										  ,dd.prop2 
										  ,dd.prop3 
										  ,DD.PESO
										  ,DD.VOLUMEN
										  ,dd.unidad_id 
										  ,DD.UNIDAD_PESO
										  ,DD.UNIDAD_VOLUMEN
										  ,dd.moneda_id 
										  ,dd.costo 
									FROM  rl_det_doc_trans_posicion rl (NoLock)
										  LEFT JOIN nave n (NoLock)            on rl.nave_actual = n.nave_id 
										  LEFT JOIN posicion p  (NoLock)       on rl.posicion_actual = p.posicion_id 
										  LEFT JOIN nave n2   (NoLock)         on p.nave_id = n2.nave_id 
										  LEFT JOIN calle_nave caln (NoLock)   on p.calle_id = caln.calle_id 
										  LEFT JOIN columna_nave coln (NoLock) on p.columna_id = coln.columna_id
										  LEFT JOIN nivel_nave nn  (NoLock)    on p.nivel_id = nn.nivel_id
										  ,det_documento dd (NoLock) 
										  inner join documento d (NoLock) on(dd.documento_id=d.documento_id) 
										  left join sucursal s on(s.sucursal_id=d.sucursal_origen and s.cliente_id=d.cliente_id)
										  ,det_documento_transaccion ddt (NoLock)
										  ,cliente c (NoLock)
										  ,producto prod (NoLock)
										  ,categoria_logica cl (NoLock)
										  ,documento_transaccion dt (NoLock)
									WHERE rl.doc_trans_id = ddt.doc_trans_id 
										  AND rl.nro_linea_trans = ddt.nro_linea_trans 
										  and ddt.documento_id = dd.documento_id 
										  and ddt.doc_trans_id = dt.doc_trans_id 
										  AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
										  AND DD.CLIENTE_ID = C.CLIENTE_ID 
										  AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID 
										  AND DD.CLIENTE_ID = PROD.CLIENTE_ID 
										  AND RL.CAT_LOG_ID = CL.CAT_LOG_ID 
										  AND RL.CLIENTE_ID = CL.CLIENTE_ID 
										  AND RL.DISPONIBLE= '1'
										  AND ISNULL(p.pos_lockeada,'0')='0'
										  AND DD.CLIENTE_ID = @V_CLIENTE_ID
										  AND DD.PRODUCTO_ID = @V_PRODUCTO_ID
										  AND (@V_NAVE_ID IS NULL OR RL.NAVE_ACTUAL = @V_NAVE_ID)
										  AND (@V_POSICION_ID IS NULL OR RL.POSICION_ACTUAL = @V_POSICION_ID)
									 GROUP BY rl.cat_log_id
										  ,dd.nro_serie 
										  ,dd.Nro_lote 
										  ,dd.Fecha_vencimiento 
										  ,dd.Nro_Despacho 
										  ,dd.Nro_bulto 
										  ,dd.Nro_Partida 
										  ,rl.est_merc_id 
										  ,dd.prop1 
										  ,dd.prop2 
										  ,dd.prop3 
										  ,DD.PESO
										  ,DD.VOLUMEN
										  ,dd.unidad_id 
										  ,DD.UNIDAD_PESO
										  ,DD.UNIDAD_VOLUMEN
										  ,dd.moneda_id 
										  ,dd.costo 



		BEGIN TRAN 
	
		OPEN @CUR_INT
		FETCH NEXT FROM @CUR_INT INTO @V2_CAT_LOG_ID,@V2_CANTIDAD,@V2_NRO_SERIE,@V2_NRO_LOTE,@V2_FEC_VTO,@V2_NRO_DESPACHO,
										@V2_NRO_BULTO,@V2_NRO_PARTIDA,@V2_EST_MERC_ID,@V2_PROP1,@V2_PROP2,@V2_PROP3,@V2_PESO,
										@V2_VOLUMEN,@V2_UNIDAD_ID,@V2_UNIDAD_PESO,@V2_UNIDAD_VOLUMEN,@V2_MONEDA_ID,@V2_COSTO

		WHILE @@FETCH_STATUS =0
		BEGIN
			
			IF @V2_SIGNO = '-'
				BEGIN
					IF @V2_CANTIDAD >= @V_CANT_AUX
						BEGIN
							SET @V2_CANT_AJU_ACT = @V_CANT_AUX
							SET @V_CANT_AUX =  0
						END
					ELSE
						BEGIN
							SET @V2_CANT_AJU_ACT = @V2_CANTIDAD
							SET @V_CANT_AUX = @V_CANT_AUX - @V2_CANTIDAD 
						END
				END
			ELSE
				BEGIN
					SET @V2_CANT_AJU_ACT = @V_CANT_AUX
					SET @V_CANT_AUX =  0
				END

			BEGIN TRY

				EXEC DBO.FUNCIONES_INVENTARIO_API#REALIZAR_AJUSTE_INV @V_CLIENTE_ID, @V_PRODUCTO_ID, 
								@V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA,
								@V2_CANT_AJU_ACT, @V2_SIGNO

			END TRY
			BEGIN CATCH
				SET @V_CANT_AUX = 1
				BREAK
			END CATCH
			
			IF @V_CANT_AUX = 0
				BREAK

			FETCH NEXT FROM @CUR_INT INTO @V2_CAT_LOG_ID,@V2_CANTIDAD,@V2_NRO_SERIE,@V2_NRO_LOTE,@V2_FEC_VTO,@V2_NRO_DESPACHO,
										@V2_NRO_BULTO,@V2_NRO_PARTIDA,@V2_EST_MERC_ID,@V2_PROP1,@V2_PROP2,@V2_PROP3,@V2_PESO,
										@V2_VOLUMEN,@V2_UNIDAD_ID,@V2_UNIDAD_PESO,@V2_UNIDAD_VOLUMEN,@V2_MONEDA_ID,@V2_COSTO
			

		END --END WHILE @CUR_INT

		CLOSE @CUR_INT
		DEALLOCATE @CUR_INT
		
		IF @V_CANT_AUX = 0
			BEGIN

				--HACER UPDATE DEL REGISTRO DEL CURSOR EXTERNO EN UN CAMPO NUEVO , PARA MARCAR QUE SE COMPLETO EL AJUSTE
				UPDATE DET_INVENTARIO_AJU SET PROCESADO = 'S' WHERE CURRENT OF @cur

				
				
				COMMIT TRAN 
			END
		ELSE
			BEGIN

				ROLLBACK TRAN 
				UPDATE DET_INVENTARIO_AJU SET PROCESADO = 'E' WHERE CURRENT OF @cur

			END

		Fetch Next From @cur into @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA
		
		
	End	--End While @cur.

	CLOSE @cur
	DEALLOCATE @cur
	

	PRINT 'FINALIZADO LOS AJUSTES'	
   
	------ahora creo los documentos por la mercadería que no esta en sistema-----------------------------------------------------------




    DECLARE @V_Cliente_Id_EXT VARCHAR(15)
	DECLARE @CUR_EXTERNA AS CURSOR
	DECLARE @PROCESO AS VARCHAR(1) --1 = PRIMER PROCESO (SIN SERIES), 2 = CON SERIES

	--PARA DOCUMENTO ING
		DECLARE @P_Documento_Id numeric
		DECLARE @P_Cliente_Id varchar(15)
		DECLARE @P_Tipo_Comprobante_Id varchar(5)
		DECLARE @P_Tipo_Operacion_Id varchar(5)
		DECLARE @P_Det_Tipo_Operacion_Id varchar(5)
		DECLARE @P_Cpte_Prefijo varchar(6)
		DECLARE @P_Cpte_Numero varchar(20)
		DECLARE @P_Fecha_Cpte varchar(20)
		DECLARE @P_Fecha_Pedida_Ent varchar(20)
		DECLARE @P_Sucursal_Origen varchar(20)
		DECLARE @P_Sucursal_Destino varchar(20)
		DECLARE @P_Anulado varchar(1)
		DECLARE @P_Motivo_Anulacion varchar(15)
		DECLARE @P_Peso_Total numeric
		DECLARE @P_Unidad_Peso varchar(5)
		DECLARE @P_Volumen_Total numeric
		DECLARE @P_Unidad_Volumen varchar(5)
		DECLARE @P_Total_Bultos numeric
		DECLARE @P_Valor_Declarado numeric
		DECLARE @P_Orden_De_Compra varchar(20)
		DECLARE @P_Cant_Items numeric
		DECLARE @P_Observaciones varchar(200)
		DECLARE @P_Status varchar(3)
		DECLARE @P_NroRemito varchar(30)
		DECLARE @P_Fecha_Alta_Gtw varchar(20)
		DECLARE @P_Fecha_Fin_Gtw varchar(20)
		DECLARE @P_Personal_Id varchar(20)
		DECLARE @P_Transporte_Id varchar(20)
		DECLARE @P_Nro_Despacho_Importacion varchar(30)
		DECLARE @P_Alto numeric
		DECLARE @P_Ancho numeric
		DECLARE @P_Largo numeric
		DECLARE @P_Unidad_Medida varchar(5)
		DECLARE @P_Grupo_Picking varchar(50)
		DECLARE @P_Prioridad_Picking numeric
		
--PARA DOCUMENTO_TRANSACCION
		DECLARE @P_Completado varchar(1)
		DECLARE @P_Transaccion_Id varchar(15)
		DECLARE @P_Estacion_Actual varchar(15)
		DECLARE @P_Est_Mov_Actual varchar(20)
		DECLARE @P_Orden_Id numeric
		DECLARE @P_It_Mover varchar(1)
		DECLARE @P_Orden_Estacion numeric
		--DECLARE @P_Tipo_Operacion_Id varchar(5)
		DECLARE @P_Tr_Pos_Completa varchar(1)
		DECLARE @P_Tr_Activo varchar(1)
		DECLARE @P_Usuario_Id varchar(20)
		DECLARE @P_Terminal varchar(20)
		--DECLARE @P_Fecha_Alta_Gtw datetime
		DECLARE @P_Tr_Activo_Id varchar(10)
		DECLARE @P_Session_Id varchar(60)
		DECLARE @P_Fecha_Cambio_Tr datetime
		--DECLARE @P_Fecha_Fin_Gtw datetime
		DECLARE @P_Doc_Trans_Id numeric
		
--PARA DET_DOCUMENTO
		DECLARE @P_Nro_Linea numeric
		DECLARE @P_Cantidad numeric
		DECLARE @P_Nro_Serie varchar(50)
		DECLARE @P_Nro_Serie_Padre varchar(50)
		DECLARE @P_Est_Merc_Id varchar(15)
		DECLARE @P_Cat_Log_Id varchar(50)
		DECLARE @P_Nro_Bulto varchar(50)
		DECLARE @P_Descripcion varchar(200)
		DECLARE @P_Nro_Lote varchar(50)
		DECLARE @P_Fecha_Vencimiento datetime
		DECLARE @P_Nro_Despacho varchar(50)
		DECLARE @P_Nro_Partida varchar(50)
		DECLARE @P_Unidad_Id varchar(5)
		DECLARE @P_Peso numeric
		--DECLARE @P_Unidad_Peso varchar(5)
		DECLARE @P_Volumen numeric
		--DECLARE @P_Unidad_Volumen varchar(5)
		DECLARE @P_Busc_Individual varchar(1)
		DECLARE @P_Tie_In varchar(1)
		DECLARE @P_Nro_Tie_In_Padre varchar(100)
		DECLARE @P_Nro_Tie_In varchar(100)
		DECLARE @P_Item_Ok varchar(1)
		DECLARE @P_Moneda_Id varchar(20)
		DECLARE @P_Costo numeric
		DECLARE @P_Cat_Log_Id_Final varchar(50)
		DECLARE @P_Prop1 varchar(100)
		DECLARE @P_Prop2 varchar(100)
		DECLARE @P_Prop3 varchar(100)
		--DECLARE @P_Largo numeric
		--DECLARE @P_Alto numeric
		--DECLARE @P_Ancho numeric
		DECLARE @P_Volumen_Unitario varchar(1)
		DECLARE @P_Peso_Unitario varchar(1)
		DECLARE @P_Cant_Solicitada numeric
		--variables para el det_documento_transaccion
		--DECLARE @P_Doc_Trans_Id numeric
		DECLARE @P_Nro_Linea_Trans numeric
		--DECLARE @P_Documento_Id numeric
		DECLARE @P_Nro_Linea_Doc numeric
		DECLARE @P_Motivo_Id varchar(15)
		--DECLARE @P_Est_Merc_Id varchar(15)
		--DECLARE @P_Cliente_Id varchar(15)
		--DECLARE @P_Cat_Log_Id varchar(50)
		--DECLARE @P_Item_Ok varchar(1)
		DECLARE @P_Movimiento_Pendiente varchar(1)
		declare @RL_ID numeric(20)
		DECLARE @FL_CONTENEDORA VARCHAR(1)
		DECLARE @SEC_CONTENEDORA   int  


	
	SET @CUR_EXTERNA = CURSOR FOR
		SELECT distinct A.CLIENTE_ID
		FROM DET_INVENTARIO_AJU A
			INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
		WHERE (A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
			AND I.MODO_INGRESO = 'M'
			AND A.CANT_AJU > 0
			AND A.INVENTARIO_ID = @P_INVENTARIO_ID 
		order by 1


	BEGIN TRAN

	open @CUR_EXTERNA
	FETCH NEXT FROM @CUR_EXTERNA INTO @V_Cliente_Id_EXT
    WHILE @@FETCH_STATUS = 0 
	BEGIN
		

		SET @PROCESO = '1' --EMPIEZO A CREAR EL DOCUMENTO DE INGRESO PARA LOS PRODUCTOS SIN SERIES


		INICIO_DOC:
		
		set @P_Documento_Id = null
		
		IF @PROCESO = '1' --SIN SERIES
		BEGIN
		
			Set @cur = Cursor For
				Select A.CLIENTE_ID, A.PRODUCTO_ID, A.MARBETE, A.CANT_AJU, A.NAVE_ID, A.POSICION_ID, I.NRO_LOTE, I.NRO_PARTIDA
				FROM DET_INVENTARIO_AJU A
					INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
					INNER JOIN PRODUCTO P ON (P.CLIENTE_ID = I.CLIENTE_ID AND P.PRODUCTO_ID = I.PRODUCTO_ID)
				WHERE (A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
					AND I.MODO_INGRESO = 'M'
					AND A.CANT_AJU > 0
					AND A.INVENTARIO_ID = @P_INVENTARIO_ID 
					AND A.CLIENTE_ID = @V_Cliente_Id_EXT
					AND (P.SERIE_ING IS NULL OR P.SERIE_ING <> '1')
		END
		ELSE --CON SERIES
		BEGIN
		
			Set @cur = Cursor For
				Select A.CLIENTE_ID, A.PRODUCTO_ID, A.MARBETE, A.CANT_AJU, A.NAVE_ID, A.POSICION_ID, I.NRO_LOTE, I.NRO_PARTIDA
				FROM DET_INVENTARIO_AJU A
					INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
					INNER JOIN PRODUCTO P ON (P.CLIENTE_ID = I.CLIENTE_ID AND P.PRODUCTO_ID = I.PRODUCTO_ID)
				WHERE (A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
					AND I.MODO_INGRESO = 'M'
					AND A.CANT_AJU > 0
					AND A.INVENTARIO_ID = @P_INVENTARIO_ID 
					AND A.CLIENTE_ID = @V_Cliente_Id_EXT
					AND P.SERIE_ING = '1'
		END
		


				
		Open @cur
		Fetch Next From @cur into  @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA

		if @@fetch_status = 0 
			begin
				--CREAR DOS DOCUMENTO POR CLIENTE, UNO PARA LOS PRODUCTOS SIN SERIALIZACION Y OTRO PARA LOS PRODUCTOS SERIELIZADOS
				--EL DOCUMENTO POR LOS SERIALIZADOS QUEDARA EN D30 PARA QUE SE PUEDAN CARGAR LAS SERIES DE LA FORMA HABITUAL

				--CREA EL DOCUMENTO
				PRINT 'COMIENZA EL AJUSTE POR DOC INGRESO'
				

				SET @P_Cliente_Id = @V_CLIENTE_ID
				SET @P_Tipo_Comprobante_Id = 'IM'
				SET @P_Tipo_Operacion_Id = 'ING'
				SET @P_Det_Tipo_Operacion_Id = 'MAN'
				IF @PROCESO = '1'
					SET @P_Cpte_Prefijo='0001'
				ELSE
					SET @P_Cpte_Prefijo='0002'
					
				SET @P_Cpte_Numero=replicate('0',8 - len(convert(varchar(100), @P_INVENTARIO_ID, 1))) + convert(varchar(100), @P_INVENTARIO_ID)
				SET @P_Fecha_Cpte = CONVERT(datetime,CONVERT(VARCHAR,GETDATE(),101),101)--PARA DEVOLVER FECHA SIN HORAS Y MINUTOS
				SET @P_Observaciones = 'AJUSTE POR INVENTARIO NRO: ' + CAST(@P_INVENTARIO_ID AS VARCHAR)
				SET @P_Status = 'D40'
				SET @P_Unidad_Peso = NULL
		
				
				EXECUTE [dbo].[Documento_Api#InsertRecord] 
							   @P_Documento_Id OUTPUT
							  ,@P_Cliente_Id
							  ,@P_Tipo_Comprobante_Id
							  ,@P_Tipo_Operacion_Id
							  ,@P_Det_Tipo_Operacion_Id
							  ,@P_Cpte_Prefijo
							  ,@P_Cpte_Numero
							  ,@P_Fecha_Cpte
							  ,@P_Fecha_Pedida_Ent
							  ,@P_Sucursal_Origen
							  ,@P_Sucursal_Destino
							  ,@P_Anulado
							  ,@P_Motivo_Anulacion
							  ,@P_Peso_Total
							  ,@P_Unidad_Peso
							  ,@P_Volumen_Total
							  ,@P_Unidad_Volumen
							  ,@P_Total_Bultos
							  ,@P_Valor_Declarado
							  ,@P_Orden_De_Compra
							  ,@P_Cant_Items
							  ,@P_Observaciones
							  ,@P_Status
							  ,@P_NroRemito
							  ,@P_Fecha_Alta_Gtw
							  ,@P_Fecha_Fin_Gtw
							  ,@P_Personal_Id
							  ,@P_Transporte_Id
							  ,@P_Nro_Despacho_Importacion
							  ,@P_Alto
							  ,@P_Ancho
							  ,@P_Largo
							  ,@P_Unidad_Medida
							  ,@P_Grupo_Picking
							  ,@P_Prioridad_Picking
				

				--CREA EL DOCUMENTO_TRANSACCION

				


				IF @PROCESO = '1'  --PARA DOC_INGRESO SIN SERIES
				BEGIN
					SET @P_Completado = '0'
					SET @P_Transaccion_Id = 'ING_ABAST_F'
					SET @P_Status = 'T40'
					SET @P_Tipo_Operacion_Id = 'ING'
					SET @P_Tr_Activo = '0'
				end
				else
				begin -- PARA DOC_INGRESO CON SERIES
					SET @P_Completado = '0'
					SET @P_Transaccion_Id = 'ING_ABAST_F'
					SET @P_Status = 'T10'
					SET @P_Tipo_Operacion_Id = 'ING'
					SET @P_Tr_Activo = '0'
					SET @P_Estacion_Actual  = 'RECEP_ABAST'
					SET @P_Est_Mov_Actual = 'A'
					SET @P_It_Mover = '0'
					SET @P_Orden_Estacion = '1'
				end
				
				
				
				
				
				EXEC [dbo].[Documento_Transaccion_Api#InsertRecord] 
						@P_Completado
						,@P_Observaciones
						,@P_Transaccion_Id
						,@P_Estacion_Actual
						,@P_Status
						,@P_Est_Mov_Actual
						,@P_Orden_Id
						,@P_It_Mover
						,@P_Orden_Estacion
						,@P_Tipo_Operacion_Id
						,@P_Tr_Pos_Completa
						,@P_Tr_Activo
						,@P_Usuario_Id
						,@P_Terminal
						,@P_Fecha_Alta_Gtw
						,@P_Tr_Activo_Id
						,@P_Session_Id
						,@P_Fecha_Cambio_Tr
						,@P_Fecha_Fin_Gtw
						,@P_Doc_Trans_Id OUTPUT
				
				
			end


			
			
			
		While @@Fetch_Status=0
		Begin	

			--Creo el detalle de det_documento


			
			set @P_Cantidad = @V_CANTIDAD
			SET @P_Cant_Solicitada = @V_CANTIDAD
			SET @P_Cat_Log_Id = 'DISPONIBLE'
			SET @P_Cat_Log_Id_Final = 'DISPONIBLE'
			set @P_Volumen_Unitario = '1'
			set @P_Peso_Unitario = '1'
			SET @P_Tie_In = '0'
            SET @P_Nro_Lote = @V_NRO_LOTE
			SET @P_Nro_Partida = @V_NRO_PARTIDA
			select @P_Descripcion = descripcion, @P_Unidad_Id = unidad_id, @P_Unidad_Peso = unidad_peso, @P_Unidad_Volumen = unidad_volumen 
				from producto
				where producto_id =@V_PRODUCTO_ID
				
			
			
			SELECT @FL_CONTENEDORA = FLG_CONTENEDORA FROM PRODUCTO 
			WHERE CLIENTE_ID =@P_Cliente_Id AND PRODUCTO_ID = @V_PRODUCTO_ID
			
			
			IF @FL_CONTENEDORA = '1' 
			BEGIN
				EXEC GET_VALUE_FOR_SEQUENCE 'CONTENEDORA', @SEC_CONTENEDORA OUTPUT
				set @P_Nro_Bulto = @SEC_CONTENEDORA
				
			END
			
						
			
		
			EXECUTE [dbo].[Det_Documento_Api#InsertRecord] 
			   @P_Documento_Id
			  ,@P_Nro_Linea
			  ,@P_Cliente_Id
			  ,@V_PRODUCTO_ID
			  ,@P_Cantidad
			  ,@P_Nro_Serie
			  ,@P_Nro_Serie_Padre
			  ,@P_Est_Merc_Id
			  ,@P_Cat_Log_Id
			  ,@P_Nro_Bulto
			  ,@P_Descripcion
			  ,@P_Nro_Lote
			  ,@P_Fecha_Vencimiento
			  ,@P_Nro_Despacho
			  ,@P_Nro_Partida
			  ,@P_Unidad_Id
			  ,@P_Peso
			  ,@P_Unidad_Peso
			  ,@P_Volumen
			  ,@P_Unidad_Volumen
			  ,@P_Busc_Individual
			  ,@P_Tie_In
			  ,@P_Nro_Tie_In_Padre
			  ,@P_Nro_Tie_In
			  ,@P_Item_Ok
			  ,@P_Moneda_Id
			  ,@P_Costo
			  ,@P_Cat_Log_Id_Final
			  ,@P_Prop1
			  ,@P_Prop2
			  ,@P_Prop3
			  ,@P_Largo
			  ,@P_Alto
			  ,@P_Ancho
			  ,@P_Volumen_Unitario
			  ,@P_Peso_Unitario
			  ,@P_Cant_Solicitada		

			
		

			SELECT @P_Nro_Linea_Trans = max(NRO_LINEA) FROM DET_DOCUMENTO WHERE documento_id = @P_Documento_Id
			set @P_Nro_Linea_Doc = @P_Nro_Linea_Trans
			set @P_Item_Ok= '0'
			set @P_Movimiento_Pendiente = '0'
			
			exec [Det_Documento_Transaccion_Api#InsertRecord] 
				   @P_Doc_Trans_Id
				  ,@P_Nro_Linea_Trans
				  ,@P_Documento_Id
				  ,@P_Nro_Linea_Doc
				  ,@P_Motivo_Id
				  ,@P_Est_Merc_Id
				  ,@P_Cliente_Id
				  ,@P_Cat_Log_Id
				  ,@P_Item_Ok
				  ,@P_Movimiento_Pendiente
			
			Insert Into RL_DET_DOC_TRANS_POSICION (
						DOC_TRANS_ID,				NRO_LINEA_TRANS,
						POSICION_ANTERIOR,		POSICION_ACTUAL,
						CANTIDAD,					TIPO_MOVIMIENTO_ID, --ver TIPO_MOVIMIENTO_ID
						ULTIMA_ESTACION,			ULTIMA_SECUENCIA,
						NAVE_ANTERIOR,				NAVE_ACTUAL,
						DOCUMENTO_ID,				NRO_LINEA,
						DISPONIBLE,					DOC_TRANS_ID_EGR,
						NRO_LINEA_TRANS_EGR,		DOC_TRANS_ID_TR,
						NRO_LINEA_TRANS_TR,		CLIENTE_ID,
						CAT_LOG_ID,				CAT_LOG_ID_FINAL,
						EST_MERC_ID)
			Values (@P_Doc_Trans_Id, @P_Nro_Linea_Trans, NULL, @V_POSICION_ID, @P_Cantidad, NULL, NULL, NULL, null,@V_NAVE_ID, @P_Documento_Id, @P_Nro_Linea_Doc, '1', null, null, null, null, @V_CLIENTE_ID, @P_Cat_Log_Id,@P_Cat_Log_Id,null)

			
			set @RL_ID = scope_identity()

			EXEC Funciones_Historicos_api#Actualizar_Historicos_X_Mov @RL_ID

			UPDATE DET_INVENTARIO_AJU SET PROCESADO = 'S' WHERE CURRENT OF @cur

			Fetch Next From @cur into  @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA

		End	--End While @cur.

		
		IF @P_Documento_Id IS NOT NULL
			BEGIN 
		
				UPDATE DOCUMENTO SET STATUS = 'D20' WHERE DOCUMENTO_ID = @P_Documento_Id

				/*NO HAY QUE ASIGNAR TRATAMIENTO, PORQUE YA ESTA CREADO EL DOCUMENTO TRANSACCION
				-----------------------------------------------------------------------------------------------------------------
				--ASIGNO TRATAMIENTO...
				-----------------------------------------------------------------------------------------------------------------
				exec asigna_tratamiento#asigna_tratamiento_ing @P_Documento_Id
				*/

				Exec Am_Funciones_Estacion_Api#UpdateStatusDoc @P_Documento_Id, 'D30'
                Exec Am_Funciones_Estacion_Api#DocID_A_DocTrID @P_Documento_Id
			


				------------------------------------------------------------------------------------------------------------------------------------
				--Guardo en la tabla de auditoria
				-----------------------------------------------------------------------------------------------------------------
				exec dbo.AUDITORIA_HIST_INSERT_ING_AJU_INV @P_Documento_Id	
				
				IF @PROCESO = '1'
				BEGIN
					UPDATE DOCUMENTO SET STATUS = 'D40' WHERE DOCUMENTO_ID = @P_Documento_Id
				END
				
				
				
			END
		
		
		IF @PROCESO = '1' 
		BEGIN
			SET @PROCESO = 2 --PARA PROCESAR LOS PRDUCTOS CON SERIES.
			GOTO INICIO_DOC
		END
		


		CLOSE @cur
		DEALLOCATE @cur
		
		

		FETCH NEXT FROM @CUR_EXTERNA INTO @V_Cliente_Id_EXT
	END --WHILE @@FECTH_STATUS = 0 DE @CUR_EXTERNA
	CLOSE @CUR_EXTERNA
	DEALLOCATE @CUR_EXTERNA

	COMMIT TRAN

	UPDATE	DET_INVENTARIO SET MODO_INGRESO=T.MODO_INGRESO
	FROM	DET_INVENTARIO DD INNER JOIN #INV_T T
			ON(DD.INVENTARIO_ID=T.INVENTARIO_ID AND DD.MARBETE=T.MARBETE)
	WHERE	DD.INVENTARIO_ID = @P_INVENTARIO_ID

	SELECT @P_RESULTADO=COUNT(*) FROM DET_INVENTARIO_AJU WHERE INVENTARIO_ID = @P_INVENTARIO_ID AND PROCESADO <> 'S' AND CANT_AJU <> 0


	update inventario set aju_realizado = '1' , fecha_aju=getdate() where inventario_id = @P_INVENTARIO_ID
	


END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRAN 
    EXEC usp_RethrowError;
END CATCH

end
