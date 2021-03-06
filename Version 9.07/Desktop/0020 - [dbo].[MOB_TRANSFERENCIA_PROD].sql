ALTER PROCEDURE [dbo].[MOB_TRANSFERENCIA_PROD]
	@CLIENTE_ID AS VARCHAR(50),
	@POSICION_O	AS VARCHAR(45),
	@POSICION_D AS VARCHAR(45),
	@Producto_id as varchar(30),
	@USUARIO 	AS VARCHAR(30),
	@Cantidad	numeric(20,5),
	@CAT_LOG_ID VARCHAR(100)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @ICANTIDAD		AS NUMERIC(20,5)
	DECLARE @DIFERENCIA		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(20,0)	
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)	
	DECLARE @vNEW_RLID		AS NUMERIC(20,0)
	declare @msg			as varchar(max)
	DECLARE @CANT_ORIG		AS numeric(20,5)	


	begin try
		SET @CANT_ORIG = @Cantidad

		EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
		IF @OUT='1'
		BEGIN
			RETURN
		END

		DECLARE CUR_RL_TR CURSOR FOR
		SELECT	rl.rl_id
		FROM	rl_det_doc_trans_posicion rl
				inner join	det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join	det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				left join	posicion p on (rl.posicion_actual=p.posicion_id)
				left join	nave n on (rl.nave_actual=n.nave_id)
				inner join	categoria_logica cl on (rl.cat_log_id=cl.cat_log_id and cl.disp_transf='1' and cl.cliente_id =dd.cliente_id)
				left join	estado_mercaderia_rl em on (rl.est_merc_id=em.est_merc_id and em.cliente_id=dd.cliente_id and (em.disp_transf='1' or em.disp_transf is null))		
		WHERE	dd.producto_id=@Producto_id
				and (n.nave_cod=@POSICION_O or p.posicion_cod=@POSICION_O) and rl.cantidad > 0
				and rl.disponible='1'	
				AND RL.CAT_LOG_ID = @CAT_LOG_ID
				AND RL.CLIENTE_ID = @CLIENTE_ID
				and rl.doc_trans_id_egr is null and rl.nro_linea_trans_egr is null
				and rl.doc_trans_id_tr is null and rl.nro_linea_trans_tr is null
				and rl.documento_id is null and rl.nro_linea is null

			SELECT	@EXISTE = COUNT(*)
			FROM	TRANSACCION T
					INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
					INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
					AND RTC.CLIENTE_ID IN (	SELECT	CLIENTE_ID 
											FROM	CLIENTE
											WHERE	(	SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
														FROM   RL_SYS_CLIENTE_USUARIO
														WHERE  CLIENTE_ID = RTC.CLIENTE_ID
																AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)
			WHERE	T.TIPO_OPERACION_ID='TR' 
					AND RTE.ORDEN=1
			
		
			IF @EXISTE = 0
			BEGIN
				RAISERROR('El usuario %s no posee clientes asignados',16,1,@USUARIO)
				return
			END

			--GENERO EL DOCUMENTO DE TRANSACCION .--
			EXEC CREAR_DOC_TRANSFERENCIA @USUARIO=@USUARIO

			--OBTENGO EL DOC_TRANS_ID INSERTADO.--
			SET @VDOCNEW=@@IDENTITY

			UPDATE DOCUMENTO_TRANSACCION SET TR_POS_COMPLETA= '0' WHERE DOC_TRANS_ID=@VDOCNEW

			--ABRO EL CURSOR PARA SU POSTERIOR USO	
			OPEN CUR_RL_TR

			SET @CONT_LINEA= 0
			SET @LIM_CONT=0
			SET @ICANTIDAD=@CANTIDAD

			FETCH NEXT FROM CUR_RL_TR INTO @VRLID--,@CLIENTE_ID
			WHILE (@@FETCH_STATUS=0)
			BEGIN
					SET @CONT_LINEA= @CONT_LINEA + 1 
					INSERT INTO DET_DOCUMENTO_TRANSACCION (
							DOC_TRANS_ID,     NRO_LINEA_TRANS,
							DOCUMENTO_ID,     NRO_LINEA_DOC,
							MOTIVO_ID,        EST_MERC_ID,
							CLIENTE_ID,       CAT_LOG_ID,
							ITEM_OK,          MOVIMIENTO_PENDIENTE,
							DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF)
					VALUES (
							@VDOCNEW
							,@CONT_LINEA --NRO DE LINEA DE DET_DOCUMENTO_TRANSACCION
							,NULL   ,NULL   ,NULL     ,NULL
							,@CLIENTE_ID
							,NULL ,'0' ,'0' ,NULL     ,NULL)

			
					SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@POSICION_D) AS INT)
					SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@POSICION_D) AS INT)

					
					select @VCANTIDAD=cantidad from RL_DET_DOC_TRANS_POSICION where RL_ID = @vRLID
					
					
					if @cantidad >0 
					begin
						IF @CANTIDAD = @VCANTIDAD
							BEGIN
								INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @VCANTIDAD--, CANTIDAD
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 
						             					             
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @CANTIDAD
						             
									 DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
						             
									 SET @CANTIDAD=0
						             
							END
						ELSE
							IF @CANTIDAD < @VCANTIDAD --CANTIDAD A TRANSFERIR ES MENOR A CANT RL
								BEGIN						
									
									SET @DIFERENCIA=@VCANTIDAD - @CANTIDAD
									
									INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @CANTIDAD  -- CANTIDAD TRANSFERIDA
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 
									 
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @CANTIDAD
						             
									 UPDATE RL_DET_DOC_TRANS_POSICION SET cantidad=@DIFERENCIA --CANTIDAD REMANENTE EN LA RL
									 WHERE RL_ID = @vRLID
									 
									 
									 
									 
									 
									 SET @CANTIDAD=0
								END
							ELSE
								BEGIN		--CANTIDAD CANTIDAD A TRANSFERIR MAYOR A LA RL
									SET @DIFERENCIA=@CANTIDAD - @VCANTIDAD	
									set @CANTIDAD =@CANTIDAD - @VCANTIDAD	--@CANTIDAD AHORA ES EL RESTO A TRANSFERIR
									INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @VCANTIDAD--, CANTIDAD RL
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 								 
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @VCANTIDAD
						             
						             
									 DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			
								END
					END --if @cantidad >0
					
				FETCH NEXT FROM CUR_RL_TR INTO @VRLID --,@CLIENTE_ID
				
			END
			
			CLOSE CUR_RL_TR
			DEALLOCATE CUR_RL_TR

			
			IF @CANTIDAD > 0 
				begin
					set @msg = 'Solo se pueden transferir ' + cast((@CANT_ORIG - @cantidad) as varchar) + ' .Por favor cancele la operación y comience de nuevo.'
					RAISERROR(@msg,16,1)		
				end


			INSERT INTO IMPRESION_RODC VALUES(@VDOCNEW,0,'D',0,'')
			
			UPDATE POSICION SET POS_VACIA='0' 
						WHERE POSICION_ID IN (SELECT DISTINCT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)

			UPDATE POSICION SET POS_VACIA='1' 
						WHERE POSICION_ID  NOT IN (SELECT DISTINCT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)		

			-- DEVOLUCION
			EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW

			--FINALIZA LA TRANSFERENCIA	
			EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
	end try
	begin catch
		exec usp_RethrowError
	end catch
		
END
