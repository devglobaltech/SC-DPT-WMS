/****** OBJECT:  STOREDPROCEDURE [DBO].[LOCATOREGRESO]    SCRIPT DATE: 03/30/2015 10:42:47 ******/
IF  EXISTS (SELECT * FROM SYS.OBJECTS WHERE OBJECT_ID = OBJECT_ID(N'[DBO].[ABAST_LOCATOR]') AND TYPE IN (N'P', N'PC'))
DROP PROCEDURE [DBO].[ABAST_LOCATOR]
GO

CREATE        PROCEDURE [DBO].[ABAST_LOCATOR]
@CLIENTE_ID		VARCHAR(15)		OUTPUT,
@PRODUCTO_ID	VARCHAR(30)		OUTPUT,
@CANTIDAD		NUMERIC(20,5)	OUTPUT,
@ABAST_ID		BIGINT			OUTPUT
AS
BEGIN

	DECLARE @FECHA_VTO				AS DATETIME
	DECLARE @ORDENPICKING			AS NUMERIC(10,0)
	DECLARE @TIPO_POSICION			AS VARCHAR(10)
	DECLARE @CODIGO_POSICION		AS VARCHAR(100)
	DECLARE @AUX					AS VARCHAR(50)
	DECLARE @NEWPRODUCTO			AS VARCHAR(30)
	DECLARE @OLDPRODUCTO			AS VARCHAR(30)
	DECLARE @VQTYRESTO				AS NUMERIC(20,5)
	DECLARE @VRL_ID					AS NUMERIC(20)
	DECLARE @QTYSOL					AS NUMERIC(20,5)
	DECLARE @VNROLINEA				AS NUMERIC(20)
	DECLARE @NRO_BULTO				AS VARCHAR(50)
	DECLARE @NRO_LOTE				AS VARCHAR(50)
	DECLARE @EST_MERC_ID			AS VARCHAR(15)
	DECLARE @NRO_DESPACHO			AS VARCHAR(50)
	DECLARE @NRO_PARTIDA			AS VARCHAR(50)
	DECLARE @UNIDAD_ID				AS VARCHAR(5)
	DECLARE @PROP1					AS VARCHAR(100)
	DECLARE @PROP2					AS VARCHAR(100)
	DECLARE @PROP3					AS VARCHAR(100)
	DECLARE @DESC					AS VARCHAR(200)
	DECLARE @CAT_LOG_ID				AS VARCHAR(50)
	DECLARE @CONSUMO_ID				AS NUMERIC(20,0)
	DECLARE @DOCUMENTO_ID 			AS NUMERIC(20,0)
	DECLARE @SALDO					AS NUMERIC(20,5)
	DECLARE @TIPOSALDO				AS VARCHAR(20)
	DECLARE @DOC_TRANS 				AS NUMERIC(20)
	DECLARE @QTYDETDOCUMENTO		AS NUMERIC(20)
	DECLARE @VUSUARIO_ID			AS VARCHAR(50)
	DECLARE @VTERMINAL				AS VARCHAR(50)
	DECLARE @RSEXIST				AS CURSOR
	DECLARE @RSACTURL				AS CURSOR
	DECLARE @CRIT1					AS VARCHAR(30)
	DECLARE @CRIT2					AS VARCHAR(30)
	DECLARE @CRIT3					AS VARCHAR(30)
	DECLARE @FECHA_ALTA_GTW			AS DATETIME
	DECLARE @NRO_SERIE				AS VARCHAR(50)
	DECLARE @RSDOCEGR				AS CURSOR
	DECLARE @CONTROL				AS NUMERIC(20,0)
	DECLARE @ASIGNACION				AS NUMERIC(20,0)
	SET NOCOUNT ON;
	SET @VNROLINEA = 0
	
	SELECT	@CONTROL=COUNT(*)
	FROM	ABAST_CONSUMO_LOCATOR
	WHERE	ABAST_ID=@ABAST_ID
	
	IF @CONTROL>0
	BEGIN
		--RETORNO, BASICAMENTE PORQUE YA FUE ASIGNADO EL LOTE TRANSFERIBLE.
		RETURN;
	END
	
	--OBTENGO LOS CRITERIOS DE ORDENAMIENTO.
	SELECT	@CRIT1=CRITERIO_1, @CRIT2=CRITERIO_2, @CRIT3=CRITERIO_3
	FROM	RL_CLIENTE_LOCATOR
	WHERE	CLIENTE_ID=@CLIENTE_ID

	IF (@CRIT1 IS NULL) AND (@CRIT2 IS NULL) AND (@CRIT3 IS NULL)
	BEGIN
		--SI TODOS SON NULOS ENTONCES X DEFAULT SALGO CON ORDEN DE PICKING.
		SET @CRIT1='ORDEN_PICKING'
	END


	SET @QTYSOL=0
	SET @VQTYRESTO=@CANTIDAD

	SET @RSEXIST = CURSOR FOR
		SELECT	X.*
		FROM	(
			SELECT	 DD.FECHA_VENCIMIENTO
					,ISNULL(P.ORDEN_PICKING,9999) AS ORDEN_PICKING
					,'POS' AS UBICACION
					,P.POSICION_COD AS POSICION
					,DD.CLIENTE_ID
					,DD.PRODUCTO_ID AS PRODUCTO
					,RL.CANTIDAD
					,RL.RL_ID
					,DD.NRO_BULTO
					,DD.NRO_LOTE
					,RL.EST_MERC_ID
					,DD.NRO_DESPACHO
					,DD.NRO_PARTIDA
					,DD.UNIDAD_ID
					,DD.PROP1
					,DD.PROP2
					,DD.PROP3
					,DD.DESCRIPCION
					,RL.CAT_LOG_ID
					,D.FECHA_ALTA_GTW
					,DD.NRO_SERIE
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					INNER JOIN CATEGORIA_LOGICA CL ON (RL.CLIENTE_ID=CL.CLIENTE_ID AND RL.CAT_LOG_ID=CL.CAT_LOG_ID )
					INNER JOIN POSICION P ON (RL.POSICION_ACTUAL=P.POSICION_ID AND P.ABASTECIBLE='0' AND P.PICKING='0')
					LEFT JOIN ESTADO_MERCADERIA_RL EM ON (RL.CLIENTE_ID=EM.CLIENTE_ID AND RL.EST_MERC_ID=EM.EST_MERC_ID) 	
					INNER JOIN DOCUMENTO D ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			WHERE	RL.DOC_TRANS_ID_EGR IS NULL
					AND RL.NRO_LINEA_TRANS_EGR IS NULL
					AND RL.DISPONIBLE='1'
					AND ISNULL(EM.DISP_EGRESO,'1')='1'
					AND ISNULL(EM.PICKING,'1')='1'
					AND P.POS_LOCKEADA='0' 
					AND CL.DISP_EGRESO='1' 
					AND CL.PICKING='1'
					AND RL.CAT_LOG_ID<>'TRAN_EGR' --PARA ASEGURARME QUE NO ESTE EN PROCESO DE EGRESO
					AND D.CLIENTE_ID = @CLIENTE_ID
					AND DD.PRODUCTO_ID=@PRODUCTO_ID
					
			UNION
			SELECT	 DD.FECHA_VENCIMIENTO
					,ISNULL(N.ORDEN_LOCATOR,9999) AS ORDEN_PICKING
					,'NAV' AS UBICACION
					,N.NAVE_COD AS POSICION
					,DD.CLIENTE_ID
					,DD.PRODUCTO_ID AS PRODUCTO
					,RL.CANTIDAD
					,RL.RL_ID
					,DD.NRO_BULTO
					,DD.NRO_LOTE
					,RL.EST_MERC_ID
					,DD.NRO_DESPACHO
					,DD.NRO_PARTIDA
					,DD.UNIDAD_ID
					,DD.PROP1
					,DD.PROP2
					,DD.PROP3
					,DD.DESCRIPCION
					,RL.CAT_LOG_ID
					,D.FECHA_ALTA_GTW
					,DD.NRO_SERIE
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					INNER JOIN CATEGORIA_LOGICA CL ON (RL.CLIENTE_ID=CL.CLIENTE_ID AND RL.CAT_LOG_ID=CL.CAT_LOG_ID )
					INNER JOIN NAVE N ON (RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN ESTADO_MERCADERIA_RL EM ON (RL.CLIENTE_ID=EM.CLIENTE_ID AND RL.EST_MERC_ID=EM.EST_MERC_ID) 
					INNER JOIN DOCUMENTO D ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			WHERE	RL.DOC_TRANS_ID_EGR IS NULL
					AND RL.NRO_LINEA_TRANS_EGR IS NULL
					AND RL.DISPONIBLE='1'
					AND ISNULL(EM.DISP_EGRESO,'1')='1'
					AND ISNULL(EM.PICKING,'1')='1'
					AND RL.CAT_LOG_ID<>'TRAN_EGR'
					AND N.DISP_EGRESO='1' 
					AND N.PRE_EGRESO='0' 
					AND N.PRE_INGRESO='0' 
					AND N.PICKING='0'
					AND CL.DISP_EGRESO='1' 
					AND CL.PICKING='1'
					AND D.CLIENTE_ID = @CLIENTE_ID
					AND DD.PRODUCTO_ID= @PRODUCTO_ID
			)X		
			ORDER BY--ORDER BY PRODUCTO,DD.FECHA_VENCIMIENTO ASC,ORDEN 
					X.NRO_BULTO, 
					(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --ES NECESARIO PARA QUE QUEDE ORDENADO EL FOUND SET.
					(CASE WHEN @CRIT1 = 'FECHA_VENCIMIENTO'	THEN X.FECHA_VENCIMIENTO END),
					(CASE WHEN @CRIT1 = 'ORDEN_PICKING'		THEN X.ORDEN_PICKING END),
					(CASE WHEN @CRIT1 = 'NRO_BULTO'			THEN X.NRO_BULTO END),
					(CASE WHEN @CRIT1 = 'NRO_LOTE'			THEN X.NRO_LOTE END),
					(CASE WHEN @CRIT1 = 'EST_MERC_ID'		THEN X.EST_MERC_ID END),
					(CASE WHEN @CRIT1 = 'NRO_DESPACHO'		THEN X.NRO_DESPACHO END),
					(CASE WHEN @CRIT1 = 'NRO_PARTIDA'		THEN X.NRO_PARTIDA END),
					(CASE WHEN @CRIT1 = 'UNIDAD_ID'			THEN X.UNIDAD_ID END),
					(CASE WHEN @CRIT1 = 'PROP1'				THEN X.PROP1 END),
					(CASE WHEN @CRIT1 = 'PROP2'				THEN X.PROP2 END),
					(CASE WHEN @CRIT1 = 'PROP3'				THEN X.PROP3 END),
					(CASE WHEN @CRIT1 = 'CAT_LOG_ID'		THEN X.CAT_LOG_ID END),
					(CASE WHEN @CRIT1 = 'FECHA_ALTA_GTW'	THEN X.FECHA_ALTA_GTW END),
					 --2
					(CASE WHEN @CRIT2 = 'FECHA_VENCIMIENTO'	THEN X.FECHA_VENCIMIENTO END),
					(CASE WHEN @CRIT2 = 'ORDEN_PICKING'		THEN X.ORDEN_PICKING END),
					(CASE WHEN @CRIT2 = 'NRO_BULTO'			THEN X.NRO_BULTO END),
					(CASE WHEN @CRIT2 = 'NRO_LOTE'			THEN X.NRO_LOTE END),
					(CASE WHEN @CRIT2 = 'EST_MERC_ID'		THEN X.EST_MERC_ID END),
					(CASE WHEN @CRIT2 = 'NRO_DESPACHO'		THEN X.NRO_DESPACHO END),
					(CASE WHEN @CRIT2 = 'NRO_PARTIDA'		THEN X.NRO_PARTIDA END),
					(CASE WHEN @CRIT2 = 'UNIDAD_ID'			THEN X.UNIDAD_ID END),
					(CASE WHEN @CRIT2 = 'PROP1'				THEN X.PROP1 END),
					(CASE WHEN @CRIT2 = 'PROP2'				THEN X.PROP2 END),
					(CASE WHEN @CRIT2 = 'PROP3'				THEN X.PROP3 END),
					(CASE WHEN @CRIT2 = 'CAT_LOG_ID'		THEN X.CAT_LOG_ID END),
					(CASE WHEN @CRIT2 = 'FECHA_ALTA_GTW'	THEN X.FECHA_ALTA_GTW END),
					--3
					(CASE WHEN @CRIT3 = 'FECHA_VENCIMIENTO'	THEN X.FECHA_VENCIMIENTO END),
					(CASE WHEN @CRIT3 = 'ORDEN_PICKING'		THEN X.ORDEN_PICKING END),
					(CASE WHEN @CRIT3 = 'NRO_BULTO'			THEN X.NRO_BULTO END),
					(CASE WHEN @CRIT3 = 'NRO_LOTE'			THEN X.NRO_LOTE END),
					(CASE WHEN @CRIT3 = 'EST_MERC_ID'		THEN X.EST_MERC_ID END),
					(CASE WHEN @CRIT3 = 'NRO_DESPACHO'		THEN X.NRO_DESPACHO END),
					(CASE WHEN @CRIT3 = 'NRO_PARTIDA'		THEN X.NRO_PARTIDA END),
					(CASE WHEN @CRIT3 = 'UNIDAD_ID'			THEN X.UNIDAD_ID END),
					(CASE WHEN @CRIT3 = 'PROP1'				THEN X.PROP1 END),
					(CASE WHEN @CRIT3 = 'PROP2'				THEN X.PROP2 END),
					(CASE WHEN @CRIT3 = 'PROP3'				THEN X.PROP3 END),
					(CASE WHEN @CRIT3 = 'CAT_LOG_ID'		THEN X.CAT_LOG_ID END),
					(CASE WHEN @CRIT3 = 'FECHA_ALTA_GTW'	THEN X.FECHA_ALTA_GTW END)
			
	OPEN @RSEXIST
	FETCH NEXT FROM @RSEXIST INTO	@FECHA_VTO,
									@ORDENPICKING,
									@TIPO_POSICION,
									@CODIGO_POSICION,
									@CLIENTE_ID,
									@PRODUCTO_ID,
									@CANTIDAD,
									@VRL_ID,
									@NRO_BULTO,
									@NRO_LOTE,				
									@EST_MERC_ID,			
									@NRO_DESPACHO,		
									@NRO_PARTIDA,			
									@UNIDAD_ID,			
									@PROP1,					
									@PROP2,					
									@PROP3,
									@DESC,
									@CAT_LOG_ID,
									@FECHA_ALTA_GTW,
									@NRO_SERIE


	WHILE @@FETCH_STATUS=0 AND @VQTYRESTO>0
	BEGIN	

		IF (@VQTYRESTO>0) BEGIN   
				IF (@VQTYRESTO>=@CANTIDAD) BEGIN
					SET @VNROLINEA=@VNROLINEA+1
					SET @VQTYRESTO=@VQTYRESTO-@CANTIDAD
					
					INSERT INTO [DBO].[ABAST_CONSUMO_LOCATOR]	(ABAST_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD, RL_ID, SALDO, TIPO, FECHA, PROCESADO) 
														VALUES  (@ABAST_ID,@VNROLINEA,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD,@VRL_ID,@CANTIDAD-@CANTIDAD,'1',GETDATE(),'N')

				END
				ELSE BEGIN
					SET @VNROLINEA=@VNROLINEA+1
					INSERT INTO [DBO].[ABAST_CONSUMO_LOCATOR]	(ABAST_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD, RL_ID, SALDO, TIPO, FECHA, PROCESADO) 
														VALUES	(@ABAST_ID,@VNROLINEA,@CLIENTE_ID,@PRODUCTO_ID,@VQTYRESTO,@VRL_ID,@CANTIDAD-@VQTYRESTO,'2',GETDATE(),'N')

					SET @VQTYRESTO=0
				END --IF
		END --IF
		FETCH NEXT FROM @RSEXIST INTO	@FECHA_VTO,
										@ORDENPICKING,
										@TIPO_POSICION,
										@CODIGO_POSICION,
										@CLIENTE_ID,
										@PRODUCTO_ID,
										@CANTIDAD,
										@VRL_ID,
										@NRO_BULTO,
										@NRO_LOTE,				
										@EST_MERC_ID,			
										@NRO_DESPACHO,		
										@NRO_PARTIDA,			
										@UNIDAD_ID,			
										@PROP1,					
										@PROP2,					
										@PROP3,
										@DESC,
										@CAT_LOG_ID,
										@FECHA_ALTA_GTW,
										@NRO_SERIE
	END	--END WHILE @RSEXIST.

	CLOSE @RSEXIST
	DEALLOCATE @RSEXIST

	SET @ASIGNACION=0

	--HAGO LA RESERVA EN RL
	SET @RSACTURL = CURSOR FOR 
		SELECT	CONSUMO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD, RL_ID, SALDO, TIPO
		FROM	[DBO].[ABAST_CONSUMO_LOCATOR] 
		WHERE	PROCESADO='N'
				AND ABAST_ID=@ABAST_ID
				
	OPEN @RSACTURL
	FETCH NEXT FROM @RSACTURL INTO	@CONSUMO_ID, @VNROLINEA, @CLIENTE_ID, @PRODUCTO_ID, @CANTIDAD, @VRL_ID, @SALDO,@TIPOSALDO

	WHILE @@FETCH_STATUS=0
	BEGIN
		SET @ASIGNACION=1;
		
		IF (@SALDO=0) BEGIN
			UPDATE	RL_DET_DOC_TRANS_POSICION 
			SET		DISPONIBLE='0'
			WHERE	RL_ID=@VRL_ID
			
			UPDATE [DBO].[ABAST_CONSUMO_LOCATOR] SET PROCESADO='S' WHERE CONSUMO_ID=@CONSUMO_ID
		END --IF	

		IF (@SALDO>0) BEGIN
			-----------------------------------------------------------------------------------------------------------------------------------
			--SPLIT EXISTENCIA.
			-----------------------------------------------------------------------------------------------------------------------------------
			INSERT INTO RL_DET_DOC_TRANS_POSICION (	DOC_TRANS_ID,NRO_LINEA_TRANS,POSICION_ANTERIOR,POSICION_ACTUAL,CANTIDAD,TIPO_MOVIMIENTO_ID,
													ULTIMA_ESTACION,ULTIMA_SECUENCIA,NAVE_ANTERIOR,NAVE_ACTUAL,DOCUMENTO_ID,NRO_LINEA,
													DISPONIBLE,DOC_TRANS_ID_EGR,NRO_LINEA_TRANS_EGR,DOC_TRANS_ID_TR,NRO_LINEA_TRANS_TR,
													CLIENTE_ID,CAT_LOG_ID,CAT_LOG_ID_FINAL,EST_MERC_ID)
			SELECT	DOC_TRANS_ID,NRO_LINEA_TRANS,POSICION_ANTERIOR,POSICION_ACTUAL,@SALDO,TIPO_MOVIMIENTO_ID,
					ULTIMA_ESTACION,ULTIMA_SECUENCIA,NAVE_ANTERIOR,NAVE_ACTUAL,DOCUMENTO_ID,NRO_LINEA,
					DISPONIBLE,DOC_TRANS_ID_EGR,NRO_LINEA_TRANS_EGR,DOC_TRANS_ID_TR,NRO_LINEA_TRANS_TR,
					CLIENTE_ID,CAT_LOG_ID,CAT_LOG_ID_FINAL,EST_MERC_ID
			FROM		RL_DET_DOC_TRANS_POSICION 
			WHERE		RL_ID=@VRL_ID 	
			-----------------------------------------------------------------------------------------------------------------------------------
						  
			UPDATE	RL_DET_DOC_TRANS_POSICION 
			SET		DISPONIBLE='0',
					CANTIDAD=@CANTIDAD
			WHERE	RL_ID=@VRL_ID
															
			UPDATE [DBO].[ABAST_CONSUMO_LOCATOR] SET PROCESADO='S' WHERE CONSUMO_ID=@CONSUMO_ID
		END --IF	

		FETCH NEXT FROM @RSACTURL INTO @CONSUMO_ID, @VNROLINEA, @CLIENTE_ID, @PRODUCTO_ID, @CANTIDAD, @VRL_ID, @SALDO,@TIPOSALDO
	END	--END WHILE @RSACTURL.
	CLOSE @RSACTURL
	DEALLOCATE @RSACTURL

	IF @ASIGNACION=0 BEGIN
		RAISERROR('NO SE ENCONTRO MATERIAL PARA REALIZAR LA TAREA DE ABASTECIMIENTO. ',16,1);
		RETURN
	END
SET NOCOUNT OFF;
END -- FIN PROCEDURE.

GO


