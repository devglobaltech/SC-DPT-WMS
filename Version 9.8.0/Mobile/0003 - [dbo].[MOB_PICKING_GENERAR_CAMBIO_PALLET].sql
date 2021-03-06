
ALTER PROCEDURE  [dbo].[MOB_PICKING_GENERAR_CAMBIO_PALLET]
  @NEWRL_ID			NUMERIC(20,0) OUTPUT,
  @PICKING_ID		NUMERIC(20,0) OUTPUT
AS
BEGIN
	SET XACT_ABORT ON
	-----------------------------------------------------------------------------
	--DECLARACION DE VARIABLES.
	-----------------------------------------------------------------------------
	DECLARE @OLDRL_ID			    AS NUMERIC(20,0)
	DECLARE @QTYPICKING			    AS FLOAT
	DECLARE @QTYRL				    AS FLOAT
	DECLARE @DOCUMENTO_ID		    AS NUMERIC(20,0)
	DECLARE @NRO_LINEA			    AS NUMERIC(10,0)
	DECLARE @PREEGRID			    AS NUMERIC(20,0)
	DECLARE @DOC_TRANS_IDEGR		AS NUMERIC(20,0)
	DECLARE @NRO_LINEA_TRANSEGR		AS NUMERIC(10,0)
	DECLARE @DOCUMENTO_IDNEW		AS NUMERIC(20,0)
	DECLARE @NRO_LINEANEW		    AS NUMERIC(10,0)
	DECLARE @DIF				    AS FLOAT
	DECLARE @MAXLINEA			    AS NUMERIC(10,0)
	DECLARE @DOC_TRANS_ID		    AS NUMERIC(20,0)
	DECLARE @MAXLINEADDT		    AS NUMERIC(10,0)
	DECLARE @SPLITRL			    AS NUMERIC(20,0)
	DECLARE @PRODUCTO_IDC		    AS VARCHAR(30)
	DECLARE @CLIENTE_IDC		    AS VARCHAR(15)
	DECLARE @CAT_LOG_ID_FINAL		AS VARCHAR(50)
	-----------------------------------------------------------------------------
	DECLARE @NRO_SERIE			    AS VARCHAR(50)
	DECLARE @NRO_SERIE_PADRE		AS VARCHAR(50)
	DECLARE @EST_MERC_ID		    AS VARCHAR(15)
	DECLARE @CAT_LOG_ID			    AS VARCHAR(15)
	DECLARE @NRO_BULTO			    AS VARCHAR(50)
	DECLARE @DESCRIPCION		    AS VARCHAR(200)
	DECLARE @NRO_LOTE			    AS VARCHAR(50)
	DECLARE @FECHA_VENCIMIENTO		AS DATETIME
	DECLARE @NRO_DESPACHO		    AS VARCHAR(50)
	DECLARE @NRO_PARTIDA		    AS VARCHAR(50)
	DECLARE @UNIDAD_ID			    AS VARCHAR(5)
	DECLARE @PESO				    AS NUMERIC(20,5)
	DECLARE @UNIDAD_PESO		    AS VARCHAR(5)
	DECLARE @VOLUMEN			    AS NUMERIC(20,5)
	DECLARE @UNIDAD_VOLUMEN			AS VARCHAR(5)
	DECLARE @BUSC_INDIVIDUAL		AS VARCHAR(1)
	DECLARE @TIE_IN				    AS VARCHAR(1)
	DECLARE @NRO_TIE_IN			    AS VARCHAR(100)
	DECLARE @ITEM_OK			    AS VARCHAR(1)
	DECLARE @MONEDA_ID			    AS VARCHAR(20)
	DECLARE @COSTO				    AS NUMERIC(20,3)
	DECLARE @PROP1				    AS VARCHAR(100)
	DECLARE @PROP2				    AS VARCHAR(100)
	DECLARE @PROP3				    AS VARCHAR(100)
	DECLARE @LARGO				    AS NUMERIC(10,3)
	DECLARE @ALTO				    AS NUMERIC(10,3)
	DECLARE @ANCHO				    AS NUMERIC(10,3)
	DECLARE @VOLUMEN_UNITARIO		AS VARCHAR(1)
	DECLARE @PESO_UNITARIO			AS VARCHAR(1)
	DECLARE @CANT_SOLICITADA		AS NUMERIC(20,5)	
	-----------------------------------------------------------------------------
	DECLARE @PALLET_HOMBRE			AS CHAR(1)
	DECLARE @TRANSF				    AS CHAR(1)
	DECLARE @RUTA					AS VARCHAR(100)
	
	DECLARE @QTYPAUX			    AS FLOAT
	DECLARE @QTYPAUX2			    AS FLOAT
	DECLARE @DIF2				    AS FLOAT
	DECLARE @QTYRL2				    AS FLOAT
	DECLARE @NEWRL					NUMERIC(20,0)
	DECLARE @CANTRL					NUMERIC(20,0)
	DECLARE @POSPICK				NUMERIC(20,0)
	DECLARE @NAVEPICK				NUMERIC(20,0)
	DECLARE @POSPICKV				VARCHAR(45)
	DECLARE @NAVEPICKV				VARCHAR(15)
	DECLARE @TIENE_LAYOUT			AS CHAR(1)

	--OBTENGO LAS CANTIDADES.
	SELECT @QTYPICKING=CANTIDAD FROM PICKING WHERE PICKING_ID=@PICKING_ID
	SELECT @QTYRL=CANTIDAD FROM RL_DET_DOC_TRANS_POSICION WHERE RL_ID=@NEWRL_ID
	
	SELECT @POSPICKV = POSICION_COD FROM PICKING WHERE PICKING_ID=@PICKING_ID
	SELECT @NAVEPICKV = NAVE_COD FROM PICKING WHERE PICKING_ID=@PICKING_ID
	
	SELECT @TIENE_LAYOUT = NAVE_TIENE_LAYOUT FROM NAVE WHERE NAVE_COD = @NAVEPICKV
	
	IF (@TIENE_LAYOUT = 1)
	BEGIN
		SELECT @POSPICK = POSICION_ID FROM POSICION WHERE POSICION_COD = @POSPICKV
		SET @NAVEPICK = NULL
	END ELSE
	BEGIN
		SELECT @NAVEPICK = NAVE_ID FROM NAVE WHERE NAVE_COD = @NAVEPICKV
		SET @POSPICK = NULL
	END
	
	/*	
	--VERIFICO QUE AL MOMENTO DE HACER EL CAMBIO NO ESTE TOMADA LA TAREA DE PICKING
	IF DBO.PICKING_INPROCESS(@PICKING_ID)=1
	BEGIN
		RAISERROR('LA TAREA DE PICKING YA FUE ASIGNADA. NO ES POSIBLE REALIZAR EL CAMBIO.',16,1);
		RETURN
	END
	*/

	--ESTOS VALORES ME VAN A SERVIR MAS ADELANTE.
	SELECT	 @DOCUMENTO_ID	=DOCUMENTO_ID
			 ,@NRO_LINEA 	=NRO_LINEA
	FROM	 PICKING
	WHERE	 PICKING_ID	=@PICKING_ID

	SELECT	@PALLET_HOMBRE=FLG_PALLET_HOMBRE
	FROM	CLIENTE_PARAMETROS C INNER JOIN DOCUMENTO D
			ON(C.CLIENTE_ID=D.CLIENTE_ID)
	WHERE	D.DOCUMENTO_ID=@DOCUMENTO_ID

	--SACO LA NAVE DE PREEGRESO.
	SELECT	@PREEGRID=NAVE_ID
	FROM	NAVE
	WHERE	PRE_EGRESO='1'

	--OBTENGO EL NUEVO DOCUMENTO Y NUMERO DE LINEA PARA UPDETEAR.
	SELECT 	DISTINCT
			@DOCUMENTO_IDNEW	=DD.DOCUMENTO_ID
			,@NRO_LINEANEW		=DD.NRO_LINEA
	FROM	RL_DET_DOC_TRANS_POSICION RL
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
	WHERE	RL.RL_ID=@NEWRL_ID
	
	IF (@QTYPICKING = @QTYRL)
	BEGIN
			--OBTENGO LA RL ANTERIOR.
			SELECT 	@OLDRL_ID=RL.RL_ID
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS_EGR=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			WHERE   DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
			
			SELECT	@CAT_LOG_ID_FINAL=CAT_LOG_ID_FINAL FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
			
			--OBTENGO EL DOCUMENTO DE TRANSACCION Y EL NUMERO DE LINEA PARA CONSUMIR LA NUEVA RL.
			SELECT	@DOC_TRANS_IDEGR	=DOC_TRANS_ID_EGR,
					@NRO_LINEA_TRANSEGR=NRO_LINEA_TRANS_EGR
			FROm	RL_DET_DOC_TRANS_POSICION
			WHERE	RL_ID=@OLDRL_ID

			--RESTAURO LA RL ANTERIOR
			DELETE FROM CONSUMO_LOCATOR_EGR WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

			UPDATE 	 RL_DET_DOC_TRANS_POSICION 
			SET 	 DISPONIBLE				='1'
					,DOC_TRANS_ID_EGR		=NULL
					,NRO_LINEA_TRANS_EGR	=NULL
					,POSICION_ACTUAL		=POSICION_ANTERIOR
					,POSICION_ANTERIOR		=NULL
					,NAVE_ACTUAL			=NAVE_ANTERIOR
					,NAVE_ANTERIOR			=1
					,CAT_LOG_ID				=@CAT_LOG_ID_FINAL
			WHERE	RL_ID=@OLDRL_ID
			
			--CONSUMO LA NUEVA RL
			SELECT	@CLIENTE_IDC= CLIENTE_ID,
					@PRODUCTO_IDC= PRODUCTO_ID
			FROM	DET_DOCUMENTO 
			WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
					AND NRO_LINEA=@NRO_LINEA

			INSERT INTO CONSUMO_LOCATOR_EGR (DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD, RL_ID,SALDO, TIPO, FECHA, PROCESADO)
			VALUES(@DOCUMENTO_ID, @NRO_LINEA, @CLIENTE_IDC, @PRODUCTO_IDC, @QTYPICKING,@NEWRL_ID,0,2,GETDATE(),'S')

			UPDATE	 RL_DET_DOC_TRANS_POSICION 
			SET 	 DISPONIBLE='0'
					,POSICION_ANTERIOR  =POSICION_ACTUAL
					,POSICION_ACTUAL    =NULL
					,NAVE_ANTERIOR      =NAVE_ACTUAL
					,NAVE_ACTUAL        =@PREEGRID
					,DOC_TRANS_ID_EGR   =@DOC_TRANS_IDEGR
					,NRO_LINEA_TRANS_EGR=@NRO_LINEA_TRANSEGR
					,CAT_LOG_ID='TRAN_EGR'
			WHERE	RL_ID=@NEWRL_ID

			--SACO LOS VALORES DE LA NUEVA LINEA DE DET_DOCUMENTO
			SELECT	  @NRO_SERIE			=NRO_SERIE
					, @NRO_SERIE_PADRE		=NRO_SERIE_PADRE
					, @EST_MERC_ID			=EST_MERC_ID
					, @CAT_LOG_ID			=CAT_LOG_ID
					, @NRO_BULTO			=NRO_BULTO
					, @DESCRIPCION			=DESCRIPCION
					, @NRO_LOTE				=NRO_LOTE
					, @FECHA_VENCIMIENTO	=FECHA_VENCIMIENTO
					, @NRO_DESPACHO			=NRO_DESPACHO
					, @NRO_PARTIDA			=NRO_PARTIDA
					, @UNIDAD_ID			=UNIDAD_ID
					, @PESO					=PESO
					, @UNIDAD_PESO			=UNIDAD_PESO
					, @VOLUMEN				=VOLUMEN
					, @UNIDAD_VOLUMEN		=UNIDAD_VOLUMEN
					, @BUSC_INDIVIDUAL		=BUSC_INDIVIDUAL
					, @TIE_IN				=TIE_IN
					, @NRO_TIE_IN			=NRO_TIE_IN
					, @ITEM_OK				=ITEM_OK
					, @MONEDA_ID			=MONEDA_ID
					, @COSTO				=COSTO
					, @PROP1				=PROP1
					, @PROP2				=PROP2
					, @PROP3				=PROP3
					, @LARGO				=LARGO
					, @ALTO					=ALTO
					, @ANCHO				=ANCHO
					, @VOLUMEN_UNITARIO		=VOLUMEN_UNITARIO
					, @PESO_UNITARIO		=PESO_UNITARIO
					, @CANT_SOLICITADA		=CANT_SOLICITADA
			FROM	DET_DOCUMENTO				
			WHERE	DOCUMENTO_ID=@DOCUMENTO_IDNEW
					AND NRO_LINEA=@NRO_LINEANEW

			--ACTUALIZO DET_DOCUMENTO
			UPDATE  DET_DOCUMENTO
			SET       NRO_SERIE			    =@NRO_SERIE				
					, NRO_SERIE_PADRE		=@NRO_SERIE_PADRE		
					, EST_MERC_ID		    =@EST_MERC_ID			
					, CAT_LOG_ID		    ='TRAN_EGR'				
					, NRO_BULTO			    =@NRO_BULTO				
					, DESCRIPCION		    =@DESCRIPCION			
					, NRO_LOTE			    =@NRO_LOTE				
					, FECHA_VENCIMIENTO		=@FECHA_VENCIMIENTO		
					, NRO_DESPACHO			=@NRO_DESPACHO			
					, NRO_PARTIDA		    =@NRO_PARTIDA			
					, UNIDAD_ID			    =@UNIDAD_ID				
					, PESO				    =@PESO					
					, UNIDAD_PESO		    =@UNIDAD_PESO			
					, VOLUMEN			    =@VOLUMEN				
					, UNIDAD_VOLUMEN		=@UNIDAD_VOLUMEN			
					, BUSC_INDIVIDUAL		=@BUSC_INDIVIDUAL		
					, TIE_IN			    =@TIE_IN					
					, NRO_TIE_IN		    =@NRO_TIE_IN				
					, ITEM_OK			    =@ITEM_OK				
					, MONEDA_ID			    =@MONEDA_ID				
					, COSTO				    =@COSTO					
					, PROP1				    =@PROP1					
					, PROP2				    =@PROP2					
					, PROP3				    =@PROP3					
					, LARGO				    =@LARGO					
					, ALTO				    =@ALTO					
					, ANCHO			  	    =@ANCHO					
					, VOLUMEN_UNITARIO		=@VOLUMEN_UNITARIO		
					, PESO_UNITARIO			=@PESO_UNITARIO		
					, CANT_SOLICITADA		=ISNULL(@CANT_SOLICITADA,CANTIDAD)
			WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
					AND NRO_LINEA=@NRO_LINEA
					
			--ELIMINO LA LINEA DE PICKING
			SELECT @RUTA=RUTA FROM PICKING WHERE  DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA;
			
			DELETE FROM PICKING WHERE PICKING_ID=@PICKING_ID

			--INSERTO LA NUEVA LINEA DE PICKING.
			INSERT INTO PICKING 
			SELECT 	DISTINCT
					 DD.DOCUMENTO_ID
					,DD.NRO_LINEA
					,DD.CLIENTE_ID
					,DD.PRODUCTO_ID 
					,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
					,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
					,P.DESCRIPCION
					,DD.CANTIDAD
					,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
					,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
					,@RUTA
					,DD.PROP1
					,NULL AS FECHA_INICIO
					,NULL AS FECHA_FIN
					,NULL AS USUARIO
					,NULL AS CANT_CONFIRMADA
					,NULL AS PALLET_PICKING
					,0 	  AS SALTO_PICKING
					,'0'  AS PALLET_CONTROLADO
					,NULL AS USUARIO_CONTROL_PICKING
					,'0'  AS ST_ETIQUETAS
					,'0'  AS ST_CAMION
					,'0'  AS FACTURADO
					,'0'  AS FIN_PICKING
					,'0'  AS ST_CONTROL_EXP
					,NULL AS FECHA_CONTROL_PALLET
					,NULL AS TERMINAL_CONTROL_PALLET
					,NULL AS FECHA_CONTROL_EXP
					,NULL AS USUARIO_CONTROL_EXP
					,NULL AS TERMINAL_CONTROL_EXPEDICION
					,NULL AS FECHA_CONTROL_FAC
					,NULL AS USUARIO_CONTROL_FAC
					,NULL AS TERMINAL_CONTROL_FAC
					,NULL AS VEHICULO_ID
					,NULL AS PALLET_COMPLETO
					,NULL AS HIJO
					,NULL AS QTY_CONTROLADO
					,NULL AS PALLET_FINAL
					,NULL AS PALLET_CERRADO
					,NULL AS USUARIO_PF
					,NULL AS TERMINAL_PF
					,'0'  AS REMITO_IMPRESO
					,NULL AS NRO_REMITO_PF
					,NULL AS PICKING_ID_REF
					,NULL AS BULTOS_CONTROLADOS
					,NULL AS BULTOS_NO_CONTROLADOS
					,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
					,0	  AS TRANSF_TERMINANDA
					,DD.NRO_LOTE    AS NRO_LOTE
					,DD.NRO_PARTIDA AS NRO_PARTIDA
					,DD.NRO_SERIE	AS NRO_SERIE
					,NULL			AS ESTADO
					,NULL			AS NRO_UCDESCONSOLIDACION
					,NULL			AS FECHA_DESCONSOLIDACION
					,NULL			AS USUARIO_DESCONSOLIDACION
					,NULL			AS TERMINAL_DESCONSOLIDACION
					,NULL			AS NRO_UCEMPAQUETADO
					,NULL			AS UCEMPAQUETADO_MEDIDAS
					,NULL			AS FECHA_UCEMPAQUETADO
					,NULL			AS UCEMPAQUETADO_PESO
			FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD   ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
					INNER JOIN PRODUCTO P                     ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT  ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL   ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
					LEFT JOIN NAVE N                          ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
					LEFT JOIN POSICION POS                    ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
					LEFT JOIN NAVE N2           ON(POS.NAVE_ID=N2.NAVE_ID)
			WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
					AND DD.NRO_LINEA=@NRO_LINEA
					
			UPDATE PICKING SET TIPO_CAJA=0 WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA AND LTRIM(RTRIM(TIPO_CAJA))='';

	
	END--FIN PICKING=RL 1ER. CASO

	IF (@QTYPICKING < @QTYRL)
	BEGIN	
		SET @DIF= @QTYRL - @QTYPICKING

		--OBTENGO LA RL ANTERIOR QUE PUEDE O NO TENER PARTE CONFIRMADA EN PICKING
		--PRIMERO VERIFICO SI HAY MAS DE UNA
		
		SELECT 	@CANTRL=COUNT(*)
		FROM	RL_DET_DOC_TRANS_POSICION RL
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS_EGR=DDT.NRO_LINEA_TRANS)
				INNER JOIN DET_DOCUMENTO DD
				ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
		
		IF (@CANTRL > 1) BEGIN

		/*SELECT 	@OLDRL_ID=RL.RL_ID, @QTYRL2=RL.CANTIDAD
		FROM	RL_DET_DOC_TRANS_POSICION RL
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS_EGR=DDT.NRO_LINEA_TRANS)
				INNER JOIN DET_DOCUMENTO DD
				ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA 
				AND (RL.NAVE_ANTERIOR = @NAVEPICK) OR (RL.POSICION_ANTERIOR = @POSPICK)
		*/
			RAISERROR('NO ES POSIBLE REALIZAR MÁS DE UN CAMBIO DE UBICACIÓN',16,1)
			RETURN		
		END ELSE
		BEGIN
		
		SELECT 	@OLDRL_ID=RL.RL_ID, @QTYRL2=RL.CANTIDAD
		FROM	RL_DET_DOC_TRANS_POSICION RL
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS_EGR=DDT.NRO_LINEA_TRANS)
				INNER JOIN DET_DOCUMENTO DD
				ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
		
		END
		
		SELECT @CAT_LOG_ID_FINAL=CAT_LOG_ID_FINAL FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
		
		--OBTENGO EL DOCUMENTO DE TRANSACCION Y EL NUMERO DE LINEA PARA CONSUMIR LA NUEVA RL.
		SELECT	 @DOC_TRANS_IDEGR	=DOC_TRANS_ID_EGR
				,@NRO_LINEA_TRANSEGR=NRO_LINEA_TRANS_EGR
		FROM	RL_DET_DOC_TRANS_POSICION
		WHERE	RL_ID=@OLDRL_ID
				
		--OBTENGO LA CANTIDAD CONFIRMADA EN PICKING VS LA CANTIDAD QUE QUIERO CAMBIAR DE LUGAR
		
		SELECT 	@QTYPAUX=SUM(ISNULL(P.CANT_CONFIRMADA,0))
		FROM	PICKING P
				INNER JOIN DET_DOCUMENTO DD ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND P.NRO_LINEA = DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
		
		SELECT 	@QTYPAUX2=SUM(ISNULL(P.CANTIDAD,0))
		FROM	PICKING P
				INNER JOIN DET_DOCUMENTO DD ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND P.NRO_LINEA = DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
		
		--SI SON DISTINTAS QUIERE DECIR QUE YA SE PICKEO PARTE, 
		--POR LO QUE CREO UNA NUEVA RL CON LA CANTIDAD PICKEADA Y LIBERO LA OTRA
		IF (@QTYPAUX <> @QTYPAUX2) AND (@QTYPAUX <> 0)
		BEGIN
		
			INSERT INTO RL_DET_DOC_TRANS_POSICION
			SELECT 	 DOC_TRANS_ID
					,NRO_LINEA_TRANS
					,POSICION_ANTERIOR
					,POSICION_ACTUAL
					,@QTYPAUX	--CANTIDAD
					,TIPO_MOVIMIENTO_ID
					,ULTIMA_ESTACION
					,ULTIMA_SECUENCIA
					,NAVE_ANTERIOR
					,NAVE_ACTUAL
					,DOCUMENTO_ID
					,NRO_LINEA
					,DISPONIBLE
					,DOC_TRANS_ID_EGR
					,NRO_LINEA_TRANS_EGR
					,DOC_TRANS_ID_TR
					,NRO_LINEA_TRANS_TR
					,CLIENTE_ID
					,CAT_LOG_ID
					,CAT_LOG_ID_FINAL
					,EST_MERC_ID
			FROM	RL_DET_DOC_TRANS_POSICION
			WHERE	RL_ID=@OLDRL_ID		
			
			SET @NEWRL= SCOPE_IDENTITY()
			
			--LIBERA.
			UPDATE 	 RL_DET_DOC_TRANS_POSICION 
			SET 	 DISPONIBLE				='1'
					,CANTIDAD				= @QTYPAUX2 - @QTYPAUX
					,DOC_TRANS_ID_EGR		=NULL
					,NRO_LINEA_TRANS_EGR	=NULL
					,POSICION_ACTUAL		=POSICION_ANTERIOR
					,POSICION_ANTERIOR		=NULL
					,NAVE_ACTUAL			=NAVE_ANTERIOR
					,NAVE_ANTERIOR			='1'
					,CAT_LOG_ID				=@CAT_LOG_ID_FINAL
			WHERE	RL_ID					=@OLDRL_ID			
		
		END
		
		--SPLITEO LA RL NUEVA
		INSERT INTO RL_DET_DOC_TRANS_POSICION
		SELECT 	 DOC_TRANS_ID
				,NRO_LINEA_TRANS
				,POSICION_ANTERIOR
				,POSICION_ACTUAL
				,@DIF	--CANTIDAD
				,TIPO_MOVIMIENTO_ID
				,ULTIMA_ESTACION
				,ULTIMA_SECUENCIA
				,NAVE_ANTERIOR
				,NAVE_ACTUAL
				,DOCUMENTO_ID
				,NRO_LINEA
				,DISPONIBLE
				,DOC_TRANS_ID_EGR
				,NRO_LINEA_TRANS_EGR
				,DOC_TRANS_ID_TR
				,NRO_LINEA_TRANS_TR
				,CLIENTE_ID
				,CAT_LOG_ID
				,CAT_LOG_ID_FINAL
				,EST_MERC_ID
		FROM	RL_DET_DOC_TRANS_POSICION
		WHERE	RL_ID=@NEWRL_ID

		--CONSUMO LA RL.
		UPDATE	RL_DET_DOC_TRANS_POSICION 
		SET 	 DISPONIBLE='0'
				,CANTIDAD=@QTYPICKING
				,POSICION_ANTERIOR=POSICION_ACTUAL
				,POSICION_ACTUAL=NULL
				,NAVE_ANTERIOR=NAVE_ACTUAL
				,NAVE_ACTUAL=@PREEGRID
				,DOC_TRANS_ID_EGR=@DOC_TRANS_IDEGR
				,NRO_LINEA_TRANS_EGR=@NRO_LINEA_TRANSEGR
				,CAT_LOG_ID='TRAN_EGR'
		WHERE	RL_ID=@NEWRL_ID

		--RESTAURO LA RL ANTERIOR.
		UPDATE 	 RL_DET_DOC_TRANS_POSICION 
		SET 	 DISPONIBLE				='1'
				,DOC_TRANS_ID_EGR		=NULL
				,NRO_LINEA_TRANS_EGR	=NULL
				,POSICION_ACTUAL		=POSICION_ANTERIOR
				,POSICION_ANTERIOR		=NULL
				,NAVE_ACTUAL			=NAVE_ANTERIOR
				,NAVE_ANTERIOR			='1'
				,CAT_LOG_ID				=@CAT_LOG_ID_FINAL
		WHERE	RL_ID					=@OLDRL_ID
		
		--SACO LOS VALORES DE LA NUEVA LINEA DE DET_DOCUMENTO.
		SELECT	  @NRO_SERIE				=NRO_SERIE
				, @NRO_SERIE_PADRE			=NRO_SERIE_PADRE
				, @EST_MERC_ID				=EST_MERC_ID
				, @CAT_LOG_ID				=CAT_LOG_ID
				, @NRO_BULTO				=NRO_BULTO
				, @DESCRIPCION				=DESCRIPCION
				, @NRO_LOTE					=NRO_LOTE
				, @FECHA_VENCIMIENTO		=FECHA_VENCIMIENTO
				, @NRO_DESPACHO				=NRO_DESPACHO
				, @NRO_PARTIDA				=NRO_PARTIDA
				, @UNIDAD_ID				=UNIDAD_ID
				, @PESO						=PESO
				, @UNIDAD_PESO				=UNIDAD_PESO
				, @VOLUMEN					=VOLUMEN
				, @UNIDAD_VOLUMEN			=UNIDAD_VOLUMEN
				, @BUSC_INDIVIDUAL			=BUSC_INDIVIDUAL
				, @TIE_IN					=TIE_IN
				, @NRO_TIE_IN				=NRO_TIE_IN
				, @ITEM_OK					=ITEM_OK
				--, @CAT_LOG_ID_FINAL			=CAT_LOG_ID_FINAL
				, @MONEDA_ID				=MONEDA_ID
				, @COSTO					=COSTO
				, @PROP1					=PROP1
				, @PROP2					=PROP2
				, @PROP3					=PROP3
				, @LARGO					=LARGO
				, @ALTO						=ALTO
				, @ANCHO					=ANCHO
				, @VOLUMEN_UNITARIO			=VOLUMEN_UNITARIO
				, @PESO_UNITARIO			=PESO_UNITARIO
				, @CANT_SOLICITADA			=CANT_SOLICITADA
		FROM 	DET_DOCUMENTO				
		WHERE	DOCUMENTO_ID=@DOCUMENTO_IDNEW
				AND NRO_LINEA=@NRO_LINEANEW

		--ACTUALIZO DET_DOCUMENTO
		UPDATE DET_DOCUMENTO
		SET
				  NRO_SERIE			=@NRO_SERIE				
				, NRO_SERIE_PADRE	=@NRO_SERIE_PADRE		
				, EST_MERC_ID		=@EST_MERC_ID			
				, CAT_LOG_ID		='TRAN_EGR'				
				, NRO_BULTO			=@NRO_BULTO				
				, DESCRIPCION		=@DESCRIPCION			
				, NRO_LOTE			=@NRO_LOTE				
				, FECHA_VENCIMIENTO	=@FECHA_VENCIMIENTO		
				, NRO_DESPACHO		=@NRO_DESPACHO			
				, NRO_PARTIDA		=@NRO_PARTIDA			
				, UNIDAD_ID			=@UNIDAD_ID				
				, PESO				=@PESO					
				, UNIDAD_PESO		=@UNIDAD_PESO			
				, VOLUMEN			=@VOLUMEN				
				, UNIDAD_VOLUMEN	=@UNIDAD_VOLUMEN			
				, BUSC_INDIVIDUAL	=@BUSC_INDIVIDUAL		
				, TIE_IN			=@TIE_IN					
				, NRO_TIE_IN		=@NRO_TIE_IN				
				, ITEM_OK			=@ITEM_OK				
				--, CAT_LOG_ID_FINAL	=@CAT_LOG_ID_FINAL		
				, MONEDA_ID			=@MONEDA_ID				
				, COSTO				=@COSTO					
				, PROP1				=@PROP1					
				, PROP2				=@PROP2					
				, PROP3				=@PROP3					
				, LARGO				=@LARGO					
				, ALTO				=@ALTO					
				, ANCHO				=@ANCHO					
				, VOLUMEN_UNITARIO	=@VOLUMEN_UNITARIO		
				, PESO_UNITARIO		=@PESO_UNITARIO		
				, CANT_SOLICITADA	=ISNULL(@CANT_SOLICITADA,CANTIDAD)
		WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
				AND NRO_LINEA=@NRO_LINEANEW--@NRO_LINEA

		--ELIMINO LA LINEA DE PICKING
		SELECT @RUTA=RUTA FROM PICKING WHERE  DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA;

		DELETE FROM PICKING WHERE PICKING_ID=@PICKING_ID

		--INSERTO LA NUEVA LINEA DE PICKING.
		INSERT INTO PICKING 
		SELECT 	 DISTINCT
				 DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,DD.CLIENTE_ID
				,DD.PRODUCTO_ID 
				,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
				,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
				,P.DESCRIPCION
				,DD.CANTIDAD - @QTYPAUX
				,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
				,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
				,@RUTA--ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
				,DD.PROP1
				,NULL			AS FECHA_INICIO
				,NULL			AS FECHA_FIN
				,NULL			AS USUARIO
				,NULL			AS CANT_CONFIRMADA
				,NULL			AS PALLET_PICKING
				,0 				AS SALTO_PICKING
				,'0'			AS PALLET_CONTROLADO
				,NULL			AS USUARIO_CONTROL_PICKING
				,'0'			AS ST_ETIQUETAS
				,'0'			AS ST_CAMION
				,'0'			AS FACTURADO
				,'0'			AS FIN_PICKING
				,'0'			AS ST_CONTROL_EXP
				,NULL			AS FECHA_CONTROL_PALLET
				,NULL			AS TERMINAL_CONTROL_PALLET
				,NULL			AS FECHA_CONTROL_EXP
				,NULL			AS USUARIO_CONTROL_EXP
				,NULL			AS TERMINAL_CONTROL_EXPEDICION
				,NULL			AS FECHA_CONTROL_FAC
				,NULL			AS USUARIO_CONTROL_FAC
				,NULL			AS TERMINAL_CONTROL_FAC
				,NULL			AS VEHICULO_ID
				,NULL			AS PALLET_COMPLETO
				,NULL			AS HIJO
				,NULL			AS QTY_CONTROLADO
				,NULL			AS PALLET_FINAL
				,NULL			AS PALLET_CERRADO
				,NULL			AS USUARIO_PF
				,NULL			AS TERMINAL_PF
				,'0'			AS REMITO_IMPRESO
				,NULL			AS NRO_REMITO_PF
				,NULL			AS PICKING_ID_REF
				,NULL			AS BULTOS_CONTROLADOS
				,NULL			AS BULTOS_NO_CONTROLADOS
				,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
				,0				AS TRANSF_TERMINANDA
				,DD.NRO_LOTE	AS NRO_LOTE
				,DD.NRO_PARTIDA AS NRO_PARTIDA
				,DD.NRO_SERIE	AS NRO_SERIE
				,NULL			AS ESTADO
				,NULL			AS NRO_UCDESCONSOLIDACION
				,NULL			AS FECHA_DESCONSOLIDACION
				,NULL			AS USUARIO_DESCONSOLIDACION
				,NULL			AS TERMINAL_DESCONSOLIDACION
				,NULL			AS NRO_UCEMPAQUETADO
				,NULL			AS UCEMPAQUETADO_MEDIDAS
				,NULL			AS FECHA_UCEMPAQUETADO
				,NULL			AS UCEMPAQUETADO_PESO				
		FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD     ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN PRODUCTO P                       ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT    ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL     ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
				LEFT JOIN NAVE N                            ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
				LEFT JOIN POSICION POS                      ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
				LEFT JOIN NAVE N2                           ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				AND DD.NRO_LINEA=@NRO_LINEA
				AND RL.RL_ID = @NEWRL_ID

		UPDATE PICKING SET TIPO_CAJA=0 WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA AND LTRIM(RTRIM(TIPO_CAJA))='';

		SELECT 	@CLIENTE_IDC= CLIENTE_ID,
				@PRODUCTO_IDC= PRODUCTO_ID
		FROM	DET_DOCUMENTO 
		WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
				AND NRO_LINEA=@NRO_LINEA

		DELETE FROM CONSUMO_LOCATOR_EGR WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
		
		IF (@QTYPAUX <> @QTYPAUX2)
		BEGIN
		INSERT INTO CONSUMO_LOCATOR_EGR (DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD, RL_ID,SALDO, TIPO, FECHA, PROCESADO)
		VALUES(@DOCUMENTO_ID, @NRO_LINEA, @CLIENTE_IDC, @PRODUCTO_IDC, @QTYPAUX ,@NEWRL,0,2,GETDATE(),'S')		
		END		

		INSERT INTO CONSUMO_LOCATOR_EGR (DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD, RL_ID,SALDO, TIPO, FECHA, PROCESADO)
		VALUES(@DOCUMENTO_ID, @NRO_LINEA, @CLIENTE_IDC, @PRODUCTO_IDC, @QTYPICKING,@NEWRL_ID,0,2,GETDATE(),'S')

	END --FIN @QTYPICKING < @QTYRL 2DO. CASO.

	IF (@QTYPICKING > @QTYRL)	
	BEGIN
		SET @DIF= @QTYPICKING - @QTYRL

		--OBTENGO LA RL ANTERIOR.
		SELECT 	@OLDRL_ID=RL.RL_ID
		FROM	RL_DET_DOC_TRANS_POSICION RL
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS_EGR=DDT.NRO_LINEA_TRANS)
				INNER JOIN DET_DOCUMENTO DD
				ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA

		--OBTENGO EL DOCUMENTO DE TRANSACCION Y EL NUMERO DE LINEA PARA CONSUMIR LA NUEVA RL.
		SELECT	 @DOC_TRANS_IDEGR	=DOC_TRANS_ID_EGR
				,@NRO_LINEA_TRANSEGR=NRO_LINEA_TRANS_EGR
		FROM	RL_DET_DOC_TRANS_POSICION
		WHERE	RL_ID=@OLDRL_ID
					
		--ACTUALIZO LA CANTIDAD EN LA LINEA ORIGINAL DE DET_DOCUMENTO.	
		UPDATE DET_DOCUMENTO SET CANTIDAD=@DIF, CANT_SOLICITADA=@DIF WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

		--YA TENGO EL NUEVO NRO_LINEA PARA EL SPLIT	
		SELECT @MAXLINEA=MAX(NRO_LINEA) + 1 FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

		--HAGO EL SPLIT DE LA LINEA DE DET_DOCUMENTO.
		INSERT INTO DET_DOCUMENTO
		SELECT	DOCUMENTO_ID, @MAXLINEA, CLIENTE_ID, PRODUCTO_ID, @QTYRL,	NRO_SERIE, NRO_SERIE_PADRE, EST_MERC_ID, CAT_LOG_ID, NRO_BULTO,
				DESCRIPCION, NRO_LOTE, FECHA_VENCIMIENTO, NRO_DESPACHO, NRO_PARTIDA, UNIDAD_ID, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN,
				BUSC_INDIVIDUAL, TIE_IN, NRO_TIE_IN_PADRE, NRO_TIE_IN, ITEM_OK, CAT_LOG_ID_FINAL, MONEDA_ID, COSTO, PROP1, PROP2, PROP3,
				LARGO, ALTO, ANCHO, VOLUMEN_UNITARIO, PESO_UNITARIO, CANT_SOLICITADA, TRACE_BACK_ORDER
		FROM 	DET_DOCUMENTO
		WHERE	DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

		SELECT @MAXLINEADDT=MAX(NRO_LINEA_DOC) + 1 FROM DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@DOCUMENTO_ID

		--SACO EL DOCUMENTO DE TRANSACCION PARA PODER HACER LA INSERCION DE DDT
		SELECT @DOC_TRANS_ID=DOC_TRANS_ID FROM DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA_DOC=@NRO_LINEA

		--INSERTO EN DET_DOCUMENTO_TRANSACCION.	

		INSERT INTO DET_DOCUMENTO_TRANSACCION
		SELECT 	 DOC_TRANS_ID
				,@MAXLINEADDT
				,@DOCUMENTO_ID
				,@MAXLINEA
				,MOTIVO_ID
				,EST_MERC_ID
				,CLIENTE_ID
				,CAT_LOG_ID
				,ITEM_OK
				,MOVIMIENTO_PENDIENTE
				,DOC_TRANS_ID_REF
				,NRO_LINEA_TRANS_REF
		FROM	DET_DOCUMENTO_TRANSACCION
		WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
				AND NRO_LINEA_DOC=@NRO_LINEA

		UPDATE RL_DET_DOC_TRANS_POSICION SET CANTIDAD=@QTYPICKING - @QTYRL WHERE RL_ID=@OLDRL_ID
		
		--CONSUMO LA RL.
		UPDATE	 RL_DET_DOC_TRANS_POSICION 
		SET 	 DISPONIBLE='0'
				,POSICION_ANTERIOR=POSICION_ACTUAL
				,POSICION_ACTUAL=NULL
				,NAVE_ANTERIOR=NAVE_ACTUAL
				,NAVE_ACTUAL=@PREEGRID
				,DOC_TRANS_ID_EGR=@DOC_TRANS_IDEGR
				,NRO_LINEA_TRANS_EGR=@MAXLINEADDT
				,CAT_LOG_ID='TRAN_EGR'
		WHERE	RL_ID=@NEWRL_ID

		--DEBO HACER EL SPLIT DE LA LINEA DE RL ANTERIOR.
		INSERT INTO RL_DET_DOC_TRANS_POSICION
		SELECT 	 DOC_TRANS_ID
				,NRO_LINEA_TRANS
				,POSICION_ANTERIOR
				,POSICION_ACTUAL
				,@DIF	--CANTIDAD
				,TIPO_MOVIMIENTO_ID
				,ULTIMA_ESTACION
				,ULTIMA_SECUENCIA
				,NAVE_ANTERIOR
				,NAVE_ACTUAL
				,DOCUMENTO_ID
				,NRO_LINEA
				,DISPONIBLE
				,DOC_TRANS_ID_EGR
				,NRO_LINEA_TRANS_EGR
				,DOC_TRANS_ID_TR
				,NRO_LINEA_TRANS_TR
				,CLIENTE_ID
				,CAT_LOG_ID
				,CAT_LOG_ID_FINAL
				,EST_MERC_ID
		FROM	RL_DET_DOC_TRANS_POSICION
		WHERE	RL_ID=@OLDRL_ID

		--NECESARIO PARA SABER Q RL DEBO LIBERAR.
		SELECT @SPLITRL=SCOPE_IDENTITY()

		SELECT @CAT_LOG_ID_FINAL=CAT_LOG_ID_FINAL FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

		--RL NUEVA LIBERADA
		UPDATE 	 RL_DET_DOC_TRANS_POSICION 
		SET 	 DISPONIBLE				='1'
				,CANTIDAD				=@QTYRL
				,DOC_TRANS_ID_EGR		=NULL
				,NRO_LINEA_TRANS_EGR	=NULL
				,POSICION_ACTUAL		=POSICION_ANTERIOR
				,POSICION_ANTERIOR		=NULL
				,NAVE_ACTUAL			=NAVE_ANTERIOR
				,NAVE_ANTERIOR			='1'
				,CAT_LOG_ID				=@CAT_LOG_ID_FINAL
		WHERE	RL_ID					=@SPLITRL
		
		UPDATE PICKING SET CANTIDAD=@DIF WHERE PICKING_ID=@PICKING_ID
		
		SELECT @RUTA=RUTA FROM PICKING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

		--INSERTO LA NUEVA LINEA DE PICKING.
		INSERT INTO PICKING 
		SELECT 	 DISTINCT
				 DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,DD.CLIENTE_ID
				,DD.PRODUCTO_ID 
				,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
				,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
				,P.DESCRIPCION
				,DD.CANTIDAD
				,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
				,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
				,@RUTA	--ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
				,DD.PROP1
				,NULL			AS FECHA_INICIO
				,NULL			AS FECHA_FIN
				,NULL			AS USUARIO
				,NULL			AS CANT_CONFIRMADA
				,NULL			AS PALLET_PICKING
				,0 				AS SALTO_PICKING
				,'0'			AS PALLET_CONTROLADO
				,NULL			AS USUARIO_CONTROL_PICKING
				,'0'			AS ST_ETIQUETAS
				,'0'			AS ST_CAMION
				,'0'			AS FACTURADO
				,'0'			AS FIN_PICKING
				,'0'			AS ST_CONTROL_EXP
				,NULL			AS FECHA_CONTROL_PALLET
				,NULL			AS TERMINAL_CONTROL_PALLET
				,NULL			AS FECHA_CONTROL_EXP
				,NULL			AS USUARIO_CONTROL_EXP
				,NULL			AS TERMINAL_CONTROL_EXPEDICION
				,NULL			AS FECHA_CONTROL_FAC
				,NULL			AS USUARIO_CONTROL_FAC
				,NULL			AS TERMINAL_CONTROL_FAC
				,NULL			AS VEHICULO_ID
				,NULL			AS PALLET_COMPLETO
				,NULL			AS HIJO
				,NULL			AS QTY_CONTROLADO
				,NULL			AS PALLET_FINAL
				,NULL			AS PALLET_CERRADO
				,NULL			AS USUARIO_PF
				,NULL			AS TERMINAL_PF
				,'0'			AS REMITO_IMPRESO
				,NULL			AS NRO_REMITO_PF
				,NULL			AS PICKING_ID_REF
				,NULL			AS BULTOS_CONTROLADOS
				,NULL			AS BULTOS_NO_CONTROLADOS
				,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
				,0				AS TRANSF_TERMINANDA
				,DD.NRO_LOTE	AS NRO_LOTE
				,DD.NRO_PARTIDA AS NRO_PARTIDA
				,DD.NRO_SERIE	AS NRO_SERIE
				,'0'			AS ESTADO
				,NULL			AS NRO_UCDESCONSOLIDACION
				,NULL			AS FECHA_DESCONSOLIDACION
				,NULL			AS USUARIO_DESCONSOLIDACION
				,NULL			AS TERMINAL_DESCONSOLIDACION
				,NULL			AS NRO_UCEMPAQUETADO
				,NULL			AS UCEMPAQUETADO_MEDIDAS
				,NULL			AS FECHA_UCEMPAQUETADO
				,NULL			AS UCEMPAQUETADO_PESO	            
		FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD     ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN PRODUCTO P                       ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT    ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL     ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
				LEFT JOIN NAVE N                            ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
				LEFT JOIN POSICION POS                      ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
				LEFT JOIN NAVE N2                           ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	RL.DOC_TRANS_ID_EGR=@DOC_TRANS_IDEGR
				AND RL.NRO_LINEA_TRANS_EGR=@MAXLINEADDT	
		
		UPDATE	PICKING SET TIPO_CAJA=0 WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA AND LTRIM(RTRIM(TIPO_CAJA))='';

		UPDATE 	CONSUMO_LOCATOR_EGR 
		SET 	CANTIDAD= @QTYPICKING - @QTYRL ,
				SALDO 	= (SALDO + (@QTYPICKING - @QTYRL))
		WHERE	DOCUMENTO_ID=DOCUMENTO_ID
				AND NRO_LINEA=@NRO_LINEA

		SELECT 	@CLIENTE_IDC= CLIENTE_ID,
				@PRODUCTO_IDC= PRODUCTO_ID
		FROM	DET_DOCUMENTO 
		WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
				AND NRO_LINEA=@NRO_LINEA

		INSERT INTO CONSUMO_LOCATOR_EGR (DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD, RL_ID,SALDO, TIPO, FECHA, PROCESADO)
		VALUES(@DOCUMENTO_ID, @MAXLINEA, @CLIENTE_IDC, @PRODUCTO_IDC, @QTYRL, @NEWRL_ID, 0, 2, GETDATE(),'S')

	END -- FIN 	IF (@QTYPICKING > @QTYRL) 3ER. CASO.

	IF @@ERROR<>0
	BEGIN
		RAISERROR('SE PRODUJO UN ERROR INESPERADO.',16,1)
		RETURN
	END
END --FIN PROCEDURE.



