ALTER PROCEDURE DBO.MOB_CAMBIO_CONTENEDORA
	@NEWRL_ID		NUMERIC(20,0),
	@VIAJE_ID		VARCHAR(100),
	@PRODUCTO_ID	VARCHAR(30),
	@CANTIDAD		NUMERIC(20,5),
	@USR			VARCHAR(20)
AS
BEGIN
	-----------------------------------------------------------
	--SEGMENTO PARA LA DECLARACION DE LA VARIABLES UTILIZADAS.
	-----------------------------------------------------------
	DECLARE @CURPICK			cursor;
	DECLARE	@DOCUMENTO_ID		numeric(20,0)
	DECLARE @NRO_LINEA			numeric(10,0)
	DECLARE @CLIENTE_ID			varchar(15)
	DECLARE @NRO_SERIE			varchar(50)
	DECLARE @NRO_SERIE_PADRE	varchar(50)
	DECLARE @EST_MERC_ID		varchar(15)
	DECLARE @CAT_LOG_ID			varchar(50)
	DECLARE @NRO_BULTO			varchar(50)
	DECLARE @DESCRIPCION		varchar(200)
	DECLARE @NRO_LOTE			varchar(50)
	DECLARE @FECHA_VENCIMIENTO	datetime
	DECLARE @NRO_DESPACHO		varchar(50)
	DECLARE @NRO_PARTIDA		varchar(50)
	DECLARE @UNIDAD_ID			varchar(5)
	DECLARE @PESO				numeric(20)
	DECLARE @UNIDAD_PESO		varchar(5)
	DECLARE @VOLUMEN			numeric(20)
	DECLARE @UNIDAD_VOLUMEN		varchar(5)
	DECLARE @BUSC_INDIVIDUAL	varchar(1)
	DECLARE @TIE_IN				varchar(1)
	DECLARE @NRO_TIE_IN_PADRE	varchar(100)
	DECLARE @NRO_TIE_IN			varchar(100)
	DECLARE @ITEM_OK			varchar(1)
	DECLARE @CAT_LOG_ID_FINAL	varchar(50)
	DECLARE @MONEDA_ID			varchar(20)
	DECLARE @COSTO				numeric(10)
	DECLARE @PROP1				varchar(100)
	DECLARE @PROP2				varchar(100)
	DECLARE @PROP3				varchar(100)
	DECLARE @LARGO				numeric(10)
	DECLARE @ALTO				numeric(10)
	DECLARE @ANCHO				numeric(10)
	DECLARE @VOLUMEN_UNITARIO	varchar(1)
	DECLARE @PESO_UNITARIO		varchar(1)
	DECLARE @CANT_SOLICITADA	numeric(20)
	DECLARE @TRACE_BACK_ORDER	varchar(1)
	DECLARE @POSICION_COD		varchar(45)
	DECLARE @NAVE_COD			varchar(45)
	DECLARE @QTY_PICK			numeric(20,5)
	DECLARE @OLD_RL_ID			numeric(20,0)
	DECLARE @NRL_ID				numeric(20,0)
	DECLARE @ERROR				char(1)
	DECLARE @DOC_TRANS_ID		numeric(20,0)
	DECLARE @NRO_LINEA_TRANS	numeric(20,0)
	DECLARE @VCANTIDAD			numeric(20,5)
	DECLARE	@CANT_RL			numeric(20,5)
	DECLARE @PICKING_ID			numeric(20,0)
	DECLARE @OUT_SP				char(1)
	DECLARE @DSPLIT				numeric(20,5)
	DECLARE @SUM_PIK			numeric(20,5)
	DECLARE @T_TOM				numeric(20,0)
	DECLARE @T_NTOM				numeric(20,0)
	DECLARE @PALLET_PICKING		numeric(20,0)
	DECLARE @FECHA_INICIO		datetime
	-----------------------------------------------------------
	--COMIENZO UNA TRANSACCION PARA TODO EL SP.
	-----------------------------------------------------------
	BEGIN TRANSACTION

	SET @VCANTIDAD=@CANTIDAD;
	-----------------------------------------------------------------------------------------------------------------------------------
	--RECUPERO EN VARIABLES TODAS LAS CARACTERISTICAS DE LA NUEVA RL POR LA QUE ESTOY REALIZANDO EL CAMBIO DE CONTENEDORA.
	-----------------------------------------------------------------------------------------------------------------------------------
	SELECT	 @CLIENTE_ID		=DD.CLIENTE_ID			,@NRO_SERIE			=DD.NRO_SERIE		,@NRO_SERIE_PADRE	=DD.NRO_SERIE_PADRE
			,@EST_MERC_ID		=RL.EST_MERC_ID			,@CAT_LOG_ID		='TRAN_EGR'			,@NRO_BULTO			=DD.NRO_BULTO
			,@DESCRIPCION		=DD.DESCRIPCION			,@NRO_LOTE			=DD.NRO_LOTE		,@FECHA_VENCIMIENTO	=DD.FECHA_VENCIMIENTO
			,@NRO_DESPACHO		=DD.NRO_DESPACHO		,@NRO_PARTIDA		=DD.NRO_PARTIDA		,@UNIDAD_ID			=DD.UNIDAD_ID
			,@PESO				=DD.PESO				,@UNIDAD_PESO		=DD.UNIDAD_PESO		,@VOLUMEN			=DD.VOLUMEN
			,@UNIDAD_VOLUMEN	=DD.UNIDAD_VOLUMEN		,@BUSC_INDIVIDUAL	=DD.BUSC_INDIVIDUAL	,@TIE_IN			=ISNULL(DD.TIE_IN,'0')
			,@NRO_TIE_IN_PADRE	=DD.NRO_TIE_IN_PADRE	,@NRO_TIE_IN		=DD.NRO_TIE_IN		,@ITEM_OK			=DD.ITEM_OK
			,@CAT_LOG_ID_FINAL	=RL.CAT_LOG_ID			,@MONEDA_ID			=DD.MONEDA_ID		,@COSTO				=DD.COSTO
			,@PROP1				=DD.PROP1				,@PROP2				=DD.PROP2			,@PROP3				=DD.PROP3
			,@LARGO				=DD.LARGO				,@ALTO				=DD.ALTO			,@ANCHO				=DD.ANCHO
			,@VOLUMEN_UNITARIO	=DD.VOLUMEN_UNITARIO	,@PESO_UNITARIO		=DD.PESO_UNITARIO	,@CANT_SOLICITADA	=DD.CANT_SOLICITADA
			,@TRACE_BACK_ORDER	=DD.TRACE_BACK_ORDER	,@NAVE_COD			=ISNULL(N.NAVE_COD,N2.NAVE_COD)
			,@POSICION_COD		=ISNULL(P.POSICION_COD,N2.NAVE_COD)
	FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD												ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN POSICION P													ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			LEFT JOIN NAVE N														ON(P.NAVE_ID=N.NAVE_ID)
			LEFT JOIN NAVE N2														ON(RL.NAVE_ACTUAL=N2.NAVE_ID)
	WHERE	RL.RL_ID=@NEWRL_ID;
	-----------------------------------------------------------------------------------------------------------------------------------
	BEGIN TRY
		--Tengo que saber si voy por un solo cambio o si voy por todos los picking.
			SELECT	@SUM_PIK=sum(P.CANTIDAD)
			FROM	PICKING P
			WHERE	P.VIAJE_ID=@VIAJE_ID
					AND P.PRODUCTO_ID=@PRODUCTO_ID
					AND P.USUARIO=@USR
					AND P.FECHA_INICIO IS NOT NULL
					AND P.FECHA_FIN IS NULL;		

		-----------------------------------------------------------------------------------------------------------------------------------
		--COMO PUEDE TENER MAS DE UNA LINEA DE PICKING LEVANTO UN CURSOR PARA HACER LOS CAMBIOS.
		-----------------------------------------------------------------------------------------------------------------------------------
		IF(@SUM_PIK>=@CANTIDAD)
		BEGIN 
			SET @CURPICK=CURSOR FOR
				SELECT	 P.DOCUMENTO_ID
						,P.NRO_LINEA
						,P.CANTIDAD
						,P.PICKING_ID
				FROM	PICKING P
				WHERE	P.VIAJE_ID=@VIAJE_ID
						AND P.PRODUCTO_ID=@PRODUCTO_ID
						AND P.USUARIO=@USR
						AND P.FECHA_INICIO IS NOT NULL
						AND P.FECHA_FIN IS NULL;
		END
		ELSE
		BEGIN
			IF(@CANTIDAD>@SUM_PIK)
			BEGIN
				SET @CURPICK=CURSOR FOR
					SELECT	 P.DOCUMENTO_ID
							,P.NRO_LINEA
							,P.CANTIDAD
							,P.PICKING_ID
					FROM	PICKING P
					WHERE	P.VIAJE_ID=@VIAJE_ID
							AND P.PRODUCTO_ID=@PRODUCTO_ID
							AND P.CANT_CONFIRMADA IS NULL;			
			END
		END
		OPEN @CURPICK
		FETCH NEXT FROM @CURPICK INTO @DOCUMENTO_ID,@NRO_LINEA,@QTY_PICK,@PICKING_ID
		WHILE @@FETCH_STATUS=0
		BEGIN
			IF(@VCANTIDAD-@QTY_PICK)<0
			BEGIN
				--ESTO INDICA QUE ES PARCIALIZADO Y SACO EL VALOR ABSOLUTO DEL RESULTADO.
				SET @DSPLIT=ABS(@VCANTIDAD-@QTY_PICK)
				
				EXEC DBO.SPLIT_PICKING_CONTENEDORA @PICKING_ID,@DSPLIT, @OUT_SP OUTPUT
				IF(@OUT_SP<>'0')
				BEGIN
					RAISERROR('SPLIT_PICKING_CONTENEDORA - Error crear registros remanentes.',16,1)
				END
			END
			--------------------------------------------------------------------------------------------------------------
			--REALIZO CAMBIO EN LA TABLA DE PICKING.
			--------------------------------------------------------------------------------------------------------------
			UPDATE	PICKING	SET	NAVE_COD=@NAVE_COD, POSICION_COD=@POSICION_COD, 
								NRO_LOTE=@NRO_LOTE, NRO_PARTIDA=@NRO_PARTIDA,
								NRO_SERIE=@NRO_SERIE
			WHERE	DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA;
			--------------------------------------------------------------------------------------------------------------
			--REALIZO CAMBIOS EN LA TABLA DET_DOCUMENTO.
			--------------------------------------------------------------------------------------------------------------
			UPDATE	DET_DOCUMENTO 
			SET		 NRO_SERIE			=@NRO_SERIE			,NRO_SERIE_PADRE	=@NRO_SERIE_PADRE		,EST_MERC_ID		=@EST_MERC_ID
					,CAT_LOG_ID			=@CAT_LOG_ID		,NRO_BULTO			=@NRO_BULTO				,NRO_LOTE			=@NRO_LOTE
					,FECHA_VENCIMIENTO	=@FECHA_VENCIMIENTO	,NRO_DESPACHO		=@NRO_DESPACHO			,NRO_PARTIDA		=@NRO_PARTIDA
					,UNIDAD_ID			=@UNIDAD_ID			,PESO				=@PESO					,UNIDAD_PESO		=@UNIDAD_PESO
					,VOLUMEN			=@VOLUMEN			,UNIDAD_VOLUMEN		=@UNIDAD_VOLUMEN		,BUSC_INDIVIDUAL	=@BUSC_INDIVIDUAL
					,TIE_IN				=@TIE_IN			,NRO_TIE_IN_PADRE	=@NRO_TIE_IN_PADRE		,NRO_TIE_IN			=@NRO_TIE_IN
					,ITEM_OK			=@ITEM_OK			,CAT_LOG_ID_FINAL	=@CAT_LOG_ID_FINAL		,MONEDA_ID			=@MONEDA_ID
					,COSTO				=@COSTO				,PROP1				=@PROP1					,PROP2				=@PROP2
					,PROP3				=@PROP3				,LARGO				=@LARGO					,ALTO				=@ALTO
					,ANCHO				=@ANCHO				,VOLUMEN_UNITARIO	=@VOLUMEN_UNITARIO		,PESO_UNITARIO		=@PESO_UNITARIO
					,CANT_SOLICITADA	=@CANT_SOLICITADA	,TRACE_BACK_ORDER	=@TRACE_BACK_ORDER
			WHERE	DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA;
			--------------------------------------------------------------------------------------------------------------
			--TENGO QUE LIBERAR LA RL QUE SE TOMO ANTERIORMENTE Y DEJARLA EN LA NAVE DE ANTERIOR PARA SACARLO DE LA NAVE PRE-EGRESO.
			--------------------------------------------------------------------------------------------------------------
			SELECT	@OLD_RL_ID=RL.RL_ID,@DOC_TRANS_ID=DDT.DOC_TRANS_ID,@NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS
			FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID_EGR AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
			WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA;
			
			--QUITO EL CONSUMO DE LA TABLA.
			DELETE FROM CONSUMO_LOCATOR_EGR WHERE DOCUMENTO_ID=@DOCUMENTO_ID;
			
			--LIBERO LA RL AFECTADA.			
			UPDATE	RL_DET_DOC_TRANS_POSICION 
			SET		POSICION_ACTUAL=POSICION_ANTERIOR,
					POSICION_ANTERIOR=NULL,
					DISPONIBLE='1',
					NAVE_ACTUAL=NAVE_ANTERIOR,
					NAVE_ANTERIOR='1',
					CAT_LOG_ID='DISPONIBLE',
					DOC_TRANS_ID_EGR=NULL,
					NRO_LINEA_TRANS_EGR=NULL
			WHERE	RL_ID=@OLD_RL_ID

			--PARTICIONO LA RL DE ACUERDO A MIS NECESIDADES.
			IF (@CANTIDAD<@QTY_PICK)
			BEGIN
				EXEC	DBO.MOB_CAMBIOCONTENEDORA_SPLIT_RL @NEWRL_ID, @CANTIDAD, @NRL_ID OUTPUT, @ERROR OUTPUT
			END
			ELSE
			BEGIN
				IF(@VCANTIDAD-@QTY_PICK)>0
				BEGIN
					EXEC	DBO.MOB_CAMBIOCONTENEDORA_SPLIT_RL @NEWRL_ID, @QTY_PICK, @NRL_ID OUTPUT, @ERROR OUTPUT
				END
				ELSE
				BEGIN
					EXEC	DBO.MOB_CAMBIOCONTENEDORA_SPLIT_RL @NEWRL_ID, @VCANTIDAD, @NRL_ID OUTPUT, @ERROR OUTPUT
				END
			END
			
			IF @ERROR='1'
			BEGIN
				RAISERROR('OCURRIO UN ERROR INESPERADO AL EJECUTAR MOB_CAMBIOCONTENEDORA_SPLIT_RL.',16,1)
			END 
			--TOMO LA RL PARA CONSUMIRLA.
			UPDATE RL_DET_DOC_TRANS_POSICION
			SET		POSICION_ANTERIOR=POSICION_ACTUAL,
					POSICION_ACTUAL=NULL,
					DISPONIBLE='0',
					NAVE_ANTERIOR=NAVE_ACTUAL,
					NAVE_ACTUAL='2',
					CAT_LOG_ID='TRAN_EGR',
					DOC_TRANS_ID_EGR=@DOC_TRANS_ID,
					NRO_LINEA_TRANS_EGR=@NRO_LINEA_TRANS
			WHERE	RL_ID=@NRL_ID;
			
			--GENERO EL CONSUMO EN RL.
			INSERT INTO CONSUMO_LOCATOR_EGR (DOCUMENTO_ID	,NRO_LINEA	,CLIENTE_ID	,PRODUCTO_ID	,CANTIDAD	,RL_ID	,SALDO	,TIPO	,FECHA		,PROCESADO) 
			SELECT  DD.DOCUMENTO_ID,DD.NRO_LINEA, DD.CLIENTE_ID,DD.PRODUCTO_ID,
					RL.CANTIDAD,RL.RL_ID,0,1,GETDATE(),'S'
			FROM    DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID_EGR AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
			WHERE   DD.DOCUMENTO_ID=@DOCUMENTO_ID
			
			SET @VCANTIDAD=@VCANTIDAD-@QTY_PICK
			
			IF (@VCANTIDAD<=0)
			BEGIN
				BREAK
			END 
			FETCH NEXT FROM @CURPICK INTO @DOCUMENTO_ID,@NRO_LINEA,@QTY_PICK,@PICKING_ID
		END--FIN LOOP CURSOR.
		CLOSE @CURPICK
		DEALLOCATE @CURPICK
		----------------------------------------------------------
		--PARA MANTENER TODAS LAS TAREAS TOMADAS SI CORRESPONDE.
		----------------------------------------------------------
		SELECT	@T_TOM			=COUNT(P.PRODUCTO_ID),
				@POSICION_COD	=P.POSICION_COD,
				@NRO_BULTO		=DD.NRO_BULTO,
				@NRO_LOTE		=P.NRO_LOTE, 
				@NRO_PARTIDA	=P.NRO_PARTIDA,
				@PALLET_PICKING	=P.PALLET_PICKING,
				@FECHA_INICIO	=P.FECHA_INICIO
		FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
				ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
		WHERE	VIAJE_ID=@VIAJE_ID
				AND USUARIO	=@USR
				AND P.PRODUCTO_ID=@PRODUCTO_ID
				AND FECHA_INICIO IS NOT NULL
		GROUP BY
				P.VIAJE_ID,P.PRODUCTO_ID,P.POSICION_COD,DD.NRO_BULTO,P.NRO_LOTE, P.NRO_PARTIDA,P.PALLET_PICKING,P.FECHA_INICIO


		SELECT	@T_NTOM			=COUNT(P.PRODUCTO_ID)
		FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
				ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
		WHERE	P.VIAJE_ID=@VIAJE_ID
				AND P.PRODUCTO_ID=@PRODUCTO_ID
				AND P.POSICION_COD=@POSICION_COD
				AND DD.NRO_BULTO=@NRO_BULTO
				AND P.NRO_LOTE=@NRO_LOTE
				AND P.NRO_PARTIDA=@NRO_PARTIDA
		GROUP BY
				P.VIAJE_ID,P.PRODUCTO_ID,P.POSICION_COD,DD.NRO_BULTO,P.NRO_LOTE, P.NRO_PARTIDA
		IF(@T_TOM<>@T_NTOM)
		BEGIN
			UPDATE	PICKING SET USUARIO=@USR, PALLET_PICKING=@PALLET_PICKING,FECHA_INICIO=@FECHA_INICIO
			FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
					ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			WHERE	P.VIAJE_ID=@VIAJE_ID
					AND P.PRODUCTO_ID=@PRODUCTO_ID
					AND P.POSICION_COD=@POSICION_COD
					AND DD.NRO_BULTO=@NRO_BULTO
					AND P.NRO_LOTE=@NRO_LOTE
					AND P.NRO_PARTIDA=@NRO_PARTIDA
		END
		COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
		ROLLBACK TRANSACTION
		EXEC usp_RethrowError
	END CATCH; --FIN CONTROL ERRORES
END--FIN DEL PROCEDIMIENTO ALMACENADO.