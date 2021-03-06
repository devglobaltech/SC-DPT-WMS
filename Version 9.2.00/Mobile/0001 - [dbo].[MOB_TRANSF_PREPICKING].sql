/****** Object:  StoredProcedure [dbo].[MOB_TRANSF_PREPICKING]    Script Date: 06/18/2014 10:10:54 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MOB_TRANSF_PREPICKING]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[MOB_TRANSF_PREPICKING]
GO

CREATE                          PROCEDURE [dbo].[MOB_TRANSF_PREPICKING]
	@POSICION_O	AS VARCHAR(45),
	@POSICION_D AS VARCHAR(45),
	@USUARIO 	AS VARCHAR(30),
	@PALLET		AS VARCHAR(100),
	@CONTENEDORA AS VARCHAR(50),
	@DOCUMENTO_ID AS NUMERIC(20,0),
	@NRO_LINEA	AS NUMERIC(10,0)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	DECLARE @CLIENTE_ID		AS VARCHAR(5)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(1,0)
	DECLARE @LIMITE			AS NUMERIC(20,0)
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)
	---------	Variables para determinar el doc trans de egreso.
	DECLARE @DC_EGR			AS NUMERIC(20,0)
	DECLARE @LN_EGR			AS NUMERIC(20,0)
	---------	Var. para la cat log de rl
	DECLARE @RL_CATLOG		AS VARCHAR(50)
	
	SET XACT_ABORT ON
	--DETERMINO EL DOC Y LA LINEA DE TRANSACCION.
	SELECT	@DC_EGR=DDT.DOC_TRANS_ID,@LN_EGR=DDT.NRO_LINEA_TRANS
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.NRO_LINEA=@NRO_LINEA


	EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
	IF @OUT='1'
		BEGIN
			RETURN
		END
	if ltrim(rtrim(@CONTENEDORA))=''
	begin
		SET @CONTENEDORA=NULL
	end

	IF CURSOR_STATUS('global','CUR_RL_TR')>=-1
	BEGIN
		DEALLOCATE CUR_RL_TR
	END

	--OBTENGO LA RL DEL PALLET EN UN CURSOR POR SI HAY MAS DE UNA LINEA EN RL.--
	DECLARE CUR_RL_TR CURSOR FOR
		/*
		SELECT 	RL.RL_ID,DD.CLIENTE_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ANTERIOR  =(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ANTERIOR=(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND ((RL.DOC_TRANS_ID_EGR=@DC_EGR AND RL.NRO_LINEA_TRANS_EGR=@LN_EGR) OR (RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL))
				AND RL.CANTIDAD >0*/
		SELECT 	RL.RL_ID,DD.CLIENTE_ID, RL.CAT_LOG_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	((@PALLET is null) or(PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ANTERIOR  =(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ANTERIOR=(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND (RL.DOC_TRANS_ID_EGR=@DC_EGR AND RL.NRO_LINEA_TRANS_EGR=@LN_EGR) 
				AND RL.CANTIDAD >0
		UNION ALL
		SELECT 	RL.RL_ID,DD.CLIENTE_ID, RL.CAT_LOG_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	((@PALLET is null) or(PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ACTUAL  =(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL 
				AND RL.CANTIDAD >0				

		--Aca verifico que el pallet este en la posicion indicada.
		SELECT 	@CONTROL=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	((@PALLET is null) or(PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ANTERIOR=	(	SELECT 	NAVE_ID		FROM NAVE		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ANTERIOR=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		IF @CONTROL=0
			BEGIN
				RAISERROR('1-El pallet no esta en la posicion especificada.',16,1)
				DEALLOCATE CUR_RL_TR
				RETURN
			END
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		--	Controlo que la categoria logica no sea TRAN_ING, TRAN_EGR
		/*
		Exec Mob_Transf_VerificaCatLog @Pallet, @Posicion_O, @TRANSFIERE OUTPUT
		If @Transfiere=0
		Begin
			Return
		End */
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		-- Voy por las posiciones de Picking
		Exec Verifica_Picking @PALLET, @PICKING OUTPUT
		If @Picking=1
		Begin
			--Aca tengo que verificar que la posicion de destino sea de picking.
			Select @Picking=Dbo.IsPosPicking(@POSICION_D)
			If @Picking=0
			Begin
				RAISERROR('1- La ubicacion destino no es una ubicacion de Picking.',16,1,@CAT_LOG)
				DEALLOCATE CUR_RL_TR
				RETURN
			End
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		SELECT 	@LIMITE=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	((@PALLET is null) or(PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
				AND ((@CONTENEDORA IS NULL) OR (DD.NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.DOC_TRANS_ID_EGR=@DC_EGR AND RL.NRO_LINEA_TRANS_EGR=@LN_EGR
				AND RL.CANTIDAD >0

		SELECT @EXISTE = COUNT(*)
		FROM  TRANSACCION T
			  INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
			  INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
			  AND RTC.CLIENTE_ID IN (SELECT CLIENTE_ID FROM CLIENTE
									 WHERE (SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
										   FROM   RL_SYS_CLIENTE_USUARIO
										   WHERE  CLIENTE_ID = RTC.CLIENTE_ID
										   AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)WHERE T.TIPO_OPERACION_ID='TR' AND RTE.ORDEN=1	
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

		FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID,@RL_CATLOG
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
	
				IF @RL_CATLOG='TRAN_EGR'
				BEGIN
					--TRATO LAS QUE SON TRAN EGR.
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
								DOC_TRANS_ID_EGR,
								NRO_LINEA_TRANS_EGR,
								DOC_TRANS_ID_TR,
								NRO_LINEA_TRANS_TR,
								CLIENTE_ID,
								CAT_LOG_ID,
								EST_MERC_ID)
								(SELECT   DOC_TRANS_ID 
										, NRO_LINEA_TRANS
										, POSICION_ACTUAL
										, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
										, CANTIDAD
										, NULL
										, NULL
										, NAVE_ACTUAL
										, @NEWNAVE
										, NULL
										, NULL
										, 0
										, DOC_TRANS_ID_EGR
										, NRO_LINEA_TRANS_EGR									
										, @vDocNew
										, 1 
										, CLIENTE_ID
										, CAT_LOG_ID
										, EST_MERC_ID
								 FROM RL_DET_DOC_TRANS_POSICION
								 WHERE RL_ID = @vRLID
								 ) 
			END
			ELSE
			BEGIN
					--ACA TRATO LAS QUE NO SON TRAN EGR.
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
							, @NEWPOS
							, CANTIDAD
							, NULL
							, NULL
							, NAVE_ACTUAL							
							, @NEWNAVE
							, NULL
							, NULL
							, 1
							, @vDocNew
							, 1 
							, CLIENTE_ID
							, CAT_LOG_ID
							, EST_MERC_ID
					FROM	RL_DET_DOC_TRANS_POSICION
					WHERE	RL_ID = @vRLID
					) 
			END
			EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE
			DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			----------------------------------------------------------------------------
			--	ESTO ES SEGURAMENTE UN BUG DE SQL SERVER. REVISAR PARA CORREGIR
			--	ESTE ALGORITMO.
			SET @LIM_CONT=@LIM_CONT +1
			IF (@LIMITE=@LIM_CONT)
				BEGIN
					BREAK
				END
			----------------------------------------------------------------------------
			FETCH NEXT FROM CUR_RL_TR INTO @VRLID, @CLIENTE_ID, @RL_CATLOG
			
		END

		-- DEVOLUCION
		EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW
	
		--FINALIZA LA TRANSFERENCIA	
		EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
			
		-- BORRO POR SI QUEDA ALGUNA RL EN 0
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE CANTIDAD =0 AND DOC_TRANS_ID=@VDOCNEW
	
		UPDATE POSICION SET POS_VACIA='0' WHERE POSICION_ID IN (SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
			
		UPDATE POSICION SET POS_VACIA='1' WHERE POSICION_ID  NOT IN
		(SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
	
		CLOSE CUR_RL_TR
		DEALLOCATE CUR_RL_TR
			
END


GO


