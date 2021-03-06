IF EXISTS(SELECT * FROM SYS.objects WHERE object_id = OBJECT_ID(N'[DBO].[MOB_REUBICACION_CONTENEDORA]'))
	DROP PROCEDURE [dbo].[MOB_REUBICACION_CONTENEDORA] 
GO

CREATE PROCEDURE [dbo].[MOB_REUBICACION_CONTENEDORA]
	@CONTENEDORA				AS VARCHAR(50),
	@POS_COD					AS VARCHAR(45)		OUTPUT,
	@POS_OR						AS VARCHAR(45)=NULL
AS

DECLARE @CLIENTEID 			AS VARCHAR(15)
DECLARE @PRODUCTOID			AS VARCHAR(30)
DECLARE @VCANT 				AS NUMERIC(20)

----VARIABLES PARA HACER INSERT
DECLARE @POSICION_ID 		AS NUMERIC(20,0)
DECLARE @POSICION_COD 		AS VARCHAR(45)
DECLARE @NAVE_ID			AS NUMERIC(20,0)
DECLARE @ORDEN_LOCATOR 		AS NUMERIC(6)
DECLARE @CASO				AS INT
DECLARE @VPOS_EVAL			AS NUMERIC(20,0)
DECLARE @FLG_VOL_PESO		AS CHAR(1)
DECLARE @DOCUMENTO_ID		AS NUMERIC(20)
DECLARE @POS_ID				AS NUMERIC(20,0)

BEGIN

	SELECT	@CLIENTEID=DD.CLIENTE_ID, @PRODUCTOID=DD.PRODUCTO_ID, @DOCUMENTO_ID=DD.DOCUMENTO_ID
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL						ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.NRO_BULTO=@CONTENEDORA


	IF @DOCUMENTO_ID IS NULL
	BEGIN
			RAISERROR ('EL PARAMETRO @DOCUMENTO_ID NO PUEDE SER NULO. SQLSERVER', 16, 1)
	END

	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ

	SELECT	@FLG_VOL_PESO=ISNULL(C.FLG_ING_VOL_PESO,'0')
	FROM	DOCUMENTO D INNER JOIN CLIENTE_PARAMETROS C	ON(D.CLIENTE_ID=C.CLIENTE_ID)
	WHERE	D.DOCUMENTO_ID=@DOCUMENTO_ID

	SELECT 	TOP 1 @CLIENTEID=DD.CLIENTE_ID,@PRODUCTOID=DD.PRODUCTO_ID
	FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
			ON (DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN NAVE N
			ON(RL.NAVE_ACTUAL=N.NAVE_ID)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND D.STATUS IN('D30','D35')
			AND (N.PRE_INGRESO='1' OR N.INTERMEDIA='1')
			AND DD.PRODUCTO_ID=@PRODUCTOID

	BEGIN
		SELECT 	@VCANT=COUNT(*)
		FROM 	RL_PRODUCTO_POSICION_PERMITIDA
		WHERE 	CLIENTE_ID=@CLIENTEID AND PRODUCTO_ID=@PRODUCTOID
		
		IF CURSOR_STATUS('global','CUR_POS')>=-1 BEGIN
			DEALLOCATE CUR_POS
		END
		
		IF @VCANT>0 BEGIN
		
			--ARMO CURSOR PARA LAS POSICIONES PERMITIDAS.
			DECLARE CUR_POS CURSOR FOR
			SELECT	ISNULL(R.POSICION_ID,R.NAVE_ID)
			FROM 	RL_PRODUCTO_POSICION_PERMITIDA R LEFT JOIN POSICION P
					ON(R.POSICION_ID=P.POSICION_ID)
					LEFT JOIN NAVE N
					ON(R.NAVE_ID=P.NAVE_ID)
			WHERE 	1=1
					AND R.CLIENTE_ID=@CLIENTEID 
					AND R.PRODUCTO_ID=@PRODUCTOID
					AND P.POS_LOCKEADA='0'
					AND ((@POS_OR IS NULL)OR(P.POSICION_COD NOT IN(@POS_OR)))
			ORDER BY
					ISNULL(P.ORDEN_LOCATOR,99999)				
					
		END ELSE BEGIN
			--ARMO POSICIONES TOTALES, PORQUE NO TIENE POSICIONES PERMITIDAS O ES UN MULTIPRODUCTO.
			DECLARE CUR_POS CURSOR FOR
			SELECT	X.POSICION_ID
			FROM	(	SELECT	POSICION_ID, ORDEN_LOCATOR
						FROM	POSICION P
						WHERE	1=1
								AND P.POS_LOCKEADA='0'
								AND ((@POS_OR IS NULL)OR(P.POSICION_COD NOT IN(@POS_OR)))
						UNION ALL
						SELECT	NAVE_ID,99999
						FROM	NAVE N
						WHERE	N.DISP_INGRESO='1' 
								AND N.PRE_INGRESO='0' 
								AND PRE_EGRESO='0'
								AND ((@POS_OR IS NULL)OR(N.NAVE_COD NOT IN(@POS_OR)))
					)X
			ORDER BY
					ISNULL(X.ORDEN_LOCATOR,99999)

		END				

		IF @VCANT > 0
			BEGIN
			
				OPEN CUR_POS
				FETCH NEXT FROM CUR_POS INTO @VPOS_EVAL
				WHILE (@@FETCH_STATUS=0) BEGIN	
					
					SELECT TOP 1
							 @POSICION_ID=X.POSICION_ID
							,@POSICION_COD=ISNULL((SELECT DBO.MOB_BUSCAR_POSICION_GUARDADO(@CLIENTEID, @PRODUCTOID)),X.POSICION_COD)
							,@NAVE_ID=X.NAVE_ID
							,@ORDEN_LOCATOR=X.ORDENLOCATOR
							,@CASO=X1
					FROM(	SELECT 	 
									 P.POSICION_ID  AS POSICION_ID
									,P.POSICION_COD AS POSICION_COD
									,NULL AS NAVE_ID
									,ISNULL(P.ORDEN_LOCATOR,99999) AS ORDENLOCATOR
									,1 AS X1
							FROM 	POSICION P INNER JOIN
									RL_PRODUCTO_POSICION_PERMITIDA RLPP
									ON(P.POSICION_ID=RLPP.POSICION_ID)
									LEFT JOIN RL_POSICION_PROHIBIDA_CLIENTE PROH --AGREGUE ESTO 30-07-2009
									ON (P.POSICION_ID = PROH.POSICION_ID) --AGREGUE ESTO 30-07-2009
							WHERE	1=1 AND P.POS_LOCKEADA='0'
									AND RLPP.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID))) 
									AND RLPP.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
									AND ((@FLG_VOL_PESO='0') OR([dbo].[TRANSFERENCIA_CONTENEDOR_PESO](@DOCUMENTO_ID,@CONTENEDORA,P.POSICION_ID)='1'))
									AND	((@FLG_VOL_PESO='0') OR ([dbo].[TRANSFERENCIA_CONTENEDOR_VOLUMEN](@DOCUMENTO_ID,@CONTENEDORA,P.POSICION_ID)='1'))
									AND PROH.POSICION_ID IS NULL --AGREGUE ESTO 30-07-2009
									AND RLPP.POSICION_ID=@VPOS_EVAL
							UNION ALL
							SELECT 	 
									 NULL AS 	POSICION_ID
									,N.NAVE_COD AS POSICION_COD
									,N.NAVE_ID  AS NAVE_ID
									,ISNULL(N.ORDEN_LOCATOR,99999) AS ORDENLOCATOR
									,0 AS X1
							FROM 	NAVE N INNER JOIN
									RL_PRODUCTO_POSICION_PERMITIDA RLPP
									ON(N.NAVE_ID=RLPP.NAVE_ID)
							WHERE	N.DISP_INGRESO='1' AND N.PRE_INGRESO='0' 
									AND PRE_EGRESO='0'
									AND RLPP.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID)))
									AND RLPP.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
									AND RLPP.NAVE_ID =@VPOS_EVAL
					
					)AS X
					ORDER BY X.ORDENLOCATOR ASC
					
				IF @POSICION_COD IS NOT NULL BEGIN
					BREAK
				END
				
				FETCH NEXT FROM CUR_POS INTO @VPOS_EVAL
				
			END --FIN (@@FETCH_STATUS=0) OR (@POSICION_ID IS NOT NULL)
					
				IF @CASO=1
				BEGIN
					
					IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
					BEGIN
						RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PRODUCTO.',16,1)
					END
					ELSE
					BEGIN
						SET @POS_COD=@POSICION_COD
					END
				END
				ELSE
				BEGIN

					IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
					BEGIN
						RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PRODUCTO.',16,1)
					END
					ELSE
					BEGIN

						SET @POS_COD=@POSICION_COD

					END

				END

			END		
		ELSE
			BEGIN
				OPEN CUR_POS
				FETCH NEXT FROM CUR_POS INTO @VPOS_EVAL
				WHILE (@@FETCH_STATUS=0) BEGIN	

					SELECT TOP 1
							 @POSICION_ID=X.POSICION_ID
							,@POSICION_COD=X.POSICION_COD
							,@NAVE_ID=X.NAVE_ID
							,@ORDEN_LOCATOR=X.ORDENLOCATOR
							,@CASO=X1
					FROM(
						SELECT 	 P.POSICION_ID  AS POSICION_ID
								,POSICION_COD AS POSICION_COD
								,NULL AS NAVE_ID
								,ISNULL(ORDEN_LOCATOR,99999) AS ORDENLOCATOR
								,1 AS X1
						FROM 	POSICION P 
								LEFT JOIN RL_POSICION_PROHIBIDA_CLIENTE PROH --AGREGUE ESTO 30-07-2009
								ON (P.POSICION_ID = PROH.POSICION_ID) --AGREGUE ESTO 30-07-2009
								LEFT JOIN RL_DET_DOC_TRANS_POSICION TP --AGREGUE ESTO 31-07-2009
								ON (P.POSICION_ID = TP.POSICION_ACTUAL) --AGREGUE ESTO 31-07-2009
						WHERE	P.POS_LOCKEADA='0'
								AND ((@FLG_VOL_PESO='0') OR([dbo].[TRANSFERENCIA_CONTENEDOR_PESO](@DOCUMENTO_ID,@CONTENEDORA,P.POSICION_ID)='1'))
								AND	((@FLG_VOL_PESO='0') OR ([dbo].[TRANSFERENCIA_CONTENEDOR_VOLUMEN](@DOCUMENTO_ID,@CONTENEDORA,P.POSICION_ID)='1'))
								AND PROH.POSICION_ID IS NULL --AGREGUE ESTO 30-07-2009
								--AND TP.POSICION_ACTUAL IS NULL --AGREGUE ESTO 31-07-2009
								AND P.POSICION_ID=@VPOS_EVAL
						UNION ALL
						SELECT 	 
								 NULL AS POSICION_ID
								,NAVE_COD AS POSICION_COD
								,NAVE_ID  AS NAVE_ID
								,ISNULL(ORDEN_LOCATOR,99999) AS ORDENLOCATOR
								,0 AS X1
						FROM 	NAVE N
						WHERE	N.DISP_INGRESO='1' AND N.PRE_INGRESO='0' 
								AND PRE_EGRESO='0'
								AND NAVE_TIENE_LAYOUT='0'
								AND N.NAVE_ID=@VPOS_EVAL

					)AS X
					ORDER BY X.ORDENLOCATOR
	                 
					FETCH NEXT FROM CUR_POS INTO @VPOS_EVAL
					
					IF @POSICION_COD IS NOT NULL BEGIN
						BREAK
					END
					
				END --FIN (@@FETCH_STATUS=0) OR (@POSICION_ID IS NOT NULL)
					

				IF @CASO=1
				BEGIN
	
					IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
					BEGIN
						RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PRODUCTO.',16,1)
					END
					ELSE
					BEGIN

						SET @POS_COD=@POSICION_COD

					END
				END
				ELSE
				BEGIN
					IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
					BEGIN
						RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PRODUCTO.',16,1)
					END
					ELSE
					BEGIN
						SET @POS_COD=@POSICION_COD
					END
				END

		END
	END

END





GO


