/****** Object:  StoredProcedure [dbo].[Mob_Guardado_Loc]    Script Date: 01/15/2015 10:54:26 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Guardado_Loc]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Guardado_Loc]
GO

CREATE                          PROCEDURE [dbo].[Mob_Guardado_Loc]
@DOCUMENTO_ID	AS NUMERIC(20),
@NRO_LINEA		AS NUMERIC(20),
@POS_ID			AS NUMERIC(20,0)	OUTPUT,
@POS_COD		AS VARCHAR(45)		OUTPUT,
@NAV_COD		AS VARCHAR(45)		OUTPUT,
@NAV_ID			AS NUMERIC(20,0)	OUTPUT,
@QTY_UBICACION	AS NUMERIC(20,5)	OUTPUT
AS

DECLARE @CLIENTEID 	AS VARCHAR(15)
DECLARE @PRODUCTOID AS VARCHAR(30)
DECLARE @VCANT 		AS NUMERIC(20)

----VARIABLES PARA HACER INSERT
DECLARE @POSICION_ID 			AS NUMERIC(20,0)
DECLARE @POSICION_COD 		AS VARCHAR(45)
DECLARE @NAVE_ID				AS NUMERIC(20,0)
DECLARE @ORDEN_LOCATOR 		AS NUMERIC(6)
DECLARE @CASO				AS INT

BEGIN
	

	IF @DOCUMENTO_ID IS NULL
	BEGIN
			RAISERROR ('EL PARAMETRO @DOCUMENTO_ID NO PUEDE SER NULO. SQLSERVER', 16, 1)
	END
	IF @NRO_LINEA IS NULL
	BEGIN
			RAISERROR ('EL PARAMETRO @NRO_LINEA NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END


	DELETE FROM SYS_LOCATOR_ING WHERE POSICION_ID IS NULL AND NAVE_ID IS NULL


	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ


	SELECT 	@CLIENTEID=DD.CLIENTE_ID,@PRODUCTOID=DD.PRODUCTO_ID
	FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
			ON (DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN NAVE N
			ON(RL.NAVE_ACTUAL=N.NAVE_ID)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.NRO_LINEA=@NRO_LINEA
			AND D.STATUS IN('D30','D35')
			AND (N.PRE_INGRESO='1' OR N.INTERMEDIA='1')

	BEGIN
		SELECT 	@VCANT=COUNT(*)
		FROM 	RL_PRODUCTO_POSICION_PERMITIDA
		WHERE 	CLIENTE_ID=@CLIENTEID AND PRODUCTO_ID=@PRODUCTOID
				

		IF @VCANT > 0
			BEGIN
				SELECT TOP 1
						 @POSICION_ID=X.POSICION_ID
						,@POSICION_COD=X.POSICION_COD
						,@NAVE_ID=X.NAVE_ID
						,@ORDEN_LOCATOR=X.ORDENLOCATOR
						,@CASO=X1
						,@QTY_UBICACION=X.QTY 
				FROM(
					SELECT 	 
							 P.POSICION_ID  AS POSICION_ID
							,P.POSICION_COD AS POSICION_COD
							,NULL AS NAVE_ID
							,ISNULL(P.ORDEN_LOCATOR,99999) AS ORDENLOCATOR
							,1 AS X1
							,DBO.GUARDADO_SC_INGRESO_VOLUMEN_PESO(@DOCUMENTO_ID,@NRO_LINEA,P.POSICION_ID) AS QTY
					FROM 	POSICION P INNER JOIN
							RL_PRODUCTO_POSICION_PERMITIDA RLPP
							ON(P.POSICION_ID=RLPP.POSICION_ID)
							LEFT JOIN RL_POSICION_PROHIBIDA_CLIENTE PROH --AGREGUE ESTO 30-07-2009
							ON (P.POSICION_ID = PROH.POSICION_ID) --AGREGUE ESTO 30-07-2009
					WHERE	1=1 AND P.POS_LOCKEADA='0'
							AND RLPP.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID))) 
							AND RLPP.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
							AND PROH.POSICION_ID IS NULL --AGREGUE ESTO 30-07-2009
							AND DBO.GUARDADO_SC_INGRESO_VOLUMEN_PESO(@DOCUMENTO_ID,@NRO_LINEA,P.POSICION_ID)>0
					UNION ALL
				
					SELECT 	 
							 NULL AS 	POSICION_ID
							,N.NAVE_COD AS POSICION_COD
							,N.NAVE_ID  AS NAVE_ID
							,ISNULL(N.ORDEN_LOCATOR,99999) AS ORDENLOCATOR
							,0 AS X1
							,DBO.GUARDADO_SC_INGRESO_VOLUMEN_PESO(@DOCUMENTO_ID,@NRO_LINEA,99999999) AS QTY
					FROM 	NAVE N INNER JOIN
							RL_PRODUCTO_POSICION_PERMITIDA RLPP
							ON(N.NAVE_ID=RLPP.NAVE_ID)
					WHERE	N.DISP_INGRESO='1' AND N.PRE_INGRESO='0' 
							AND PRE_EGRESO='0'
							AND RLPP.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID)))
							AND RLPP.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
				
				)AS X
				ORDER BY X.ORDENLOCATOR ASC
				BEGIN TRANSACTION
				IF @CASO=1
				BEGIN
					
					DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
					IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
					BEGIN
						RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PRODUCTO.',16,1)
					END
					ELSE
					BEGIN
						SET @POS_ID=@POSICION_ID
						SET @POS_COD=@POSICION_COD
						SET @NAV_COD=NULL
						SET @NAV_ID=@NAVE_ID
					END
				END
				ELSE
				BEGIN
					DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
					IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
					BEGIN
						RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PRODUCTO.',16,1)
					END
					ELSE
					BEGIN
						SET @POS_ID=@POSICION_ID
						SET @POS_COD=@POSICION_COD
						SET @NAV_COD=@POSICION_COD
						SET @NAV_ID=@NAVE_ID
					END

				END

				COMMIT TRANSACTION

			END		
		ELSE
			BEGIN

					SELECT TOP 1
							 @POSICION_ID=X.POSICION_ID
							,@POSICION_COD=X.POSICION_COD
							,@NAVE_ID=X.NAVE_ID
							,@ORDEN_LOCATOR=X.ORDENLOCATOR
							,@CASO=X1
							,@QTY_UBICACION=X.QTY 
					FROM(
						SELECT 	 P.POSICION_ID  AS POSICION_ID
								,POSICION_COD AS POSICION_COD
								,NULL AS NAVE_ID
								,ISNULL(ORDEN_LOCATOR,99999) AS ORDENLOCATOR
								,1 AS X1
								,DBO.GUARDADO_SC_INGRESO_VOLUMEN_PESO(@DOCUMENTO_ID,@NRO_LINEA,P.POSICION_ID) AS QTY
						FROM 	POSICION P 
								LEFT JOIN RL_POSICION_PROHIBIDA_CLIENTE PROH --AGREGUE ESTO 30-07-2009
								ON (P.POSICION_ID = PROH.POSICION_ID) --AGREGUE ESTO 30-07-2009
								LEFT JOIN RL_DET_DOC_TRANS_POSICION TP --AGREGUE ESTO 31-07-2009
								ON (P.POSICION_ID = TP.POSICION_ACTUAL) --AGREGUE ESTO 31-07-2009
						WHERE	P.POS_LOCKEADA='0'
								AND PROH.POSICION_ID IS NULL --AGREGUE ESTO 30-07-2009
								AND TP.POSICION_ACTUAL IS NULL --AGREGUE ESTO 31-07-2009
								AND DBO.GUARDADO_SC_INGRESO_VOLUMEN_PESO(@DOCUMENTO_ID,@NRO_LINEA,P.POSICION_ID)>0
						UNION ALL
						SELECT 	 
								 NULL AS POSICION_ID
								,NAVE_COD AS POSICION_COD
								,NAVE_ID  AS NAVE_ID
								,ISNULL(ORDEN_LOCATOR,99999) AS ORDENLOCATOR
								,0 AS X1
								,DBO.GUARDADO_SC_INGRESO_VOLUMEN_PESO(@DOCUMENTO_ID,@NRO_LINEA,99999999) AS QTY
						FROM 	NAVE N
						WHERE	N.DISP_INGRESO='1' AND N.PRE_INGRESO='0' 
								AND PRE_EGRESO='0'
								AND NAVE_TIENE_LAYOUT='0'

					)AS X
					ORDER BY X.ORDENLOCATOR
				BEGIN TRANSACTION
				IF @CASO=1
				BEGIN
					DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
					IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
					BEGIN
						RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PRODUCTO.',16,1)
					END
					ELSE
					BEGIN
						SET @POS_ID=@POSICION_ID
						SET @POS_COD=@POSICION_COD
						SET @NAV_COD=@POSICION_COD
						SET @NAV_ID=@NAVE_ID
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
						SET @POS_ID=@POSICION_ID
						SET @POS_COD=@POSICION_COD
						SET @NAV_COD=@POSICION_COD
						SET @NAV_ID=@NAVE_ID
					END
				END
				COMMIT TRANSACTION
		END
	END

END

GO

