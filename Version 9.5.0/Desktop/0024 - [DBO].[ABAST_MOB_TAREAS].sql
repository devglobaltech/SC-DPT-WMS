/****** Object:  StoredProcedure [dbo].[ABAST_MOB_TAREAS]    Script Date: 04/24/2015 15:20:35 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ABAST_MOB_TAREAS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ABAST_MOB_TAREAS]
GO

CREATE PROCEDURE [dbo].[ABAST_MOB_TAREAS]
	@USUARIO	VARCHAR(100)
AS
BEGIN	
	--ESTE PROCEDIMIENTO ALMACENADO DEVUELVE LAS TAREAS A TOMAR POR UN USUARIO.
	DECLARE @POSICION_COD		VARCHAR(45)
	DECLARE @PRIORIDAD			NUMERIC(20,0)
	DECLARE	@CLIENTE_ID			VARCHAR(15)
	DECLARE @PRODUCTO_ID		VARCHAR(30)
	DECLARE @CANT_A_REPONER		NUMERIC(20,5)
	DECLARE @ABAST_ID			BIGINT
	DECLARE @DESCRIPCION		VARCHAR(100)
	DECLARE @CONTROL			NUMERIC(20,0)
	
	--PARA NO GENERAR MAS COMPLEJIDAD, DESDE AQUI AVERIGUO SI TIENE PENDIENTES.
	SELECT	@CONTROL=COUNT(D.ABAST_ID) 
	FROM	DET_ABASTECIMIENTO D
	WHERE	USUARIO=@USUARIO
			AND EN_PROGRESO='1' 
			--AND FINALIZADO='0'
			AND EXISTS(	SELECT	1
						FROM	ABAST_CONSUMO_LOCATOR A
						WHERE	A.ABAST_ID=D.ABAST_ID
								AND ISNULL(A.EN_CONTENEDOR,'0')='0')
	
	IF @CONTROL=0 BEGIN		
								
		SELECT	@CONTROL=COUNT(D.ABAST_ID) 
		FROM	DET_ABASTECIMIENTO D
		WHERE	USUARIO=@USUARIO
				AND EN_PROGRESO='0' 
				AND EXISTS(	SELECT	1
							FROM	ABAST_CONSUMO_LOCATOR A
							WHERE	A.ABAST_ID=D.ABAST_ID
									AND ISNULL(A.EN_CONTENEDOR,'0')='0')								
	END									
	IF @CONTROL=0 BEGIN
		-- NO HABIA TAREAS EN PROGRESO O PENDIENTES, ASI QUE TENGO QUE GENERAR UNA.
		SELECT  TOP 1
				@POSICION_COD	=P.POSICION_COD,
				@PRIORIDAD		=P.ORDEN_LOCATOR,
				@CLIENTE_ID		=R.CLIENTE_ID,
				@PRODUCTO_ID	=R.PRODUCTO_ID,
				@CANT_A_REPONER	=(R.OCUPACION_MAX-ISNULL(X.QTY_POS,0))
		FROM    RL_PRODUCTO_POSICION_PERMITIDA R INNER JOIN POSICION P
				ON(R.POSICION_ID=P.POSICION_ID)
				LEFT JOIN (	SELECT   dd.cliente_id
                                    ,dd.producto_id
                                    ,rl.posicion_actual
                                    ,SUM(rl.cantidad) AS Qty_Pos
                            FROM    rl_det_doc_trans_posicion rl
                                    inner join det_documento_transaccion ddt	on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
                                    inner join det_documento dd                 on(ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
                                    inner join categoria_logica cl              on(rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
                                    inner join posicion p                       on(rl.posicion_actual=p.posicion_id)
                                    left join estado_mercaderia_rl em           on(rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)     
                                    inner join documento d                      on(dd.documento_id=d.documento_id)
                            WHERE    rl.doc_trans_id_egr is null
                                    and rl.nro_linea_trans_egr is null
                                    and rl.disponible='1'
                                    and isnull(em.disp_egreso,'1')='1'
                                    and isnull(em.picking,'1')='1'
                                    and p.pos_lockeada='0' 
                                    and p.picking='1'
                                    and cl.disp_egreso='1' 
                                    and cl.picking='1'
                                    and rl.cat_log_id<>'TRAN_EGR'
                            GROUP BY
                                    dd.cliente_id, dd.producto_id, rl.posicion_actual)X
				ON(X.CLIENTE_ID=R.CLIENTE_ID AND X.PRODUCTO_ID=R.PRODUCTO_ID AND R.POSICION_ID=X.POSICION_ACTUAL) 
				INNER JOIN	RL_USUARIO_NAVE RL
				ON(P.NAVE_ID =RL.NAVE_ID AND RL.USUARIO_ID=@USUARIO)	
				INNER JOIN RL_SYS_CLIENTE_USUARIO CU 
				ON(R.CLIENTE_ID=CU.CLIENTE_ID AND CU.USUARIO_ID=@USUARIO)
		WHERE   P.ABASTECIBLE='1'
				AND [dbo].[ABAST_TIENE_STOCK](R.CLIENTE_ID, R.PRODUCTO_ID)='1'
				AND ISNULL(X.QTY_POS,0)<R.OCUPACION_MIN
				AND NOT EXISTS (SELECT	1
								FROM	DET_ABASTECIMIENTO DA
								WHERE	DA.CLIENTE_ID=R.CLIENTE_ID
										AND DA.PRODUCTO_ID=R.PRODUCTO_ID
										AND DA.POSICION_ID=P.POSICION_ID
										AND DA.FINALIZADO='0')
		ORDER BY
				(R.OCUPACION_MAX-X.QTY_POS) ASC,P.ORDEN_LOCATOR ASC

		IF (@CLIENTE_ID IS NOT NULL)AND(@PRODUCTO_ID IS NOT NULL)AND(@POSICION_COD IS NOT NULL) BEGIN
		
			EXEC [DBO].[ABAST_GEN_TAREA]	@CLIENTE_ID, @PRODUCTO_ID, @POSICION_COD, @PRIORIDAD, @USUARIO, @CANT_A_REPONER
			
			SELECT	@ABAST_ID= ABAST_ID
			FROM	DET_ABASTECIMIENTO DA INNER JOIN POSICION P
					ON(DA.POSICION_ID=P.POSICION_ID)
			WHERE	DA.CLIENTE_ID=@CLIENTE_ID
					AND DA.PRODUCTO_ID=@PRODUCTO_ID
					AND P.POSICION_COD=@POSICION_COD
					AND DA.USUARIO=@USUARIO
					
			SELECT	@DESCRIPCION=DESCRIPCION
			FROM	PRODUCTO				
			WHERE	CLIENTE_ID=@CLIENTE_ID
					AND PRODUCTO_ID=@PRODUCTO_ID
			
			SELECT	@CLIENTE_ID		AS CLIENTE_ID, 
					@PRODUCTO_ID	AS PRODUCTO_ID, 
					@DESCRIPCION	AS DESCRIPCION,
					@POSICION_COD	AS POSICION_COD, 
					@PRIORIDAD		AS PRIORIDAD, 
					@ABAST_ID		AS ABAST_ID
		END
		ELSE
		BEGIN
			RAISERROR('Usted no posee tareas de abastecimiento para realizar.',16,1);
			RETURN;
		END
	END --FIN CASO EN EL QUE NO TIENE TAREAS TOMADAS.
	ELSE
	BEGIN
		--EL TIPO TENIA TAREAS GENERADAS ASI QUE RETOMO LA TAREA.
		SELECT	TOP 1
				DA.CLIENTE_ID,
				DA.PRODUCTO_ID,
				P.DESCRIPCION,
				PS.POSICION_COD,
				DA.PRIORIDAD,
				DA.ABAST_ID
		FROM	DET_ABASTECIMIENTO DA INNER JOIN PRODUCTO P
				ON(DA.CLIENTE_ID=P.CLIENTE_ID AND DA.PRODUCTO_ID=P.PRODUCTO_ID)
				INNER JOIN POSICION PS
				ON(DA.POSICION_ID=PS.POSICION_ID)
		WHERE	USUARIO=@USUARIO 
				--AND EN_PROGRESO='1' 
				AND FINALIZADO='0'	
				AND NOT EXISTS(	SELECT	1
								FROM	ABAST_CONSUMO_LOCATOR A
								WHERE	A.ABAST_ID=DA.ABAST_ID
										AND ISNULL(A.EN_CONTENEDOR,'0')='1')
		ORDER BY
				DA.PRIORIDAD ASC
		
	END
END            

GO


