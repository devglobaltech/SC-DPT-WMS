
ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#ACT_STOCK]
@P_INVENTARIO_ID AS NUMERIC(20) OUTPUT,
@P_MARBETE AS INT
AS
BEGIN
	BEGIN TRY
		DECLARE @V_CAT_LOG      AS INT
		DECLARE @V_FOM_PROD     AS INT
		DECLARE @V_NUM_STOCK    AS TINYINT
		DECLARE @V_PRODUCTO_ID  AS VARCHAR(100)
		DECLARE @V_CLIENTE_ID   AS VARCHAR(100)
		DECLARE @V_POSICION_ID  AS NUMERIC(20)
		DECLARE @V_NAVE_ID      AS NUMERIC(20)
		DECLARE @V_CANT         AS NUMERIC(20,5)
		DECLARE @V_NRO_LOTE     AS VARCHAR(100)
		DECLARE @V_NRO_PARTIDA  AS VARCHAR(100)


		SELECT @V_CAT_LOG=COUNT(*)        FROM DET_INVENTARIO_CAT_LOG   WHERE INVENTARIO_ID = @P_INVENTARIO_ID
		SELECT @V_FOM_PROD=COUNT(*)       FROM DET_INVENTARIO_FAM_PROD  WHERE INVENTARIO_ID = @P_INVENTARIO_ID
		SELECT @V_NUM_STOCK = NRO_CONTEO  FROM INVENTARIO               WHERE INVENTARIO_ID = @P_INVENTARIO_ID
    
		IF @P_MARBETE <> 0 
    BEGIN 
		  SELECT  @V_PRODUCTO_ID = PRODUCTO_ID,@V_CLIENTE_ID = CLIENTE_ID, @V_POSICION_ID = POSICION_ID, @V_NAVE_ID = NAVE_ID, @V_NRO_LOTE = NRO_LOTE, @V_NRO_PARTIDA = NRO_PARTIDA
			FROM    DET_INVENTARIO 
			WHERE   INVENTARIO_ID = @P_INVENTARIO_ID AND MARBETE = @P_MARBETE
		END



		UPDATE  DET_INVENTARIO 
    SET     CANT_STOCK_CONT_1 = CASE WHEN @V_NUM_STOCK = 1 THEN X.CANTIDAD ELSE CANT_STOCK_CONT_1 END,
						CANT_STOCK_CONT_2 = CASE WHEN @V_NUM_STOCK = 2 THEN X.CANTIDAD ELSE CANT_STOCK_CONT_2 END,
						CANT_STOCK_CONT_3 = CASE WHEN @V_NUM_STOCK = 3 THEN X.CANTIDAD ELSE CANT_STOCK_CONT_3 END
		FROM    DET_INVENTARIO DI 
            INNER JOIN (  SELECT  XX.nave_id,                      
					                        XX.posicion_id,                   
                                  XX.cliente_id,                    
                                  XX.producto_id,                  
                                  max(XX.pos_lockeada) as pos_lockeada,                  
                                  sum(XX.cantidad) AS cantidad,
                                  XX.nro_lote,
					                        XX.nro_partida 
                          FROM (  SELECT  DISTINCT
							                            nave.nave_id,                      
							                            pos.posicion_id,                   
                                          cli.cliente_id,                    
                                          prod.producto_id,                  
                                          pos.pos_lockeada,                  
                                          sum(ex.cantidad) AS cantidad,
                                          case when isnull(pr.ingLoteProveedor,'0') ='1'  then det_doc.nro_lote     else '9999XX' end as nro_lote,
                                          case when isnull(pr.ingpartida,'0')       ='1'  then det_doc.nro_partida  else '9999XX' end as nro_partida
						                      FROM    documento doc                                          
                                          INNER JOIN det_documento det_doc                ON (doc.documento_id=det_doc.documento_id) 
                                          INNER JOIN cliente cli                          ON (det_doc.cliente_id=cli.cliente_id)               
                                          INNER JOIN det_documento_transaccion det_doc_t  ON (det_doc.documento_id=det_doc_t.documento_id AND det_doc.nro_linea=det_doc_t.nro_linea_doc) 
                                          INNER JOIN documento_transaccion dt             ON (det_doc_t.doc_trans_id=dt.doc_trans_id) 
                                          INNER JOIN producto prod                        ON (det_doc.cliente_id=prod.cliente_id AND det_doc.producto_id=prod.producto_id) 
                                          INNER JOIN familia_producto flia                ON (prod.familia_id=flia.familia_id) 
                                          INNER JOIN rl_det_doc_trans_posicion ex         ON (det_doc_t.doc_trans_id=ex.doc_trans_id AND det_doc_t.nro_linea_trans=ex.nro_linea_trans) 
                                          LEFT  JOIN categoria_logica cat                 ON ex.cliente_id=cat.cliente_id AND ex.cat_log_id=cat.cat_log_id 
                                          LEFT  JOIN posicion pos                         ON ex.posicion_actual=pos.posicion_id 
                                          LEFT  JOIN nave nave                            on ex.nave_actual=nave.nave_id 
                                          INNER JOIN PRODUCTO Pr                          ON(det_doc.cliente_id=pr.cliente_id and det_doc.producto_id=pr.producto_id)
						                      WHERE   ((ex.nave_actual not in (select nave_id from nave where nave_cod = 'PRE-EGRESO')) or ( ex.nave_actual is null))
						                              AND (@V_CAT_LOG   = 0     OR ex.cat_log_id IN (SELECT DCL.CAT_LOG_ID FROM DET_INVENTARIO_CAT_LOG DCL WHERE DCL.INVENTARIO_ID = @P_INVENTARIO_ID AND DCL.CLIENTE_ID = EX.CLIENTE_ID))
						                              AND (@V_FOM_PROD  = 0     OR PROD.FAMILIA_ID IN (SELECT FAM.FAMILIA_ID FROM DET_INVENTARIO_FAM_PROD FAM WHERE FAM.INVENTARIO_ID = @P_INVENTARIO_ID))
						                              AND (@P_MARBETE   = 0     OR (PROD.PRODUCTO_ID = @V_PRODUCTO_ID AND EX.CLIENTE_ID = @V_CLIENTE_ID AND (NAVE.NAVE_ID = @V_NAVE_ID OR POS.POSICION_ID = @V_POSICION_ID)
								                          AND (@V_NRO_LOTE IS NULL  OR DET_DOC.NRO_LOTE = @V_NRO_LOTE)	
								                          AND (@V_NRO_PARTIDA IS NULL OR DET_DOC.NRO_PARTIDA = @V_NRO_PARTIDA)))
						                      GROUP BY 
                                          nave.nave_id, pos.posicion_id, cli.cliente_id, prod.producto_id, pos.pos_lockeada,
                                          case when isnull(pr.ingLoteProveedor,'0') ='1'  then det_doc.nro_lote     else '9999XX' end,
                                          case when isnull(pr.ingpartida,'0')       ='1'  then det_doc.nro_partida  else '9999XX' end
                                  ------------------------------------------------------------------------------
						                      --MERCADERIA ASIGNADA NO PICKEADA 
                                  ------------------------------------------------------------------------------
						                      UNION ALL
						                      SELECT  rl.nave_anterior as nave,                                
                                          rl.posicion_anterior as posicion,                            
                                          dd.cliente_id,                                           
                                          dd.producto_id, 0 as pos_lockeada,                    
                                          sum(dd.cantidad - (ISNULL(P.CANT_CONFIRMADA,0))) AS CANTIDAD,
                                          case when isnull(pr.ingLoteProveedor,'0') ='1'  then dd.nro_lote    else '9999XX' end as nro_lote,
                                          case when isnull(pr.ingpartida,'0')       ='1'  then dd.nro_partida else '9999XX' end as nro_partida
										              FROM    det_documento_transaccion ddt                                
                                          inner join det_documento dd                     on (ddt.documento_id = dd.documento_id And ddt.nro_linea_doc = dd.nro_linea)   
                                          inner join rl_det_doc_trans_posicion rl         on (rl.doc_trans_id_egr = ddt.doc_trans_id And rl.nro_linea_trans_egr = ddt.nro_linea_trans)  
                                          inner join documento d                          on (d.documento_id  =dd.documento_id)                                                                          
                                          inner join categoria_logica cl                  on (rl.cliente_id = cl.cliente_id And rl.cat_log_id = cl.cat_log_id) 
                                          inner join producto pr                          on (dd.CLIENTE_ID=pr.cliente_id and dd.PRODUCTO_ID=pr.PRODUCTO_ID) 
                                          INNER JOIN (  SELECT DOCUMENTO_ID, NRO_LINEA, SUM(CANT_CONFIRMADA) AS CANT_CONFIRMADA     
															                          FROM PICKING GROUP BY DOCUMENTO_ID, NRO_LINEA) P                          
                                                                                          ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)                  
										              WHERE   d.status = 'D30'                                                                     
												                  and nave_actual in (select nave_id from nave where nave_cod = 'PRE-EGRESO')             
												                  and cl.categ_stock_id = 'TRAN_EGR'                                                      
										              group by 
                                          rl.nave_anterior,rl.posicion_anterior,dd.cliente_id,dd.producto_id,
                                          case when isnull(pr.ingLoteProveedor,'0') ='1'  then dd.nro_lote     else '9999XX' end,
                                          case when isnull(pr.ingpartida,'0')       ='1'  then dd.nro_partida  else '9999XX' end
                                  ) XX
					                GROUP BY  XX.nave_id, XX.posicion_id, XX.cliente_id, XX.producto_id,  XX.nro_lote,	 XX.nro_partida
                          )
				    X ON (DI.PRODUCTO_ID = X.PRODUCTO_ID AND DI.CLIENTE_ID = X.CLIENTE_ID AND (DI.NAVE_ID = X.NAVE_ID OR DI.POSICION_ID=X.POSICION_ID)
                  AND (DI.NRO_LOTE IS NULL OR DI.NRO_LOTE = X.NRO_LOTE)
						      AND (DI.NRO_PARTIDA IS NULL OR DI.NRO_PARTIDA = X.NRO_PARTIDA))

		WHERE   (@P_MARBETE = 0 OR DI.MARBETE = @P_MARBETE)
				    AND DI.INVENTARIO_ID = @P_INVENTARIO_ID
				    AND ((@V_NUM_STOCK = 1 AND DI.CANT_STOCK_CONT_1 IS NULL)OR(@V_NUM_STOCK = 2 AND DI.CANT_STOCK_CONT_2 IS NULL)OR(@V_NUM_STOCK = 3 AND DI.CANT_STOCK_CONT_3 IS NULL))
            
            
		--ACA COLOCO O (CERO) SI LA CONSULTA ANTERIOR NO ENCONTRO NADA EN LA POSICION, PARA QUE NO QUEDE NULL
		UPDATE DET_INVENTARIO SET CANT_STOCK_CONT_1 = CASE WHEN @V_NUM_STOCK = 1 THEN 0 ELSE CANT_STOCK_CONT_1 END,
								  CANT_STOCK_CONT_2 = CASE WHEN @V_NUM_STOCK = 2 THEN 0 ELSE CANT_STOCK_CONT_2 END,
								  CANT_STOCK_CONT_3 = CASE WHEN @V_NUM_STOCK = 3 THEN 0 ELSE CANT_STOCK_CONT_3 END 
		WHERE (@P_MARBETE = 0 OR MARBETE = @P_MARBETE)
		AND INVENTARIO_ID = @P_INVENTARIO_ID
		AND MODO_INGRESO = 'S'
		AND ((@V_NUM_STOCK = 1 AND CANT_STOCK_CONT_1 IS NULL)
			OR(@V_NUM_STOCK = 2 AND CANT_STOCK_CONT_2 IS NULL)
			OR(@V_NUM_STOCK = 3 AND CANT_STOCK_CONT_3 IS NULL))



	
	END TRY
	BEGIN CATCH
		EXEC USP_RETHROWERROR
	END CATCH
END

GO
