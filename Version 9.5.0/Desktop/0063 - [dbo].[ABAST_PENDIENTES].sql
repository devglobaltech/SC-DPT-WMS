/****** Object:  StoredProcedure [dbo].[ABAST_PENDIENTES]    Script Date: 05/26/2015 13:14:09 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ABAST_PENDIENTES]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ABAST_PENDIENTES]
GO

CREATE  PROCEDURE [dbo].[ABAST_PENDIENTES]
@P_CLIENTE_ID	VARCHAR(100) OUTPUT,
@P_PRODUCTO_ID	VARCHAR(100) OUTPUT,
@P_POSICION_COD	VARCHAR(100) OUTPUT
AS
BEGIN

		SELECT  '0'                         AS CHK,
                P.POSICION_COD              AS POSICION_COD,
                P.ORDEN_LOCATOR             AS PRIORIDAD,
                R.CLIENTE_ID				AS CLIENTE_ID,
                R.PRODUCTO_ID               AS PRODUCTO_ID,
                PR.DESCRIPCION				AS DESCRIPCION,
                ISNULL(X.QTY_POS,0)         AS CANT_STOCK,
                --(R.OCUPACION_MAX-ISNULL(X.QTY_POS,0)) C
                DBO.ABAST_CONTROL_VOL_PESO((R.OCUPACION_MAX-ISNULL(X.QTY_POS,0)),R.CLIENTE_ID,R.PRODUCTO_ID,R.POSICION_ID) 
											AS CANT_REPONER             
        FROM    RL_PRODUCTO_POSICION_PERMITIDA R INNER JOIN POSICION P
                ON(R.POSICION_ID=P.POSICION_ID)
                INNER JOIN PRODUCTO PR
                ON(R.CLIENTE_ID = PR.CLIENTE_ID AND R.PRODUCTO_ID =PR.PRODUCTO_ID)
                LEFT JOIN (SELECT   dd.cliente_id
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
                INNER JOIN RL_SYS_CLIENTE_USUARIO RCU 
                ON(R.CLIENTE_ID =RCU.CLIENTE_ID)
        WHERE   P.ABASTECIBLE='1'
                AND ((@P_CLIENTE_ID IS NULL)OR(R.CLIENTE_ID=@P_CLIENTE_ID))
                AND ((@P_PRODUCTO_ID IS NULL)OR(R.PRODUCTO_ID=@P_PRODUCTO_ID))
                AND ((@P_POSICION_COD IS NULL)OR(P.POSICION_COD like '%' + @P_POSICION_COD + '%'))
                AND ISNULL(X.QTY_POS,0)<R.OCUPACION_MIN
                AND DBO.ABAST_CONTROL_VOL_PESO((R.OCUPACION_MAX-ISNULL(X.QTY_POS,0)),R.CLIENTE_ID,R.PRODUCTO_ID,R.POSICION_ID) >0
                AND RCU.USUARIO_ID IN(SELECT USUARIO_ID FROM #TEMP_USUARIO_LOGGIN)
                AND NOT EXISTS (SELECT	1
								FROM	DET_ABASTECIMIENTO DA
								WHERE	DA.CLIENTE_ID=R.CLIENTE_ID
										AND DA.PRODUCTO_ID=R.PRODUCTO_ID
										AND DA.POSICION_ID=P.POSICION_ID
										AND DA.FINALIZADO='0')
        ORDER BY
                (R.OCUPACION_MAX-X.QTY_POS) ASC,P.ORDEN_LOCATOR ASC

END


GO


