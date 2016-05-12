/****** Object:  StoredProcedure [dbo].[FILL_SALDOS_DEP1]    Script Date: 10/10/2013 14:55:11 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[FILL_SALDOS_DEP1]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[FILL_SALDOS_DEP1]
GO

CREATE PROCEDURE [dbo].[FILL_SALDOS_DEP1]
@CLIENTE	VARCHAR(15)=NULL,
@PRODUCTO	VARCHAR(30)=NULL
AS  
BEGIN  
	SET NOCOUNT ON 
	----------Tracker id:4188 
	update STOCK_BY_DEP set DEP1=0,DEP2=0,DEP3=0,DEP4=0,DEP5=0
	where	((@CLIENTE IS NULL) OR(cliente_id=@CLIENTE))
			AND ((@PRODUCTO IS NULL) OR (producto_id=@PRODUCTO));
	----------Tracker id:4188
	 --actualizo catalogo de productos de la tabla 
	INSERT INTO STOCK_BY_DEP  
	SELECT CLIENTE_ID, PRODUCTO_ID, 0, 0, 0, 0, 0, NULL, NULL, NULL   
	FROM	PRODUCTO P  
	WHERE	NOT EXISTS (SELECT	1  
						FROM	STOCK_BY_DEP S  
						WHERE	S.CLIENTE_ID=P.CLIENTE_ID  
								AND S.PRODUCTO_ID=P.PRODUCTO_ID) 
			AND ((@CLIENTE IS NULL) OR(P.CLIENTE_ID=@CLIENTE))
			AND ((@PRODUCTO IS NULL) OR(P.PRODUCTO_ID=@PRODUCTO))
   
	--actualizo stock 
	UPDATE STOCK_BY_DEP   
	SET  DEP1= X.CANTIDAD  
	FROM (	select ISNULL(sum(x.cantidad),0)CANTIDAD, X.CLIENTE_ID, X.PRODUCTO_ID  
			from ( SELECT	dd.cliente_id ,dd.producto_id ,rl.cantidad  
					FROM	rl_det_doc_trans_posicion rl  
							inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)  
							inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)  
							inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )  
							inner join posicion p on (rl.posicion_actual=p.posicion_id)   
							inner join documento d on(dd.documento_id=d.documento_id)  
					WHERE	rl.doc_trans_id_egr is null	and rl.nro_linea_trans_egr is null  
							and rl.disponible='1'		and p.pos_lockeada='0'   
							and p.picking='1'			and cl.disp_egreso='1'   
							and cl.picking='1'			and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso  
					UNION ALL
					--esto traera los que estan disponibles pero no hubicados?  
					SELECT	dd.cliente_id ,dd.producto_id ,rl.cantidad  
					FROM	rl_det_doc_trans_posicion rl  
							inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)  
							inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)  
							inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )  
							inner join nave n on (rl.nave_actual=n.nave_id)  
							inner join documento d on(dd.documento_id=d.documento_id)  
					WHERE	rl.doc_trans_id_egr is null	and rl.nro_linea_trans_egr is null  
							and rl.disponible='1'		and rl.cat_log_id<>'TRAN_EGR'  
							and n.disp_egreso='1'		and n.pre_egreso='0'   
							and n.pre_ingreso='0'		and n.picking='1'  
							and cl.disp_egreso='1'		and cl.picking='1'
					UNION ALL		
					SELECT	dd.cliente_id ,dd.producto_id ,0  
					FROM	rl_det_doc_trans_posicion rl  
							inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)  
							inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)  
							inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )  
							inner join posicion p on (rl.posicion_actual=p.posicion_id)   
							inner join documento d on(dd.documento_id=d.documento_id)           
			)x  
		group by  
			x.cliente_id, x.producto_id
		)X INNER JOIN STOCK_BY_DEP		ON(X.CLIENTE_ID=STOCK_BY_DEP.CLIENTE_ID AND X.PRODUCTO_ID=STOCK_BY_DEP.PRODUCTO_ID)  
	where	((@CLIENTE IS NULL) OR(x.cliente_id=@CLIENTE))
			AND ((@PRODUCTO IS NULL) OR (x.producto_id=@PRODUCTO));

END

GO


