
/****** Object:  StoredProcedure [dbo].[QRY_RAPIDA_X_FILTROS_MULTIPLES]    Script Date: 10/19/2013 11:20:30 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[QRY_RAPIDA_X_FILTROS_MULTIPLES]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[QRY_RAPIDA_X_FILTROS_MULTIPLES]
GO

create procedure [dbo].[QRY_RAPIDA_X_FILTROS_MULTIPLES]
@PREDICADO	VARCHAR(4000) OUTPUT
AS
BEGIN
	DECLARE @SQL1 VARCHAR(4000)
	DECLARE @SQL2 VARCHAR(4000)
	DECLARE @SQL3 VARCHAR(4000)


	SET @SQL1 = '	SELECT * 
					FROM (	SELECT	''EN PROCESO DE EGRESO'' AS ESTADO,
									dd.cliente_id AS CLIENTE_ID,
									PK.VIAJE_ID AS[CODIGO DE VIAJE],
									D.NRO_REMITO AS PEDIDO,
									rl.cat_log_id as [ESTADO LOGICO],
									pk.POSICION_COD AS [Posicion],
									dd.producto_id AS [Cod. Producto],
									PRO.DESCRIPCION,
									0  AS [CANTIDAD EN STOCK],
									Sum(PK.cantidad) AS [CANTIDAD ENVIADA A PICKEAR],
									SUM(PK.CANT_CONFIRMADA) AS [CANTIDAD PICKEADA],
									PK.usuario_pick AS [USUARIO PICKING],
									PK.fecha_pick AS [FECHA PICKING],
									PK.NRO_UCDESCONSOLIDACION AS [NRO DESCONSOLIDACION],
									PK.Fecha_Desconsilidacion AS [FECHA DESCONSOLIDACION],
									PK.usuario_desconsolidacion AS [USUARIO DESCONSOLIDACION],
									PK.NRO_UCEMPAQUETADO AS [NRO EMPAQUETADO], 
									PK.Fecha_empaquetado AS [FECHA EMPAQUETADO],
									PK.NRO_GUIA [NRO. GUIA],
									PK.NRO_HOJACARGA [NRO. HOJA CARGA],
									PK.FECHA_CONTROL_EXP [FECHA CONTROL EXP.]
							FROM	det_documento_transaccion ddt
									INNER JOIN det_documento dd ON (ddt.documento_id = dd.documento_id And ddt.nro_linea_doc = dd.nro_linea)
									INNER JOIN PRODUCTO PRO ON (DD.CLIENTE_ID = PRO.CLIENTE_ID AND DD.PRODUCTO_ID = PRO.PRODUCTO_ID)
									INNER JOIN rl_det_doc_trans_posicion rl ON (rl.doc_trans_id_egr = ddt.doc_trans_id And rl.nro_linea_trans_egr = ddt.nro_linea_trans)
									INNER JOIN documento d ON (dd.documento_id = d.documento_id)
									INNER JOIN categoria_logica cl ON (rl.cat_log_id = cl.cat_log_id AND CL.cliente_id = DD.cliente_id)
									INNER JOIN (SELECT	p.VIAJE_ID, P.DOCUMENTO_ID, P.NRO_LINEA,p.POSICION_COD,CONVERT(VARCHAR(19), MAX(p.fecha_fin), 120) as fecha_pick ,
														P.PRODUCTO_ID,SUM(P.CANTIDAD) AS CANTIDAD, SUM(P.CANT_CONFIRMADA) as  CANT_CONFIRMADA,P.USUARIO as usuario_pick, P.NRO_UCEMPAQUETADO,
														CONVERT(VARCHAR(19), max(p.FECHA_UCEMPAQUETADO), 120) as Fecha_empaquetado ,P.NRO_UCDESCONSOLIDACION,CONVERT(VARCHAR(19), 
														max(p.Fecha_Desconsolidacion), 120) as Fecha_Desconsilidacion,p.usuario_desconsolidacion ,EM.NRO_GUIA, EM.NRO_HOJACARGA,  
														MAX(P.FECHA_CONTROL_EXP) AS FECHA_CONTROL_EXP  
												FROM	PICKING P
														LEFT JOIN UC_EMPAQUE EM ON (P.NRO_UCEMPAQUETADO = EM.UC_EMPAQUE)
												GROUP BY 
														p.VIAJE_ID,P.DOCUMENTO_ID, P.NRO_LINEA, p.POSICION_COD, P.PRODUCTO_ID,P.USUARIO, P.NRO_UCEMPAQUETADO,P.NRO_UCDESCONSOLIDACION,
														p.usuario_desconsolidacion ,EM.NRO_GUIA, EM.NRO_HOJACARGA)
									PK ON (PK.DOCUMENTO_ID = DD.DOCUMENTO_ID AND PK.NRO_LINEA = DD.NRO_LINEA)
							WHERE	d.status = ''D30''
									And cl.categ_stock_id = ''TRAN_EGR''
					GROUP BY dd.cliente_id,PK.VIAJE_ID,D.NRO_REMITO, pk.POSICION_COD, dd.producto_id, PRO.DESCRIPCION,rl.cat_log_id ,
					PK.fecha_pick,PK.Fecha_empaquetado,PK.Fecha_Desconsilidacion,PK.usuario_desconsolidacion,
					cl.categ_stock_id,PK.usuario_pick, PK.NRO_UCEMPAQUETADO, PK.NRO_UCDESCONSOLIDACION, PK.NRO_GUIA, PK.NRO_HOJACARGA, PK.FECHA_CONTROL_EXP '
		 
SET @SQL2=	 ' UNION ALL
		 SELECT    ''EN STOCK'' AS ESTADO
				  ,dd.cliente_id AS ClienteID 
				  ,null AS[CODIGO DE VIAJE]
				  ,null AS PEDIDO
				  ,rl.cat_log_id as CategLogID 
				  ,isnull(p.POSICION_COD,n.NAVE_COD) as POSICION_COD		  
				  ,dd.producto_id AS ProductoID 
				  ,prod.DESCRIPCION 
				  ,sum(ISNULL(rl.cantidad,0)) AS [CANTIDAD EN STOCK]
				  ,NULL AS [CANTIDAD ENVIADA A PICKEAR]
				  ,NULL AS [CANTIDAD PICKEADA]
				  ,NULL AS [USUARIO PICKING]
				 ,NULL AS [FECHA_PICKING]
				 ,NULL AS [NRO DESCONSOLIDACION]
				 ,NULL AS [FECHA DESCONSOLIDACION]
				 ,NULL AS [USUARIO DESCONSOLIDACION]
				 ,NULL AS [NRO EMPAQUETADO]
				 ,NULL AS [FECHA_EMPQQUETADO]
				 ,NULL AS NRO_GUIA
				 ,NULL AS NRO_HOJACARGA
				 ,NULL AS FECHA_CONTROL_EXP
			FROM  rl_det_doc_trans_posicion rl (NoLock)
				  LEFT OUTER JOIN nave n (NoLock)            on rl.nave_actual = n.nave_id 
				  LEFT OUTER JOIN posicion p  (NoLock)       on rl.posicion_actual = p.posicion_id 
				  LEFT OUTER JOIN nave n2   (NoLock)         on p.nave_id = n2.nave_id 
				  ,det_documento_transaccion ddt (NoLock)
				  ,det_documento dd (NoLock) inner join documento d (NoLock) on(dd.documento_id=d.documento_id) left join sucursal s on(s.sucursal_id=d.sucursal_origen and s.cliente_id=d.cliente_id)
				  ,producto prod (NoLock)
				  ,categoria_logica cl (NoLock)
				  ,documento_transaccion dt (NoLock)
			WHERE 1<>0 
				  AND rl.doc_trans_id = ddt.doc_trans_id 
				  AND rl.nro_linea_trans = ddt.nro_linea_trans 
				  and ddt.documento_id = dd.documento_id 
				  and ddt.doc_trans_id = dt.doc_trans_id 
							 AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
				  AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID 
				  AND DD.CLIENTE_ID = PROD.CLIENTE_ID 
				  AND RL.CAT_LOG_ID = CL.CAT_LOG_ID 
				  AND RL.CLIENTE_ID = CL.CLIENTE_ID 
				  AND RL.DISPONIBLE= ''1''
				  AND ISNULL(p.pos_lockeada,''0'')=''0''
				  AND ISNULL(n.deposito_id,n2.deposito_Id)=''DEFAULT''
		      
			 GROUP BY rl.cat_log_id 
					 ,dd.cliente_id 
					 ,dd.producto_id 
					 ,isnull(p.POSICION_COD,n.NAVE_COD) 
					 ,prod.DESCRIPCION '


SET @SQL3='	UNION ALL
		SELECT	''YA PICKEADO'' AS ESTADO,
				dd.cliente_id AS CLIENTE_ID,
				PK.VIAJE_ID AS[CODIGO DE VIAJE],
				D.NRO_REMITO AS PEDIDO,
				DD.CAT_LOG_ID_FINAL as CategLogID,
				pk.POSICION_COD,
				dd.producto_id AS PRODUCTO_ID,
				PRO.DESCRIPCION,
				0  AS [CANTIDAD EN STOCK],
				Sum(PK.cantidad) AS [CANTIDAD ENVIADA A PICKEAR],
				SUM(PK.CANT_CONFIRMADA) AS [CANTIDAD PICKEADA],
				PK.usuario_pick AS [USUARIO PICKING],
				PK.fecha_pick AS [FECHA_PICKING],
				PK.NRO_UCDESCONSOLIDACION AS [NRO DESCONSOLIDACION],
				PK.Fecha_Desconsilidacion AS [FECHA DESCONSOLIDACION],
				PK.usuario_desconsolidacion AS [USUARIO DESCONSOLIDACION],
				PK.NRO_UCEMPAQUETADO AS [NRO EMPAQUETADO], 
				PK.Fecha_empaquetado AS [FECHA_EMPAQUETADO],
				PK.NRO_GUIA,
				PK.NRO_HOJACARGA,
				PK.FECHA_CONTROL_EXP
		 FROM  det_documento_transaccion ddt
				INNER JOIN det_documento dd ON (ddt.documento_id = dd.documento_id And ddt.nro_linea_doc = dd.nro_linea)
				INNER JOIN PRODUCTO PRO ON (DD.CLIENTE_ID = PRO.CLIENTE_ID AND DD.PRODUCTO_ID = PRO.PRODUCTO_ID)
				INNER JOIN documento d ON (dd.documento_id = d.documento_id)
				INNER JOIN (SELECT p.VIAJE_ID, P.DOCUMENTO_ID, P.NRO_LINEA,p.POSICION_COD,CONVERT(VARCHAR(19), MAX(p.fecha_fin), 120) as fecha_pick ,P.PRODUCTO_ID,SUM(P.CANTIDAD) AS CANTIDAD, SUM(P.CANT_CONFIRMADA) as  CANT_CONFIRMADA,P.USUARIO as usuario_pick, P.NRO_UCEMPAQUETADO,CONVERT(VARCHAR(19), max(p.FECHA_UCEMPAQUETADO), 120) as Fecha_empaquetado ,P.NRO_UCDESCONSOLIDACION,CONVERT(VARCHAR(19), max(p.Fecha_Desconsolidacion), 120) as Fecha_Desconsilidacion,p.usuario_desconsolidacion ,EM.NRO_GUIA, EM.NRO_HOJACARGA, MAX(P.FECHA_CONTROL_EXP) AS FECHA_CONTROL_EXP FROM VPICKING P
							LEFT JOIN UC_EMPAQUE EM ON (P.NRO_UCEMPAQUETADO = EM.UC_EMPAQUE)
							GROUP BY p.VIAJE_ID,P.DOCUMENTO_ID, P.NRO_LINEA, p.POSICION_COD, P.PRODUCTO_ID,P.USUARIO, P.NRO_UCEMPAQUETADO,P.NRO_UCDESCONSOLIDACION,p.usuario_desconsolidacion ,EM.NRO_GUIA, EM.NRO_HOJACARGA)
							 PK ON (PK.DOCUMENTO_ID = DD.DOCUMENTO_ID AND PK.NRO_LINEA = DD.NRO_LINEA)

		 WHERE 
			   d.status = ''D40''
		       

		 GROUP BY dd.cliente_id,PK.VIAJE_ID,D.NRO_REMITO, pk.POSICION_COD, dd.producto_id, PRO.DESCRIPCION,
				 PK.fecha_pick,PK.Fecha_empaquetado,PK.Fecha_Desconsilidacion,PK.usuario_desconsolidacion,
				DD.CAT_LOG_ID_FINAL,PK.usuario_pick, PK.NRO_UCEMPAQUETADO, PK.NRO_UCDESCONSOLIDACION, PK.NRO_GUIA, PK.NRO_HOJACARGA,PK.FECHA_CONTROL_EXP


		) X
		WHERE 1=1 '

		EXECUTE (@SQL1 + @SQL2 + @SQL3 + @PREDICADO)

END

--ESTE SP SE USA EN FRONTERA.frmConsultaEstadoStock


GO


