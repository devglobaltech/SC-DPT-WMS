/****** Object:  View [dbo].[view_web_devoluciones]    Script Date: 11/18/2015 16:27:34 ******/
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[view_web_devoluciones]'))
DROP VIEW [dbo].[view_web_devoluciones]
GO

CREATE VIEW [dbo].[view_web_devoluciones]
AS
	SELECT	*
	FROM	(
				SELECT	DISTINCT
						D.CLIENTE_ID							as cliente_id,
						C.RAZON_SOCIAL							as razon_social,
						D.FECHA_FIN_GTW							as anio,
						D.FECHA_FIN_GTW							as mes,
						D.FECHA_FIN_GTW							as fecha_operacion,
						D.TIPO_OPERACION_ID						as tipo_operacion_id,
						D.TIPO_COMPROBANTE_ID					as tipo_comprobante_id,
						D.NRO_REMITO							as doc_ext,
						S.NOMBRE								as nombre,
						P.PRODUCTO_ID							as producto_id,
						P.DESCRIPCION							as descripcion,
						P.CANT_CONFIRMADA						as cant_confirmada,
						P.NRO_LOTE								as nro_lote,
						P.NRO_PARTIDA							as nro_partida,
						P.NRO_SERIE								as nro_serie,
						D.NRO_DESPACHO_IMPORTACION				as nro_despacho_importacion,
						DD.PROP3								as property3,
						DD.PROP2								as lote_proveedor							
				FROM	MOB_DEVOLUCIONES_TMP T INNER JOIN DOCUMENTO D	ON(T.DOCUMENTO_ID_EGRESO=D.DOCUMENTO_ID)
						INNER JOIN CLIENTE C							ON(D.CLIENTE_ID=C.CLIENTE_ID)
						INNER JOIN SYS_INT_DOCUMENTO SD					ON(D.CLIENTE_ID=SD.CLIENTE_ID AND D.NRO_REMITO=SD.DOC_EXT)
						INNER JOIN SUCURSAL S							ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
						INNER JOIN PICKING P							ON(T.DOCUMENTO_ID_EGRESO=P.DOCUMENTO_ID AND T.NRO_LINEA_EGRESO=P.NRO_LINEA AND T.NRO_SERIE=P.NRO_SERIE)
						INNER JOIN PRODUCTO PR							ON(P.CLIENTE_ID=PR.CLIENTE_ID AND P.PRODUCTO_ID=PR.PRODUCTO_ID)
						INNER JOIN DET_DOCUMENTO DD						ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
						
				WHERE	ISNULL(PR.SERIE_EGR,'0')='1'
				UNION ALL
				SELECT	D.CLIENTE_ID							as cliente_id,
						C.RAZON_SOCIAL							as razon_social,
						D.FECHA_FIN_GTW							as anio,
						D.FECHA_FIN_GTW							as mes,
						D.FECHA_FIN_GTW							as fecha_operacion,
						D.TIPO_OPERACION_ID						as tipo_operacion_id,
						D.TIPO_COMPROBANTE_ID					as tipo_comprobante_id,
						D.NRO_REMITO							as doc_ext,
						S.NOMBRE								as nombre,
						DD.PRODUCTO_ID							as producto_id,
						DD.DESCRIPCION							as descripcion,
						DD.CANTIDAD								as cantidad,
						DD.NRO_LOTE								as nro_lote,
						DD.NRO_PARTIDA							as nro_partida,
						T.NRO_SERIE								as nro_serie,
						D.NRO_DESPACHO_IMPORTACION				as nro_despacho_importacion,
						DD.PROP3								as property3,
						DD.PROP2								as lote_proveedor
				FROM	MOB_DEVOLUCIONES_TMP T INNER JOIN DOCUMENTO D	ON(T.DOCUMENTO_ID_DEVOLUCION=D.DOCUMENTO_ID)
						INNER JOIN CLIENTE C							ON(D.CLIENTE_ID=C.CLIENTE_ID)
						INNER JOIN DET_DOCUMENTO DD						ON(T.DOCUMENTO_ID_DEVOLUCION=DD.DOCUMENTO_ID AND T.NRO_LINEA_DEVOLUCION=DD.NRO_LINEA)
						INNER JOIN PRODUCTO PR							ON(DD.CLIENTE_ID=PR.CLIENTE_ID AND DD.PRODUCTO_ID=PR.PRODUCTO_ID)
						INNER JOIN DOCUMENTO D2							ON(T.DOCUMENTO_ID_EGRESO=D2.DOCUMENTO_ID)
						LEFT JOIN SUCURSAL S							ON(D2.CLIENTE_ID=S.CLIENTE_ID AND D2.SUCURSAL_DESTINO=S.SUCURSAL_ID)
				WHERE	PR.SERIE_EGR='1'
				UNION ALL
				SELECT	DISTINCT
						D.CLIENTE_ID							as cliente_id,
						C.RAZON_SOCIAL							as razon_socia,
						D.FECHA_FIN_GTW							as anio,
						D.FECHA_FIN_GTW							as mes,
						D.FECHA_FIN_GTW							as fecha_operacion,
						D.TIPO_OPERACION_ID						as tipo_operacion_id,
						D.TIPO_COMPROBANTE_ID					as tipo_comprobante_id,
						D.NRO_REMITO							as doc_ext,
						S.NOMBRE								as nombre,
						P.PRODUCTO_ID							as producto_id,
						P.DESCRIPCION							as descripcion,
						P.CANT_CONFIRMADA						as cantidad,
						P.NRO_LOTE								as nro_lote,
						P.NRO_PARTIDA							as nro_partida,
						P.NRO_SERIE								as nro_serie,
						D.NRO_DESPACHO_IMPORTACION				as nro_despacho_importacion,
						DD.PROP3								as property3,
						DD.PROP2								as lote_proveedor
				FROM	DOCUMENTO D	INNER JOIN CLIENTE C				ON(D.CLIENTE_ID=C.CLIENTE_ID)
						INNER JOIN SYS_INT_DOCUMENTO SD					ON(D.CLIENTE_ID=SD.CLIENTE_ID AND D.NRO_REMITO=SD.DOC_EXT)
						INNER JOIN SUCURSAL S							ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
						INNER JOIN DET_DOCUMENTO DD						ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
						INNER JOIN PICKING P							ON(DD.DOCUMENTO_ID=P.DOCUMENTO_ID AND DD.NRO_LINEA=P.NRO_LINEA)
						INNER JOIN PRODUCTO PR							ON(P.CLIENTE_ID=PR.CLIENTE_ID AND P.PRODUCTO_ID=PR.PRODUCTO_ID)
				WHERE	ISNULL(PR.SERIE_EGR,'0')='1'
						AND NOT EXISTS (SELECT	1
										FROM	MOB_DEVOLUCIONES_TMP T
										WHERE	T.DOCUMENTO_ID_EGRESO=P.DOCUMENTO_ID 
												AND T.NRO_LINEA_EGRESO=P.NRO_LINEA 
												AND T.NRO_SERIE=P.NRO_SERIE)				
			)X
	
GO


