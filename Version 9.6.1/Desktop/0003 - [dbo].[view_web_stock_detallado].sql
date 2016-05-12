/****** Object:  View [dbo].[view_web_stock_detallado]    Script Date: 12/10/2015 17:42:40 ******/
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[view_web_stock_detallado]'))
DROP VIEW [dbo].[view_web_stock_detallado]
GO

CREATE view [dbo].[view_web_stock_detallado] 
As
SELECT	t2.clienteid																as cliente_id, 
		c.razon_social																as razon_social,
		t2.productoid																as producto_id, 
		p.DESCRIPCION																as descripcion,
		Sum(Isnull(t2.cantidad, 0))													as cantidad, 
		p.UNIDAD_ID																	as unidad_medida,
		t2.storage																	as nave, 
		t2.callecod																	as calle,
		t2.columnacod																as columna,
		t2.nivelcod																	as nivel_profundidad, 
		t2.posicion_cod																as cod_posicion_unificado,
		t2.prop1																	as nro_pallet,
		t2.nro_bulto																as nro_contenedora, 		
		t2.categlogid																as categoria_logica,
		t2.est_merc_id																as estado_mercaderia, 
		t2.nro_lote																	as nro_lote, 		
		t2.nro_partida																as nro_partida, 
		t2.nro_serie																as nro_serie, 
		t2.fecha_vencimiento														as f_vencimiento, 		
		t2.nro_despacho																as nro_despacho, 
		t2.prop3																	as property3,
		t2.prop2																	as lote_proveedor,
		T2.FECHA_ALTA_GTW															as fecha_alta_gtw,
		Sum(Isnull(t2.cantidad, 0)) * ( ( p.alto * p.ancho * p.largo ) / 1000000 )	as volumen , 
		Isnull(t2.unidad_volumen, 'M3')												as unidad_volumen,		
		Sum(Isnull(t2.cantidad, 0)) * Isnull(P.peso, 0)								as peso, 
		t2.unidad_peso																as unidad_peso, 
		f.DESCRIPCION																as familia,
		sf.DESCRIPCION																as sub_familia,
		t2.tie_in_padre																as tie_in_padre, 
		t2.tie_in																	as tie_in,
		(select imagen from producto_img pim where t2.clienteid=pim.cliente_id and t2.productoid=pim.producto_id)as imagen
FROM   cliente C (nolock), 
       producto P (nolock), 
       familia_producto f (nolock),
       sub_familia sf (nolock),
       (SELECT T2.clienteid, 
               t2.productoid, 
               Sum(t2.cantidad) AS cantidad, 
               t2.nro_serie, 
               t2.nro_lote, 
               t2.nro_partida, 
               t2.fecha_vencimiento, 
               t2.nro_despacho, 
               t2.nro_bulto, 
               t2.peso, 
               t2.volumen, 
               t2.tie_in, 
               t2.nro_tie_in, 
               t2.nro_tie_in_padre, 
               t2.est_merc_id, 
               t2.unidad_peso, 
               t2.unidad_volumen, 
               t2.prop1, 
               t2.prop2, 
               t2.prop3, 
               t2.unidad_id, 
               t2.moneda_id, 
               t2.costo, 
               t2.cat_log_id_final, 
               t2.descripcion 
        FROM   (SELECT dd.cliente_id               ClienteID, 
                       dd.producto_id              ProductoID, 
                       Sum(Isnull(dd.cantidad, 0)) AS cantidad, 
                       dd.nro_serie, 
                       dd.nro_lote, 
                       dd.nro_partida, 
                       dd.fecha_vencimiento, 
                       dd.nro_despacho, 
                       dd.nro_bulto, 
                       dd.peso, 
                       dd.unidad_peso, 
                       dd.volumen, 
                       dd.unidad_volumen, 
                       dd.tie_in, 
                       dd.nro_tie_in, 
                       dd.nro_tie_in_padre, 
                       dd.est_merc_id, 
                       dd.prop1, 
                       dd.prop2, 
                       dd.prop3, 
                       dd.unidad_id, 
                       dd.moneda_id, 
                       dd.costo, 
                       dd.cat_log_id_final, 
                       dd.descripcion 
                FROM   documento d (nolock), 
                       det_documento dd (nolock) 
                WHERE  d.documento_id = dd.documento_id 
                       AND d.status = 'D20' 
                       AND d.tipo_operacion_id = 'EGR' 
                GROUP  BY dd.cliente_id,		dd.producto_id,			dd.nro_serie,		dd.nro_lote, 
                          dd.nro_partida,		dd.fecha_vencimiento,	dd.nro_despacho,	dd.nro_bulto, 
                          dd.peso,				dd.unidad_peso,			dd.volumen,			dd.unidad_volumen, 
                          dd.tie_in,			dd.nro_tie_in,			dd.nro_tie_in_padre,dd.est_merc_id, 
                          dd.prop1,				dd.prop2,				dd.prop3,			dd.unidad_id, 
                          dd.moneda_id,			dd.costo,				dd.cat_log_id_final,dd.descripcion 
                UNION ALL 
                SELECT dd.cliente_id               ClienteID, 
                       dd.producto_id              ProductoID, 
                       Sum(Isnull(dd.cantidad, 0)) AS cantidad, 
                       dd.nro_serie, 
                       dd.nro_lote, 
                       dd.nro_partida, 
                       dd.fecha_vencimiento, 
                       dd.nro_despacho, 
                       dd.nro_bulto, 
                       dd.peso, 
                       dd.unidad_peso, 
                       dd.volumen, 
                       dd.unidad_volumen, 
                       dd.tie_in, 
                       dd.nro_tie_in, 
                       dd.nro_tie_in_padre, 
                       dd.est_merc_id, 
                       dd.prop1, 
                       dd.prop2, 
                       dd.prop3, 
                       dd.unidad_id, 
                       dd.moneda_id, 
                       dd.costo, 
                       dd.cat_log_id_final, 
                       dd.descripcion 
                FROM   det_documento dd (nolock), 
                       det_documento_transaccion ddt (nolock), 
                       documento_transaccion dt (nolock) 
                WHERE  1 <> 0 
                       AND ddt.documento_id = dd.documento_id 
                       AND ddt.nro_linea_doc = dd.nro_linea 
                       AND dt.doc_trans_id = ddt.doc_trans_id 
                       AND dt.status = 'T10' 
                       AND dt.tipo_operacion_id = 'EGR' 
                       AND NOT EXISTS (SELECT rl_id 
                                       FROM   rl_det_doc_trans_posicion rl (nolock) 
                                       WHERE  rl.doc_trans_id_egr = ddt.doc_trans_id 
                                              AND rl.nro_linea_trans_egr = ddt.nro_linea_trans) 
                GROUP  BY dd.cliente_id,		dd.producto_id,			dd.nro_serie, 
                          dd.nro_lote,			dd.nro_partida,			dd.fecha_vencimiento, 
                          dd.nro_despacho,		dd.nro_bulto,			dd.peso, 
                          dd.unidad_peso,		dd.volumen,				dd.unidad_volumen, 
                          dd.tie_in,			dd.nro_tie_in,			dd.nro_tie_in_padre, 
                          dd.est_merc_id,		dd.prop1,				dd.prop2, 
                          dd.prop3,				dd.unidad_id,			dd.moneda_id, 
                          dd.costo,				dd.cat_log_id_final,	dd.descripcion) t2 
        WHERE  1 <> 0 
        GROUP  BY t2.clienteid, 
                  t2.productoid, 
                  t2.nro_serie, 
                  t2.nro_lote, 
                  t2.nro_partida, 
                  t2.fecha_vencimiento, 
                  t2.nro_despacho, 
                  t2.nro_bulto, 
                  t2.peso, 
                  t2.unidad_peso, 
                  t2.volumen, 
                  t2.unidad_volumen, 
                  t2.tie_in, 
                  t2.nro_tie_in, 
                  t2.nro_tie_in_padre, 
                  t2.est_merc_id, 
                  t2.prop1, 
                  t2.prop2, 
                  t2.prop3, 
                  t2.unidad_id, 
                  t2.moneda_id, 
                  t2.costo, 
                  t2.cat_log_id_final, 
                  t2.descripcion) T1 
       RIGHT OUTER JOIN (SELECT rl.cat_log_id				AS CategLogID, 
                                dd.cliente_id				AS ClienteID, 
                                dd.producto_id				AS ProductoID, 
                                Sum(Isnull(rl.cantidad, 0)) AS Cantidad, 
                                dd.nro_serie, 
                                dd.nro_lote, 
                                dd.fecha_vencimiento, 
                                dd.nro_despacho, 
                                dd.nro_bulto, 
                                dd.nro_partida, 
                                Sum(Isnull(rl.cantidad, 0)) * Isnull(prod.peso, 0)		AS Peso, 
                                dd.unidad_peso, 
                                Sum(Isnull(rl.cantidad, 0)) * ( Isnull(prod.alto, 0) * Isnull(prod.ancho, 0) * Isnull(prod.largo, 0) ) AS Volumen, 
								dd.unidad_volumen, 
								prod.kit							AS Kit, 
								dd.tie_in							AS TIE_IN, 
								dd.nro_tie_in_padre					AS TIE_IN_PADRE, 
								dd.nro_tie_in						AS NRO_TIE_IN, 
								rl.est_merc_id, 
								Isnull(n.nave_cod, n2.nave_cod)		AS Storage, 
								Isnull(rl.nave_actual, p.nave_id)	AS NaveID, 
								caln.calle_cod						AS CalleCod, 
								caln.calle_id						AS CalleID, 
								coln.columna_cod					AS ColumnaCod, 
								coln.columna_id						AS ColumnaID, 
								nn.nivel_cod						AS NivelCod, 
								nn.nivel_id							AS NivelID, 
								dd.prop1, 
								dd.prop2, 
								dd.prop3, 
								dd.unidad_id, 
								dd.moneda_id, 
								dd.costo, 
								s.nombre, 
								dd.descripcion, 
								prod.familia_id, 
								prod.sub_familia_id, 
								d.fecha_cpte, 
								p.posicion_cod,
								d.FECHA_ALTA_GTW
                         FROM   rl_det_doc_trans_posicion rl (nolock) 
                                LEFT OUTER JOIN nave n (nolock)				ON rl.nave_actual = n.nave_id 
                                LEFT OUTER JOIN posicion p (nolock)			ON rl.posicion_actual = p.posicion_id 
                                LEFT OUTER JOIN nave n2 (nolock)			ON p.nave_id = n2.nave_id 
                                LEFT OUTER JOIN calle_nave caln (nolock)	ON p.calle_id = caln.calle_id 
                                LEFT OUTER JOIN columna_nave coln (nolock)	ON p.columna_id = coln.columna_id 
                                LEFT OUTER JOIN nivel_nave nn (nolock)		ON p.nivel_id = nn.nivel_id, 
                                det_documento_transaccion ddt (nolock), 
                                det_documento dd (nolock) 
                                INNER JOIN documento d (nolock)				ON( dd.documento_id = d.documento_id ) 
                                LEFT JOIN sucursal s						ON( s.sucursal_id = d.sucursal_origen AND s.cliente_id = d.cliente_id ), 
                                cliente c (nolock), 
                                producto prod (nolock), 
                                categoria_logica cl (nolock), 
                                documento_transaccion dt (nolock) 
                         WHERE  1 <> 0 
                                AND rl.doc_trans_id = ddt.doc_trans_id 
                                AND rl.nro_linea_trans = ddt.nro_linea_trans 
                                AND ddt.documento_id = dd.documento_id 
                                AND ddt.doc_trans_id = dt.doc_trans_id 
                                AND DDT.nro_linea_doc = DD.nro_linea 
                                AND DD.cliente_id = C.cliente_id 
                                AND DD.producto_id = PROD.producto_id 
                                AND DD.cliente_id = PROD.cliente_id 
                                AND RL.cat_log_id = CL.cat_log_id 
                                AND RL.cliente_id = CL.cliente_id 
                                AND RL.disponible = '1' 
                                AND Isnull(p.pos_lockeada, '0') = '0' 
                                AND Isnull(n.deposito_id, n2.deposito_id) = 'DEFAULT' 
                                AND 0 = (SELECT ( CASE	WHEN ( Count (posicion_id) ) > 0 THEN 1 ELSE 0 END ) AS valor 
                                         FROM   rl_posicion_prohibida_cliente (nolock) 
                                         WHERE  posicion_id = Isnull(p.nivel_id, 0) 
                                                AND cliente_id = dd.cliente_id) 
                         GROUP  BY rl.cat_log_id,			dd.cliente_id,			dd.producto_id,			dd.nro_serie, 
                                   dd.nro_lote,				dd.fecha_vencimiento,	dd.nro_despacho,		dd.nro_bulto, 
                                   dd.nro_partida,			Isnull(prod.peso, 0),	dd.unidad_peso,			dd.volumen, 
                                   dd.unidad_volumen,		rl.nave_actual,			p.nave_id,				n.nave_cod, 
                                   n2.nave_cod,				caln.calle_cod,			caln.calle_id,			coln.columna_cod, 
                                   coln.columna_id,			nn.nivel_cod,			nn.nivel_id,			prod.kit, 
                                   dd.tie_in,				dd.nro_tie_in_padre,	dd.nro_tie_in,			rl.est_merc_id, 
                                   dd.prop1,				dd.prop2,				dd.prop3,				dd.unidad_id, 
                                   dd.moneda_id,			dd.costo,				s.nombre,				dd.descripcion, 
                                   prod.familia_id,			prod.sub_familia_id,	d.fecha_cpte,			prod.alto, 
                                   prod.ancho,				prod.largo,				p.posicion_cod,			d.FECHA_ALTA_GTW) T2 
                     ON ( Isnull(T2.clienteid, 0) = Isnull(T1.clienteid, 0) 
                          AND Isnull(T2.productoid, 0) = Isnull(T1.productoid, 0) 
                          AND Isnull(T2.nro_serie, 0) = Isnull(T1.nro_serie, 0) 
                          AND Isnull(T2.nro_lote, 0) = Isnull(T1.nro_lote, 0) 
                          AND Isnull(T2.nro_despacho, 0) = Isnull(T1.nro_despacho, 0) 
                          AND Isnull(T2.nro_bulto, 0) = Isnull(T1.nro_bulto, 0) 
                          AND Isnull(T2.nro_partida, 0) = Isnull(T1.nro_partida, 0) 
                          AND Isnull(T2.prop1, 0) = Isnull(T1.prop1, 0) 
                          AND Isnull(T2.prop2, 0) = Isnull(T1.prop2, 0) 
                          AND Isnull(T2.prop3, 0) = Isnull(T1.prop3, 0) 
                          AND Isnull(T2.unidad_id, 0) = Isnull(T1.unidad_id, 0) 
                          AND Isnull(T2.fecha_vencimiento, '01/01/1900') = Isnull(T1.fecha_vencimiento, '01/01/1900') 
                          AND Isnull(T2.est_merc_id, 0) = Isnull(T1.est_merc_id, 0) 
                          AND Isnull(T2.categlogid, 0) = Isnull(T1.cat_log_id_final, 0) 
                        ) 
		LEFT OUTER JOIN estado_mercaderia_rl EMRL (nolock) 
                    ON ( T2.clienteid = EMRL.cliente_id AND T2.est_merc_id = EMRL.est_merc_id ) 
WHERE  1 <> 0 
       AND T2.clienteid = C.cliente_id 
       AND T2.clienteid = P.cliente_id 
       AND T2.productoid = P.producto_id 
       and p.familia_id=f.familia_id
       and p.sub_familia_id=sf.sub_familia_id
GROUP  BY 
		t2.clienteid,			t2.productoid,		t2.storage,				t2.callecod,		t2.columnacod,			
		t2.nivelcod,			t2.nivelid,			t2.est_merc_id,			t2.categlogid,		t2.nro_serie,		
		t2.nro_bulto,			t2.nro_lote,		t2.nro_despacho,		t2.nro_partida,		t2.prop1,			
		t2.prop2,				t2.prop3,			t2.fecha_vencimiento,	t2.peso,			t2.unidad_peso,		
		t2.volumen,				t2.unidad_volumen, 
        t2.kit,					t2.tie_in,			t2.tie_in_padre,		t2.nro_tie_in,		C.razon_social, 
        P.descripcion,			t2.unidad_id,		t2.moneda_id,			t2.costo,			t2.nombre, 
        t2.descripcion,			t2.familia_id,		t2.sub_familia_id,		t2.fecha_cpte,		p.alto, 
        p.ancho,				p.largo,			Isnull(p.peso, 0),		T2.POSICION_COD,	c.razon_social,
        P.DESCRIPCION,			p.unidad_id,		T2.FECHA_ALTA_GTW,		f.DESCRIPCION,		sf.DESCRIPCION 
        
GO


