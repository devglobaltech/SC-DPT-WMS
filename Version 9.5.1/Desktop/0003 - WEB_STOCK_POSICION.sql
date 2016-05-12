CREATE VIEW [dbo].[WEB_STOCK_POSICION] 

as

select a.*,pim.IMAGEN  

from 

	(
		SELECT	RL.CLIENTE_ID																as [Clienteid]
			,c.razon_social																	as [Razon_social]
			,DD.PRODUCTO_ID																	as [Producto_id]
			,PROD.DESCRIPCION																as [Descr_producto]
			,CONVERT(nvarchar(30),CONVERT(DECIMAL(10,2), REPLACE(SUM(RL.CANTIDAD), ',','.')))  as [Cantidad]
			,P.POSICION_COD																	as [Posicion]
			,dd.nro_lote																	as [Nro_Lote]
			,dd.prop1																		as [Pallet]
			,dd.nro_bulto																	as [Nro_Bulto]
			,dd.prop2																		as [Lote_Proveedor]
			,dd.nro_partida																	as [Nro_Partida]
			,dd.nro_serie																	as [Nro_Serie]
			,RL.CAT_LOG_ID																	as [Cat_Logica]
			,isnull(RL.EST_MERC_ID,'-------')												as [Est_Mercaderia]
			FROM	RL_DET_DOC_TRANS_POSICION RL
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
			inner join CLIENTE c on d.CLIENTE_ID = c.cliente_id
			INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID)
			INNER JOIN POSICION P ON (RL.POSICION_ACTUAL = P.POSICION_ID)
			WHERE	
			d.STATUS='D40' AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL
			GROUP BY	RL.CLIENTE_ID,
						c.RAZON_SOCIAL,
						DD.PRODUCTO_ID,
						PROD.DESCRIPCION,
						P.POSICION_COD,
						dd.NRO_LOTE,
						dd.PROP1,
						dd.NRO_BULTO,
						dd.PROP2,
						dd.NRO_PARTIDA,
						dd.NRO_SERIE,
						RL.CAT_LOG_ID,
						RL.EST_MERC_ID
		UNION
			SELECT	RL.CLIENTE_ID																as [Clienteid]
					,c.razon_social																as [Razon_social]
					,DD.PRODUCTO_ID																as [Producto_id]
					,PROD.DESCRIPCION															as [Descr_producto]
					,CONVERT(nvarchar(30),CONVERT(DECIMAL(10,2), REPLACE(SUM(RL.CANTIDAD), ',','.')))  as [Cantidad]
					,N.NAVE_COD																	as [Posicion]
					,dd.nro_lote																as [Nro_Lote]
					,dd.prop1																	as [Pallet]
					,dd.nro_bulto																as [Nro_Bulto]
					,dd.prop2																	as [Lote_Proveedor]
					,dd.nro_partida																as [Nro_Partida]
					,dd.nro_serie																as [Nro_Serie]
					,RL.CAT_LOG_ID																as [Cat_Logica]
					,isnull(RL.EST_MERC_ID,'-------')											as [Est_Mercaderia]
			FROM	RL_DET_DOC_TRANS_POSICION RL
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
			inner join CLIENTE c on d.CLIENTE_ID = c.CLIENTE_ID
			INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID)
			INNER JOIN NAVE N ON (RL.NAVE_ACTUAL = N.NAVE_ID)
			WHERE	
			d.STATUS='D40' AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL
			GROUP BY	RL.CLIENTE_ID,
						c.RAZON_SOCIAL,
						DD.PRODUCTO_ID,
						PROD.DESCRIPCION,
						n.NAVE_COD,
						dd.NRO_LOTE,
						dd.PROP1,
						dd.NRO_BULTO,
						dd.PROP2,
						dd.NRO_PARTIDA,
						dd.NRO_SERIE,
						RL.CAT_LOG_ID,
						RL.EST_MERC_ID

) a

	left join PRODUCTO_IMG pim on (a.CLIENTEID=pim.CLIENTE_ID and a.PRODUCTO_ID=pim.PRODUCTO_ID)

	GO