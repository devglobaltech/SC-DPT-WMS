IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[consulta_stk_pos_web]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[consulta_stk_pos_web]
GO
/*Vista de stock y posicion*/
create procedure [dbo].[consulta_stk_pos_web]
@UserId uniqueidentifier
as
begin
	SELECT	RL.CLIENTE_ID as [Cliente_id]
			,c.razon_social as [Razon_social]
			,DD.PRODUCTO_ID as [Producto_id]
			,PROD.DESCRIPCION as [Descr_producto]
			,CONVERT(nvarchar(30),CONVERT(DECIMAL(10,2), REPLACE(SUM(RL.CANTIDAD), ',','.')))  as [Cantidad]
			--,CAST(SUM(RL.CANTIDAD) as numeric(20,2)) as [Cantidad]
			,P.POSICION_COD as [Posición]
			,dd.nro_lote	as [Nro. Lote]
			,dd.prop1		as [Pallet]
			,dd.nro_bulto	as [Nro. Bulto]
			,dd.prop2		as [Lote. Proveedor]
			,dd.nro_partida	as [Nro. Partida]
			,dd.nro_serie	as [Nro. Serie]
			,RL.CAT_LOG_ID as [Cat. Lógica]
			,isnull(RL.EST_MERC_ID,'-------') as [Estado Mercadería]
	FROM	RL_DET_DOC_TRANS_POSICION RL
	INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
	INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
	INNER JOIN DOCUMENTO D ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
	inner join CLIENTE c on d.CLIENTE_ID = c.cliente_id
	INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID)
	INNER JOIN POSICION P ON (RL.POSICION_ACTUAL = P.POSICION_ID)
	INNER JOIN USUARIOWEBCLIENTES UC ON (UC.USERID = @UserId and RL.CLIENTE_ID = UC.CLIENTE_ID)
	WHERE	UC.USERID = @UserId and d.STATUS='D40' AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL
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
	SELECT	RL.CLIENTE_ID as [Cod. Cliente]
			,c.razon_social as [Razon_social]
			,DD.PRODUCTO_ID as [Producto_id]
			,PROD.DESCRIPCION as [Descr_producto]
			,CONVERT(nvarchar(30),CONVERT(DECIMAL(10,2), REPLACE(SUM(RL.CANTIDAD), ',','.')))  as [Cantidad]
			--,CAST(SUM(RL.CANTIDAD) as numeric(20,0)) as [Cantidad]
			,N.NAVE_COD as [Posición]
			,dd.nro_lote	as [Nro. Lote]
			,dd.prop1		as [Pallet]
			,dd.nro_bulto	as [Nro. Bulto]
			,dd.prop2		as [Lote. Proveedor]
			,dd.nro_partida	as [Nro. Partida]
			,dd.nro_serie	as [Nro. Serie]
			,RL.CAT_LOG_ID as [Cat. Lógica]
			,isnull(RL.EST_MERC_ID,'-------') as [Estado Mercadería]
	FROM	RL_DET_DOC_TRANS_POSICION RL
	INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
	INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
	INNER JOIN DOCUMENTO D ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
	inner join CLIENTE c on d.CLIENTE_ID = c.CLIENTE_ID
	INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID)
	INNER JOIN NAVE N ON (RL.NAVE_ACTUAL = N.NAVE_ID)
	INNER JOIN USUARIOWEBCLIENTES UC ON (UC.USERID = @UserId and RL.CLIENTE_ID = UC.CLIENTE_ID)
	WHERE	UC.USERID = @UserId and d.STATUS='D40' AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL
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
end