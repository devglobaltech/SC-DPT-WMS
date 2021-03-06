

alter PROCEDURE [dbo].[get_Stock_Web] 
	-- Add the parameters for the stored procedure here
	(@UserId uniqueidentifier)
AS
BEGIN

	SELECT	C.CLIENTE_ID as [Cliente_id],
			C.RAZON_SOCIAL as [Razon_social],
			DD.PRODUCTO_ID as [Producto_id],
			PROD.DESCRIPCION as [Descr_producto],
			CONVERT(nvarchar(30),CONVERT(DECIMAL(10,2), REPLACE(SUM(RL.CANTIDAD), ',','.')))  as [Cantidad],
			--cast(SUM(RL.CANTIDAD) as numeric(20,2)) as [Cantidad],
			--P.POSICION_COD as [Posición],
			RL.CAT_LOG_ID as [Cat. Lógica],
			isnull(RL.EST_MERC_ID,'-------') as [Estado Mercadería],
			MIN(case when isnull(CAT.DISP_EGRESO,'0') = '1' AND ((EST.DISP_EGRESO IS NOT NULL AND EST.DISP_EGRESO='1') OR (EST.DISP_EGRESO IS NULL)) then 'SI' else 'NO' end) as [Es Egresable],
			MIN(case when isnull(CAT.PICKING,'0') = '1' AND ((EST.PICKING IS NOT NULL AND EST.PICKING='1') OR (EST.PICKING IS NULL)) then 'SI' else 'NO' end) as [Es Pickeable]
	FROM	RL_DET_DOC_TRANS_POSICION RL
	INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
	INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
	INNER JOIN DOCUMENTO D ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
	INNER JOIN CLIENTE C ON (D.CLIENTE_ID = C.CLIENTE_ID)
	INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID)
	INNER JOIN POSICION P ON (RL.POSICION_ACTUAL = P.POSICION_ID)
	INNER JOIN CATEGORIA_LOGICA CAT ON (RL.CLIENTE_ID = CAT.CLIENTE_ID AND RL.CAT_LOG_ID = CAT.CAT_LOG_ID)
	LEFT JOIN ESTADO_MERCADERIA_RL EST ON (RL.CLIENTE_ID = EST.CLIENTE_ID AND RL.EST_MERC_ID = EST.EST_MERC_ID)
	INNER JOIN USUARIOWEBCLIENTES UC ON (UC.USERID = @UserId and RL.CLIENTE_ID = UC.CLIENTE_ID)
	WHERE	UC.USERID = @UserId and d.STATUS='D40' AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL
	GROUP BY	C.CLIENTE_ID,
				C.RAZON_SOCIAL,
				DD.PRODUCTO_ID,
				PROD.DESCRIPCION,
				--P.POSICION_COD,
				RL.CAT_LOG_ID,
				RL.EST_MERC_ID
UNION
	SELECT	C.CLIENTE_ID as [Cod. Cliente],
			C.RAZON_SOCIAL as [Razon_social],
			DD.PRODUCTO_ID as [Producto_id],
			PROD.DESCRIPCION as [Descr_producto],
			CONVERT(nvarchar(30),CONVERT(DECIMAL(10,2), REPLACE(SUM(RL.CANTIDAD), ',','.')))  as [Cantidad],
			--cast(SUM(RL.CANTIDAD) as numeric(20,2)) as [Cantidad],
			--N.NAVE_COD as [Posición],
			RL.CAT_LOG_ID as [Cat. Lógica],
			isnull(RL.EST_MERC_ID,'-------') as [Estado Mercadería],
			MIN(case when isnull(CAT.DISP_EGRESO,'0') = '1' AND ((EST.DISP_EGRESO IS NOT NULL AND EST.DISP_EGRESO='1') OR (EST.DISP_EGRESO IS NULL)) then 'SI' else 'NO' end) as [Es Egresable],
			MIN(case when isnull(CAT.PICKING,'0') = '1' AND ((EST.PICKING IS NOT NULL AND EST.PICKING='1') OR (EST.PICKING IS NULL)) then 'SI' else 'NO' end) as [Es Pickeable]
	FROM	RL_DET_DOC_TRANS_POSICION RL
	INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
	INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
	INNER JOIN DOCUMENTO D ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
	INNER JOIN CLIENTE C ON (D.CLIENTE_ID = C.CLIENTE_ID)
	INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID)
	INNER JOIN NAVE N ON (RL.NAVE_ACTUAL = N.NAVE_ID)
	INNER JOIN CATEGORIA_LOGICA CAT ON (RL.CLIENTE_ID = CAT.CLIENTE_ID AND RL.CAT_LOG_ID = CAT.CAT_LOG_ID)
	LEFT JOIN ESTADO_MERCADERIA_RL EST ON (RL.CLIENTE_ID = EST.CLIENTE_ID AND RL.EST_MERC_ID = EST.EST_MERC_ID)
	INNER JOIN USUARIOWEBCLIENTES UC ON (UC.USERID = @UserId and RL.CLIENTE_ID = UC.CLIENTE_ID)
	WHERE	UC.USERID = @UserId and d.STATUS='D40' AND RL.DOC_TRANS_ID_EGR IS NULL AND RL.NRO_LINEA_TRANS_EGR IS NULL
	GROUP BY	C.CLIENTE_ID,
				C.RAZON_SOCIAL,
				DD.PRODUCTO_ID,
				PROD.DESCRIPCION,
				--N.NAVE_COD,
				RL.CAT_LOG_ID,
				RL.EST_MERC_ID
END

