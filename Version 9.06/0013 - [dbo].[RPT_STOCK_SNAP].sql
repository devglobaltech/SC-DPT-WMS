/****** Object:  StoredProcedure [dbo].[GENERAR_SNAPSHOT]    Script Date: 03/14/2013 15:43:02 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RPT_STOCK_SNAP]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[RPT_STOCK_SNAP]
GO
CREATE PROCEDURE [dbo].[RPT_STOCK_SNAP]
	@FDESDE			DATETIME OUTPUT,
	@FHASTA			DATETIME OUTPUT,
	@CLIENTE_ID		VARCHAR(15)OUTPUT,
	@NRO_LOTE		VARCHAR(50)OUTPUT,
	@PRODUCTO_ID	VARCHAR(30)OUTPUT,
	@NRO_PARTIDA	VARCHAR(50)OUTPUT,
	@CAT_LOG_ID		VARCHAR(50)OUTPUT,
	@EST_MERC_ID	VARCHAR(15)OUTPUT
AS
BEGIN
	SELECT	CONVERT(VARCHAR,X.FECHA,103)	AS FECHA,
			X.CLIENTE						AS CLIENTE,
			X.PRODUCTO_ID					AS PRODUCTO_ID,
			X.PRODUCTO						AS PRODUCTO,
			X.NRO_LOTE						AS NRO_LOTE,
			X.NRO_PARTIDA					AS NRO_PARTIDA,
			X.CAT_LOG						AS CAT_LOG,
			X.EST_MERC						AS EST_MERC,
			SUM(X.QTY)						AS QTY,
			X.UN							AS UN,
			X.CAT_LOG_ID					AS CAT_LOG_ID,
			X.EST_MERC_ID					AS EST_MERC_ID,
			X.CLIENTE_ID					AS CLIENTE_ID,
			GETDATE()						AS F_IMP,
			HOST_NAME()						AS TERMINAL
	FROM	(	SELECT  DISTINCT  
						SE.F_SNAP								AS FECHA,
						SE.CLIENTE_ID							AS CLIENTE_ID,
						C.RAZON_SOCIAL							AS CLIENTE,
						PR.PRODUCTO_ID							AS PRODUCTO_ID,
						PR.PRODUCTO_ID + ' - '+ PR.DESCRIPCION	AS PRODUCTO,
						DD.NRO_LOTE								AS NRO_LOTE,
						DD.NRO_PARTIDA							AS NRO_PARTIDA,
						CL.DESCRIPCION							AS CAT_LOG,
						CL.CAT_LOG_ID							AS CAT_LOG_ID,
						EM.DESCRIPCION							AS EST_MERC,
						EM.EST_MERC_ID							AS EST_MERC_ID,
						SE.CANTIDAD								AS QTY,
						UPPER(UM.DESCRIPCION)					AS UN
				FROM    SNAP_EXISTENCIAS SE INNER JOIN POSICION P	ON(SE.POSICION_ACTUAL=P.POSICION_ID)
						INNER JOIN NAVE N							ON(P.NAVE_ID = N.NAVE_ID)
						INNER JOIN CLIENTE C						ON(SE.CLIENTE_ID=C.CLIENTE_ID)
						INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(SE.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND SE.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
						INNER JOIN DET_DOCUMENTO DD					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
						INNER JOIN PRODUCTO PR						ON(DD.CLIENTE_ID=PR.CLIENTE_ID AND DD.PRODUCTO_ID=PR.PRODUCTO_ID)
						INNER JOIN UNIDAD_MEDIDA UM					ON(PR.UNIDAD_ID=UM.UNIDAD_ID)
						INNER JOIN CATEGORIA_LOGICA CL				ON(SE.CLIENTE_ID=CL.CLIENTE_ID AND SE.CAT_LOG_ID=CL.CAT_LOG_ID)
						LEFT JOIN ESTADO_MERCADERIA_RL EM			ON(SE.CLIENTE_ID=EM.CLIENTE_ID AND SE.EST_MERC_ID=EM.EST_MERC_ID)
				WHERE   SE.DISPONIBLE='1'
				UNION ALL
				SELECT  DISTINCT  
						SE.F_SNAP								AS FECHA,
						SE.CLIENTE_ID							AS CLIENTE_ID,
						C.RAZON_SOCIAL							AS CLIENTE,
						PR.PRODUCTO_ID							AS PRODUCTO_ID,						
						PR.PRODUCTO_ID + ' - '+ PR.DESCRIPCION	AS PRODUCTO,
						DD.NRO_LOTE								AS NRO_LOTE,
						DD.NRO_PARTIDA							AS NRO_PARTIDA,
						CL.DESCRIPCION							AS CAT_LOG,
						CL.CAT_LOG_ID							AS CAT_LOG_ID,
						EM.DESCRIPCION							AS EST_MERC,
						EM.EST_MERC_ID							AS EST_MERC_ID,
						SE.CANTIDAD								AS QTY,
						UPPER(UM.DESCRIPCION)					AS UN
				FROM    SNAP_EXISTENCIAS SE INNER JOIN NAVE N		ON(SE.NAVE_ACTUAL = N.NAVE_ID)
						INNER JOIN CLIENTE C						ON(SE.CLIENTE_ID=C.CLIENTE_ID)
						INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(SE.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND SE.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
						INNER JOIN DET_DOCUMENTO DD					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
						INNER JOIN PRODUCTO PR						ON(DD.CLIENTE_ID=PR.CLIENTE_ID AND DD.PRODUCTO_ID=PR.PRODUCTO_ID)
						INNER JOIN UNIDAD_MEDIDA UM					ON(PR.UNIDAD_ID=UM.UNIDAD_ID)
						INNER JOIN CATEGORIA_LOGICA CL				ON(SE.CLIENTE_ID=CL.CLIENTE_ID AND SE.CAT_LOG_ID=CL.CAT_LOG_ID)
						LEFT JOIN ESTADO_MERCADERIA_RL EM			ON(SE.CLIENTE_ID=EM.CLIENTE_ID AND SE.EST_MERC_ID=EM.EST_MERC_ID)
				WHERE   SE.DISPONIBLE='1')X	
	WHERE	((@FDESDE IS NULL)OR(DBO.TRUNC(X.FECHA) BETWEEN @FDESDE AND @FHASTA))
			AND ((@CLIENTE_ID IS NULL)OR(X.CLIENTE_ID=@CLIENTE_ID))
			AND ((@NRO_LOTE IS NULL)OR(X.NRO_LOTE=@NRO_LOTE))
			AND ((@PRODUCTO_ID IS NULL)OR(X.PRODUCTO_ID=@PRODUCTO_ID))
			AND ((@NRO_PARTIDA IS NULL)OR(X.NRO_PARTIDA=@NRO_PARTIDA))
			AND ((@CAT_LOG_ID IS NULL)OR(X.CAT_LOG_ID=@CAT_LOG_ID))
			AND ((@EST_MERC_ID IS NULL)OR(X.EST_MERC_ID=@EST_MERC_ID))
	GROUP BY
			X.FECHA,X.CLIENTE,X.PRODUCTO_ID,X.PRODUCTO,X.NRO_LOTE,X.NRO_PARTIDA,X.CAT_LOG,X.EST_MERC,X.UN,X.CAT_LOG_ID,X.EST_MERC_ID,X.CLIENTE_ID
	ORDER BY
			1,2,3,4,5,6;				
END			

