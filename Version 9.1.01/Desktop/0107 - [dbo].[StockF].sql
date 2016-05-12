/****** Object:  View [dbo].[StockF]    Script Date: 10/10/2013 17:32:20 ******/
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[StockF]'))
DROP VIEW [dbo].[StockF]
GO

CREATE  VIEW [dbo].[StockF]
AS
SELECT 	 T2.PRODUCTOID								[ARTICULO]
		,SUM(ISNULL(T2.CANTIDAD,0))					[CANTIDAD]
		,T2.STORAGE									[NAVE]
FROM   PRODUCTO P (NOLOCK)
		,  (	SELECT	DD.PRODUCTO_ID AS PRODUCTOID 
						,SUM(ISNULL(RL.CANTIDAD,0)) AS CANTIDAD 
						,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS STORAGE 
				FROM  RL_DET_DOC_TRANS_POSICION RL (NOLOCK)
					  LEFT OUTER JOIN NAVE N (NOLOCK)            ON RL.NAVE_ACTUAL = N.NAVE_ID 
			          LEFT OUTER JOIN POSICION P  (NOLOCK)       ON RL.POSICION_ACTUAL = P.POSICION_ID 
					  LEFT OUTER JOIN NAVE N2   (NOLOCK)         ON P.NAVE_ID = N2.NAVE_ID 
					  ,DET_DOCUMENTO_TRANSACCION DDT (NOLOCK)
					  ,DET_DOCUMENTO DD (NOLOCK) 
					  INNER JOIN DOCUMENTO D (NOLOCK) ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					  ,CATEGORIA_LOGICA CL (NOLOCK)
					  ,DOCUMENTO_TRANSACCION DT (NOLOCK)
				WHERE 1<>0 
					  AND RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID 
					  AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS 
					  AND DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID 
					  AND DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID 
					  AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
					  AND RL.CAT_LOG_ID = CL.CAT_LOG_ID 
					  AND RL.CLIENTE_ID = CL.CLIENTE_ID 
					  AND RL.DISPONIBLE= '1'
					  AND ISNULL(N.DEPOSITO_ID,N2.DEPOSITO_ID)='DEFAULT'

				 GROUP BY DD.PRODUCTO_ID
						 ,N.NAVE_COD  
						 ,N2.NAVE_COD
						 ) T2 
     WHERE T2.PRODUCTOID = P.PRODUCTO_ID 

GROUP BY T2.PRODUCTOID 
         ,T2.STORAGE

GO

