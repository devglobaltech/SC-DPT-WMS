
/****** Object:  StoredProcedure [dbo].[STOCK_FECHA]    Script Date: 04/11/2014 12:41:30 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[STOCK_FECHA]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[STOCK_FECHA]
GO

/****** Object:  StoredProcedure [dbo].[STOCK_FECHA]    Script Date: 04/11/2014 12:41:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[STOCK_FECHA]

	@FECHA	VARCHAR(8)
	
AS 

BEGIN

SELECT	Q.CLIENTE_ID,
		Q.PRODUCTO_ID,
		P.DESCRIPCION,
		Q.CANTIDAD
FROM(
SELECT	P.CLIENTE_ID,
		P.PRODUCTO_ID,
		P.PC+ISNULL(S.SC,0)+ISNULL(E.EC,0)-ISNULL(I.IC,0)-ISNULL(A.AC,0)+ISNULL(N.NC,0)+ISNULL(X.XC,0)+ISNULL(R.RC,0)+ISNULL(T.TC,0)+ISNULL(TR.TRC,0) AS [CANTIDAD]
FROM
(
SELECT	CLIENTE_ID, PRODUCTO_ID, '0' AS PC
FROM	PRODUCTO
)P
LEFT JOIN
(
--PARTE 1: STOCK DISPONIBLE (NO SE CONSIDERA MERCADERIA EN TRANSITO DE EGRESO NI EN TRANSFERENCIAS)
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID, SUM(RL.CANTIDAD) AS SC
FROM	RL_DET_DOC_TRANS_POSICION RL
		INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
			ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
		INNER JOIN DET_DOCUMENTO DD 
			ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
		INNER JOIN DOCUMENTO D 
			ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
WHERE	RL.DOC_TRANS_ID_EGR IS NULL
		AND RL.DISPONIBLE = '1'
GROUP BY 
		DD.CLIENTE_ID, DD.PRODUCTO_ID
) S 
	ON P.CLIENTE_ID = S.CLIENTE_ID AND P.PRODUCTO_ID = S.PRODUCTO_ID

LEFT JOIN
(
--PARTE 2: PRODUCTOS PROCESADOS Y PICKEADOS
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID, SUM(P.CANT_CONFIRMADA) AS EC
FROM	VDOCUMENTO D
		INNER JOIN VDET_DOCUMENTO DD
			ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
		INNER JOIN VPICKING P
			ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
WHERE	P.CANT_CONFIRMADA IS NOT NULL
		AND cast(D.FECHA_CPTE as DATE) > cast(@FECHA as date)--'20140101'
GROUP BY 
		DD.CLIENTE_ID, DD.PRODUCTO_ID
) E
	ON E.CLIENTE_ID= P.CLIENTE_ID AND E.PRODUCTO_ID = P.PRODUCTO_ID
	
LEFT JOIN
(
--PARTE 3: PICKINGS PROCESADOS PERO TODAVIA NO PICKEADOS
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID, SUM(P.CANTIDAD) AS NC
FROM	VDOCUMENTO D
		INNER JOIN VDET_DOCUMENTO DD
			ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
		INNER JOIN VPICKING P
			ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
WHERE	P.CANT_CONFIRMADA IS NULL
		AND cast(D.FECHA_CPTE as DATE) >= cast(@FECHA as date)--'20140101'		
GROUP BY 
		DD.CLIENTE_ID, DD.PRODUCTO_ID
) N
	ON N.CLIENTE_ID= P.CLIENTE_ID AND N.PRODUCTO_ID = P.PRODUCTO_ID

LEFT JOIN
(
--PARTE 4: INGRESOS FINALIZADOS
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID, SUM(DD.CANTIDAD) AS IC
FROM	VDOCUMENTO D
		INNER JOIN VDET_DOCUMENTO DD
			ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
WHERE	D.TIPO_OPERACION_ID = 'ING'
		AND cast(D.FECHA_FIN_GTW as DATE) >= cast(@FECHA as date)--'20140101'
GROUP BY 
		DD.CLIENTE_ID, DD.PRODUCTO_ID
) I
	ON I.CLIENTE_ID= P.CLIENTE_ID AND I.PRODUCTO_ID = P.PRODUCTO_ID
LEFT JOIN
(
--PARTE 5: AJUSTES
SELECT	A.CLIENTE_ID, A.PRODUCTO_ID, SUM(A.CANTIDAD) AS AC
FROM	AUDITORIA_HISTORICOS A 
		INNER JOIN PARAMETROS_AUDITORIA PA 
			ON(A.TIPO_AUDITORIA_ID=PA.TIPO_AUDITORIA_ID)
WHERE	A.TIPO_AUDITORIA_ID IN ('15')
		AND cast(A.FECHA_AUDITORIA as DATE) >= cast(@FECHA as date)--'20140101'
GROUP BY A.PRODUCTO_ID,A.CLIENTE_ID
) A
	ON A.CLIENTE_ID= P.CLIENTE_ID AND A.PRODUCTO_ID = P.PRODUCTO_ID

LEFT JOIN
--PARTE 6: TODO LO QUE ESTABA EN PRE-EGRESO PERO TODAV�A NO SE HAB�A FINALIZADO EL DOCUMENTO
(
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID, SUM(P.CANT_CONFIRMADA) AS XC
FROM	VDOCUMENTO D
		INNER JOIN VDET_DOCUMENTO DD
			ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
		INNER JOIN VPICKING P
			ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
WHERE	cast(D.FECHA_FIN_GTW as DATE) >= cast(@FECHA as date)--'20140101'
		AND cast(P.FECHA_FIN as date) <= cast(@FECHA as date)--'20140101' DATEADD(DD,1,'20131231')
GROUP BY 
		DD.CLIENTE_ID, DD.PRODUCTO_ID
) X
	ON X.CLIENTE_ID= P.CLIENTE_ID AND X.PRODUCTO_ID = P.PRODUCTO_ID
	
LEFT JOIN
--PARTE 7: TODO LO QUE ESTABA EN PRE-EGRESO Y TODAVIA NO SE FINALIZO EL DOCUMENTO
(
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID, SUM(P.CANT_CONFIRMADA) AS RC
FROM	VDOCUMENTO D
		INNER JOIN VDET_DOCUMENTO DD
			ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
		INNER JOIN VPICKING P
			ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
WHERE	D.FECHA_FIN_GTW IS NULL
		AND cast(P.FECHA_FIN as DATE) <= cast(@FECHA as date)--'20140101'
GROUP BY 
		DD.CLIENTE_ID, DD.PRODUCTO_ID
) R
	ON R.CLIENTE_ID= P.CLIENTE_ID AND R.PRODUCTO_ID = P.PRODUCTO_ID
	
LEFT JOIN
--PARTE 8: LO QUE SE PICKEO POR UNA CANTIDAD MENOR Y NO FUE FINALIZADO
(
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID,SUM(P.CANTIDAD)-SUM(P.CANT_CONFIRMADA) AS TC
FROM	VDOCUMENTO D
		INNER JOIN VDET_DOCUMENTO DD
			ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
		INNER JOIN VPICKING P
			ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
WHERE	P.CANT_CONFIRMADA IS NOT NULL
		AND cast(D.FECHA_CPTE AS DATE) >= cast(@FECHA as date)--'20140101'
		AND D.STATUS <> 'D40'
GROUP BY DD.CLIENTE_ID, DD.PRODUCTO_ID
)T
	ON T.CLIENTE_ID= P.CLIENTE_ID AND T.PRODUCTO_ID = P.PRODUCTO_ID
	
LEFT JOIN 
(
--PARTE 9: TRANSFERENCIAS SIN FINALIZAR
SELECT	DD.CLIENTE_ID, DD.PRODUCTO_ID, SUM(RL.CANTIDAD) AS TRC
FROM	RL_DET_DOC_TRANS_POSICION RL
		INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
			ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
		INNER JOIN DET_DOCUMENTO DD 
			ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
		INNER JOIN DOCUMENTO D 
			ON (DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
WHERE	RL.DOC_TRANS_ID_TR IS NOT NULL

GROUP BY 
		DD.CLIENTE_ID, DD.PRODUCTO_ID
)TR
	ON TR.CLIENTE_ID =P.CLIENTE_ID AND TR.PRODUCTO_ID = P.PRODUCTO_ID

)Q	INNER JOIN PRODUCTO P ON (Q.CLIENTE_ID = P.CLIENTE_ID AND Q.PRODUCTO_ID = P.PRODUCTO_ID)
	WHERE CANTIDAD > 0	
ORDER BY 1,2

END
GO


