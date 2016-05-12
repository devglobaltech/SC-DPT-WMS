/****** OBJECT:  STOREDPROCEDURE [DBO].[LOCATOREGRESO]    SCRIPT DATE: 03/30/2015 10:42:47 ******/
IF  EXISTS (SELECT * FROM SYS.OBJECTS WHERE OBJECT_ID = OBJECT_ID(N'[DBO].[ABAST_REPORTE]') AND TYPE IN (N'P', N'PC'))
DROP PROCEDURE [DBO].[ABAST_REPORTE]
GO

CREATE        PROCEDURE [DBO].[ABAST_REPORTE]
@ABAST_ID	BIGINT	OUTPUT
AS
BEGIN
	SELECT	--CABECERA.
			DA.ABAST_ID			AS [NRO.TAREA ABAST.],
			DA.CLIENTE_ID		AS [COD.CLIENTE],
			DA.PRODUCTO_ID		AS [COD.PRODUCTO],
			P.POSICION_COD		AS [COD.POSICION],
			DA.PRIORIDAD		AS [PRIORIDAD],
			DA.USUARIO			AS [USUARIO],
			DA.CANT_A_ABASTECER	AS [CANT_A_ABASTECER],
			--DETALLE.
			PACL.POSICION_COD	AS [FROM_POSICION],
			SUM(RL.CANTIDAD)	AS [FROM_QTY],
			DD.NRO_LOTE			AS [FROM_NRO_LOTE],
			DD.NRO_PARTIDA		AS [FROM_NRO_PARTIDA],
			DD.NRO_SERIE		AS [FROM_NRO_SERIE]
			--
	FROM	DET_ABASTECIMIENTO DA INNER JOIN ABAST_CONSUMO_LOCATOR ACL		ON(DA.ABAST_ID=ACL.ABAST_ID)
			INNER JOIN POSICION P											ON(DA.POSICION_ID=P.POSICION_ID)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL							ON(ACL.RL_ID=RL.RL_ID)
			INNER JOIN POSICION PACL										ON(RL.POSICION_ACTUAL=PACL.POSICION_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT						ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD										ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
	WHERE	((@ABAST_ID IS NULL) OR(DA.ABAST_ID=@ABAST_ID))
	GROUP BY
			DA.ABAST_ID,		DA.CLIENTE_ID,			DA.PRODUCTO_ID,			P.POSICION_COD,
			DA.PRIORIDAD,		DA.USUARIO,				PACL.POSICION_COD,		DD.NRO_LOTE,
			DD.NRO_PARTIDA,		DD.NRO_PARTIDA,			DD.NRO_SERIE,			PACL.ORDEN_PICKING,
			DA.CANT_A_ABASTECER,P.ORDEN_LOCATOR
	ORDER BY
			P.ORDEN_LOCATOR,PACL.ORDEN_PICKING		
END			