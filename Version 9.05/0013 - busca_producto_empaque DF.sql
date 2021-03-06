
/****** Object:  StoredProcedure [dbo].[busca_producto_empaque]    Script Date: 02/05/2013 14:37:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		LRojas
-- Create date: 17/04/2012
-- Description:	Procedimiento para buscar productos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[busca_producto_empaque] 
	@CLIENTE_ID as varchar(15) OUTPUT,
    @PEDIDO_ID as varchar(30) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
SELECT	PR.PRODUCTO_ID as ProductoID
		,ISNULL(P.NRO_LOTE,'')	as LoteProveedor
		,ISNULL(P.NRO_PARTIDA,'') as NroPartida
		,ISNULL(P.NRO_SERIE,'') as NroSerie
		,SUM(P.CANT_CONFIRMADA) as CANTIDAD_PICKEADA
		,ISNULL(TMP.CANT_CONFIRMADA, 0) CANTIDAD_CONTROLADA
		,PR.UNIDAD_ID as Unidad
		,PR.DESCRIPCION as DescrProd
FROM DOCUMENTO D
INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID)
INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
LEFT JOIN	(SELECT DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, SUM(CANT_CONFIRMADA) CANT_CONFIRMADA, PALLET_CONTROLADO 
			FROM PICKING WHERE PALLET_CONTROLADO <> '0'
			GROUP BY DOCUMENTO_ID,NRO_LOTE,NRO_PARTIDA,NRO_SERIE, PRODUCTO_ID, PALLET_CONTROLADO
			) TMP ON TMP.DOCUMENTO_ID = P.DOCUMENTO_ID AND TMP.PRODUCTO_ID = P.PRODUCTO_ID AND ISNULL(TMP.NRO_LOTE,'') = ISNULL(P.NRO_LOTE,'') AND ISNULL(TMP.NRO_PARTIDA,'') = ISNULL(P.NRO_PARTIDA,'') AND ISNULL(TMP.NRO_SERIE,'') = ISNULL(P.NRO_SERIE,'')
WHERE	P.CLIENTE_ID = @CLIENTE_ID
		AND D.NRO_REMITO = @PEDIDO_ID
		AND P.CANT_CONFIRMADA >0 --se agrega para que no aparezcan productos pickeados en cero
GROUP BY	PR.PRODUCTO_ID
			,ISNULL(TMP.CANT_CONFIRMADA, 0)
			,P.NRO_LOTE
			,P.NRO_PARTIDA
			,ISNULL(P.NRO_SERIE,'')
			,PR.UNIDAD_ID
			,PR.DESCRIPCION
END

