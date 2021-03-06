IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[busca_caja_contenedora_empaque]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[busca_caja_contenedora_empaque]
GO

-- =============================================
-- Author:		LRojas
-- Create date: 19/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
create PROCEDURE [dbo].[busca_caja_contenedora_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(100) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    SELECT P.PRODUCTO_ID [Cod Producto], ISNULL(P.NRO_LOTE,'') AS NRO_LOTE, ISNULL(P.NRO_PARTIDA,'') AS NRO_PARTIDA, ISNULL(P.NRO_SERIE,'') AS NRO_SERIE, SUM(P.CANT_CONFIRMADA) [Cantidad], PR.UNIDAD_ID [Unidad], PR.DESCRIPCION [Descripci�n], 'Quitar Producto' [Acci�n]
	  FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
	 INNER JOIN PRODUCTO PR ON (P.PRODUCTO_ID = PR.PRODUCTO_ID AND P.CLIENTE_ID = PR.CLIENTE_ID)
	 WHERE P.CLIENTE_ID = @CLIENTE_ID AND D.NRO_REMITO = @PEDIDO_ID AND P.PALLET_PICKING = @NRO_CONTENEDORA AND P.PALLET_CONTROLADO <> '0'
     GROUP BY P.PRODUCTO_ID,P.NRO_LOTE,P.NRO_PARTIDA,P.NRO_SERIE, PR.UNIDAD_ID, PR.DESCRIPCION
END
