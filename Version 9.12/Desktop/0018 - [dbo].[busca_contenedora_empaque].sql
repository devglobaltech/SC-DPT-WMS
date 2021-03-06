IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[busca_contenedora_empaque]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[busca_contenedora_empaque]
GO

-- =============================================
-- Author:		LRojas
-- Create date: 17/04/2012
-- Description:	Procedimiento para buscar productos para empaquetar
-- =============================================
create PROCEDURE [dbo].[busca_contenedora_empaque] 
	@CLIENTE_ID as varchar(15) OUTPUT,
    @PEDIDO_ID as varchar(100) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    SELECT DISTINCT P.PALLET_PICKING, 
           'Abrir Contenedora'      [Abrir], 
           'Ver Contenido'          [Ver], 
           'Eliminar Contenedora'   [Eliminar], 
           'Imprimir Etiqueta'      [Imprimir] 
      FROM DOCUMENTO D 
     INNER JOIN PICKING P ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
     WHERE P.CLIENTE_ID = @CLIENTE_ID
       AND D.NRO_REMITO = @PEDIDO_ID
       AND P.PALLET_CONTROLADO <> '0'
END
