
/****** Object:  StoredProcedure [dbo].[EMPAQUE_SAVE_TRANSP]    Script Date: 10/03/2013 13:46:57 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EMPAQUE_SAVE_TRANSP]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EMPAQUE_SAVE_TRANSP]
GO

CREATE PROCEDURE [dbo].[EMPAQUE_SAVE_TRANSP]
@TR AS VARCHAR(100) OUTPUT
AS 
BEGIN
UPDATE	UC_EMPAQUE 
SET		TRANSPORTE_ID=@TR
WHERE	UC_EMPAQUE in (select p.NRO_UCEMPAQUETADO
						FROM PICKING P   
						INNER JOIN DOCUMENTO D ON(P.DOCUMENTO_ID=D.DOCUMENTO_ID)    
						INNER JOIN UC_EMPAQUE U ON(P.NRO_UCEMPAQUETADO=U.UC_EMPAQUE)    
						WHERE EXISTS (SELECT 1 FROM #TMP_EMPAQUE_CAB T WHERE T.CLIENTE_ID = D.CLIENTE_ID AND T.PEDIDO=D.NRO_REMITO AND T.SUCURSAL_ID = D.SUCURSAL_DESTINO)
						AND (P.PALLET_CERRADO='1'))
END


GO


