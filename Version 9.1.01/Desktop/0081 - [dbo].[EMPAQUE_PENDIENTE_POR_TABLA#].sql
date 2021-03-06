/****** Object:  StoredProcedure [dbo].[EMPAQUE_PENDIENTE_POR_TABLA#]    Script Date: 10/03/2013 13:45:21 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EMPAQUE_PENDIENTE_POR_TABLA#]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EMPAQUE_PENDIENTE_POR_TABLA#]
GO

CREATE PROCEDURE [dbo].[EMPAQUE_PENDIENTE_POR_TABLA#]  
@QTY  BIGINT OUTPUT  
AS  
BEGIN  
 SELECT @QTY = count(*)  
        FROM PICKING P     
				INNER JOIN DOCUMENTO D 
						ON(P.DOCUMENTO_ID=D.DOCUMENTO_ID)      
        WHERE EXISTS (SELECT 1 FROM #TMP_EMPAQUE_CAB T WHERE T.CLIENTE_ID = D.CLIENTE_ID AND T.PEDIDO=D.NRO_REMITO AND T.SUCURSAL_ID = D.SUCURSAL_DESTINO)  
				AND P.CANT_CONFIRMADA > 0
   AND (P.PALLET_CERRADO<>'1' OR P.NRO_UCEMPAQUETADO IS NULL)  
 IF @QTY IS NULL  
 BEGIN  
  SET @QTY=0  
 END  
 --truncate table tmp   
 --insert into tmp select distinct PEDIDO from #TMP_EMPAQUE_CAB  
  
END


GO


