/****** Object:  StoredProcedure [dbo].[EMPAQUE_GET_CLIENTE]    Script Date: 10/03/2013 12:18:22 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EMPAQUE_GET_CLIENTE]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EMPAQUE_GET_CLIENTE]
GO


CREATE PROCEDURE [dbo].[EMPAQUE_GET_CLIENTE]    
 @CLIENTE VARCHAR(15) OUTPUT,    
 @SUCURSAL VARCHAR(20) OUTPUT    
AS    
BEGIN    
 SELECT  S.cliente_id AS CLIENTE  
   ,S.NOMBRE    
   ,ISNULL(S.CALLE,'')+ ISNULL(S.NUMERO,'')+ ISNULL(S.LOCALIDAD,'') DOM    
 FROM SUCURSAL s  
 inner join cliente c  
 on c.cliente_id = s.cliente_id  
 WHERE S.CLIENTE_ID=@CLIENTE    
   AND S.SUCURSAL_ID=@SUCURSAL    
END


GO


