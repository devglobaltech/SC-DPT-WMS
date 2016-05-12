/****** Object:  StoredProcedure [dbo].[ValidarBultoenGuia]    Script Date: 10/08/2013 10:54:24 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ValidarBultoenGuia]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ValidarBultoenGuia]
GO

CREATE procedure [dbo].[ValidarBultoenGuia]           
 @BULTO VARCHAR(100),      
 @GUIA VARCHAR(20),          
 @VALUE AS NUMERIC(1) OUTPUT              
as           
SET @VALUE = (          
 select CASE WHEN count(uc_empaque) > 0 THEN 1 ELSE 0 END          
 from uc_empaque          
 where uc_empaque = @BULTO
 and nro_guia = @GUIA
 )


GO


