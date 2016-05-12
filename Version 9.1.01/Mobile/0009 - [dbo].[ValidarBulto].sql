
/****** Object:  StoredProcedure [dbo].[ValidarBulto]    Script Date: 10/08/2013 10:55:14 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ValidarBulto]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ValidarBulto]
GO

        
CREATE procedure [dbo].[ValidarBulto]                   
 @BULTO VARCHAR(100),        
 @GUIA  VARCHAR(20),        
 @VALUE varchar(1) OUTPUT                      
                  
as                   
--DECLARE @DOCKGUIA AS varchar(20)        
DECLARE @GUIABULTO AS VARCHAR(20)
SET @VALUE = 0      

--VALIDO QUE EXISTA EL BULTO.      
IF NOT exists (SELECT UC_EMPAQUE FROM UC_EMPAQUE WHERE UC_EMPAQUE = @BULTO)      
 BEGIN            
  RETURN      
 END   

--VALIDO QUE EL BULTO PERTENESCA A LA GUIA
SET @GUIABULTO = (SELECT NRO_GUIA FROM UC_EMPAQUE WHERE UC_EMPAQUE = @BULTO)
IF @GUIA = @GUIABULTO
	BEGIN
		SET @VALUE = 1
		RETURN
	END
   
/*
--OBTENGO DOCK DE LA GUIA        
SELECT @DOCKGUIA = d.dock_cod FROM UC_EMPAQUE E inner join docks d on e.dock_id = d.dock_id WHERE UC_EMPAQUE = @BULTO    
    
--SI LA GUIA NO ESTA EN NINGUN DOCK TODO OK        
IF @DOCKGUIA IS NULL        
 BEGIN        
  SET @VALUE = 1       
 RETURN       
 END         
--SI LA GUIA SE ENCUENTRA EN ALGUN DOCK        
IF @DOCKGUIA IS NOT NULL        
 BEGIN        
  --SI EL DOCK ACTUAL ES IGUAL AL DOCKs DE LA GUIA        
  IF @DOCK = @DOCKGUIA        
   BEGIN        
    SET @VALUE = 1        
  RETURN      
   END        
  ELSE        
   BEGIN          
  --SI LA GUIA SE ENCUENTRA PARCIALMENTE HUBICADA EN OTRO DOCK        
  SET @VALUE = 2        
  RETURN         
  END        
 END 

*/


GO


