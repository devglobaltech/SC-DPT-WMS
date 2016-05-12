/****** Object:  StoredProcedure [dbo].[ValidarGuia]    Script Date: 10/08/2013 10:58:39 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ValidarGuia]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ValidarGuia]
GO

CREATE procedure [dbo].[ValidarGuia]               
 @GUIA VARCHAR(20),          
 @VALUE AS NUMERIC(1) OUTPUT                  
              
as               
IF NOT EXISTS (SELECT U.DOCK_ID 
							 FROM PICKING P 
								 INNER JOIN  UC_EMPAQUE U ON U.UC_EMPAQUE = P.NRO_UCEMPAQUETADO   
							 WHERE P.ST_CONTROL_EXP = '0' 
									AND P.FACTURADO = '0'    
									AND U.NRO_GUIA IS NOT NULL AND U.NRO_GUIA = @guia
							 )    
 BEGIN    
  SET @VALUE = 0      
 END  
else  
 begin  
  set @value = 1   
 end


GO


