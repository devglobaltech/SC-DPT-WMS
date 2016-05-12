/****** Object:  StoredProcedure [dbo].[ValidaDock]    Script Date: 10/08/2013 10:52:53 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ValidaDock]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ValidaDock]
GO

CREATE procedure [dbo].[ValidaDock]           
 @DOCK VARCHAR(50),      
 @VALUE AS NUMERIC(1) OUTPUT              
          
as           
          
SET @VALUE = (          
 select CASE WHEN count(dock_id) > 0 THEN 1 ELSE 0 END          
 from docks
 where           
 dock_cod = @dock
 and activo = 1
 )


GO


