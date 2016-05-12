/****** Object:  StoredProcedure [dbo].[ubicarBultoenDock]    Script Date: 10/08/2013 10:53:40 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ubicarBultoenDock]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ubicarBultoenDock]
GO

--select * from docks  
CREATE PROCEDURE [dbo].[ubicarBultoenDock]  
@USUARIO VARCHAR(10),
@DOCK VARCHAR(50)  
  
AS  
  
UPDATE UC_EMPAQUE  
SET DOCK_ID = (SELECT DOCK_ID FROM DOCKS WHERE DOCK_COD = @DOCK)  
WHERE UC_EMPAQUE IN (SELECT BULTO FROM TMPBULTO_DOCK WHERE USUARIO = @USUARIO)


GO


