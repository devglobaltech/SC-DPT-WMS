/****** Object:  StoredProcedure [dbo].[ReubicarGuia]    Script Date: 10/08/2013 10:56:07 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ReubicarGuia]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ReubicarGuia]
GO

CREATE PROCEDURE [dbo].[ReubicarGuia]  
@DOCK VARCHAR(50),  
@GUIA VARCHAR(20)  
  
AS  
  
update uc_empaque set dock_id = (select dock_id from docks where UPPER(dock_cod) = UPPER(@DOCK))  
where nro_guia = @GUIA  
  
exec DropTMPBulto_DOCK


GO


