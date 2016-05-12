/****** Object:  StoredProcedure [dbo].[EliminarTMPBulto_Dock]    Script Date: 10/08/2013 10:34:02 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EliminarTMPBulto_Dock]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EliminarTMPBulto_Dock]
GO

CREATE PROCEDURE [dbo].[EliminarTMPBulto_Dock]
AS
DROP TABLE #TMPBULTO_DOCK


GO


