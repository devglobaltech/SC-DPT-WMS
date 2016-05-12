/****** Object:  StoredProcedure [dbo].[DropTMPBulto_DOCK]    Script Date: 10/08/2013 11:23:22 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DropTMPBulto_DOCK]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[DropTMPBulto_DOCK]
GO

CREATE procedure [dbo].[DropTMPBulto_DOCK]

as

drop table TMPBULTO_DOCK


GO


