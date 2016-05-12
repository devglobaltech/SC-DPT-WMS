/****** Object:  View [dbo].[vPICKING]    Script Date: 10/19/2013 12:29:52 ******/
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vPICKING]'))
DROP VIEW [dbo].[vPICKING]
GO

CREATE  VIEW [dbo].[vPICKING]
as
Select * from picking (nolock)
union 
Select * from Picking_Historico (nolock)


GO


