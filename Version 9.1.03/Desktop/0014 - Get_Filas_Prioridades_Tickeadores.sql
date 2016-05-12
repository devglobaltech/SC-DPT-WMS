IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Get_Filas_Prioridades_Tickeadores]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Get_Filas_Prioridades_Tickeadores]
GO

CREATE PROCEDURE  [dbo].[Get_Filas_Prioridades_Tickeadores]
 @OUT int output

AS
set @OUT = ( select count(*) from prioridades_Pickeadores)

print @OUT


GO


