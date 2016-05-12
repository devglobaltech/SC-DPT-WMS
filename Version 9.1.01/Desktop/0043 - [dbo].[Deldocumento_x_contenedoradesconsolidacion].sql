/****** Object:  StoredProcedure [dbo].[Deldocumento_x_contenedoradesconsolidacion]    Script Date: 09/19/2013 12:35:50 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Deldocumento_x_contenedoradesconsolidacion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Deldocumento_x_contenedoradesconsolidacion]
GO

CREATE procedure [dbo].[Deldocumento_x_contenedoradesconsolidacion]
@documento_id as varchar(50) output
as
begin
	delete from documento_x_contenedoradesconsolidacion 
	where documento_id = @documento_id
end



GO


