/****** Object:  StoredProcedure [dbo].[Anular_Prioridad_Pickeadores]    Script Date: 10/18/2013 15:43:38 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Anular_Prioridad_Pickeadores]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Anular_Prioridad_Pickeadores]
GO

CREATE PROCEDURE  [dbo].[Anular_Prioridad_Pickeadores]
AS
begin
	declare @Usuario_Anulacion varchar(20)
	declare @Terminal_Anulacion varchar(20)

	select @Usuario_Anulacion  = usuario_id, @Terminal_Anulacion=terminal  
	from #temp_usuario_loggin
	
	UPDATE	Prioridades_Pickeadores 
	SET		Proceso_Activo = 0, 
			Usuario_Anulacion = @Usuario_Anulacion,
			Terminal_Anulacion = @Terminal_Anulacion,
			Fecha_Anulacion =GETDATE()
	where	ID = (	SELECT max (ID) 
					FROM Prioridades_Pickeadores ) -- toma el ùltimo registro
end

GO


