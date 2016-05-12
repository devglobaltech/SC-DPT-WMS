IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[Trigger_Prioridades_Pickeadores]'))
DROP TRIGGER [dbo].[Trigger_Prioridades_Pickeadores]
GO

CREATE TRIGGER [dbo].[Trigger_Prioridades_Pickeadores]    
      ON            [dbo].[SYS_INT_DOCUMENTO]     
   AFTER INSERT  
AS     
    
BEGIN    
    exec Asignacion_Automatica_Prioridad_Picking     
END

GO


