
/****** Object:  Trigger [sys_usuario_insert_trg]    Script Date: 04/11/2014 12:32:39 ******/
IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[sys_usuario_insert_trg]'))
DROP TRIGGER [dbo].[sys_usuario_insert_trg]
GO
