
/****** Object:  StoredProcedure [dbo].[CrearTMPBulto_DOCK]    Script Date: 10/08/2013 10:33:13 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CrearTMPBulto_DOCK]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[CrearTMPBulto_DOCK]
GO

CREATE procedure [dbo].[CrearTMPBulto_DOCK]    
@USUARIO VARCHAR(10)  
as      
IF OBJECT_ID('TMPBULTO_DOCK') IS NULL
	BEGIN
		create table TMPBULTO_DOCK(BULTO VARCHAR(100),USUARIO VARCHAR(10))        
	END
ELSE
	BEGIN
		DELETE FROM TMPBULTO_DOCK WHERE USUARIO = @USUARIO
	END
--DROP TABLE TMPBULTO_DOCK


GO


