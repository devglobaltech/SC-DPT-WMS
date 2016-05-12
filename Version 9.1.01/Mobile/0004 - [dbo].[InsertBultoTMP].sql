/****** Object:  StoredProcedure [dbo].[InsertBultoTMP]    Script Date: 10/08/2013 10:34:57 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[InsertBultoTMP]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[InsertBultoTMP]
GO

CREATE procedure [dbo].[InsertBultoTMP]      
@bulto varchar(100),
@USUARIO VARCHAR(10)      
as      
    
if not exists (select bulto from TMPBulto_DOCK where bulto = @bulto AND USUARIO = @USUARIO )    
begin    
 insert into TMPBulto_DOCK (bulto,USUARIO) values (@bulto,@USUARIO)      
end


GO


