
/****** Object:  StoredProcedure [dbo].[SetUCDesconsolidacion]    Script Date: 09/18/2013 17:28:48 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SetUCDesconsolidacion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SetUCDesconsolidacion]
GO

CREATE procedure [dbo].[SetUCDesconsolidacion]    
@nroucdesconsolidacion AS varchar(100) OUTPUT,    
@documento_id AS varchar(20) OUTPUT    
    
as    
Begin    
  

insert into documento_x_contenedoradesconsolidacion   
(documento_id,nroucdesconsolidacion)  
values  
(@documento_id,@nroucdesconsolidacion)  
    

end 


GO


