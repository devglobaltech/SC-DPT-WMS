/****** Object:  UserDefinedFunction [dbo].[GETContenedoras]    Script Date: 09/18/2013 17:31:30 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GETContenedoras]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[GETContenedoras]
GO

CREATE FUNCTION [dbo].[GETContenedoras]        
(
	@documento_id varchar(50),@PRODUCTO_ID VARCHAR(30)
)        

RETURNS VARCHAR(500)         
        
AS        
BEGIN        
 DECLARE @USUARIO VARCHAR(50)        
 DECLARE @FINAL VARCHAR(400)        
 DECLARE @SEP   VARCHAR(1)
 DECLARE @FECHA   DATETIME      
      
 DECLARE PCUR CURSOR FOR        
  
	SELECT distinct 
			NroUCDesconsolidacion,D.FECHA_ALTA_GTW      
	from documento_x_contenedoradesconsolidacion  DC
	INNER JOIN DOCUMENTO D
		ON D.NRO_REMITO = DC.DOCUMENTO_ID
	INNER JOIN PICKING P
		ON P.DOCUMENTO_ID = D.DOCUMENTO_ID
		AND P.CLIENTE_ID = D.CLIENTE_ID
		AND P.nro_ucdesconsolidacion IS NULL
		AND P.PRODUCTO_ID = @PRODUCTO_ID--'100006'
	
	where DC.documento_id = @documento_id      
  ORDER BY D.fecha_alta_gtw    

 OPEN PCUR        
 FETCH NEXT FROM PCUR INTO @USUARIO,@FECHA      
 WHILE @@FETCH_STATUS = 0        
 BEGIN        
  IF (ISNULL(@FINAL,'')='')        
  BEGIN        
     SET @SEP=''        
  END        
  ELSE        
         BEGIN           
     SET @SEP=';'         
  END        
  SET @FINAL=CAST(ISNULL(@FINAL,'') + @SEP + @USUARIO AS VARCHAR(400))        
  FETCH NEXT FROM PCUR INTO @USUARIO  ,@FECHA      
 END        
 CLOSE PCUR        
 DEALLOCATE PCUR        
 RETURN (ISNULL(CAST(@FINAL AS VARCHAR(400)),''))        
END

GO


