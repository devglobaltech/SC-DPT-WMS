/****** Object:  StoredProcedure [dbo].[GETPENDIENTESGUIA]    Script Date: 10/08/2013 11:02:36 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GETPENDIENTESGUIA]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GETPENDIENTESGUIA]
GO

CREATE PROCEDURE [dbo].[GETPENDIENTESGUIA]  
@GUIA VARCHAR(20) ,
@USUARIO VARCHAR(10) 
AS  
select   
 UC_EMPAQUE  
 ,ALTO  
 ,ANCHO  
 ,LARGO  
 ,NRO_GUIA  
from uc_empaque   
where nro_guia = @GUIA  
and uc_empaque NOT in (select bulto from tmpbulto_dock where usuario = @usuario)


GO


