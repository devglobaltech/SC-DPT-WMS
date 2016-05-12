/****** Object:  StoredProcedure [dbo].[GETUBICADOSGUIA]    Script Date: 10/08/2013 11:01:56 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GETUBICADOSGUIA]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GETUBICADOSGUIA]
GO

CREATE PROCEDURE [dbo].[GETUBICADOSGUIA]  
@GUIA VARCHAR(20),
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
and uc_empaque in (select bulto from tmpbulto_dock where usuario = @usuario)


GO


