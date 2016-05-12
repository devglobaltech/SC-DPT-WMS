/****** Object:  StoredProcedure [dbo].[cancelardesconsolidacion]    Script Date: 09/19/2013 12:34:19 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cancelardesconsolidacion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[cancelardesconsolidacion]
GO

CREATE procedure [dbo].[cancelardesconsolidacion]      
@viaje_id varchar(30) output    
      
as      
      
update picking set estado = 0, nro_ucdesconsolidacion = null       
where viaje_id = @viaje_id

DELETE FROM DOCUMENTO_X_CONTENEDORADESCONSOLIDACION 
WHERE DOCUMENTO_ID IN (	SELECT	distinct DD.DOC_EXT 
						FROM	SYS_INT_DET_DOCUMENTO DD, PICKING P
						WHERE	P.VIAJE_ID = @VIAJE_ID
								 AND dd.documento_id = p.documento_id
								 and dd.nro_linea = p.nro_linea
								 and dd.cliente_id = p.cliente_id)




GO


