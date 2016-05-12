/****** Object:  StoredProcedure [dbo].[GetValoresProducto]    Script Date: 09/18/2013 17:30:04 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetValoresProducto]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetValoresProducto]
GO
  
CREATE procedure [dbo].[GetValoresProducto]                                 
@producto_id varchar(30) output,                                
@viaje_id varchar(50) output                      
                                
as                                
begin                                
  DECLARE @COUNT  SMALLINT          
  DECLARE @CLIENTE VARCHAR(15)          
           
           
--  SELECT DISTINCT          
--   @CLIENTE=CLIENTE_ID          
--  FROM PICKING          
--  WHERE VIAJE_ID=@VIAJE_ID          
            
  SELECT	@COUNT=COUNT(*)          
  FROM		RL_PRODUCTO_CODIGOS          
  WHERE		CLIENTE_ID IN (SELECT CLIENTE_ID FROM PICKING WHERE VIAJE_ID = @VIAJE_ID)
			AND CODIGO=@PRODUCTO_ID          
           
  IF @COUNT>0          
  BEGIN          
  --QUIERE DECIR QUE ES UN CODIGO.          
	  SELECT	@PRODUCTO_ID=PRODUCTO_ID          
	  FROM		RL_PRODUCTO_CODIGOS          
	  WHERE		CLIENTE_ID IN (SELECT CLIENTE_ID FROM PICKING WHERE VIAJE_ID = @VIAJE_ID)         
				AND CODIGO=@PRODUCTO_ID          
  END           
           
 select top 1        
	p.producto_id,        
	p.descripcion,        
	sum(cant_confirmada ) as CANT_CONFIRMADA,        
	dbo.GETContenedoras(d.nro_remito,P.PRODUCTO_ID) as NroUCDesconsolidacion               
 from picking p        
 inner join documento d on (p.documento_id = d.documento_id)
 where	viaje_id = @viaje_id--'8888'        
		and p.producto_id = @producto_id        
		and nro_ucdesconsolidacion is null 
		AND P.CANT_CONFIRMADA > 0      
 group by p.producto_id, d.fecha_alta_gtw, p.descripcion,d.nro_remito        
 order by d.fecha_alta_gtw,D.NRO_REMITO        
end  



GO


