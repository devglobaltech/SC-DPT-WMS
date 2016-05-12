
/****** Object:  StoredProcedure [dbo].[FUNCIONES_INVENTARIO_API#GETSELECTCONTROL]    Script Date: 04/21/2015 16:12:55 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[FUNCIONES_INVENTARIO_API#GETSELECTCONTROL]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#GETSELECTCONTROL]
GO

CREATE PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#GETSELECTCONTROL]
@P_DOC_TRANS_ID NUMERIC(20) output,
@P_LISTADO numeric(1) output
AS 
BEGIN


SELECT i.inventario_id AS inventario_id,     
   dc.marbete AS marbete,                    
   i.nro_conteo AS nro_conteo,               
	ISNULL(di.cant_stock_cont_1,0) AS ExistenciaConteo1,
   dc.conteo1 as Conteo1,                    
   (dc.conteo1 - di.CANT_STOCK_CONT_1) as DifConteo1, 
	ISNULL(di.cant_stock_cont_2,0) AS ExistenciaConteo2,
   dc.conteo2 as Conteo2,                    
   (dc.conteo2 - di.CANT_STOCK_CONT_2) as DifConteo2, 
	ISNULL(di.cant_stock_cont_3,0) AS ExistenciaConteo3,
   dc.conteo3 as Conteo3,                    
   (dc.conteo3 - di.CANT_STOCK_CONT_3) as DifConteo3, 
   CASE WHEN A.CANT_AJU IS NOT NULL THEN
		ISNULL(A.CANT_AJU, ISNULL(DC.CONTEO3, ISNULL(DC.CONTEO2, DC.CONTEO1 ))) 
   ELSE
		0
   END AS CANT_AJU,
   a.PROCESADO, 
   dc.obsconteo1 as obsconteo1,              
   dc.obsconteo2 as obsconteo2,              
   dc.obsconteo3 as obsconteo3,              
   cli.razon_social AS cli_razon_social,     
   prod.producto_id AS producto_id,          
   prod.codigo_producto AS cod_producto,     
   prod.descripcion AS prod_descripcion,     
   di.nro_lote,
   di.nro_partida,
   dc.posicion_id AS posicion_id,            
   dep.descripcion AS deposito_cod,          
   nave.nave_cod AS nave_cod,                
   calle.calle_cod AS calle_cod,             
   col.columna_cod AS columna_cod,           
   nivel.nivel_cod AS nivel_cod              
   ,di.modo_ingreso as modo_Ingreso          
   ,CASE WHEN di.pos_lockeada = 1 THEN 'S' ELSE 'N' end as lockeada
FROM                                         
   inventario i 
	inner join det_inventario di on (i.inventario_id=di.inventario_id)
	left join DET_INVENTARIO_AJU A on (A.INVENTARIO_ID = DI.INVENTARIO_ID AND A.MARBETE = DI.MARBETE)
	inner join det_conteo dc on (di.inventario_id=dc.inventario_id AND di.marbete=dc.marbete)
	inner join producto prod on (dc.cliente_id=prod.cliente_id AND dc.producto_id=prod.producto_id)
	inner join cliente cli on (dc.cliente_id=cli.cliente_id)
  	inner join posicion pos  on (dc.posicion_id=pos.posicion_id)
  	inner join nave  on (pos.nave_id=nave.nave_id)
  	inner join deposito dep  on (nave.deposito_id=dep.deposito_id)
  	inner join calle_nave calle  on (pos.nave_id=calle.nave_id AND pos.calle_id=calle.calle_id)
  	inner join columna_nave col  on (pos.nave_id=col.nave_id  AND pos.calle_id=col.calle_id AND pos.columna_id=col.columna_id)
  	inner join nivel_nave nivel  on (pos.nave_id=nivel.nave_id AND pos.calle_id=nivel.calle_id AND pos.columna_id=nivel.columna_id AND pos.nivel_id=nivel.nivel_id)
WHERE                                        
	   (	  @P_LISTADO = 1 
		 OR ( @P_LISTADO = 2 AND (isnull(dc.conteo3 - di.CANT_STOCK_CONT_3,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) <> 0))
		 OR ( @P_LISTADO = 3 AND (isnull(dc.conteo3 - di.cantidad,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) = 0))
		 OR ( @P_LISTADO = 4 AND di.modo_ingreso in ('M'))
		 OR ( @P_LISTADO = 5 AND di.pos_lockeada=1)
       )
	AND i.doc_trans_id=@P_DOC_TRANS_ID
 UNION                                       
SELECT                                       
   i.inventario_id AS inventario_id,         
   dc.marbete AS marbete,                    
   i.nro_conteo AS nro_conteo,               
	ISNULL(di.cant_stock_cont_1,0) AS ExistenciaConteo1,
   dc.conteo1 as Conteo1,                    
   (dc.conteo1 - di.CANT_STOCK_CONT_1) as DifConteo1, 
	ISNULL(di.cant_stock_cont_2,0) AS ExistenciaConteo2,
   dc.conteo2 as Conteo2,                    
   (dc.conteo2 - di.CANT_STOCK_CONT_2) as DifConteo2, 
	ISNULL(di.cant_stock_cont_3,0) AS ExistenciaConteo3,
   dc.conteo3 as Conteo3,                    
   (dc.conteo3 - di.CANT_STOCK_CONT_3) as DifConteo3,
   CASE WHEN A.CANT_AJU IS NOT NULL THEN
		ISNULL(A.CANT_AJU, ISNULL(DC.CONTEO3, ISNULL(DC.CONTEO2, DC.CONTEO1 ))) 
   ELSE
		0
   END AS CANT_AJU,
   a.PROCESADO,
   dc.obsconteo1 as obsconteo1,              
   dc.obsconteo2 as obsconteo2,              
   dc.obsconteo3 as obsconteo3,              
   cli.razon_social AS cli_razon_social,     
   prod.producto_id AS producto_id,          
   prod.codigo_producto AS cod_producto,     
   prod.descripcion AS prod_descripcion,  
   di.nro_lote,
   di.nro_partida,   
   dc.posicion_id AS posicion_id,            
   dep.descripcion AS deposito_cod,          
   nave.nave_cod AS nave_cod,                
   null AS calle_cod,                        
   null AS columna_cod,                      
   null AS nivel_cod                         
   ,di.modo_ingreso as modo_Ingreso          
   ,CASE WHEN di.pos_lockeada = 1 THEN 'S' ELSE 'N' end as lockeada
FROM 
    inventario i 
    inner join det_inventario di on (i.inventario_id=di.inventario_id)
	left join DET_INVENTARIO_AJU A on (A.INVENTARIO_ID = DI.INVENTARIO_ID AND A.MARBETE = DI.MARBETE)
	inner join det_conteo dc on (di.inventario_id=dc.inventario_id AND di.marbete=dc.marbete)
	inner join producto prod on (dc.cliente_id=prod.cliente_id AND dc.producto_id=prod.producto_id)
	inner join cliente cli on (dc.cliente_id=cli.cliente_id)
  	inner join nave  on (DC.nave_id=nave.nave_id)
  	inner join deposito dep  on (nave.deposito_id=dep.deposito_id)
WHERE                                        
      (	  @P_LISTADO = 1 
		 OR ( @P_LISTADO = 2 AND (isnull(dc.conteo3 - di.CANT_STOCK_CONT_3,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) <> 0))
		 OR ( @P_LISTADO = 3 AND (isnull(dc.conteo3 - di.CANT_STOCK_CONT_3,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) = 0))
		 OR ( @P_LISTADO = 4 AND di.modo_ingreso in ('M'))
		 OR ( @P_LISTADO = 5 AND di.pos_lockeada=1)
       )

   AND I.doc_trans_id=@P_DOC_TRANS_ID
ORDER BY 2                                   

END

GO


