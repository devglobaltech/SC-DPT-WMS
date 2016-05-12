ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#GETDIFINVENTARIO]
@INVENTARIO_ID AS NUMERIC(20,0) output
AS 
BEGIN

  declare @usu_id as varchar(100)
  declare @usu_nombre as varchar(100)
  declare @terminal as varchar(100)

	select top 1 @usu_id = usuario_id, @terminal = terminal from #temp_usuario_loggin

	select @usu_nombre = nombre from  sys_usuario where usuario_id =@usu_id

  SELECT  prod.cliente_id AS cliente_id         
          ,cli.razon_social AS cli_razon_social  
          ,prod.producto_id AS producto_id       
          ,prod.codigo_producto AS cod_producto  
          ,prod.descripcion AS prod_descripcion  
          ,dc.posicion_id AS posicion_id         
          ,dep.deposito_id AS deposito_id        
          ,dep.descripcion AS deposito_cod       
          ,nave.nave_id AS nave_id               
          ,nave.nave_cod AS nave_cod             
          ,calle.calle_id AS calle_id            
          ,calle.calle_cod AS calle_cod
          ,col.columna_id AS columna_id           
          ,col.columna_cod AS  columna_cod
          ,nivel.nivel_id AS nivel_id             
          ,nivel.nivel_cod AS nivel_cod 
          ,uc.uc_id AS unidad_contenedora_id      
          ,uc.nro_serie AS ucNro_Serie            
          ,uc.descripcion AS ucDescripcion        
          ,dc.inventario_id AS inventario_id      
          ,dc.marbete AS marbete                  
          ,dc.conteo1 AS Conteo1                  
          ,dc.conteo2 AS Conteo2                  
          ,dc.conteo3 AS Conteo3                  
          ,di.cant_stock_cont_1 as Stock_1                 
          ,di.cant_stock_cont_2 as Stock_2                 
          ,di.cant_stock_cont_3 as Stock_3                 
          ,(dc.conteo1-di.cant_stock_cont_1) AS diffConteo1 
          ,(dc.conteo2-di.cant_stock_cont_2) AS diffConteo2 
          ,(dc.conteo3-di.cant_stock_cont_3) AS diffConteo3 
          ,dc.obsconteo1 AS obsconteo1            
          ,dc.obsconteo2 AS obsconteo2            
          ,dc.obsconteo3 AS obsconteo3            
          ,@usu_nombre as USOINTERNOUsuario         
          ,@terminal AS USOINTERNOTerminal
          ,case when isnull(prod.ingLoteProveedor,'0') ='1'  then di.nro_lote     else '' end as nro_lote
          ,case when isnull(prod.ingpartida,'0')       ='1'  then di.nro_partida  else '' end as nro_partida
  FROM    det_inventario di,det_conteo dc,producto prod,cliente cli,posicion pos                       
          LEFT OUTER JOIN unidad_contenedora uc on pos.posicion_id=uc.posicion_id
          ,nave,deposito dep,calle_nave calle,columna_nave col,nivel_nave nivel                   
  WHERE   di.inventario_id=dc.inventario_id      
          AND di.marbete=dc.marbete              
          AND dc.cliente_id=prod.cliente_id      
          AND dc.producto_id=prod.producto_id    
          AND dc.cliente_id=cli.cliente_id       
          AND dc.posicion_id=pos.posicion_id     
          AND pos.nave_id=nave.nave_id           
          AND nave.deposito_id=dep.deposito_id   
          AND pos.nave_id=calle.nave_id          
          AND pos.calle_id=calle.calle_id        
          AND pos.nave_id=col.nave_id            
          AND pos.calle_id=col.calle_id          
          AND pos.columna_id=col.columna_id      
          AND pos.nave_id=nivel.nave_id          
          AND pos.calle_id=nivel.calle_id        
          AND pos.columna_id=nivel.columna_id    
          AND pos.nivel_id=nivel.nivel_id        
          AND di.pos_lockeada=0                  
          AND di.inventario_id = @INVENTARIO_ID
  UNION                                               
  SELECT  prod.cliente_id AS cliente_id         
          ,cli.razon_social AS cli_razon_social 
          ,prod.producto_id AS producto_id      
          ,prod.codigo_producto AS cod_producto 
          ,prod.descripcion AS prod_descripcion 
          ,NULL AS posicion_id                  
          ,dep.deposito_id AS deposito_id       
          ,dep.descripcion AS deposito_cod      
          ,nave.nave_id AS nave_id              
          ,nave.nave_cod AS nave_cod            
          ,NULL AS calle_id                     
          ,NULL AS calle_cod                    
          ,NULL AS columna_id                   
          ,NULL AS columna_cod                  
          ,NULL AS nivel_id                     
          ,NULL AS nivel_cod                    
          ,NULL AS unidad_contenedora_id        
          ,NULL AS ucNro_Serie                  
          ,NULL AS ucDescripcion                
          ,dc.inventario_id AS inventario_id    
          ,dc.marbete AS marbete                
          ,dc.conteo1 AS Conteo1                
          ,dc.conteo2 AS Conteo2                
          ,dc.conteo3 AS Conteo3                
          ,di.cant_stock_cont_1 as Stock_1                 
          ,di.cant_stock_cont_2 as Stock_2                 
          ,di.cant_stock_cont_3 as Stock_3                 
          ,(dc.conteo1-di.cant_stock_cont_1) AS diffConteo1 
          ,(dc.conteo2-di.cant_stock_cont_2) AS diffConteo2 
          ,(dc.conteo3-di.cant_stock_cont_3) AS diffConteo3 
          ,dc.obsconteo1 AS obsconteo1          
          ,dc.obsconteo2 AS obsconteo2          
          ,dc.obsconteo3 AS obsconteo3          
          ,@usu_nombre as USOINTERNOUsuario            
          ,@terminal AS USOINTERNOTerminal    
          ,case when isnull(prod.ingLoteProveedor,'0') ='1'  then di.nro_lote     else '' end as nro_lote
          ,case when isnull(prod.ingpartida,'0')       ='1'  then di.nro_partida  else '' end as nro_partida
  FROM    det_inventario di,det_conteo dc,producto prod,cliente cli,nave,deposito dep                            
  WHERE   di.inventario_id=dc.inventario_id        
          AND di.marbete=dc.marbete                
          AND dc.cliente_id=prod.cliente_id        
          AND dc.producto_id=prod.producto_id      
          AND dc.cliente_id=cli.cliente_id         
          AND dc.nave_id=nave.nave_id              
          AND nave.deposito_id=dep.deposito_id     
          AND di.pos_lockeada=0                    
          AND di.inventario_id=@INVENTARIO_ID
  ORDER BY 
          20,1,3,21
END

GO
