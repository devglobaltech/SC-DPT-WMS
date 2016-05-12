
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

ALTER PROCEDURE [DBO].[PRINTETIPALLETPICKING]
@PalletPicking	varchar(30) output,
@Viaje_ID		varchar(100)output,
@EtiquetaId		varchar(20) output
as
Begin
	Declare @Cliente as varchar(30)
	--Obtengo el cliente.
	select Distinct @cliente=cliente_ID from picking where pallet_picking=@PalletPicking
	--Obtengo la etiqueta.
	Select	@EtiquetaId=ISNULL(etiqueta_id,0) from etiqueta_producto 
	where	cliente_id=(select distinct cliente_id from picking where pallet_picking=@PalletPicking)--@cliente 
			and Producto_id='ETI_PALLET_EGR' and tipo_operacion_id='EGR'
	If @EtiquetaId is null
	Begin
		Set @EtiquetaId=0
	End	
	--Obtengo el grupo de datos.
	SELECT	 VIAJE_ID								    [PICKING.VIAJE_ID]
          ,PALLET_PICKING							[PICKING.PALLET_PICKING]
          ,P.POSICION_COD             [PICKING.POSICION_COD]
          ,P.PROP1                    [PICKING.PROP1]
          ,P.RUTA                     [PICKING.RUTA]
          ,SUM(P.CANT_CONFIRMADA)				[PICKING.CANT_CONFIRMADA]          
          ,D.NRO_REMITO							  [DOCUMENTO_EGR.NRO_REMITO]
          ,S.CALLE                    [SUCURSAL.CALLE]
          ,S.NOMBRE                   [SUCURSAL.NOMBRE]
          ,S.SUCURSAL_ID              [SUCURSAL.SUCURSAL_ID]
          ,C.CALLE                    [CLIENTE.CALLE]
          ,C.CATEGORIA_CLIENTE_ID     [CLIENTE.CATEGORIA_CLIENTE_ID]
          ,C.CATEGORIA_IMPOSITIVA_ID  [CLIENTE.CATEGORIA_IMPOSITIVA_ID]
          ,C.CODIGO_POSTAL            [CLIENTE.CODIGO_POSTAL]
          ,C.EMAIL                    [CLIENTE.EMAIL]
          ,C.FAX                      [CLIENTE.FAX]
          ,C.LOCALIDAD                [CLIENTE.LOCALIDAD]
          ,C.CLIENTE_ID               [CLIENTE.CLIENTE_ID]
          ,C.NOMBRE                   [CLIENTE.NOMBRE]
          ,C.NRO_DOCUMENTO            [CLIENTE.NRO_DOCUMENTO]
          ,C.NUMERO                   [CLIENTE.NUMERO]
          ,C.OBSERVACIONES            [CLIENTE.OBSERVACIONES]
          ,C.PAIS_ID                  [CLIENTE.PAIS_ID]
          ,C.PROVINCIA_ID             [CLIENTE.PROVINCIA_ID]
          ,C.RAZON_SOCIAL             [CLIENTE.RAZON_SOCIAL]
          ,C.TELEFONO_1               [CLIENTE.TELEFONO_1]
          ,C.TELEFONO_2               [CLIENTE.TELEFONO_2]
          ,C.TELEFONO_3               [CLIENTE.TELEFONO_3]
          ,C.TIPO_DOCUMENTO_ID        [CLIENTE.TIPO_DOCUMENTO_ID]
          ,C.ZONA_ID                  [CLIENTE.ZONA_ID]
          ,C.REMITO_ID                [CLIENTE.REMITO_ID]   
          ,DD.PRODUCTO_ID             [DET_DOCUMENTO_EGR.PRODUCTO_ID]
          ,DD.DESCRIPCION             [DET_DOCUMENTO_EGR.DESCRIPCION]
          ,DD.FECHA_VENCIMIENTO       [DET_DOCUMENTO_EGR.FECHA_VENCIMIENTO]
          ,DD.NRO_BULTO               [DET_DOCUMENTO_EGR.PRODUCTO_ID]          
          ,DD.NRO_DESPACHO            [DET_DOCUMENTO_EGR.NRO_DESPACHO]
          ,DD.NRO_LOTE                [DET_DOCUMENTO_EGR.NRO_LOTE]
          ,DD.NRO_PARTIDA             [DET_DOCUMENTO_EGR.NRO_PARTIDA]
          ,DD.TIE_IN                  [DET_DOCUMENTO_EGR.TIE_IN]
          ,DD.PROP1                   [DET_DOCUMENTO_EGR.PROP1]
          ,DD.PROP2                   [DET_DOCUMENTO_EGR.PROP2]
          ,DD.PROP3                   [DET_DOCUMENTO_EGR.PROP3]
          ,DD.UNIDAD_ID               [DET_DOCUMENTO_EGR.UNIDAD_ID]
          ,SUM(DD.CANTIDAD)				    [DET_DOCUMENTO_EGR.CANTIDAD]  
	FROM	  PICKING P(NOLOCK) 
          INNER JOIN DOCUMENTO D ON(D.DOCUMENTO_ID=P.DOCUMENTO_ID)
          INNER JOIN CLIENTE  C ON (C.CLIENTE_ID = D.CLIENTE_ID)
          INNER JOIN SUCURSAL S ON (D.SUCURSAL_DESTINO = S.SUCURSAL_ID)
          INNER JOIN DET_DOCUMENTO DD ON (D.CLIENTE_ID = DD.CLIENTE_ID 
                                          AND D.DOCUMENTO_ID = DD.DOCUMENTO_ID 
                                          AND P.DOCUMENTO_ID = DD.DOCUMENTO_ID 
                                          AND P.NRO_LINEA = DD.NRO_LINEA)
	WHERE	  VIAJE_ID= @viaje_id
			    AND PALLET_PICKING = @palletpicking
	GROUP BY
	 		VIAJE_ID
      ,PALLET_PICKING
      ,P.POSICION_COD
      ,P.PROP1       
      ,P.RUTA        
      ,D.NRO_REMITO
      ,S.CALLE 
      ,S.NOMBRE 
      ,S.SUCURSAL_ID
      ,C.CALLE                    
      ,C.CATEGORIA_CLIENTE_ID     
      ,C.CATEGORIA_IMPOSITIVA_ID  
      ,C.CODIGO_POSTAL            
      ,C.EMAIL                    
      ,C.FAX                      
      ,C.LOCALIDAD   
      ,C.CLIENTE_ID
      ,C.NOMBRE                   
      ,C.NRO_DOCUMENTO            
      ,C.NUMERO                   
      ,C.OBSERVACIONES            
      ,C.PAIS_ID                  
      ,C.PROVINCIA_ID             
      ,C.RAZON_SOCIAL             
      ,C.TELEFONO_1               
      ,C.TELEFONO_2               
      ,C.TELEFONO_3               
      ,C.TIPO_DOCUMENTO_ID        
      ,C.ZONA_ID      
      ,C.REMITO_ID 
      ,DD.PRODUCTO_ID             
      ,DD.DESCRIPCION       
      ,DD.FECHA_VENCIMIENTO 
      ,DD.NRO_BULTO     
      ,DD.NRO_DESPACHO 
      ,DD.NRO_LOTE     
      ,DD.NRO_PARTIDA  
      ,DD.TIE_IN       
      ,DD.PROP1        
      ,DD.PROP2        
      ,DD.PROP3        
      ,DD.UNIDAD_ID          
	ORDER BY 2

End--Fin procedure.

GO
