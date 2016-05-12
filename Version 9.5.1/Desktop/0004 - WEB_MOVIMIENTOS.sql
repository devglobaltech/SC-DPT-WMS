CREATE VIEW [dbo].[WEB_MOVIMIENTOS] 
AS

Select		
			ROW_NUMBER() OVER(ORDER BY d.fecha_cpte DESC)		as [Row]
			,d.cliente_id										as [clienteid]
			,c.razon_social										as [razon_social]
			,d.tipo_operacion_id								as [tipo_operacion]
			,d.fecha_cpte										as [fecha1]
			,ISNULL(d.nro_despacho_importacion,d.nro_remito)	as [doc_externo]
			,dd.producto_id										as [producto_id]
			,p.descripcion										as [descr_producto]
			,CONVERT(nvarchar(30),CONVERT(DECIMAL(20,2), REPLACE(dd.cantidad, ',','.')))  as [cantidad]
			,dd.prop1											as [pallet]
			,dd.nro_bulto										as [bulto]
			,dd.nro_lote										as [nro_lote]
			,dd.prop2											as [lote_proveedor]
			,dd.nro_partida										as [nro_partida]
			,dd.nro_serie										as [nro_serie]
	from	DOCUMENTO d
	inner join DET_DOCUMENTO dd on d.DOCUMENTO_ID = dd.DOCUMENTO_ID
	inner join CLIENTE c on d.CLIENTE_ID = c.CLIENTE_ID
	inner join PRODUCTO p on dd.CLIENTE_ID = p.CLIENTE_ID and dd.PRODUCTO_ID = p.PRODUCTO_ID
	
	where	
	1=1 
	

GO


