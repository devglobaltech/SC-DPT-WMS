IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[consulta_movimientos_web]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[consulta_movimientos_web]
GO
create procedure [dbo].[consulta_movimientos_web]
(@UserId uniqueidentifier, @Desde datetime, @Hasta datetime)
as
begin
	Select	d.cliente_id										as [Cliente_id]
			,c.razon_social										as [Razon_social]
			,d.tipo_operacion_id								as [Tipo Operación]
			,convert(varchar,d.fecha_cpte,103)					as [Fecha]
			,ISNULL(d.nro_despacho_importacion,d.nro_remito)	as [Doc. Externo]
			,dd.producto_id										as [Producto_id]
			,p.descripcion										as [Descr_producto]
			,CONVERT(nvarchar(30),CONVERT(DECIMAL(20,2), REPLACE(dd.cantidad, ',','.')))  as [Cantidad]
			--,cast(dd.cantidad as numeric(20,2))					as [Cantidad]
			,dd.prop1											as [Pallet]
			,dd.nro_bulto										as [Bulto]
			,dd.nro_lote										as [Nro. Lote]
			,dd.prop2											as [Lote Proveedor]
			,dd.nro_partida										as [Nro. Partida]
			,dd.nro_serie										as [Nro. Serie]
	from	DOCUMENTO d
	inner join DET_DOCUMENTO dd on d.DOCUMENTO_ID = dd.DOCUMENTO_ID
	inner join CLIENTE c on d.CLIENTE_ID = c.CLIENTE_ID
	inner join PRODUCTO p on dd.CLIENTE_ID = p.CLIENTE_ID and dd.PRODUCTO_ID = p.PRODUCTO_ID
	inner join UsuarioWebClientes uw on d.CLIENTE_ID = uw.cliente_id
	where	uw.UserId = @UserId
			and CAST(fecha_cpte as datetime) >= cast(@Desde as datetime) and CAST(fecha_cpte as datetime) <= cast(@Hasta as datetime)
end

