/****** Object:  StoredProcedure [dbo].[GetPedidosAsociados]    Script Date: 09/18/2013 10:50:48 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetPedidosAsociados]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetPedidosAsociados]
GO

CREATE PROCEDURE [dbo].[GetPedidosAsociados]                
	@viaje varchar(50) output                
as                
Begin
	select  d.nro_remito,    
			rtrim(LTRIM(s.nombre)) AS nombre,    
			sum(p.cant_confirmada) as picking,    
			dbo.GetProductosDesconsolidacion(p.viaje_id,u.nroucdesconsolidacion) as descon,    
			u.nroucdesconsolidacion,  
			TC.TIPO_COMPROBANTE_ID + ' - ' + TC.DESCRIPCION AS TIPO_COMPROBANTE,
			c.razon_social,
			dbo.getclasepedido(d.nro_remito) as clase_pedido
	from	picking p    
			inner join documento d on p.documento_id = d.documento_id    
			left  join documento_X_contenedoradesconsolidacion u on u.documento_id = d.nro_remito
			inner join cliente c on d.cliente_id = c.cliente_id
			inner join sucursal s on s.sucursal_id = d.sucursal_destino and s.CLIENTE_ID = d.CLIENTE_ID
			INNER JOIN SYS_INT_DOCUMENTO SD ON d.cliente_id = sd.CLIENTE_ID and d.NRO_REMITO = SD.DOC_EXT
			LEFT JOIN TIPO_COMPROBANTE TC ON SD.TIPO_DOCUMENTO_ID = TC.TIPO_COMPROBANTE_ID  
	where   p.fin_picking = '2'                
			and p.facturado = '0'                
			and p.viaje_id = @viaje                           
	group by
			d.nro_remito ,u.nroucdesconsolidacion ,p.viaje_id ,s.nombre ,TC.TIPO_COMPROBANTE_ID
			,TC.DESCRIPCION ,c.razon_social
End
