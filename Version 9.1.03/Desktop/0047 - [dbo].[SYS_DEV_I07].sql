IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_DEV_I07]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_DEV_I07]
GO

CREATE     PROCEDURE [dbo].[SYS_DEV_I07]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
BEGIN

	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
begin 
	
	insert into sys_dev_documento
	select	d.CLIENTE_ID, 
			'I07', 
			d.CPTE_PREFIJO, 
			d.CPTE_NUMERO, 
			d.FECHA_CPTE, 
			null, 
			d.sucursal_origen, 
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			D.OBSERVACIONES, 
			d.NRO_REMITO, 
			D.NRO_DESPACHO_IMPORTACION, 
			'IM' + CAST(d.DOCUMENTO_ID AS varchar(100)),
			NULL,
			NULL,
			NULL,
			NULL,
			d.TIPO_COMPROBANTE_id, 
			NULL, 
			NULL, 
			'P', 
			GETDATE(),
			NULL, --flg_movimiento
			NULL,
			NULL,
			NULL,
			NULL AS NRO_GUIA,
			NULL AS IMPORTE_FLETE,
			NULL AS TRANSPORTE_ID,
			NULL AS INFO_ADICIONAL_4,
			NULL AS INFO_ADICIONAL_5,
			NULL AS INFO_ADICIONAL_6
	from	documento d 
	where	d.documento_id=@documento_id	

	SELECT TOP 1 * FROM SYS_DEV_DOCUMENTO
	
	insert into sys_dev_det_documento
	select	'IM' + CAST(dd.DOCUMENTO_ID AS varchar(100)),
			dd.NRO_LINEA, 
			dd.CLIENTE_ID, 
			dd.PRODUCTO_ID, 
			dd.CANT_SOLICITADA, 
			dd.cantidad, 
			dd.EST_MERC_ID, 
			dd.CAT_LOG_ID_FINAL, 
			dd.NRO_BULTO, 
			dd.DESCRIPCION, 
			dd.NRO_LOTE, 
			dd.PROP1 AS NRO_PALLET, --NRO_PALLET 
			dd.FECHA_VENCIMIENTO, 
			dd.NRO_DESPACHO, 
			dd.NRO_PARTIDA, 
			dd.UNIDAD_ID, 
			null,
			null,
			null,
			null,
			null,
			dd.PROP1, 
			dd.PROP2, --NRO_LOTE 
			dd.PROP3, 
			null,
			null,
			null,
			null,
			NULL, 
			NULL, 
			'P', 
			GETDATE(), 
			DD.DOCUMENTO_ID, 
			dbo.get_nave_id(dd.documento_id,dd.nro_linea),
			dbo.get_nave_cod(dd.documento_id,dd.nro_linea),
			NULL, --flg_movimiento
			NULL,
			NULL,
			NULL,
			NULL AS NRO_CMR
	from	det_documento dd
	where	dd.documento_id=@documento_id
end
END

GO


