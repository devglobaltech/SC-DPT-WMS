IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SYS_DEV_I04]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SYS_DEV_I04]
GO

CREATE    PROCEDURE [dbo].[SYS_DEV_I04]
@doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
DECLARE @E_SP     AS VARCHAR(300)
declare @Cliente_Id as varchar(15)  
BEGIN

select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
begin 

  if @qty=0
  BEGIN 	
    insert into sys_dev_documento
    select	top 1
            sid.CLIENTE_ID, 
            'I05', 
            sid.CPTE_PREFIJO, 
            sid.CPTE_NUMERO, 
            d.FECHA_CPTE, 
            sid.FECHA_SOLICITUD_CPTE, 
            sid.AGENTE_ID, 
            sid.PESO_TOTAL, 
            sid.UNIDAD_PESO, 
            sid.VOLUMEN_TOTAL, 
            sid.UNIDAD_VOLUMEN, 
            sid.TOTAL_BULTOS, 
            sid.ORDEN_DE_COMPRA, 
            sid.OBSERVACIONES, 
            d.NRO_REMITO, 
            sid.NRO_DESPACHO_IMPORTACION, 
            sid.DOC_EXT, 
            sid.CODIGO_VIAJE, 
            sid.INFO_ADICIONAL_1, 
            sid.INFO_ADICIONAL_2, 
            sid.INFO_ADICIONAL_3, 
            d.TIPO_COMPROBANTE_id, 
            NULL, 
            NULL, 
            'P', 
            GETDATE(),
            NULL, --flg_movimiento 
            SID.CUSTOMS_1,
            sid.CUSTOMS_2,
            sid.CUSTOMS_3,
            NULL, --NRO_GUIA
            NULL, --IMPORTE_FLETE
            NULL, --TRANSPORTE_ID
            sid.INFO_ADICIONAL_4,
            sid.INFO_ADICIONAL_5,
            sid.INFO_ADICIONAL_6
    from	sys_int_documento sid
            inner join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_despacho_importacion)
    where	sid.doc_ext=@doc_ext
  END

    insert into sys_dev_det_documento
    select	sidd.DOC_EXT, 
            isnull(@nro_lin,0) + dd.NRO_LINEA, 
            sidd.CLIENTE_ID, 
            sidd.PRODUCTO_ID, 
            sidd.CANTIDAD_SOLICITADA, 
            dd.CANTIDAD, 
            dd.EST_MERC_ID, 
            dd.CAT_LOG_ID_FINAL, 
            dd.NRO_BULTO, 
            dd.DESCRIPCION, 
            dd.NRO_LOTE, 
            dd.PROP1 AS NRO_PALLET, --NRO_PALLET 
            dd.FECHA_VENCIMIENTO, 
            dd.NRO_DESPACHO, 
            dd.NRO_PARTIDA, 
            sidd.UNIDAD_ID, 
            sidd.UNIDAD_CONTENEDORA_ID, 
            sidd.PESO, 
            sidd.UNIDAD_PESO, 
            sidd.VOLUMEN, 
            sidd.UNIDAD_VOLUMEN, 
            sidd.PROP1, 
            dd.PROP2, --NRO_LOTE 
            sidd.PROP3, 
            sidd.LARGO, 
            sidd.ALTO, 
            sidd.ANCHO, 
            sidd.DOC_BACK_ORDER, 
            NULL, 
            NULL, 
            'P', 
            GETDATE(), 
            DD.DOCUMENTO_ID, 
            dbo.get_nave_id(dd.documento_id,dd.nro_linea),
            dbo.get_nave_cod(dd.documento_id,dd.nro_linea),
            NULL, --flg_movimiento 	
            SIDD.CUSTOMS_1,
            sidd.CUSTOMS_2,
            sidd.CUSTOMS_3,
            null
    from	sys_int_documento sid
            inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)
            inner join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_despacho_importacion)
            inner join det_documento dd on (d.documento_id=dd.documento_id and sidd.producto_id = dd.producto_id)
    where	sid.doc_ext=@doc_ext 
            and sidd.estado_gt is not null 
            and dd.documento_id=@documento_id
            and sidd.documento_id=@documento_id
            
    begin try  
    
        SELECT  @E_SP=ISNULL(VALOR,'0')
        FROM    SYS_PARAMETRO_PROCESO
        WHERE   PROCESO_ID='DEV'
                AND SUBPROCESO_ID='EJECUTA_SP'
                AND PARAMETRO_ID='E_SP';
                
        if @e_sp='1' begin        
            select  top 1  
                    @cliente_id=sid.cliente_id      
            from    sys_int_documento sid  
                    inner join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_despacho_importacion)  
            where   sid.doc_ext=@doc_ext  
            begin try
				exec dbo.ERP_CARGAR_DATOS @cliente_id, @doc_ext   
			end try
			begin catch
				set @E_SP=@E_SP
			end catch
        end
    end try  
    begin catch  
    end catch              
end
END

GO


