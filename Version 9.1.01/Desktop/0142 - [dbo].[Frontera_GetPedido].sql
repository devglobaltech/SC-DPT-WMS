/****** Object:  StoredProcedure [dbo].[Frontera_GetPedido]    Script Date: 10/23/2013 17:46:27 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_GetPedido]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_GetPedido]
GO

CREATE     PROCEDURE [dbo].[Frontera_GetPedido]
@viaje_id	varchar(100) output,
@MostrarNull    CHAR(1) output
AS
BEGIN

DECLARE @XSQL AS NVARCHAR(4000)


 SET @XSQL= N' select 0 as Seleccionar,'
 SET @XSQL= @XSQL + N' d.doc_ext as DOCUMENTO'
 SET @XSQL= @XSQL + N'  ,d.agente_id as COD_CLIENTE'
 SET @XSQL= @XSQL + N'  ,s.nombre as CLIENTE'
 SET @XSQL= @XSQL + N'  ,d.info_adicional_1 as [GRUPO_PICKING/RUTA]'
 SET @XSQL= @XSQL + N'  ,count(distinct dd.producto_id) as QTY_PROD'
 SET @XSQL= @XSQL + N'  ,d.fecha_cpte as FECHA'
 SET @XSQL= @XSQL + N'  from '
 SET @XSQL= @XSQL + N'  sys_int_documento d (nolock)'
 SET @XSQL= @XSQL + N'  inner join sys_int_det_documento dd (nolock) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)'
 SET @XSQL= @XSQL + N'   inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.agente_id=s.sucursal_id)'
 SET @XSQL= @XSQL + N'   where '
 SET @XSQL= @XSQL + N'   d.tipo_documento_id in ('+ CHAR(39) + 'E04' + CHAR(39) + ',' + CHAR(39) + 'E06' + CHAR(39) + ',' + CHAR(39) + 'E01' + CHAR(39)  + ',' + CHAR(39) + 'E08' + CHAR(39)  +  ',' + CHAR(39) + 'E02' + CHAR(39)  + ',' + CHAR(39) + 'E03' + CHAR(39)+ ')' 

 If (@MostrarNull = 0) BEGIN
    SET @XSQL= @XSQL + N' and dd.estado_gt is null'
 
 END ELSE BEGIN
    SET @XSQL= @XSQL + N' and dd.estado_gt is not null'
 END --IF 
 --and dd.estado_gt is not null
 
 SET @XSQL= @XSQL + N'  and ((d.codigo_viaje=' + CHAR(39) + @viaje_id + CHAR(39) +') OR ( d.doc_ext in(select d.NRO_REMITO from PICKING p inner join DOCUMENTO d on(p.DOCUMENTO_ID=d.DOCUMENTO_ID) where p.VIAJE_ID=' + CHAR(39) + @viaje_id + CHAR(39) + ')))'
 SET @XSQL= @XSQL + N' GROUP BY d.doc_ext,d.agente_id,s.nombre,d.info_adicional_1,d.fecha_cpte'
 SET @XSQL= @XSQL + N' ORDER BY d.info_adicional_1'

 EXECUTE SP_EXECUTESQL @XSQL
END
GO


