
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 04:36 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from V9 to CORAL'
GO

IF @@TRANCOUNT > 0 COMMIT TRANSACTION
GO

SET NUMERIC_ROUNDABORT OFF
SET ANSI_PADDING, ANSI_NULLS, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT * FROM tempdb..sysobjects WHERE id=OBJECT_ID('tempdb..#tmpErrors')) DROP TABLE #tmpErrors
GO

CREATE TABLE #tmpErrors (Error int)
GO

SET XACT_ABORT OFF
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO

BEGIN TRANSACTION
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_CONSULTA_PICKING]
as

declare @CLIENTE_ID char(5)
declare @UnicID as varchar(20)

set @CLIENTE_ID = '10202'
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')
declare @headerNbr integer
set @HeaderNbr = 0
select distinct CODIGO_VIAJE,dbo.padl(rtrim(substring(B.INFO_ADICIONAL_3,10,12)),12,' ') as VMCU
 into #tViajes from GM_DEV_DOCUMENTO B where ESTADO is null and B.CLIENTE_ID = @CLIENTE_ID
 and exists(select * from SYS_INT_DOCUMENTO A where A.CLIENTE_ID = B.CLIENTE_ID and A.DOC_EXT = B.DOC_EXT 
           and A.TIPO_DOCUMENTO_ID = 'E04' and A.CLIENTE_ID = @CLIENTE_ID)

select * from #tViajes 

select 
case when B.CODIGO_VIAJE  = B.INFO_ADICIONAL_3 then 'O' 
     when B.CODIGO_VIAJE <> B.INFO_ADICIONAL_3 then 'C' end
as TRP_STAT, B.CLIENTE_ID as COO, cast(B.AGENTE_ID as integer) as AN8, dbo.padl(rtrim(substring(B.DOC_EXT,1,12)),12,' ') as MCU,
substring(B.DOC_EXT,14,2) as DCTO , cast(substring(B.DOC_EXT,18,8) as integer) as DOCO, 
cast(substring(B.INFO_ADICIONAL_3,1,8) as integer) as TRP_ORIG,  A.VMCU, A.CODIGO_VIAJE, B.DOC_EXT, B.CLIENTE_ID,
case when B.CODIGO_VIAJE like 'NUE%' then cast(substring(B.CODIGO_VIAJE,4,11) as bigint) else cast(substring(B.CODIGO_VIAJE,1,8) as bigint) end as TRP_WARP,
case when B.CODIGO_VIAJE like 'NUE%' then cast(substring(B.CODIGO_VIAJE,4,11) as bigint) else cast(substring(B.CODIGO_VIAJE,1,8) as bigint) end as TRP,
convert(CHAR(10),B.FECHA_CPTE ,112) FECHA_CPTE  
into #tPedidos
from GM_DEV_DOCUMENTO B inner join #tViajes A on B.CODIGO_VIAJE = A.CODIGO_VIAJE 

update #tPedidos set TRP_STAT = 'N', TRP = 0 where rtrim(ltrim(CODIGO_VIAJE))  like 'NUE%'

insert into #tPedidos select 'R', B.CLIENTE_ID as COO, cast(B.AGENTE_ID as integer) as AN8, 
dbo.padl(rtrim(substring(B.DOC_EXT,1,12)),12,' ') as MCU,
substring(B.DOC_EXT,14,2) as DCTO , cast(substring(B.DOC_EXT,18,8) as integer) as DOCO, 
cast(substring(B.INFO_ADICIONAL_3,1,8) as integer) as TRP_ORIG,  A.VMCU, A.CODIGO_VIAJE, B.DOC_EXT, B.CLIENTE_ID,
0 as TRP_WARP, 0 as TRP, ' ' FECHA_CPTE 
 from SYS_INT_DOCUMENTO B inner join #tViajes A on B.INFO_ADICIONAL_3 = A.CODIGO_VIAJE 
where B.TIPO_DOCUMENTO_ID = 'E04'
and not exists (select * from #tPedidos C where C.CLIENTE_ID = B.CLIENTE_ID and C.DOC_EXT = B.DOC_EXT)

select * from #tPedidos 

if (select count(*) from #tPedidos) > 0
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'


select B.TRP_STAT,@HeaderNbr as HEADER_NBR, 0 as DETAIL_NBR, ' ' as GTDFTF,
 PROP3, PROP3 + ' ' + isnull(A.NRO_LOTE,'') + ' ' +isnull(A.NRO_PALLET,'') as PK, 
 G.JULIANO as TRDJ, B.TRP_ORIG, B.TRP_WARP, TRP, B.VMCU, B.COO, B.AN8, B.MCU, B.DCTO, B.DOCO, 
 substring(PROP3,26,3) as SFX, cast(substring(PROP3,30,10) as integer) as LNID, cast(A.PRODUCTO_ID as int) as ITM,
 cast(A.CANTIDAD_SOLICITADA * 10000 as integer) as UORG, 
 cast(A.CANTIDAD * 10000 as integer) as SOQS, isnull(A.NRO_LOTE,'')  as LOTN_ORIG,isnull(A.NRO_LOTE,'')  as LOTN,
 isnull(cast(A.NRO_PALLET as int),0) as PALN, 
 A.UNIDAD_ID as UOM, @UnicId as LOTE_ID, ' ' as ESTADO, isnull(DEPOSITO_JDE,'') as  MCU_ORIG, isnull(UBIC_JDE,'') as  LOCN_ORIG,
 isnull(ESTADO_LOTE_CD, '') as LOTS, P.CODIGO_PRODUCTO as LITM, P.DESCRIPCION as DSC1
into #tDetalle
from GM_DEV_DET_DOCUMENTO A inner join #tPedidos B on A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID
left outer join GM_SUCURSAL_NAVE S on  A.NAVE_ID = S.NAVE_ID and A.CLIENTE_ID = S.CLIENTE_ID and A.CAT_LOG_ID = S.CAT_LOG_ID
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
inner join GM_FECHAS G on B.FECHA_CPTE = G.FECHA

declare @PROP3 varchar(100)
declare @CurPROP3 varchar(100)
declare @PK varchar(100)
declare @detailNbr integer
declare @GTDFTF char(1)
set @detailNbr = 0
set @CurPROP3 = ''

DECLARE dcursor CURSOR FOR select PROP3, PK from #tDetalle order by TRP_ORIG, VMCU, PROP3
open dcursor
fetch next from dcursor into @PROP3, @PK
WHILE @@FETCH_STATUS = 0
BEGIN
     set @detailNbr = @detailNbr + 1	
     if @CurPROP3 = @PROP3
	set @GTDFTF =  'A'
     else
	set @GTDFTF =  ' '
     	 
     set @CurPROP3 = @PROP3   
     update #tDetalle set GTDFTF = @GTDFTF, DETAIL_NBR = @detailNbr where PK = @PK
     fetch next from dcursor into @PROP3, @PK
END

CLOSE dcursor
DEALLOCATE dcursor



select TRP_STAT, HEADER_NBR as MJEDOC, DETAIL_NBR * 1000 as MJEDLN, GTDFTF, TRDJ, TRP_ORIG, TRP_WARP, TRP, VMCU,
COO, AN8, MCU, DCTO, DOCO, SFX, LNID, ITM, UORG, SOQS, LOTN_ORIG,LOTN, PALN, UOM, MCU_ORIG,  LOCN_ORIG, LOTS,
LITM, DSC1, LOTE_ID, ESTADO  from #tDetalle 
--where UORG <> SOQS and SOQS >0
order by MJEDLN
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_E01] as

set nocount on

declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')



select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * -10000 AS CANTIDAD,  DD.NRO_LOTE,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'S' as MJEDER, 'QS' as MJPACD,
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET into #TGM_E01
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where D.tipo_documento_id = 'E01' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null
union
select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, 
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'R' as MJEDER, 'QR' as MJPACD,
S.CUENTA_EXTERNA_2 as UBIC_JDE, dbo.padl(S.CUENTA_EXTERNA_1,12,' ') as DEPOSITO_JDE, ESTADO_LOTE_CD, NRO_PALLET 
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID  and G.CLIENTE_ID = D.CLIENTE_ID
inner join SUCURSAL S on D.AGENTE_ID = S.SUCURSAL_ID and S.CLIENTE_ID = D.CLIENTE_ID
where D.tipo_documento_id = 'E01' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null



declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @MJEDER char(1)



DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, MJEDER from #TGM_E01 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_E01 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and MJEDER = @MJEDER
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
END

CLOSE dcursor
DEALLOCATE dcursor

update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_E01 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_E01 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA

select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'E1' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'E01' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, @UnicID as M1VR01, 'N' as M1EDSP 
 from #TGM_E01 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 


select distinct DOC_EXT as  MJPNID, 'D' as MJEDTY, 1 as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'E1' as MJEDCT, 'E01' as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX, MJEDER,  MJPACD, 
@UnicID as MJVR01, 'N' as MJEDSP, 'GG' as MJDCT 
from #TGM_E01 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER  procedure [dbo].[GM_E02] as

set nocount on

declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')
select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * -10000 AS CANTIDAD,  DD.NRO_LOTE, substring(S.OBSERVACIONES,1,2) as DCT,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'S' as MJEDER, 'QS' as MJPACD,
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET into #TGM_E02
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
inner join SUCURSAL S on D.AGENTE_ID = S.SUCURSAL_ID and S.CLIENTE_ID = D.CLIENTE_ID
where D.tipo_documento_id = 'E02' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null
union
select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, substring(S.OBSERVACIONES,1,2) as DCT,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'R' as MJEDER, 'QR' as MJPACD,
'VAR' as UBIC_JDE, dbo.padl(S.CUENTA_EXTERNA_1,12,' ') as DEPOSITO_JDE, ESTADO_LOTE_CD, NRO_PALLET 
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID  and G.CLIENTE_ID = D.CLIENTE_ID
inner join SUCURSAL S on D.AGENTE_ID = S.SUCURSAL_ID and S.CLIENTE_ID = D.CLIENTE_ID
where D.tipo_documento_id = 'E02' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null



declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @MJEDER char(1)



DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, MJEDER from #TGM_E02 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_E02 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and MJEDER = @MJEDER
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
END

CLOSE dcursor
DEALLOCATE dcursor

update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_E02 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_E02 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA

select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'E2' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'E02' + DCT as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, @UnicID as M1VR01, 'N' as M1EDSP 
 from #TGM_E02 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 


select distinct DOC_EXT as  MJPNID, 'D' as MJEDTY, 1 as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'E2' as MJEDCT, 'E02' + DCT  as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX, MJEDER,  MJPACD, 
@UnicID as MJVR01, 'N' as MJEDSP , 'TB' as MJDCT
from #TGM_E02 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_E03] as
set nocount on
declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')


select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE,  D.DOC_EXT,
DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * -10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, D.AGENTE_ID,
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET, 
case when S.CLIENTE_INTERNO = 0 then 'EZ' else substring(S.OBSERVACIONES,1,2) end as DCT

 into #TGM_E03
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
inner join SUCURSAL S on D.AGENTE_ID = S.SUCURSAL_ID and S.CLIENTE_ID = D.CLIENTE_ID
where D.tipo_documento_id = 'E03' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null
and (S.CLIENTE_INTERNO = 0 or (S.CLIENTE_INTERNO=1 and substring(S.OBSERVACIONES,1,2)<>''))


declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @UBIC_JDE varchar(20)

DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, UBIC_JDE from #TGM_E03 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_E03 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and UBIC_JDE = @UBIC_JDE
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
END

CLOSE dcursor
DEALLOCATE dcursor



update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_E03 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_E03 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'E3' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'E03' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, 
@UnicID as M1VR01,  'N' as M1EDSP
 from #TGM_E03 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 

select distinct DOC_EXT as  MJPNID,'D' as MJEDTY, 1  as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'E3' as MJEDCT, 'E03'  as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX,  
case when CANTIDAD > 0 then 'R' else 'S' end as MJEDER,  
case when CANTIDAD > 0 then 'QR' else 'QS' end as MJPACD, @UnicID as MJVR01,  'N' as MJEDSP ,  DCT as MJDCT
from #TGM_E03 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER                    PROCEDURE [dbo].[GM_REPLICATE_PRODUCTOS] (@pXML ntext, @pPATH varchar(100)=null ,@pFULL smallint = 1)
as
set nocount on
SET XACT_ABORT ON

if @pPATH is null
   set @pPATH =	'/DST/PRODUCTOS'

declare @blnRunJob  tinyint
set @blnRunJob = 0

--Actualiza
--FAMILIA_PRODUCTO
--SUBFAMILIA_PRODUCTO
--TIPO_PRODUCTO
--UNIDAD_MEDIDA
--SYS_INT_PRODUCTO

DECLARE @hDoc int
EXEC sp_xml_preparedocument @hDoc OUTPUT, @PXML

SELECT distinct CLIENTE_ID, NRO_PRODUCTO, CODIGO_PRODUCTO, SUBCODIGO_1, DESCRIPCION_PRODUCTO, 
              COD_FAMILIA, NOMBRE_FAMILIA, COD_SUBFAMILIA, NOMBRE_SUBFAMILIA, NOMBRE_MARCA, COD_MEDIDA_PRIMARIA,
              NOMBRE_MEDIDA_PRIMARIA, PESO_UNITARIO, COD_PESO, NOMBRE_PESO, COD_VOLUMEN, NOMBRE_VOLUMEN, 
              COD_TIPO_PRODUCTO, NOMBRE_TIPO_PRODUCTO, TOLERANCIA
into #GM_PRODUCTOS
FROM OPENXML (@hDoc, @pPATH)  WITH 
	(CLIENTE_ID char(5) 'CLIENTE_ID', 
	NRO_PRODUCTO decimal(8, 0) 'NRO_PRODUCTO' , 
	CODIGO_PRODUCTO varchar(50) 'CODIGO_PRODUCTO', 
	SUBCODIGO_1 varchar(50) 'SUBCODIGO_1', 
	DESCRIPCION_PRODUCTO varchar(50) 'DESCRIPCION_PRODUCTO' , 
	COD_FAMILIA varchar(5) 'COD_FAMILIA', 
	NOMBRE_FAMILIA varchar(50) 'NOMBRE_FAMILIA', 
	COD_SUBFAMILIA varchar(5) 'COD_SUBFAMILIA', 
	NOMBRE_SUBFAMILIA varchar(50) 'NOMBRE_SUBFAMILIA', 
	NOMBRE_MARCA varchar(50) 'NOMBRE_MARCA', 
	COD_MEDIDA_PRIMARIA varchar(5) 'COD_MEDIDA_PRIMARIA', 
	NOMBRE_MEDIDA_PRIMARIA varchar(50) 'NOMBRE_MEDIDA_PRIMARIA', 
	PESO_UNITARIO decimal(15, 5) 'PESO_UNITARIO', 
	COD_PESO varchar(5) 'COD_PESO', 
	NOMBRE_PESO varchar(50) 'NOMBRE_PESO', 
	COD_VOLUMEN varchar(5) 'COD_VOLUMEN', 
	NOMBRE_VOLUMEN varchar(50) 'NOMBRE_VOLUMEN', 
	COD_TIPO_PRODUCTO varchar(5) 'COD_TIPO_PRODUCTO', 
	NOMBRE_TIPO_PRODUCTO varchar(50) 'NOMBRE_TIPO_PRODUCTO', 
	TOLERANCIA decimal(10, 2) 'TOLERANCIA')
EXEC sp_xml_removedocument @hDoc

--drop table GM_BORRAR_PRODUCTOS
--select * into GM_BORRAR_PRODUCTOS from #GM_PRODUCTOS

/**************************************************************************************************************************************************************/
select distinct  COD_FAMILIA as FAMILIA_ID, NOMBRE_FAMILIA as DESCRIPCION  into #FAMILIAS from #GM_PRODUCTOS
insert into FAMILIA_PRODUCTO select *  from #FAMILIAS where FAMILIA_ID not in (select FAMILIA_ID from FAMILIA_PRODUCTO)
update FAMILIA_PRODUCTO set DESCRIPCION = T.DESCRIPCION from FAMILIA_PRODUCTO R, #FAMILIAS T where (R.FAMILIA_ID = T.FAMILIA_ID and (R.DESCRIPCION <> T.DESCRIPCION))
DROP TABLE #FAMILIAS


/**************************************************************************************************************************************************************/
select distinct  COD_SUBFAMILIA as SUB_FAMILIA_ID, NOMBRE_SUBFAMILIA as DESCRIPCION  into #SUBFAMILIAS from #GM_PRODUCTOS
insert into SUB_FAMILIA select *  from #SUBFAMILIAS where SUB_FAMILIA_ID not in (select SUB_FAMILIA_ID from SUB_FAMILIA)
update SUB_FAMILIA set DESCRIPCION = T.DESCRIPCION from SUB_FAMILIA R, #SUBFAMILIAS T where (R.SUB_FAMILIA_ID = T.SUB_FAMILIA_ID and (R.DESCRIPCION <> T.DESCRIPCION))
DROP TABLE #SUBFAMILIAS


/**************************************************************************************************************************************************************/
select distinct  COD_TIPO_PRODUCTO as TIPO_PRODUCTO_ID, NOMBRE_TIPO_PRODUCTO as DESCRIPCION  into #TIPO_PRODUCTOS from #GM_PRODUCTOS
insert into TIPO_PRODUCTO select *  from #TIPO_PRODUCTOS where TIPO_PRODUCTO_ID not in (select TIPO_PRODUCTO_ID from TIPO_PRODUCTO)
update TIPO_PRODUCTO set DESCRIPCION = T.DESCRIPCION from TIPO_PRODUCTO R, #TIPO_PRODUCTOS T where (R.TIPO_PRODUCTO_ID = T.TIPO_PRODUCTO_ID and (R.DESCRIPCION <> T.DESCRIPCION))
DROP TABLE #TIPO_PRODUCTOS

/**************************************************************************************************************************************************************/
select distinct  COD_MEDIDA_PRIMARIA as UNIDAD_ID, NOMBRE_MEDIDA_PRIMARIA as DESCRIPCION  	into #UNIDADES_MEDIDA from #GM_PRODUCTOS 
union
select distinct  COD_PESO as UNIDAD_ID, NOMBRE_PESO as DESCRIPCION from #GM_PRODUCTOS
union
select distinct  COD_VOLUMEN as UNIDAD_ID, NOMBRE_VOLUMEN as DESCRIPCION from #GM_PRODUCTOS
insert into UNIDAD_MEDIDA select *  from #UNIDADES_MEDIDA where UNIDAD_ID not in (select UNIDAD_ID from UNIDAD_MEDIDA)
update UNIDAD_MEDIDA set DESCRIPCION = T.DESCRIPCION from UNIDAD_MEDIDA R, #UNIDADES_MEDIDA T where (R.UNIDAD_ID = T.UNIDAD_ID and (R.DESCRIPCION <> T.DESCRIPCION))
DROP TABLE #UNIDADES_MEDIDA

/**************************************************************************************************************************************************************/

INSERT INTO SYS_INT_PRODUCTO(
            CLIENTE_ID, PRODUCTO_ID, CODIGO_PRODUCTO, SUBCODIGO_1, SUBCODIGO_2, DESCRIPCION, 
            MARCA, FRACCIONABLE, UNIDAD_FRACCION, COSTO, UNIDAD_ID, SUBFAMILIA_ID, FAMILIA_ID, OBSERVACIONES, 
            POSICIONES_PURAS, MONEDA_COSTO_ID, LARGO, ALTO, ANCHO, UNIDAD_VOLUMEN, PESO, UNIDAD_PESO, LOTE_AUTOMATICO, 
            PALLET_AUTOMATICO, TOLERANCIA_MIN_INGRESO, TOLERANCIA_MAX_INGRESO, GENERA_BACK_ORDER, CLASIFICACION_COT, 
            CODIGO_BARRA, ING_CAT_LOG_ID, EGR_CAT_LOG_ID, PRODUCTO_ACTIVO, COD_TIPO_PRODUCTO, Ingresado, Fecha_Carga)
SELECT CLIENTE_ID, cast(NRO_PRODUCTO as varchar), CODIGO_PRODUCTO, SUBCODIGO_1,  null, DESCRIPCION_PRODUCTO,
	   NOMBRE_MARCA,'0', null, null, COD_MEDIDA_PRIMARIA, COD_SUBFAMILIA, COD_FAMILIA, null,
           '1', null, 0, 0, 0, COD_VOLUMEN, PESO_UNITARIO, COD_PESO, case substring(cod_familia,1,1) when 'S' then '1' else '0' end,
           '1', TOLERANCIA, TOLERANCIA, '1', NULL,
           null, null, null, '1',  COD_TIPO_PRODUCTO, null, null from #GM_PRODUCTOS P
where not exists (select * from SYS_INT_PRODUCTO S where S.CLIENTE_ID = P.CLIENTE_ID and S.PRODUCTO_ID = cast(P.NRO_PRODUCTO as varchar))


if @@ROWCOUNT > 0
   set @blnRunJob = 1



update SYS_INT_PRODUCTO 
set INGRESADO = null, FECHA_CARGA = NULL, CODIGO_PRODUCTO = T.CODIGO_PRODUCTO, SUBCODIGO_1 = T.SUBCODIGO_1,
DESCRIPCION = T.DESCRIPCION_PRODUCTO, MARCA = T.NOMBRE_MARCA, UNIDAD_ID = T.COD_MEDIDA_PRIMARIA, 
SUBFAMILIA_ID = T.COD_SUBFAMILIA, FAMILIA_ID = T.COD_FAMILIA, UNIDAD_VOLUMEN = T.COD_VOLUMEN,
PESO = T.PESO_UNITARIO, UNIDAD_PESO = T.COD_PESO, TOLERANCIA_MIN_INGRESO = T.TOLERANCIA,
TOLERANCIA_MAX_INGRESO = T.TOLERANCIA, COD_TIPO_PRODUCTO = T.COD_TIPO_PRODUCTO
from SYS_INT_PRODUCTO R, #GM_PRODUCTOS T 
where (R.CLIENTE_ID = T.CLIENTE_ID and R.PRODUCTO_ID = cast(T.NRO_PRODUCTO as varchar)) 
and (R.CODIGO_PRODUCTO <> T.CODIGO_PRODUCTO or R.SUBCODIGO_1 <> T.SUBCODIGO_1 or R.DESCRIPCION <> T.DESCRIPCION_PRODUCTO
or R.MARCA <> T.NOMBRE_MARCA or R.UNIDAD_ID <> T.COD_MEDIDA_PRIMARIA or R.SUBFAMILIA_ID <> T.COD_SUBFAMILIA
or R.FAMILIA_ID <> T.COD_FAMILIA or R.UNIDAD_VOLUMEN <> T.COD_VOLUMEN or R.PESO <> T.PESO_UNITARIO 
or R.UNIDAD_PESO <> T.COD_PESO or R.TOLERANCIA_MIN_INGRESO <> T.TOLERANCIA or R.TOLERANCIA_MAX_INGRESO <> T.TOLERANCIA
or R.COD_TIPO_PRODUCTO <> T.COD_TIPO_PRODUCTO)


if @@ROWCOUNT > 0
   set @blnRunJob = 1

if @blnRunJob = 1
  exec SYS_INT_INGRESA_PRODUCTOS
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER               PROCEDURE [dbo].[GM_REPLICATE_AGENTES] (@pXML ntext ,@pPATH varchar(100)=null, @pFULL smallint=1)
as
set nocount on

SET XACT_ABORT ON

--Actualiza
--PAIS
--PROVINCIA
--ZONA
--SUCURSAL


if @pPATH is null
   set @pPATH =	'/DST/AGENTES' 
DECLARE @hDoc int
EXEC sp_xml_preparedocument @hDoc OUTPUT, @pXML

SELECT distinct COD_COMPANIA, COD_AGENTE, NOMBRE_AGENTE,
    case when COD_TIPO_AGENTE in('IL','IP', 'DP','DL') or CENTRO_COSTO <> '' then '1' else '0' end as CLIENTE_INTERNO , COD_TIPO_AGENTE,
       NOMBRE_TIPO_AGENTE, ACTIVO, CUIT, COD_PAIS, NOMBRE_PAIS, 
       COD_PROVINCIA, NOMBRE_PROVINCIA, COD_ZONA, NOMBRE_ZONA, NOMBRE_LOCALIDAD, DOMICILIO, COD_POSTAL, TELEFONO, 
       DATOS_COMPLETOS, SECTOR, PLANTA, UBICACION, CENTRO_COSTO, TIPO_DOCUMENTO_ID into #GM_AGENTES
FROM OPENXML (@hDoc, @pPATH)  WITH 
	(COD_COMPANIA varchar(5) 'COD_COMPANIA', 
	COD_AGENTE varchar(12) 'COD_AGENTE', 
	NOMBRE_AGENTE varchar(30) 'NOMBRE_AGENTE', 
	COD_TIPO_AGENTE varchar(2) 'COD_TIPO_AGENTE', 
	NOMBRE_TIPO_AGENTE varchar(30) 'NOMBRE_TIPO_AGENTE',
	ACTIVO char(1) 'ACTIVO', 
	CUIT varchar(11) 'CUIT', 
	COD_PAIS varchar(3) 'COD_PAIS', 
	NOMBRE_PAIS varchar(30) 'NOMBRE_PAIS', 
	COD_PROVINCIA varchar(3) 'COD_PROVINCIA', 
	NOMBRE_PROVINCIA varchar(30) 'NOMBRE_PROVINCIA', 
	COD_ZONA varchar(3) 'COD_ZONA', 
	NOMBRE_ZONA varchar(30) 'NOMBRE_ZONA', 
	NOMBRE_LOCALIDAD varchar(30) 'NOMBRE_LOCALIDAD', 
	DOMICILIO varchar(40) 'DOMICILIO', 
	COD_POSTAL varchar(20) 'COD_POSTAL', 
	TELEFONO varchar(20) 'TELEFONO', 
	DATOS_COMPLETOS char(1) 'DATOS_COMPLETOS', 
	SECTOR varchar(12) 'SECTOR', 
	PLANTA varchar(12) 'PLANTA', 
	UBICACION varchar(12) 'UBICACION' , 
	CENTRO_COSTO varchar(12) 'CENTRO_COSTO',
	TIPO_DOCUMENTO_ID char(2) 'TIPO_DOCUMENTO_ID')
EXEC sp_xml_removedocument @hDoc

--drop table GM_AGENTES
--select * into GM_AGENTES from #GM_AGENTES 


/**************************************************************************************************************************************************************/

select distinct  COD_PAIS as PAIS_ID, NOMBRE_PAIS as DESCRIPCION  into #PAISES from #GM_AGENTES
insert into PAIS select *  from #PAISES where PAIS_ID not in (select PAIS_ID from PAIS)
update PAIS set DESCRIPCION = T.DESCRIPCION from PAIS R, #PAISES T where (R.PAIS_ID = T.PAIS_ID and (R.DESCRIPCION <> T.DESCRIPCION))
DROP TABLE #PAISES

/**************************************************************************************************************************************************************/
select distinct  COD_PAIS as PAIS_ID, COD_PROVINCIA as PROVINCIA_ID, NOMBRE_PROVINCIA as DESCRIPCION  into #PROVINCIAS from #GM_AGENTES
insert into PROVINCIA select *  from #PROVINCIAS T where not exists(select PROVINCIA_ID from PROVINCIA R where T.PAIS_ID = R.PAIS_ID and T.PROVINCIA_ID = R.PROVINCIA_ID )
update PROVINCIA set DESCRIPCION = T.DESCRIPCION from PROVINCIA R, #PROVINCIAS T where (R.PAIS_ID = T.PAIS_ID and R.PROVINCIA_ID = T.PROVINCIA_ID and (R.DESCRIPCION <> T.DESCRIPCION))
DROP TABLE #PROVINCIAS

/**************************************************************************************************************************************************************/

select distinct  COD_ZONA as ZONA_ID, max(NOMBRE_ZONA) as DESCRIPCION into #ZONAS from #GM_AGENTES group by COD_ZONA 
insert into ZONA select *  from #ZONAS where ZONA_ID not in (select ZONA_ID from ZONA)
update ZONA set DESCRIPCION = T.DESCRIPCION from ZONA R, #ZONAS T where (R.ZONA_ID = T.ZONA_ID and (R.DESCRIPCION <> T.DESCRIPCION))
drop table  #zonas

/**************************************************************************************************************************************************************/
select COD_COMPANIA CLIENTE_ID, COD_AGENTE  as SUCURSAL_ID, NOMBRE_AGENTE as NOMBRE, DOMICILIO as CALLE, NOMBRE_LOCALIDAD as LOCALIDAD,
COD_PAIS as PAIS_ID, COD_PROVINCIA as PROVINCIA_ID, COD_ZONA as ZONA_ID, TELEFONO as TELEFONO_1, CUIT as NRO_DOCUMENTO,
'A' as TIPO_SUCURSAL, COD_TIPO_AGENTE as CATEGORIA_IMPOSITIVA_ID, 1 as ACTIVA, SECTOR, PLANTA, UBICACION, CENTRO_COSTO, CLIENTE_INTERNO, TIPO_DOCUMENTO_ID into #SUCURSALES from #GM_AGENTES 

insert into SUCURSAL (CLIENTE_ID, SUCURSAL_ID, NOMBRE, CALLE, LOCALIDAD, PAIS_ID, PROVINCIA_ID, ZONA_ID, TELEFONO_1, NRO_DOCUMENTO, TIPO_SUCURSAL, ACTIVA, CUENTA_EXTERNA, CUENTA_EXTERNA_1, CUENTA_EXTERNA_2, OBSERVACIONES, CLIENTE_INTERNO, CATEGORIA_IMPOSITIVA_ID,	TIPO_DOCUMENTO_ID)
              select CLIENTE_ID, SUCURSAL_ID, NOMBRE, CALLE, LOCALIDAD, PAIS_ID, PROVINCIA_ID, ZONA_ID, TELEFONO_1, NRO_DOCUMENTO, TIPO_SUCURSAL, ACTIVA, SECTOR,          PLANTA,           UBICACION,  isnull(TIPO_DOCUMENTO_ID,'  ') + ' ' + cast(isnull(CENTRO_COSTO,'  ') as varchar), CLIENTE_INTERNO, CATEGORIA_IMPOSITIVA_ID, TIPO_DOCUMENTO_ID from #SUCURSALES T where not exists (select * from SUCURSAL S where S.SUCURSAL_ID = T.SUCURSAL_ID and S.CLIENTE_ID = T.CLIENTE_ID)
 
update SUCURSAL set ACTIVA = 0 where not exists (select * from #SUCURSALES S where SUCURSAL.SUCURSAL_ID = S.SUCURSAL_ID and SUCURSAL.CLIENTE_ID = S.CLIENTE_ID ) 

update SUCURSAL set NOMBRE = T.NOMBRE, CALLE = T.CALLE,LOCALIDAD = T.LOCALIDAD,PAIS_ID = T.PAIS_ID,PROVINCIA_ID = T.PROVINCIA_ID,ZONA_ID = T.ZONA_ID,TELEFONO_1 = T.TELEFONO_1,NRO_DOCUMENTO = T.NRO_DOCUMENTO,TIPO_SUCURSAL = T.TIPO_SUCURSAL, CUENTA_EXTERNA = T.SECTOR, CUENTA_EXTERNA_1 = T.PLANTA, CUENTA_EXTERNA_2 = T.UBICACION, OBSERVACIONES = isnull(T.TIPO_DOCUMENTO_ID,'  ') + ' ' + cast(isnull(T.CENTRO_COSTO,'  ') as varchar), CLIENTE_INTERNO = T.CLIENTE_INTERNO, CATEGORIA_IMPOSITIVA_ID = T.CATEGORIA_IMPOSITIVA_ID, TIPO_DOCUMENTO_ID = T.TIPO_DOCUMENTO_ID
 from SUCURSAL R, #SUCURSALES T where (R.SUCURSAL_ID = T.SUCURSAL_ID and R.CLIENTE_ID = T.CLIENTE_ID and (
R.NOMBRE <> T.NOMBRE or R.CALLE <> T.CALLE or R.LOCALIDAD <> T.LOCALIDAD or R.PAIS_ID <> T.PAIS_ID or R.PROVINCIA_ID <> T.PROVINCIA_ID or R.ZONA_ID <> T.ZONA_ID or R.TELEFONO_1 <> T.TELEFONO_1 or R.NRO_DOCUMENTO <> T.NRO_DOCUMENTO or R.TIPO_SUCURSAL <> T.TIPO_SUCURSAL or R.CUENTA_EXTERNA <> T.SECTOR or R.CUENTA_EXTERNA_1 <> T.PLANTA or R.CUENTA_EXTERNA_2 <> T.UBICACION or R.OBSERVACIONES <> isnull(T.TIPO_DOCUMENTO_ID,'  ') + ' ' + cast(isnull(T.CENTRO_COSTO,'  ') as varchar) or R.CLIENTE_INTERNO <> T.CLIENTE_INTERNO or R.CATEGORIA_IMPOSITIVA_ID <> T.CATEGORIA_IMPOSITIVA_ID or R.TIPO_DOCUMENTO_ID <> T.TIPO_DOCUMENTO_ID))
drop table #SUCURSALES
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[GM_E04] (@pXML ntext)
as
set nocount on
SET XACT_ABORT ON
DECLARE @hDoc int
EXEC sp_xml_preparedocument @hDoc OUTPUT, @pXML

SELECT CLIENTE_ID, SDLNID, SDSFXO, 
dbo.padr(SDMCU,12,'') + ' ' + dbo.padl(SDDCTO,2,'') +  ' ' + dbo.padl(SDDOCO,8,'0') as DOC_EXT , 
 SDAN8, AGENTE_ID, FECHA_CPTE, FECHA_SOLICITUD_CPTE,
 PRODUCTO_ID, UNIDAD_ID, CANTIDAD_SOLICITADA, INFO_ADICIONAL_1, dbo.padl(TDTRP,8,'0') + ' ' + ltrim(rtrim(cast(TDVMCU as varchar)))  as CODIGO_VIAJE, LOTE_ID 
 into #TGM_E04
FROM OPENXML (@hDoc, '/DST/E04')  WITH 
       (CLIENTE_ID varchar(5) 'CLIENTE_ID', 
	SDDOCO integer 'SDDOCO', 
	SDDCTO varchar(10) 'SDDCTO', 
	SDLNID integer 'SDLNID', 
	SDSFXO varchar(10) 'SDSFXO', 
	SDMCU varchar(12) 'SDMCU', 
	SDAN8 integer 'SDAN8', 
	AGENTE_ID integer 'AGENTE_ID', 
	FECHA_CPTE smalldatetime 'FECHA_CPTE' ,
	FECHA_SOLICITUD_CPTE smalldatetime 'FECHA_SOLICITUD_CPTE', 
	PRODUCTO_ID decimal(8,0) 'PRODUCTO_ID',         
	UNIDAD_ID varchar(50) 'UNIDAD_ID', 
	CANTIDAD_SOLICITADA decimal(20,5) 'CANTIDAD_SOLICITADA', 
	INFO_ADICIONAL_1 varchar(50) 'INFO_ADICIONAL_1', 
	TDVMCU varchar(12) 'TDVMCU', 
	TDTRP integer 'TDTRP',
	LOTE_ID varchar(20) 'LOTE_ID') as A



EXEC sp_xml_removedocument @hDoc

--drop table TGM_E04
--select * into TGM_E04 from #TGM_E04

--return

EXEC GM_REPLICATE_PRODUCTOS @pXML, '/DST/PRODUCTOS', 0
EXEC GM_REPLICATE_AGENTES @pXML, '/DST/AGENTES', 0

delete #TGM_E04  FROM #TGM_E04 A where exists (select * from SYS_INT_DOCUMENTO D where A.CLIENTE_ID = D.CLIENTE_ID and A.DOC_EXT = D.DOC_EXT)

INSERT INTO SYS_INT_DOCUMENTO (CLIENTE_ID, TIPO_DOCUMENTO_ID, FECHA_CPTE, FECHA_SOLICITUD_CPTE, AGENTE_ID, DOC_EXT, CODIGO_VIAJE, INFO_ADICIONAL_1, INFO_ADICIONAL_2, INFO_ADICIONAL_3)
SELECT distinct CLIENTE_ID, 'E04', FECHA_CPTE, FECHA_SOLICITUD_CPTE, AGENTE_ID, DOC_EXT, CODIGO_VIAJE, INFO_ADICIONAL_1, LOTE_ID, CODIGO_VIAJE 
FROM #TGM_E04 



declare @curDoc varchar(100)
set @curDoc = ''
declare @NroLinea integer
set @NroLinea = 0

declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @PRODUCTO_ID varchar(30)
declare @CANTIDAD_SOLICITADA numeric(20,5)
declare @UNIDAD_ID varchar(15)
declare @CODIGO_VIAJE varchar(100)
declare @PROP3 varchar(100)

DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, UNIDAD_ID, CODIGO_VIAJE, DOC_EXT + ' ' +dbo.padl(SDSFXO,3,'0') + ' ' + cast(SDLNID as varchar) as PROP3
from #TGM_E04 order by DOC_EXT
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @PRODUCTO_ID, @CANTIDAD_SOLICITADA, @UNIDAD_ID, @CODIGO_VIAJE, @PROP3
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT
   begin
     set @NroLinea = 1
     set @curDoc = @DOC_EXT
   end
   else 
     set @NroLinea = @NroLinea + 1	
   INSERT INTO SYS_INT_DET_DOCUMENTO (DOC_EXT, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, UNIDAD_ID, PROP1, PROP2, PROP3)
   values(@DOC_EXT, @NroLinea, @CLIENTE_ID, @PRODUCTO_ID, @CANTIDAD_SOLICITADA, @UNIDAD_ID, 'E04', @CODIGO_VIAJE, @PROP3)
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @PRODUCTO_ID, @CANTIDAD_SOLICITADA, @UNIDAD_ID, @CODIGO_VIAJE, @PROP3

END

CLOSE dcursor
DEALLOCATE dcursor
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER  procedure [dbo].[GM_E05](@CLIENTE_ID char(5))  as
set nocount on

declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')
declare @headerNbr integer
set @HeaderNbr = 0
select distinct CODIGO_VIAJE , dbo.padl(rtrim(substring(INFO_ADICIONAL_3,10,12)),12,' ') as VMCU into #tViajes 
from GM_DEV_DOCUMENTO where ESTADO is null and TIPO_DOCUMENTO_ID = 'E05' and CLIENTE_ID = @CLIENTE_ID 



select 
case when B.CODIGO_VIAJE  = B.INFO_ADICIONAL_3 then 'O' 
     when B.CODIGO_VIAJE <> B.INFO_ADICIONAL_3 then 'C' end
as TRP_STAT, B.CLIENTE_ID as COO, cast(B.AGENTE_ID as integer) as AN8, dbo.padl(rtrim(substring(B.DOC_EXT,1,12)),12,' ') as MCU,
substring(B.DOC_EXT,14,2) as DCTO , cast(substring(B.DOC_EXT,18,8) as integer) as DOCO, 
cast(substring(B.INFO_ADICIONAL_3,1,8) as integer) as TRP_ORIG,  A.VMCU, A.CODIGO_VIAJE, B.DOC_EXT, B.CLIENTE_ID,
case when B.CODIGO_VIAJE like 'NUE%' then cast(substring(B.CODIGO_VIAJE,4,11) as bigint) else cast(substring(B.CODIGO_VIAJE,1,8) as bigint) end as TRP_WARP,
case when B.CODIGO_VIAJE like 'NUE%' then cast(substring(B.CODIGO_VIAJE,4,11) as bigint) else cast(substring(B.CODIGO_VIAJE,1,8) as bigint) end as TRP,
convert(CHAR(10),B.FECHA_CPTE ,112) FECHA_CPTE  
into #tPedidos
from GM_DEV_DOCUMENTO B inner join #tViajes A on B.CODIGO_VIAJE = A.CODIGO_VIAJE 
update #tPedidos set TRP_STAT = 'N', TRP = 0 where rtrim(ltrim(CODIGO_VIAJE))  like 'NUE%'

insert into #tPedidos select 'R', B.CLIENTE_ID as COO, cast(B.AGENTE_ID as integer) as AN8, 
dbo.padl(rtrim(substring(B.DOC_EXT,1,12)),12,' ') as MCU,
substring(B.DOC_EXT,14,2) as DCTO , cast(substring(B.DOC_EXT,18,8) as integer) as DOCO, 
cast(substring(B.INFO_ADICIONAL_3,1,8) as integer) as TRP_ORIG,  A.VMCU, A.CODIGO_VIAJE, B.DOC_EXT, B.CLIENTE_ID,
0 as TRP_WARP, 0 as TRP, ' ' FECHA_CPTE 
 from SYS_INT_DOCUMENTO B inner join #tViajes A on B.INFO_ADICIONAL_3 = A.CODIGO_VIAJE 
where B.TIPO_DOCUMENTO_ID = 'E04' and B.ESTADO_GT is null
and not exists (select * from #tPedidos C where C.CLIENTE_ID = B.CLIENTE_ID and C.DOC_EXT = B.DOC_EXT)


if (select count(*) from #tPedidos) > 0
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'


select B.TRP_STAT,@HeaderNbr as HEADER_NBR, 0 as DETAIL_NBR, ' ' as GTDFTF,
 PROP3, PROP3 + cast(NRO_LINEA as varchar) as PK, 
 G.JULIANO as TRDJ, B.TRP_ORIG, B.TRP_WARP, TRP, B.VMCU, B.COO, B.AN8, B.MCU, B.DCTO, B.DOCO, 
 substring(PROP3,26,3) as SFX, cast(substring(PROP3,30,10) as integer) as LNID, cast(A.PRODUCTO_ID as int) as ITM,
 cast(A.CANTIDAD_SOLICITADA * 10000 as integer) as UORG, 
 cast(A.CANTIDAD * 10000 as integer) as SOQS, isnull(A.NRO_LOTE,'')  as LOTN_ORIG,isnull(A.NRO_LOTE,'')  as LOTN,
 isnull(cast(A.NRO_PALLET as int),0) as PALN, 
 A.UNIDAD_ID as UOM, @UnicId as LOTE_ID, ' ' as ESTADO, isnull(DEPOSITO_JDE,'') as  MCU_ORIG, isnull(UBIC_JDE,'') as  LOCN_ORIG,
 isnull(ESTADO_LOTE_CD, '') as LOTS, P.CODIGO_PRODUCTO as LITM, P.DESCRIPCION as DSC1, A.DOC_EXT, A.CLIENTE_ID, FECHA_VENCIMIENTO
into #tDetalle
from GM_DEV_DET_DOCUMENTO A inner join #tPedidos B on A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID
left join GM_SUCURSAL_NAVE S on  A.NAVE_ID = S.NAVE_ID and A.CLIENTE_ID = S.CLIENTE_ID and A.CAT_LOG_ID = S.CAT_LOG_ID
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
inner join GM_FECHAS G on B.FECHA_CPTE = G.FECHA

declare @PROP3 varchar(100)
declare @CurPROP3 varchar(100)
declare @PK varchar(100)
declare @detailNbr integer
declare @GTDFTF char(1)
set @detailNbr = 0
set @CurPROP3 = ''

DECLARE dcursor CURSOR FOR select PROP3, PK from #tDetalle order by TRP_ORIG, VMCU, PROP3, SOQS desc
open dcursor
fetch next from dcursor into @PROP3, @PK
WHILE @@FETCH_STATUS = 0
BEGIN
     set @detailNbr = @detailNbr + 1	
     if @CurPROP3 = @PROP3
	set @GTDFTF =  'A'
     else
	set @GTDFTF =  ' '
     	 
     set @CurPROP3 = @PROP3   
     update #tDetalle set GTDFTF = @GTDFTF, DETAIL_NBR = @detailNbr where PK = @PK
     fetch next from dcursor into @PROP3, @PK
END

CLOSE dcursor
DEALLOCATE dcursor

declare @NOW datetime

set @NOW = getdate()


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #tDetalle B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #tDetalle B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID 



select TRP_STAT, HEADER_NBR as MJEDOC, min(DETAIL_NBR * 1000) as MJEDLN, min(GTDFTF) as GTDFTF, TRDJ, TRP_ORIG, TRP_WARP, TRP, VMCU,
COO, AN8, MCU, DCTO, DOCO, SFX, LNID, ITM, max(UORG) as UORG, sum(SOQS) as SOQS, LOTN_ORIG,LOTN, 0 as PALN, UOM, MCU_ORIG,  LOCN_ORIG, LOTS,
LITM, DSC1, LOTE_ID, ESTADO, isnull(max(F.JULIANO),0) as MJMMEJ   
from #tDetalle D left join GM_FECHAS F on  D.FECHA_VENCIMIENTO = F.FECHA
group by TRP_STAT, HEADER_NBR, TRDJ, TRP_ORIG, TRP_WARP, TRP, VMCU, COO, AN8, MCU, DCTO, DOCO, SFX, LNID, ITM,
LOTN_ORIG,LOTN, UOM, MCU_ORIG,  LOCN_ORIG, LOTS, LITM, DSC1, LOTE_ID, ESTADO 
order by MJEDLN






SET QUOTED_IDENTIFIER OFF
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_E10] as

set nocount on

declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')



select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * -10000 AS CANTIDAD,  DD.NRO_LOTE,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'S' as MJEDER, 'QS' as MJPACD,
G.UBIC_JDE, ISNULL(G.DEPOSITO_JDE, '') AS DEPOSITO_JDE, isnull(G.ESTADO_LOTE_CD, '') as ESTADO_LOTE_CD, NRO_PALLET into #TGM_I05
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
LEFT join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where tipo_documento_id = 'E10' 
and DD.cantidad <> 0
and dd.ESTADO is null
and d.ESTADO is null
union
select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, 
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'R' as MJEDER, 'QR' as MJPACD,
substring(prop1,14,20) as UBIC_JDE, substring(dd.prop1,1,12) as DEPOSITO_JDE, '' as ESTADO_LOTE_CD, NRO_PALLET 
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
where tipo_documento_id = 'E10' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null



declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @MJEDER char(1)



DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, MJEDER from #TGM_I05 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_I05 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and MJEDER = @MJEDER
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
END

CLOSE dcursor
DEALLOCATE dcursor


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I05 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I05 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC, 'EA' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'E10' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, @UnicID as M1VR01, 'N' as M1EDSP 
 from #TGM_I05 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 


select distinct DOC_EXT as  MJPNID, 'D' as MJEDTY, 1  as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'EA' as MJEDCT, 'E10' as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX, MJEDER,  MJPACD, 
@UnicID as MJVR01, 'N' as MJEDSP, F.JULIANO as MJMMEJ 
from #TGM_I05 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
	        inner join GM_FECHAS F on A.FECHA_VENCIENTO = F.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER PROCEDURE [dbo].[GM_EVENTOS_INS] ( @EVENT_NM varchar(80), @SERVER_CD char(1), @ROWS_QTY int, @ERROR_FG bit, @ERROR_TXT text, @EVENT_CD varchar(20))
AS
SET NOCOUNT ON
INSERT INTO GM_EVENTOS ( EVENT_NM, SERVER_CD, EVENT_TS, ROWS_QTY, ERROR_FG, ERROR_TXT, EVENT_CD) VALUES ( @EVENT_NM, @SERVER_CD,getdate(), @ROWS_QTY, @ERROR_FG, @ERROR_TXT, @EVENT_CD)
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER  PROCEDURE [dbo].[GM_I01] (@pXML ntext)
as
set nocount on
SET XACT_ABORT ON
DECLARE @hDoc int
EXEC sp_xml_preparedocument @hDoc OUTPUT, @pXML


SELECT CLIENTE_ID, TIPO_DOCUMENTO_ID, FECHA_CPTE, FECHA_SOLICITUD, AGENTE_ID, 
PDOKCO + '  ' + PDOORN +  '  ' +  PDOCTO +  '  ' +  RIGHT('0000000' + CAST(PDOGNO as varchar(10)) ,7) as ORDEN_DE_COMPRA,
PDDCTO + '  ' + RIGHT('00000000' + CAST(PDDOCO as varchar(10)) ,8) +  ' ' + PDSFXO +  '  ' + RIGHT('00000000' + CAST(PDLNID as varchar(10)) ,6) as DOC_EXT, 
INFO_ADICIONAL_1, NRO_LINEA, PRODUCTO_ID, CANTIDAD_SOLICITADA, UNIDAD_ID, LOTE_ID
 into #GM_I01
FROM OPENXML (@hDoc, '/DST/I01')  WITH 
	(CLIENTE_ID varchar(5) 'CLIENTE_ID', 
	TIPO_DOCUMENTO_ID varchar(12) 'TIPO_DOCUMENTO_ID', 
	FECHA_CPTE smalldatetime 'FECHA_CPTE', 
	FECHA_SOLICITUD smalldatetime 'FECHA_SOLICITUD', 
	AGENTE_ID varchar(20) 'AGENTE_ID', 
	PDOKCO char(5) 'PDOKCO', 
	PDOORN varchar(8) 'PDOORN', 
	PDOCTO char(2) 'PDOCTO', 
	PDOGNO decimal(8,0) 'PDOGNO', 
	PDDCTO char(2) 'PDDCTO',
	PDDOCO decimaL(8,0) 'PDDOCO', 
	PDSFXO char(3) 'PDSFXO', 
	PDLNID decimal(8,0) 'PDLNID',
	INFO_ADICIONAL_1 varchar(50) 'INFO_ADICIONAL_1', 
	NRO_LINEA integer 'NRO_LINEA', 
	PRODUCTO_ID decimal(8,0) 'PRODUCTO_ID', 
	CANTIDAD_SOLICITADA decimal(20,5) 'CANTIDAD_SOLICITADA', 
	UNIDAD_ID varchar(50) 'UNIDAD_ID', 
	LOTE_ID varchar(50) 'LOTE_ID') as A 
      
-- where not exists(select A.* from SYS_INT_DOCUMENTO B WHERE A.CLIENTE_ID = B.CLIENTE_ID and B.DOC_EXT = 
--PDDCTO + '  ' + RIGHT('00000000' + CAST(PDDOCO as varchar(10)) ,8) +  ' ' + PDSFXO +  '  ' + RIGHT('00000000' + CAST(PDLNID as varchar(10)) ,6))


EXEC sp_xml_removedocument @hDoc


--select * into TEMPO_I01 from #GM_I01
--return

EXEC GM_REPLICATE_AGENTES @pXML, '/DST/AGENTES', 0
EXEC GM_REPLICATE_PRODUCTOS @pXML, '/DST/PRODUCTOS', 0

UPDATE SYS_INT_DOCUMENTO set FECHA_SOLICITUD_CPTE = T.FECHA_SOLICITUD from SYS_INT_DOCUMENTO SID, #GM_I01 T where SID.CLIENTE_ID = T.CLIENTE_ID and SID.DOC_EXT = T.DOC_EXT and SID.ESTADO_GT is null and SID.FECHA_SOLICITUD_CPTE <> T.FECHA_SOLICITUD


delete #GM_I01  FROM #GM_I01 A where exists (select * from SYS_INT_DOCUMENTO D where A.CLIENTE_ID = D.CLIENTE_ID and A.DOC_EXT = D.DOC_EXT)

INSERT INTO SYS_INT_DOCUMENTO (CLIENTE_ID, TIPO_DOCUMENTO_ID, FECHA_CPTE, FECHA_SOLICITUD_CPTE, AGENTE_ID, ORDEN_DE_COMPRA, DOC_EXT, INFO_ADICIONAL_1, INFO_ADICIONAL_2)
SELECT CLIENTE_ID, TIPO_DOCUMENTO_ID, FECHA_CPTE, FECHA_SOLICITUD, AGENTE_ID, ORDEN_DE_COMPRA, DOC_EXT, INFO_ADICIONAL_1, LOTE_ID FROM #GM_I01

INSERT INTO SYS_INT_DET_DOCUMENTO (DOC_EXT, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, NRO_LOTE, UNIDAD_ID, PROP1, PROP2)
SELECT DOC_EXT, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, LOTE_ID, UNIDAD_ID, TIPO_DOCUMENTO_ID, LOTE_ID FROM #GM_I01
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER   procedure [dbo].[GM_I02] as

set nocount on

declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')


select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, DD.PROP3 as NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT,
cast(substring(D.DOC_EXT,5,8)as integer) as DOCO,substring(D.DOC_EXT,1,2) as DCTO, substring(D.DOC_EXT,14,3) as SFXO,
cast(substring(D.DOC_EXT,19,6) as integer) as LNID,
dbo.padl(D.INFO_ADICIONAL_1,12,' ') as INFO_ADICIONAL_1, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, isnull(DD.DOC_BACK_ORDER,'') as DOC_BACK_ORDER,
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD into #TGM_I02
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where tipo_documento_id = 'I02'
and DD.cantidad <> 0
--and d.ESTADO is null
and dd.ESTADO is null



declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0


DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA from #TGM_I02 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47071'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_I02 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA
END

CLOSE dcursor
DEALLOCATE dcursor


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I02 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I02 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA



select distinct HEADER_NBR as SYEDOC, 'I2' as SYEDCT, 'I02' as SYEDFT , CLIENTE_ID as SYEKCO, '860' as SYEDST,
'R' as SYEDER, 1 as SYEDDL, '14' as SYTPUR, '2' as SYRATY, 0 as SYDOCO, DCTO as SYDCTO, CLIENTE_ID as SYKCOO, 
'000' as SYSFXO, isnull(NRO_REMITO,'') as SYRMK, F.JULIANO as SYEDDT, @UnicID as SYCNID, 'N' as SYEDSP 
 from #TGM_I02 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 

select  HEADER_NBR as SZEDOC, 'I2' as SZEDCT, 'I02' as SZEDFT , CLIENTE_ID as SZEKCO, DETAIL_NBR * 1000 as SZEDLN,
'860' as SZEDST, 'R' as SZEDER, DOCO as SZDOCO, DCTO as SZDCTO, CLIENTE_ID as SZKCOO, SFXO as SZSFXO,
LNID as SZLNID, cast(PRODUCTO_ID as integer) as SZITM, CANTIDAD as SZUREC, F.JULIANO as SZURDT,
NRO_LOTE as SZLOTN, UNIDAD_ID as SZUOM, G.JULIANO as SZADDJ, INFO_ADICIONAL_1 as SZMCU, UBIC_JDE as SZLOCN, 
ESTADO_LOTE_CD as SZURCD, case when DOC_BACK_ORDER = '' then '7' else '1' end as SZLSTS , @UnicID as SZCNID, 'N' as SZEDSP
from #TGM_I02 A inner join GM_FECHAS F on A.FECHA_VENCIENTO = F.FECHA
inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
order by HEADER_NBR , DETAIL_NBR
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER  procedure [dbo].[GM_I02_UPDATE]
(@ACCION char(1),  @UnicID as varchar(20))
as
set nocount on

declare @ESTADO varchar(20) 
declare @NOW datetime
set @NOW = getdate()

if @ACCION ='A'
   set @ESTADO = null 
else
   set @ESTADO = @ACCION + ' ' + @UnicID 

update GM_DEV_DOCUMENTO set ESTADO = @ESTADO, FECHA_ESTADO = @NOW where ESTADO = 'T ' + @UnicID
update GM_DEV_DET_DOCUMENTO set ESTADO = @ESTADO, FECHA_ESTADO = @NOW where ESTADO = 'T ' + @UnicID
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER    PROCEDURE [dbo].[GM_I04] (@pXML ntext)
as
set nocount on
SET XACT_ABORT ON
DECLARE @hDoc int
EXEC sp_xml_preparedocument @hDoc OUTPUT, @pXML

SELECT NRO_PALLET, PRODUCTO_ID, CLIENTE_ID, NRO_LOTE,
RIGHT(space(12) + isnull(UN_NEG,''),12)  + ' ' + 
LEFT(isnull(UBICACION,'')+ space(20),20) + ' ' +
LEFT(isnull(TIPO_ORDEN_GEN,'') + space(2) ,2) + ' ' + 
RIGHT('00000000' + cast(isnull(NRO_ORDEN_GEN,0) as varchar), 8) + ' ' + ESTADO_LOTE as INFO_ADICIONAL_1,
CANTIDAD_SOLICITADA, UNIDAD_ID, FECHA_SOLICITUD_CPTE, FECHA_VENCIMIENTO, LOTE_ID
 into #TGM_I04
FROM OPENXML (@hDoc, '/DST/I04')  WITH 
       ( NRO_PALLET varchar(100)  'NRO_PALLET',
	PRODUCTO_ID decimal(8,0) 'PRODUCTO_ID', 
	CLIENTE_ID varchar(5) 'CLIENTE_ID', 
	NRO_LOTE varchar(100) 'NRO_LOTE', 
        UN_NEG varchar(12) 'UN_NEG', 
        UBICACION varchar(3) 'UBICACION', 
        TIPO_ORDEN_GEN varchar(12) 'TIPO_ORDEN_GEN', 
        NRO_ORDEN_GEN int 'NRO_ORDEN_GEN',
	INFO_ADICIONAL_1 varchar(50) 'INFO_ADICIONAL_1', 
	CANTIDAD_SOLICITADA decimal(20,5) 'CANTIDAD_SOLICITADA', 
	UNIDAD_ID varchar(50) 'UNIDAD_ID', 
	FECHA_SOLICITUD_CPTE smalldatetime 'FECHA_SOLICITUD_CPTE', 
	FECHA_VENCIMIENTO smalldatetime 'FECHA_VENCIMIENTO',
	LOTE_ID varchar(50) 'LOTE_ID',
	ESTADO_LOTE char(1) 'ESTADO_LOTE'
	) as A
       where not exists(select A.* from SYS_INT_DOCUMENTO B WHERE A.CLIENTE_ID = B.CLIENTE_ID and B.DOC_EXT = cast(NRO_PALLET as varchar))


EXEC sp_xml_removedocument @hDoc

EXEC GM_REPLICATE_PRODUCTOS @pXML, '/DST/PRODUCTOS', 0

INSERT INTO SYS_INT_DOCUMENTO (CLIENTE_ID, TIPO_DOCUMENTO_ID, FECHA_CPTE, FECHA_SOLICITUD_CPTE,DOC_EXT, INFO_ADICIONAL_1,INFO_ADICIONAL_2)
SELECT CLIENTE_ID, 'I04', getdate(), FECHA_SOLICITUD_CPTE, NRO_PALLET, INFO_ADICIONAL_1, LOTE_ID FROM #TGM_I04

INSERT INTO SYS_INT_DET_DOCUMENTO (DOC_EXT, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, NRO_LOTE, NRO_PALLET, UNIDAD_ID, FECHA_VENCIMIENTO, PROP1, PROP2)
SELECT NRO_PALLET, 1, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, NRO_LOTE,  NRO_PALLET, UNIDAD_ID, FECHA_VENCIMIENTO, 'I04', LOTE_ID FROM #TGM_I04

--select * FROM #TGM_I04
--drop table #TGM_I04
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER     procedure [dbo].[GM_I05] as

set nocount on

declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')



select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'R' as MJEDER, 'QR' as MJPACD,
G.UBIC_JDE, ISNULL(G.DEPOSITO_JDE, '') AS DEPOSITO_JDE, isnull(G.ESTADO_LOTE_CD, '') as ESTADO_LOTE_CD, NRO_PALLET into #TGM_I05
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
LEFT join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where tipo_documento_id = 'I05' and prop1='I04'
and DD.cantidad <> 0
and dd.ESTADO is null
and d.ESTADO is null
union
select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * -10000 AS CANTIDAD,  DD.NRO_LOTE, 
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'S' as MJEDER, 'QS' as MJPACD,
substring(info_adicional_1,14,20) as UBIC_JDE, substring(info_adicional_1,1,12) as DEPOSITO_JDE, substring(info_adicional_1,47,1) as ESTADO_LOTE_CD, NRO_PALLET 
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
where tipo_documento_id = 'I05' and prop1='I04'
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null



declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @MJEDER char(1)



DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, MJEDER from #TGM_I05 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_I05 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and MJEDER = @MJEDER
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @MJEDER
END

CLOSE dcursor
DEALLOCATE dcursor

update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I05 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I05 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA

select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC, 'I5' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'I05' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, @UnicID as M1VR01, 'N' as M1EDSP 
 from #TGM_I05 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 


select distinct DOC_EXT as  MJPNID, 'D' as MJEDTY, 1  as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'I5' as MJEDCT, 'I05' as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX, MJEDER,  MJPACD, 
@UnicID as MJVR01, 'N' as MJEDSP, F.JULIANO as MJMMEJ 
from #TGM_I05 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
	        inner join GM_FECHAS F on A.FECHA_VENCIENTO = F.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER   procedure [dbo].[GM_I06] as
set nocount on
declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')
declare @NOW datetime
set @NOW = getdate()
declare @param varchar(5000)
set @param = ''
declare @DOC_EXT varchar(10)

select top 20 CLIENTE_ID, DOC_EXT  into #TGM_I06 from GM_DEV_DOCUMENTO where tipo_Documento_id = 'I06' and ESTADO is null


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I06 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I06 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID 



DECLARE dcursor CURSOR FOR select DOC_EXT from #TGM_I06 order by  DOC_EXT
open dcursor
fetch next from dcursor into @DOC_EXT
WHILE @@FETCH_STATUS = 0
BEGIN
   set @param = @param + @DOC_EXT + ','
   fetch next from dcursor into @DOC_EXT
END

CLOSE dcursor
DEALLOCATE dcursor

select @param as PALLETS
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER  procedure [dbo].[GM_I07IM] as
set nocount on
declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')

select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE,  D.DOC_EXT,
DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, D.AGENTE_ID,
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET, 
case when S.CLIENTE_INTERNO = 0 then 'EZ'
 else case when S.CATEGORIA_IMPOSITIVA_ID = 'IP' then 'GG' else substring(S.OBSERVACIONES,1,2) end end as DCT
 into #TGM_I07
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
inner join SUCURSAL S on D.AGENTE_ID = S.SUCURSAL_ID and S.CLIENTE_ID = D.CLIENTE_ID
where D.tipo_documento_id = 'I07' and Tipo_Comprobante='IM' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null
union 
select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE,  D.DOC_EXT,
DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * -10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, D.AGENTE_ID,
CUENTA_EXTERNA_2, space(12 - len(rtrim(ltrim(substring(CUENTA_EXTERNA_1,1,12))))) + rtrim(ltrim(substring(CUENTA_EXTERNA_1,1,12))), '', NRO_PALLET, 'GG' as DCT
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join SUCURSAL S on D.AGENTE_ID = S.SUCURSAL_ID and S.CLIENTE_ID = D.CLIENTE_ID
where D.tipo_documento_id = 'I07' and Tipo_Comprobante='IM' and S.CATEGORIA_IMPOSITIVA_ID = 'IP'  
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null

declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @UBIC_JDE varchar(20)

DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, UBIC_JDE from #TGM_I07 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_I07 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and UBIC_JDE = @UBIC_JDE
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
END

CLOSE dcursor
DEALLOCATE dcursor


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I07 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I07 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'I7' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'I07IM' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, 
@UnicID as M1VR01,  'N' as M1EDSP
 from #TGM_I07 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 

select distinct DOC_EXT as  MJPNID,'D' as MJEDTY, 1 as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'I7' as MJEDCT, 'I07IM'  as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX,  
case when CANTIDAD > 0 then 'R' else 'S' end as MJEDER,  
case when CANTIDAD > 0 then 'QR' else 'QS' end as MJPACD, @UnicID as MJVR01,  'N' as MJEDSP ,  DCT as MJDCT
from #TGM_I07 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER     procedure [dbo].[GM_I08] as

set nocount on

declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')

select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT, DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 'R' as MJEDER, 'QR' as MJPACD, G.UBIC_JDE, 
G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET, 'DEV ' + space(12 - len(rtrim(ltrim(substring(prop3,1,12))))) + rtrim(ltrim(substring(prop3,1,12))) + ' ' + substring(prop3,14,11) as REFERENCIA, 
cast(substring(dd.PROP2,1,8) as int) as AN8, isnull(cast(substring(dd.PROP2,10,3) as varchar),' ') as MJRCD
into #TGM_I08
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where tipo_documento_id = 'I08' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null

declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @REFERENCIA varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @MJEDER char(1)
declare @CurDOC_EXT varchar(50)
set @CurDOC_EXT = ''


DECLARE dcursor CURSOR FOR select DOC_EXT, REFERENCIA, CLIENTE_ID, NRO_LINEA, MJEDER from #TGM_I08 order by CLIENTE_ID, REFERENCIA, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @REFERENCIA, @CLIENTE_ID, @NRO_LINEA, @MJEDER
WHILE @@FETCH_STATUS = 0
BEGIN
   if @CurDOC_EXT <> @DOC_EXT or @curDoc <> @REFERENCIA or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @REFERENCIA
     set @curCliente = @CLIENTE_ID
     set @CurDOC_EXT = @DOC_EXT
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_I08 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where REFERENCIA = @REFERENCIA and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and MJEDER = @MJEDER and DOC_EXT = @DOC_EXT
   fetch next from dcursor into  @DOC_EXT, @REFERENCIA, @CLIENTE_ID, @NRO_LINEA, @MJEDER
END

CLOSE dcursor
DEALLOCATE dcursor

update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I08 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I08 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'I8' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'I08' as M1EDFT, '808' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, @UnicID as M1VR01, 'N' as M1EDSP,isnull(AN8,0) as  M1URAB, DEPOSITO_JDE as M1URRF 
 from #TGM_I08 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 


select DOC_EXT as  MJPNID, 'D' as MJEDTY, 1  as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'I8' as MJEDCT, 'I08' as MJEDFT, A.CLIENTE_ID as MJEKCO, '808' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, REFERENCIA as MJTREX, MJEDER,  MJPACD, 
@UnicID as MJVR01, 'N' as MJEDSP ,0 as  MJURAB,  MJRCD
from #TGM_I08 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_I99_I01] as

set nocount on

declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')


select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, D.NRO_REMITO, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE, D.AGENTE_ID, D.ORDEN_DE_COMPRA, D.DOC_EXT,
cast(substring(D.DOC_EXT,5,8)as integer) as DOCO,substring(D.DOC_EXT,1,2) as DCTO, substring(D.DOC_EXT,14,3) as SFXO,
cast(substring(D.DOC_EXT,19,6) as integer) as LNID,
dbo.padl(D.INFO_ADICIONAL_1,12,' ') as INFO_ADICIONAL_1, DD.NRO_LINEA, 
DD.PRODUCTO_ID, isnull(DD.CANTIDAD, 0) * 10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, ' '  as DOC_BACK_ORDER,
' ' UBIC_JDE, ' ' DEPOSITO_JDE, ' ' ESTADO_LOTE_CD into #TGM_I99
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
where tipo_documento_id = 'I99' and dd.prop1 = 'I01'
and d.ESTADO is null
and dd.ESTADO is null




declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0


DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA from #TGM_I99 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47071'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_I99 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA
END

CLOSE dcursor
DEALLOCATE dcursor


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I99 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I99 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA



select distinct HEADER_NBR as SYEDOC, '91' as SYEDCT, 'I99' as SYEDFT , CLIENTE_ID as SYEKCO, '860' as SYEDST,
'R' as SYEDER, 1 as SYEDDL, '14' as SYTPUR, '2' as SYRATY, 0 as SYDOCO, DCTO as SYDCTO, CLIENTE_ID as SYKCOO, '000' as SYSFXO,
isnull(NRO_REMITO,'') as SYRMK, F.JULIANO as SYEDDT, @UnicID as SYCNID,  'N' as SYEDSP 
 from #TGM_I99 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 

select  HEADER_NBR as SZEDOC, '91' as SZEDCT, 'I99' as SZEDFT , CLIENTE_ID as SZEKCO, DETAIL_NBR * 1000 as SZEDLN,
'860' as SZEDST, 'R' as SZEDER, DOCO as SZDOCO, DCTO as SZDCTO, CLIENTE_ID as SZKCOO, SFXO as SZSFXO,
LNID as SZLNID, cast(PRODUCTO_ID as integer) as SZITM, 0 as SZUREC, 0 as SZURDT,
NRO_LOTE as SZLOTN, UNIDAD_ID as SZUOM, G.JULIANO as SZTRDJ, INFO_ADICIONAL_1 as SZMCU, UBIC_JDE as SZLOCN, ESTADO_LOTE_CD as SZURCD,
'9' as SZLSTS , @UnicID as SZCNID,  'N' as SZEDSP 
from #TGM_I99 A inner join  GM_FECHAS G on A.FECHA_CPTE = G.FECHA
order by HEADER_NBR , DETAIL_NBR
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER  procedure [dbo].[GM_I99_I04] as
set nocount on
declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')
declare @NOW datetime
set @NOW = getdate()
declare @param varchar(8000)
set @param = ''
declare @DOC_EXT varchar(10)

select distinct top 500 D.CLIENTE_ID, D.DOC_EXT  
into #TGM_I99_I04 
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
where tipo_documento_id = 'I99' and dd.prop1 = 'I04'
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_I99_I04  B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_I99_I04 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID 



DECLARE dcursor CURSOR FOR select DOC_EXT from #TGM_I99_I04 order by  DOC_EXT
open dcursor
fetch next from dcursor into @DOC_EXT
WHILE @@FETCH_STATUS = 0
BEGIN
   set @param = @param + @DOC_EXT + ','
   fetch next from dcursor into @DOC_EXT
END

CLOSE dcursor
DEALLOCATE dcursor

select @param as PALLETS
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_PREINGRESO] as
set nocount on
declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')


select distinct CLIENTE_ID, DOC_EXT into #TDOC_EXC from GM_DEV_DET_DOCUMENTO  A where prop1 = 'TRAN_ING' and 
not exists (select 1 from GM_SUCURSAL_NAVE B where A.CAT_LOG_ID = B.CAT_LOG_ID  and A.NAVE_ID = B.NAVE_ID and A.CLIENTE_ID = B.CLIENTE_ID )

update GM_DEV_DET_DOCUMENTO  set ESTADO = 'INTERNO WARP', FECHA_ESTADO = Getdate()
FROM GM_DEV_DET_DOCUMENTO A, #TDOC_EXC B where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID


select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE,  D.DOC_EXT,
DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET into #TGM_PI 
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
left  join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where dd.cat_log_id='TRAN_ING'
and DD.cantidad > 0
and dd.nro_pallet is not null




declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @UBIC_JDE varchar(20)






DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, UBIC_JDE from #TGM_PI order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_PI set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and UBIC_JDE = @UBIC_JDE
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
END

CLOSE dcursor
DEALLOCATE dcursor



update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_PI B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_PI B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'WR' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'PI01' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, 
@UnicID as M1VR01,  'N' as M1EDSP 
 from #TGM_PI A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 

select distinct DOC_EXT as  MJPNID,'D' as MJEDTY, DETAIL_NBR  as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'WR' as MJEDCT, 'PI01' as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX,  
case when CANTIDAD > 0 then 'R' else 'S' end as MJEDER,  
case when CANTIDAD > 0 then 'QR' else 'QS' end as MJPACD, @UnicID as MJVR01,  'N' as MJEDSP 
from #TGM_PI A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER   PROCEDURE [dbo].[GM_REPLICATE_47122] 
as
set nocount on
SET XACT_ABORT ON

/*
ALTER   PROCEDURE dbo.GM_REPLICATE_47122 (@pXML ntext, @pPATH varchar(100)=null ,@pFULL smallint = 1)
drop table dbo.GM_47122

if @pPATH is null
   set @pPATH =	'/DST/PRODUCTOS'


DECLARE @hDoc int
EXEC sp_xml_preparedocument @hDoc OUTPUT, @PXML

SELECT CANT ,MJITM, MJMCU, MJLOTN, MJLOCN, MJTRUM
into dbo.GM_47122
FROM OPENXML (@hDoc, @pPATH)  WITH 
	(CANT int 'CANT', 
	MJITM int 'MJITM' , 
	MJMCU varchar(12) 'MJMCU', 
	MJLOTN varchar(50) 'MJLOTN',
	MJLOCN varchar(50) 'MJLOCN',
	MJTRUM varchar(2) 'MJTRUM')


EXEC sp_xml_removedocument @hDoc

*/


select cast(sum(CANTIDAD)  as integer) as  cant,  cast(DD.PRODUCTO_ID as int) as MJITM,  SUBSTRING(PROP3,1,12) as MJMCU , 
case when SUBSTRING(PROP3,1,12) = 'VICPIC' then ' ' else DD.NRO_LOTE end as MJLOTN ,
 UNIDAD_ID as MJTRUM, isnull(UBIC_JDE,'') as  MJLOCN, FECHA_CPTE, FECHA_VENCIMIENTO
into #PASADO
 from SYS_DEV_DOCUMENTO D inner join SYS_DEV_DET_DOCUMENTO DD on D.DOC_EXT = DD.DOC_EXT
left join GM_SUCURSAL_NAVE S on  DD.NAVE_ID = S.NAVE_ID and DD.CLIENTE_ID = S.CLIENTE_ID and DD.CAT_LOG_ID = S.CAT_LOG_ID
where D.ESTADO is not null and D.TIPO_DOCUMENTO_ID = 'E05' and CANTIDAD <> 0
group by DD.PRODUCTO_ID, case when SUBSTRING(PROP3,1,12) = 'VICPIC' then ' ' else DD.NRO_LOTE end, SUBSTRING(PROP3,1,12), UNIDAD_ID, isnull(UBIC_JDE,''), FECHA_CPTE, FECHA_VENCIMIENTO
union
select cast(sum(CANTIDAD * -1) as integer)  as  cant,  cast(DD.PRODUCTO_ID as int) as MJITM, ltrim(rtrim( isnull(DEPOSITO_JDE,''))) as  MCU_ORIG, 
DD.NRO_LOTE  as MJLOTN, UNIDAD_ID as MJTRUM, isnull(UBIC_JDE,'') as  MJLOCN, FECHA_CPTE, FECHA_VENCIMIENTO
 from SYS_DEV_DOCUMENTO D inner join SYS_DEV_DET_DOCUMENTO DD on D.DOC_EXT = DD.DOC_EXT
left join GM_SUCURSAL_NAVE S on  DD.NAVE_ID = S.NAVE_ID and DD.CLIENTE_ID = S.CLIENTE_ID and DD.CAT_LOG_ID = S.CAT_LOG_ID
where D.ESTADO is not null and D.TIPO_DOCUMENTO_ID = 'E05'  and CANTIDAD <> 0
group by DD.PRODUCTO_ID, DD.NRO_LOTE ,  ltrim(rtrim( isnull(DEPOSITO_JDE,''))), UNIDAD_ID,isnull(UBIC_JDE,''),FECHA_CPTE, FECHA_VENCIMIENTO
 

/*
select ' ' as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, 3 as M1EDOC,'W1' as M1EDCT, '10202' as M1EKCO, 'B' as M1EDER,
'E050708' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, 108190 as M1EDDT, 10202 as  M1AN8, 'E0520080707' as M1VR01, 'N' as M1EDSP 
union
select  ' ' as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, 4 as M1EDOC,'W1' as M1EDCT, '10202' as M1EKCO, 'B' as M1EDER,
'E050708' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, 108190 as M1EDDT, 10202 as  M1AN8, 'E0520080707' as M1VR01, 'N' as M1EDSP 


select ' ' as  MJPNID, 'D' as MJEDTY,  1 * 1000 as MJEDLN, 
3 MJEDOC, 'W1' as MJEDCT, 'E050708' as MJEDFT, '10202' as MJEKCO, '860' as MJEDST,
MJMCU, 10202 as  MJAN8, 
MJITM, isnull(cast(MJLOCN as varchar),'') as MJLOCN,  isnull(MJLOTN,'')as  MJLOTN,  '' as MJLOTS, 
 isnull(MJTRUM,'') MJTRUM, 
CANT *  -10000 as MJTRQT, 108190 as MJTRDJ, '' MJTREX, 
case when CANT * -1 > 0 then 'R' else 'S' end as MJEDER,  
case when CANT * -1 > 0 then 'QR' else 'QS' end as MJPACD,
'E050708'  as MJVR01, 'N' as MJEDSP 
from dbo.GM_47122 
union

 */
select ' ' as  MJPNID, 'D' as MJEDTY,  1 * 1000 as MJEDLN, 
23 MJEDOC, 'W5' as MJEDCT, 'E050711' as MJEDFT, '10202' as MJEKCO, '860' as MJEDST,
MJMCU, 10202 as  MJAN8, 
MJITM, isnull(cast(MJLOCN as varchar),'') as MJLOCN,  isnull(MJLOTN,'') MJLOTN,  '' as MJLOTS,  isnull(MJTRUM,'') MJTRUM,
CANT *  10000 as MJTRQT, A.JULIANO as MJTRDJ, '' MJTREX, 
case when CANT  > 0 then 'R' else 'S' end as MJEDER,  
case when CANT  > 0 then 'QR' else 'QS' end as MJPACD,
'E050708'  as MJVR01, 'N' as MJEDSP,  B.JULIANO as MJMMEJ 
from #pasado inner join GM_FECHAS A on convert(CHAR(10),FECHA_CPTE ,112)  = A.FECHA
	     inner join GM_FECHAS B on convert(CHAR(10),FECHA_VENCIMIENTO ,112)  = B.FECHA
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_ST01] as
set nocount on
declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')


select distinct CLIENTE_ID, DOC_EXT into #TDOC_EXC from GM_DEV_DET_DOCUMENTO  A where prop1 = 'ST01' and 
not exists (select 1 from GM_SUCURSAL_NAVE B where A.CAT_LOG_ID = B.CAT_LOG_ID  and A.NAVE_ID = B.NAVE_ID and A.CLIENTE_ID = B.CLIENTE_ID )

update GM_DEV_DET_DOCUMENTO  set ESTADO = 'INTERNO WARP', FECHA_ESTADO = Getdate()
FROM GM_DEV_DET_DOCUMENTO A, #TDOC_EXC B where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID


select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE,  D.DOC_EXT,
DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET into #TGM_ST01
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where tipo_documento_id = 'ST01' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null



declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @UBIC_JDE varchar(20)






DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, UBIC_JDE from #TGM_ST01 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_ST01 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and UBIC_JDE = @UBIC_JDE
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
END

CLOSE dcursor
DEALLOCATE dcursor



update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_ST01 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_ST01 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'S1' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'ST01' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, 
@UnicID as M1VR01,  'N' as M1EDSP 
 from #TGM_ST01 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 

select distinct DOC_EXT as  MJPNID,'D' as MJEDTY, 0 as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'S1' as MJEDCT, 'ST01' as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX,  
case when CANTIDAD > 0 then 'R' else 'S' end as MJEDER,  
case when CANTIDAD > 0 then 'QR' else 'QS' end as MJPACD, @UnicID as MJVR01,  'N' as MJEDSP 
from #TGM_ST01 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER procedure [dbo].[GM_ST02] as
set nocount on
declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')

select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE,  D.DOC_EXT,
DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET into #TGM_ST02
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
inner join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where tipo_documento_id = 'ST02' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null



declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @UBIC_JDE varchar(20)



DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, UBIC_JDE from #TGM_ST02 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_ST02 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and UBIC_JDE = @UBIC_JDE
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
END

CLOSE dcursor
DEALLOCATE dcursor



update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_ST02 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_ST02 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'S2' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'ST02' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, 
@UnicID as M1VR01, 'N' as M1EDSP 
 from #TGM_ST02 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 


select distinct  DOC_EXT as  MJPNID,'D' as MJEDTY, 0  as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'S2' as MJEDCT, 'ST02' as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX,  
case when CANTIDAD > 0 then 'R' else 'S' end as MJEDER,  
case when CANTIDAD > 0 then 'QR' else 'QS' end as MJPACD, @UnicID as MJVR01, 'N' as MJEDSP 
from #TGM_ST02 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  procedure [dbo].[GM_SUCURSAL_NAVE_FALTANTES]
as
set nocount on
select TIPO_DOCUMENTO_ID, DD.CAT_LOG_ID, DD.NAVE_ID, DD.PROP1, dbo.PADL(substring(D.INFO_ADICIONAL_1,1,12),12,' ') as DEPOSITO_JDE into #Temp
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
where DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null
--and (TIPO_DOCUMENTO_ID in ('I02','I05', 'E01', 'E02','E03', 'E04', 'ST01','ST02','T01','I08' ))
select distinct T.TIPO_DOCUMENTO_ID,  T.CAT_LOG_ID, T.NAVE_ID,  T.DEPOSITO_JDE, UBIC_JDE from #temp t
 left join GM_SUCURSAL_NAVE G on t.CAT_LOG_ID = G.CAT_LOG_ID and  t.NAVE_ID = G.NAVE_ID and T.DEPOSITO_JDE = G.DEPOSITO_JDE
where UBIC_JDE is null
order by 1
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[GM_T01] as
set nocount on


declare @LAST_NBR bigint
declare @UnicID as varchar(20)
set @UnicID = replace(replace(replace(replace(convert(varCHAR(30),getdate(),126),'-',''),':',''),'.',''),'T','')

select 0 as HEADER_NBR, 0 as DETAIL_NBR, D.CLIENTE_ID, 
convert(CHAR(10),D.FECHA_CPTE ,112) FECHA_CPTE,  D.DOC_EXT,
DD.NRO_LINEA, 
DD.PRODUCTO_ID, DD.CANTIDAD * 10000 AS CANTIDAD,  DD.NRO_LOTE, DD.CAT_LOG_ID, DD.NAVE_ID,
DD.UNIDAD_ID, convert(CHAR(10),DD.FECHA_VENCIMIENTO ,112) as FECHA_VENCIENTO, 
G.UBIC_JDE, G.DEPOSITO_JDE, G.ESTADO_LOTE_CD, NRO_PALLET 
into #TGM_T01Prov
from GM_DEV_DOCUMENTO d inner join GM_DEV_DET_DOCUMENTO dd
on d.doc_ext = dd.doc_ext and d.CLIENTE_ID = dd.CLIENTE_ID
left join GM_SUCURSAL_NAVE G on DD.CAT_LOG_ID = G.CAT_LOG_ID and  DD.NAVE_ID = G.NAVE_ID 
where tipo_documento_id = 'T01' 
and DD.cantidad <> 0
and d.ESTADO is null
and dd.ESTADO is null


--select * from  #TGM_T01Prov  where CAT_LOG_ID is null

select * into #TGM_T01 from #TGM_T01Prov A where not exists (select * from #TGM_T01Prov B where A.DOC_EXT = B.DOC_EXT  and A.PRODUCTO_ID = B.PRODUCTO_ID and A.DEPOSITO_JDE = B.DEPOSITO_JDE and A.UBIC_JDE = B.UBIC_JDE and A.ESTADO_LOTE_CD = B.ESTADO_LOTE_CD and A.CANTIDAD = B.CANTIDAD * -1)
declare @DOC_EXT varchar(100)
declare @CLIENTE_ID varchar(15)
declare @NRO_LINEA bigint
declare @NOW datetime

set @NOW = getdate()

declare @curDoc varchar(100)
declare @curCliente varchar(100)
set @curDoc = ''
set @curCliente = ''
declare @headerNbr integer
set @headerNbr = 0
declare @detailsNbr integer
set @detailsNbr = 0
declare @UBIC_JDE varchar(20)

DECLARE dcursor CURSOR FOR select DOC_EXT, CLIENTE_ID, NRO_LINEA, isnull(UBIC_JDE,'') UBIC_JDE from #TGM_T01 order by CLIENTE_ID, DOC_EXT, NRO_LINEA
open dcursor
fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
WHILE @@FETCH_STATUS = 0
BEGIN
   if @curDoc <> @DOC_EXT or @curCliente <> @CLIENTE_ID
   begin
     set @detailsNbr = 1
     update GM_NUMERADOR set @headerNbr = LAST_NBR = LAST_NBR + 1 where NUMERADOR_CD = '47121'
     set @curDoc = @DOC_EXT
     set @curCliente = @CLIENTE_ID
   end
   else 
     set @detailsNbr = @detailsNbr + 1	

   update #TGM_T01 set HEADER_NBR = @headerNbr, DETAIL_NBR = @detailsNbr where DOC_EXT = @DOC_EXT and CLIENTE_ID = @CLIENTE_ID and NRO_LINEA = @NRO_LINEA and isnull(UBIC_JDE,'') = @UBIC_JDE
   fetch next from dcursor into @DOC_EXT, @CLIENTE_ID, @NRO_LINEA, @UBIC_JDE
END

CLOSE dcursor
DEALLOCATE dcursor


update GM_DEV_DOCUMENTO set ESTADO =  @UnicID, FECHA_ESTADO = @NOW from GM_DEV_DOCUMENTO A, #TGM_T01 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID

update GM_DEV_DET_DOCUMENTO set ESTADO = + @UnicID,  FECHA_ESTADO = @NOW from GM_DEV_DET_DOCUMENTO A, #TGM_T01 B
where A.DOC_EXT = B.DOC_EXT and A.CLIENTE_ID = B.CLIENTE_ID and A.NRO_LINEA = B.NRO_LINEA


select distinct DOC_EXT as M1PNID, 'H' as M1EDTY, 1 as M1EDSQ, HEADER_NBR as M1EDOC,'T1' as M1EDCT, CLIENTE_ID as M1EKCO, 'B' as M1EDER,
'T01' as M1EDFT, '860' as M1EDST, 1 as M1EDDL, F.JULIANO as M1EDDT, cast(CLIENTE_ID as integer) as  M1AN8, 
@UnicID as M1VR01, 'N' as M1EDSP 
 from #TGM_T01 A inner join GM_FECHAS F on A.FECHA_CPTE = F.FECHA
order by 1 

select distinct  DOC_EXT as  MJPNID,'D' as MJEDTY, 1  as MJEDSQ, DETAIL_NBR * 1000 as MJEDLN, 
HEADER_NBR as MJEDOC, 'T1' as MJEDCT, 'T01' as MJEDFT, A.CLIENTE_ID as MJEKCO, '860' as MJEDST,
DEPOSITO_JDE as MJMCU, cast(A.CLIENTE_ID as integer) as  MJAN8, 
P.CODIGO_PRODUCTO as MJLITM, P.DESCRIPCION as MJDSC1,
cast(A.PRODUCTO_ID as integer) as MJITM,UBIC_JDE as MJLOCN,  NRO_LOTE as MJLOTN,  ESTADO_LOTE_CD as MJLOTS,
A.UNIDAD_ID as MJTRUM, CANTIDAD as MJTRQT,  G.JULIANO as MJTRDJ, NRO_PALLET as MJTREX,  
case when CANTIDAD > 0 then 'R' else 'S' end as MJEDER,  
case when CANTIDAD > 0 then 'QR' else 'QS' end as MJPACD, @UnicID as MJVR01, 'N' as MJEDSP 
from #TGM_T01 A inner join GM_FECHAS G on A.FECHA_CPTE = G.FECHA
inner join PRODUCTO P on A.PRODUCTO_ID = P.PRODUCTO_ID and A.CLIENTE_ID = P.CLIENTE_ID
order by MJPNID, MJEDOC , MJEDSQ
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

IF @@TRANCOUNT > 0
BEGIN
   IF EXISTS (SELECT * FROM #tmpErrors)
       ROLLBACK TRANSACTION
   ELSE
       COMMIT TRANSACTION
END
GO

DROP TABLE #tmpErrors
GO