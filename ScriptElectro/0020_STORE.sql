USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 04:54 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from DESARROLLO_906 to WMS_ELECTRO_906_MATCH'
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
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Frontera_ControlPicking]
@Viaje_Id as varchar(30) OUTPUT
as
Begin

	SELECT 	p.VIAJE_ID,			
		 	Su.nombre as USOINTERNOUsuario, 
	 		tul.Terminal AS USOINTERNOTerminal, 
			p.PALLET_PICKING,
			p.PRODUCTO_ID,		
			p.DESCRIPCION,
			SUM(ISNULL(p.CANT_CONFIRMADA,0)) AS CANT_CONFIRMADA,
			UM.Descripcion as UMD,
			DD.NRO_LOTE,
			CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +'/' + CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +'/'+CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)) AS FECHA_VENCIMIENTO
	FROM 	PICKING p (nolock)
			inner join producto pr  (nolock) on(p.producto_id=pr.producto_id and p.cliente_id=pr.cliente_id)
			inner join Unidad_Medida UM  (nolock) 
			on(pr.Unidad_id=UM.Unidad_ID)
			inner join det_documento dd  (nolock) on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			,#TEMP_USUARIO_LOGGIN TUL  (nolock) 
			inner join SYS_USUARIO su  (nolock) on (TUL.USUARIO_ID = SU.USUARIO_ID)
	Where	p.VIAJE_ID =@Viaje_Id
	Group by
			p.VIAJE_ID,			
		 	Su.nombre, 
	 		tul.Terminal, 
			p.PALLET_PICKING,
			p.PRODUCTO_ID,		
			p.DESCRIPCION,
			UM.DescripcioN,
			DD.NRO_LOTE,
			DD.FECHA_VENCIMIENTO
	Having	SUM(ISNULL(p.CANT_CONFIRMADA,0))>0
	order by 
			p.VIAJE_ID,p.PALLET_PICKING
End
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

ALTER    PROCEDURE [dbo].[Frontera_delete_ing_egre]
AS
declare @a				as numeric(1) 
BEGIN
     --esta bueno que no se borre esta tabla para no procesar dos veces la misma linea
		--delete from frontera_ing_egr
		 set @a=1 
END
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

ALTER PROCEDURE [dbo].[Frontera_DeleteDocumento]
@documento_id numeric(20,0) output
AS
BEGIN
	delete documento where documento_id=@documento_id
END
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

ALTER       procedure [dbo].[frontera_finalizar_viaje_Station]
@viaje_id	as varchar(100)output
as
begin
	
	declare @doc_trans_id	as numeric(20,0)
	declare @err			as int
	declare @status			as varchar(3)	
	declare @documento_id	as numeric(20,0)
	--declare @fi				as datetime --comentar solo sirve para trazar la ejecucion del proceso.

	declare cur_ffv cursor for
		select 	distinct
				ddt.doc_trans_id
		from 	sys_int_documento sd
				inner join sys_int_det_documento sdd
				--Catalina Castillo.Tracker 4909.Se agrega filtro por CLiente_Id
				on(sd.doc_ext=sdd.doc_ext) and (sd.Cliente_id=sdd.Cliente_id)
				inner join documento d
				on(sdd.documento_id=d.documento_id)
				inner join det_documento dd
				on(d.documento_id=dd.documento_id) 
				inner join det_documento_transaccion ddt
				on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
		where 	codigo_viaje=ltrim(rtrim(upper(@viaje_id)))
				and sdd.documento_id is not null

	open cur_ffv
	--set @fi=getdate()
	fetch next from cur_ffv into @doc_trans_id
	while @@fetch_status=0
	Begin
	
		select	@status=status
		from	documento_transaccion
		where	doc_trans_id=@doc_trans_id

		while @status <>'T40'
		begin
	
			exec egr_aceptar @doc_trans_id
	
			select	@status=status
			from	documento_transaccion
			where	doc_trans_id=@doc_trans_id
		end
		select distinct @documento_id=documento_id from det_documento_transaccion where doc_trans_id=@doc_trans_id
		update picking set facturado='1' where documento_id=@documento_id
		fetch next from cur_ffv into @doc_trans_id
	End

	--select datediff(ms,@fi,getdate())--comentar solo sirve para trazar la ejecucion del proceso.

	close cur_ffv
	deallocate cur_ffv

end
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

ALTER   PROCEDURE [dbo].[Frontera_GetDocumentosaPickear]
@viaje_id 		varchar(100) output
AS
BEGIN
	 select
 		d.*,pv.prioridad as prioridad
	 from sys_int_documento d
     		inner join prioridad_viaje pv on (d.codigo_viaje=pv.viaje_id)
	 where estado_gt is null 
		and codigo_viaje=@viaje_id
	 order by
		info_adicional_1
END
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

ALTER            PROCEDURE [dbo].[Frontera_GetDocumentosEgresos]
AS

BEGIN
	Declare @RolID		as varchar(5)
	Declare @Usuario_id as varchar(30)
	
	Select @RolId=rol_id,@usuario_id=usuario_id from #temp_usuario_loggin

	select 
			d.CODIGO_VIAJE as [PICKING/VIAJE],
			count(distinct d.doc_ext) AS QTY_DOC,
			count(distinct dd.producto_id) AS QTY_PROD,
			sum(dd.cantidad_solicitada) as QTY_CAJAS ,
			cast(pv.prioridad as VARCHAR(20)) as PRIORIDAD_VIAJE ,
			dbo.GetPickerMans(d.CODIGO_VIAJE) AS PICKEADORES,
			c.razon_social,
			c.cliente_id
	from 
			sys_int_documento d (nolock)
			--inner join sys_int_det_documento dd (nolock) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			inner join sys_int_det_documento dd WITH(nolock, index (IDX_SIDD_ESTADOGT)) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			inner join producto p (nolock) on (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.agente_id=s.sucursal_id)
			left  join prioridad_viaje pv (nolock) on (d.codigo_viaje=pv.viaje_id)
			inner join rl_sys_cliente_usuario su (nolock) on(d.cliente_id=su.cliente_id)
			inner join RL_ROL_INT_TIPO_DOCUMENTO rd (nolock) on(d.tipo_documento_id=rd.tipo_documento_id)--agregado SG.
			inner join cliente c on(d.cliente_id=c.cliente_id)
	where 
			d.tipo_documento_id in ('E01','E02','E03','E04','E06','E08')
			and dd.estado_gt is null
			and su.usuario_id=@usuario_id
			and rd.rol_id=@RolId --Agregado SG.
			and d.tipo_documento_id in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R (nolock) where r.rol_id=@RolId)
	GROUP BY 
			d.CODIGO_VIAJE,pv.prioridad,c.razon_social,c.cliente_id
	ORDER BY 
			ISNULL(pv.prioridad,9999999999),d.CODIGO_VIAJE, c.cliente_id

END
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

ALTER      PROCEDURE [dbo].[Frontera_GetEtiquetas]
@Viaje_id varchar(100) output
AS
BEGIN

select
  Distinct (cast(pallet_picking as varchar(100))) as pallet_picking
  ,CASE WHEN st_etiquetas='1' THEN '0' ELSE '1' END as [check]
  ,CASE WHEN st_etiquetas='1' THEN 'SI' ELSE 'NO' END as Etiqueta_Impresa
  ,CASE WHEN pallet_controlado='1' THEN 'SI' ELSE 'NO' END as pallet_controlado
  ,p.usuario_control_pick as usuario_controlador
  ,su.nombre as Nombre_Controlador
  From
  picking p (nolock)
     left join sys_usuario su (nolock) on (p.usuario_control_pick=su.usuario_id)
 Where
 	p.viaje_id=@Viaje_id
	and pallet_picking is not null
 group by pallet_picking, st_etiquetas, pallet_controlado, usuario_control_pick,su.nombre
 having sum(cant_confirmada)>0


END
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

ALTER     PROCEDURE [dbo].[Frontera_GetPedido]
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
 
 SET @XSQL= @XSQL + N'  and d.codigo_viaje=' + CHAR(39) + @viaje_id + CHAR(39) 
 SET @XSQL= @XSQL + N' GROUP BY d.doc_ext,d.agente_id,s.nombre,d.info_adicional_1,d.fecha_cpte'
 SET @XSQL= @XSQL + N' ORDER BY d.info_adicional_1'


 EXECUTE SP_EXECUTESQL @XSQL
END
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

ALTER Procedure [dbo].[Frontera_GetPendientesDevo]
As
Begin
	SELECT 	count(*)
	FROM	#FRONTERA_ING_EGR F
	WHERE	DOCUMENTO_ID NOT IN(SELECT 	DOCUMENTO_ID
								FROM	FRONTERA_ING_EGR F2
								WHERE 	F.DOCUMENTO_ID=F2.DOCUMENTO_ID
										AND F.NRO_LINEA=F2.NRO_LINEA)
End
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

ALTER  PROCEDURE [dbo].[Frontera_GetProductosaPickear]
@doc_ext varchar(100) output
AS
BEGIN
	 select dd.cliente_id
	 ,dd.producto_id
	 ,sum(dd.cantidad_solicitada) as cantidad_solicitada
	 ,p.descripcion producto_descripcion
   ,p.unidad_id as producto_unidad
   ,dd.nro_lote as nro_lote
   ,dd.nro_partida as nro_partida
	,dd.prop3 as nro_serie
	 from sys_int_det_documento dd
		 inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext) 
	         inner join producto p on (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
		 inner join #temp_gproductos_viajes tgp on (d.codigo_viaje=tgp.viaje_id and p.grupo_producto=tgp.grupo_producto_id)
	 WHERE 
		dd.DOC_EXT=@doc_ext
		and dd.estado_gt is null
	 GROUP BY
   dd.cliente_id, dd.producto_id, p.descripcion, p.unidad_id, dd.nro_lote, dd.nro_partida,dd.prop3

END
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

ALTER     PROCEDURE [dbo].[Frontera_GetTareasPickingTomadas]
@viaje_id 		varchar(100) output,
@tipo			int	     output	
AS
BEGIN
--Tareas en Curso
if @tipo=1 begin	 
	select	'0' as [check]
			,p.usuario
			,su.nombre
			,p.producto_id
			,p.nro_lote
			,p.nro_partida
			,p.nro_serie
			,p.descripcion
			,p.cantidad as qty_pick
			,p.posicion_cod
			,p.fecha_inicio
			,p.pallet_picking
			,p.tipo_caja
			,p.ruta
			,p.prop1 as pallet_pick
			,salto_picking
			,picking_id
			,dd.prop2
			,dd.nro_bulto
	from	picking p (nolock) inner join det_documento dd (nolock)
			on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			inner join sys_usuario su (nolock) on (p.usuario=su.usuario_id)
	where 	p.usuario is not null
			and p.fecha_fin is null
			and p.viaje_id=@viaje_id
			and flg_pallet_hombre=transf_terminada
end --if

--Tareas en Pendientes
if @tipo=2 begin	 
	select	'0' as [CHECK] 
			,p.producto_id
			,p.nro_lote
			,p.nro_partida
			,p.nro_serie
			,p.descripcion
			,p.cantidad as qty_pick
			,p.posicion_cod
			,p.tipo_caja
			,p.ruta
			,p.prop1 as pallet_pick
			,p.salto_picking
			,p.picking_id
			,dd.prop2
			,dd.nro_bulto
	from	picking p(nolock) inner join det_documento dd (nolock)
			on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
	where 	usuario is null
			and fecha_inicio is null
			and fecha_fin is null
			and viaje_id=@viaje_id
			and flg_pallet_hombre=transf_terminada
end --if

--Tareas Finalizadas
if @tipo=3 begin	 
	select 	'0' as [CHECK] 
			,p.usuario
			,su.nombre	
			,p.producto_id
			,p.nro_lote
			,p.nro_partida
			,p.nro_serie
			,p.descripcion
			,p.posicion_cod	
			,p.cantidad
			,p.cant_confirmada
			,p.cant_confirmada-p.cantidad as dif
			,p.fecha_inicio
			,p.fecha_fin
			,p.pallet_picking
			,p.tipo_caja
			,p.ruta
			,p.prop1 as pallet_pick
			,p.salto_picking
			,picking_id
			,dd.prop2
			,dd.nro_bulto
	from	picking p (nolock)inner join det_documento dd
			on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
			inner join sys_usuario su (nolock) on (p.usuario=su.usuario_id)	
	where 	p.usuario is not null
			and p.fecha_inicio is not null
			and p.fecha_fin is not null
			and p.viaje_id=@viaje_id
			and flg_pallet_hombre=transf_terminada
end --if

END
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

ALTER   PROCEDURE [dbo].[Frontera_GetUsuTareas]
@pViaje_id	varchar(100) output
AS

BEGIN
 select '0' as [check],su.usuario_id as COD_USUARIO,su.nombre AS USUARIO
 from 
	picking p (nolock)
	inner join sys_usuario su (nolock) on (p.usuario=su.usuario_id)
 Where
 	p.viaje_id=@pViaje_id
	and p.fecha_fin is null
	and p.fecha_inicio is not null
	and p.usuario is not null
	and p.cant_confirmada is null
 group by su.usuario_id,su.nombre
END --PROCEDURE
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

ALTER    PROCEDURE [dbo].[Frontera_GrupoProductos]
@viaje_id varchar(100) OUTPUT
AS

BEGIN

select 
'1' as [check],p.grupo_producto,tp.descripcion,count(p.producto_id) as qty_productos,sum(dd.cantidad_solicitada) as qty_cajas

from sys_int_documento d (nolock)
	inner join sys_int_det_documento dd (nolock) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
	inner join producto p (nolock) on (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
	left join tipo_producto tp (nolock) on (p.grupo_producto=tp.tipo_producto_id)
where
	codigo_viaje=@viaje_id
	and dd.estado_gt is null
group by p.grupo_producto,tp.descripcion

END
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

ALTER                PROCEDURE [dbo].[Frontera_IngresoxEgreso]
	@VIAJEID  	AS VARCHAR(30) 	output,
	@PEDIDO		AS VARCHAR(100)	output
AS
BEGIN
	SELECT 	dd.producto_id,
			dd.descripcion,
			sum(pic.cant_confirmada)as cantidad,
			'' as cant,
			'' as motivo,
			'' as observacion,
			dd.unidad_id,
			s.nombre,
			d.nro_remito,
			d.sucursal_destino,
			dd.nro_bulto,
			pic.nro_lote,
			pic.nro_partida,
			pic.nro_serie,
			dd.prop1,
			dd.prop2,
			dd.prop3,
			CONVERT(VARCHAR(23),dd.fecha_vencimiento,103) as fecha_vencimiento,
			dd.documento_id,	
			dd.nro_linea,
			'' AS motivo_id,
			p.Fraccionable
	FROM	vdocumento d (nolock)
			inner join vdet_documento dd (nolock) on (d.documento_id=dd.documento_id) 
			left join sucursal s (nolock) on (s.cliente_id=d.cliente_id and s.sucursal_id = d.sucursal_destino)
			inner join producto p (nolock) on(p.cliente_id=dd.cliente_id and p.producto_id=dd.producto_id)
			inner join picking pic(nolock) on(dd.documento_id=pic.documento_id and dd.nro_linea=pic.nro_linea)
	WHERE 	d.nro_despacho_importacion= @VIAJEID
			and ((@pedido is null) or (d.nro_remito like '%'+ @pedido + '%'))
			and d.tipo_operacion_id='EGR'
	      	and STR( dd.nro_linea)+STR(dd.documento_id) NOT IN (	select 	STR(f.nro_linea)+STR(f.documento_id) 
		  														  	from 	#frontera_ing_egr f (nolock) 
																	WHERE 	f.nro_linea = dd.nro_linea  
																			AND f.documento_id=dd.documento_id)	
			and p.envase='0'

	GROUP BY 
			d.documento_id,
			dd.nro_linea,
			dd.producto_id,
			dd.descripcion,
			dd.unidad_id,
			s.nombre,
			d.nro_remito,
			d.sucursal_destino,
			dd.nro_bulto,
			pic.nro_lote,
			pic.nro_serie,
			pic.nro_partida,
			dd.prop1,
			dd.prop2,
			dd.prop3,
			dd.fecha_vencimiento,
			dd.documento_id,	
			dd.nro_linea,
			p.Fraccionable
	HAVING	sum(pic.cant_confirmada)>0
	order by
			DD.producto_id, pic.nro_lote, pic.nro_partida,pic.nro_serie
END
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

ALTER     PROCEDURE [dbo].[Frontera_Insert_ing_egre]
@documento_id	numeric(20,0)  output,
@nro_linea	numeric(10,0)  output	
AS
BEGIN
     insert into #frontera_ing_egr values(@documento_id,@nro_linea)
END
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

ALTER   PROCEDURE [dbo].[Frontera_Insert_temp_gproductos_viajes]
@viaje_id 		varchar(100) output,
@grupo_producto_id	varchar(10)  output	
AS
BEGIN
	insert into #temp_gproductos_viajes values(@viaje_id,@grupo_producto_id)
END
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

ALTER   PROCEDURE [dbo].[Frontera_LiberarTareaPicking]
@pViaje_id     varchar(100) output,
@pUsuario_id    varchar(100) output
AS
BEGIN
	update picking set 
		usuario=null,
		fecha_inicio=null,
		fecha_fin=null,
		cant_confirmada=null,
		pallet_picking=null 
	where 
		viaje_id=@pViaje_id and usuario=@pUsuario_id
		and cant_confirmada is null

END --PROCEDURE
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

ALTER    PROCEDURE [dbo].[Frontera_NVLCumpl_Report]
@Viaje_Id 		varchar(100) output,
@iVista	 		numeric(1,0) output

AS
BEGIN
	Declare 	@Usuario			as varchar(30)
	Declare @Terminal			as varchar(100)

	Select 	@Usuario=Usuario_id From #Temp_Usuario_Loggin
	Select  	@Terminal=Host_Name()
	
	IF @iVista = 1 
		BEGIN

			SELECT	distinct dd.producto_id
				,p.descripcion AS descripcion_producto
				,@Viaje_Id AS Cod_Viaje
				,X.NRO_REMITO AS NRO_PEDIDO
				,X.SUCURSAL_ID AS COD_SUC
				,X.NOMBRE AS RAZON_SOCIAL
				,@Usuario AS Usuario
				,@Terminal AS Terminal
				,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'1') AS PEDIDO
				,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'2') AS ASIGNADO
				,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'4') AS PICKEADO
				,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'3') AS JDE
			FROM 	sys_int_det_documento dd (nolock)
				INNER JOIN sys_int_documento d (nolock) ON (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext) 
				INNER JOIN producto p (nolock) ON (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
				,
				(SELECT  DISTINCT
					 D.NRO_REMITO 
					,S.SUCURSAL_ID 
					,S.NOMBRE 
					,D.DOCUMENTO_ID
				FROM 	PICKING P (nolock) 
					INNER JOIN DOCUMENTO D (nolock) ON (P.DOCUMENTO_ID = D.DOCUMENTO_ID) 
					INNER JOIN SUCURSAL S (nolock) ON (D.CLIENTE_ID = S.CLIENTE_ID AND D.SUCURSAL_DESTINO =  S.SUCURSAL_ID )	 
				WHERE 	P.VIAJE_ID = @Viaje_Id 
				GROUP BY D.NRO_REMITO,S.SUCURSAL_ID, S.NOMBRE, D.DOCUMENTO_ID
				) X 
			WHERE	d.codigo_viaje=  @Viaje_Id AND DD.DOCUMENTO_ID=X.DOCUMENTO_ID
			ORDER BY X.NRO_REMITO 

		END 
	ELSE
		BEGIN
			IF  @iVista = 2 
				BEGIN

					SELECT	distinct dd.producto_id
						,p.descripcion AS descripcion_producto
						,@Viaje_Id AS Cod_Viaje
						,X.NRO_REMITO AS NRO_PEDIDO
						,X.SUCURSAL_ID AS COD_SUC
						,X.NOMBRE AS RAZON_SOCIAL
						,@Usuario AS Usuario
						,@Terminal AS Terminal
						,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'1') AS PEDIDO
						,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'2') AS ASIGNADO
						,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'4') AS PICKEADO
						,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'3') AS JDE
					FROM 	sys_int_det_documento dd (nolock)
						INNER JOIN sys_int_documento d (nolock) ON (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext) 
						INNER JOIN producto p (nolock) ON (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
						,
						(SELECT  DISTINCT
							 D.NRO_REMITO 
							,S.SUCURSAL_ID 
							,S.NOMBRE 
							,D.DOCUMENTO_ID
						FROM 	PICKING P (nolock)
							INNER JOIN DOCUMENTO D (nolock) ON (P.DOCUMENTO_ID = D.DOCUMENTO_ID) 
							INNER JOIN SUCURSAL S (nolock) ON (D.CLIENTE_ID = S.CLIENTE_ID AND D.SUCURSAL_DESTINO =  S.SUCURSAL_ID )	 
						WHERE 	P.VIAJE_ID = @Viaje_Id 
						GROUP BY D.NRO_REMITO,S.SUCURSAL_ID, S.NOMBRE, D.DOCUMENTO_ID
						) X 
					WHERE	d.codigo_viaje=  @Viaje_Id AND DD.DOCUMENTO_ID=X.DOCUMENTO_ID
						AND (dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'1') - dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'2') > 0)
					ORDER BY X.NRO_REMITO 

				END 
			ELSE
				BEGIN					
					IF @iVista = 3 
						BEGIN

							SELECT	distinct dd.producto_id
								,p.descripcion AS descripcion_producto
								,@Viaje_Id AS Cod_Viaje
								,X.NRO_REMITO AS NRO_PEDIDO
								,X.SUCURSAL_ID AS COD_SUC
								,X.NOMBRE AS RAZON_SOCIAL
								,@Usuario AS Usuario
								,@Terminal AS Terminal
								,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'1') AS PEDIDO
								,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'2') AS ASIGNADO
								,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'4') AS PICKEADO
								,Dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'3') AS JDE
							FROM 	sys_int_det_documento dd (nolock)
								INNER JOIN sys_int_documento d (nolock) ON (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext) 
								INNER JOIN producto p (nolock) ON (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
								,
								(SELECT  DISTINCT
									 D.NRO_REMITO 
									,S.SUCURSAL_ID 
									,S.NOMBRE 
									,D.DOCUMENTO_ID
								FROM 	PICKING P (nolock) 
									INNER JOIN DOCUMENTO D (nolock) ON (P.DOCUMENTO_ID = D.DOCUMENTO_ID) 
									INNER JOIN SUCURSAL S (nolock) ON (D.CLIENTE_ID = S.CLIENTE_ID AND D.SUCURSAL_DESTINO =  S.SUCURSAL_ID )	 
								WHERE 	P.VIAJE_ID = @Viaje_Id 
								GROUP BY D.NRO_REMITO,S.SUCURSAL_ID, S.NOMBRE, D.DOCUMENTO_ID
								) X 
							WHERE	d.codigo_viaje=  @Viaje_Id AND DD.DOCUMENTO_ID=X.DOCUMENTO_ID
								AND dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'2') - dbo.QTY_DIFPICKING_NVLCumpl(d.codigo_viaje,d.cliente_id,dd.producto_id,X.NRO_REMITO,'4') > 0
							ORDER BY X.NRO_REMITO 

						END 
				END
		END
END
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

ALTER  PROCEDURE [dbo].[Frontera_Pesiste_Datos]
@Viaje	as varchar(100) Output
As
Begin

		INSERT INTO FRONTERA_ING_EGR
		SELECT 	DISTINCT
				DOCUMENTO_ID, NRO_LINEA
		FROM	#FRONTERA_ING_EGR F
		WHERE	DOCUMENTO_ID NOT IN(SELECT 	DOCUMENTO_ID
									FROM	FRONTERA_ING_EGR F2
									WHERE 	F.DOCUMENTO_ID=F2.DOCUMENTO_ID
											AND F.NRO_LINEA=F2.NRO_LINEA)
		
		TRUNCATE TABLE #FRONTERA_ING_EGR
End
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

ALTER  PROCEDURE [dbo].[Frontera_QuitarSaltoPicking]
@pViaje_id		varchar(100) output,
@pSalto_Picking		varchar(30) output
AS
BEGIN
	update picking set salto_picking=0 where viaje_id=@pViaje_id and salto_picking=@pSalto_Picking

END --PROCEDURE
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

ALTER   PROCEDURE [dbo].[Frontera_Truncate_temp_gproductos_viajes]
AS

BEGIN

	truncate table #temp_gproductos_viajes

END
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

ALTER    PROCEDURE [dbo].[Frontera_UpdateQtyPickeada]
@pPicking_id     varchar(100) output,
@pCantidad	 Numeric(10,5) output

AS
BEGIN
   update picking set cant_confirmada=@pCantidad where picking_id=@pPicking_id

END --PROCEDURE
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

ALTER  PROCEDURE [dbo].[Frontera_ValidarTratamientoViaje]
@pViaje_id			as varchar(100) Output
AS

BEGIN

select distinct  
isnull((SELECT transaccion_id 
						From rl_producto_tratamiento 
						Where cliente_id=dd.cliente_id 
							AND TIPO_OPERACION_ID='EGR' 
							AND TIPO_COMPROBANTE_ID=D.tipo_documento_id 
							AND producto_id=dd.producto_id)
			,p.egreso)
from sys_int_det_documento dd
	  inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext)
	  inner join producto p on (dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
where 
	d.codigo_viaje=@pViaje_id

END
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

ALTER  PROCEDURE [dbo].[Frontera_ValidarViaje]
@pViaje_id			as varchar(100) Output
AS

BEGIN

	select usuario,terminal,fecha, count(viaje_id) as proceso from docxviajesprocesados where viaje_id=@pViaje_id and documento_id=0 and status='I' group by usuario,terminal,fecha 

END
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

ALTER   Procedure [dbo].[Frontera_Viaje_Proceso]
@pViaje_id			as varchar(100) Output,
@pAccion				as varchar(2) output
As
Begin
declare @vUsuario_id		as varchar(50)
declare @vTerminal		as varchar(50)

select @vUsuario_id=usuario_id, @vTerminal=Terminal from #temp_usuario_loggin

if (@pAccion='I') begin --Reserva Viaje para que otro usuario no pueda correrlo	
	insert into docxviajesprocesados values (@pViaje_id,0,'I',getdate(),@vUsuario_id,@vTerminal)
end --if

if (@pAccion='L') begin --Libera Viaje 	
	delete docxviajesprocesados where viaje_id=@pViaje_id and documento_id=0 and status='I'
end --if

end --end Procedure
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