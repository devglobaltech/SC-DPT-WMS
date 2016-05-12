
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 05:04 p.m.
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
SET QUOTED_IDENTIFIER ON
GO

ALTER         PROCEDURE [dbo].[SYS_DEV_I08]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
set xact_abort on
BEGIN

	select @qty=count(*) from sys_dev_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext

	BEGIN 
	
		IF @qty=0
			BEGIN 	
				INSERT INTO sys_dev_documento
				SELECT 
					 d.CLIENTE_ID
					,'I08'
					,d.CPTE_PREFIJO 
					,d.CPTE_NUMERO 
					,d.FECHA_CPTE
					,GetDate()
					,NULL
					,d.PESO_TOTAL
					,d.UNIDAD_PESO
					,d.VOLUMEN_TOTAL
					,d.UNIDAD_VOLUMEN
					,d.TOTAL_BULTOS
					,d.ORDEN_DE_COMPRA
					,d.OBSERVACIONES
					,d.NRO_REMITO
					,d.NRO_DESPACHO_IMPORTACION
					,'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))
					,NULL
					,NULL
					,NULL
					,NULL
					,d.TIPO_COMPROBANTE_ID
					,NULL
					,NULL
					,'P' --ESTADO_GT
					,GETDATE() 
					,null	--Flg_movimiento
				FROM documento d (nolock)
				WHERE d.documento_id = @documento_id
			END
	
			INSERT INTO sys_dev_det_documento
			SELECT 
				 'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))
				,isnull(@nro_lin,0) + dd.NRO_LINEA
				,d.CLIENTE_ID 
				,dd.PRODUCTO_ID
				,dd.CANTIDAD					--sidd.CANTIDAD_SOLICITADA
				,dd.CANTIDAD
				,dd.EST_MERC_ID
				,dd.CAT_LOG_ID_FINAL
				,dbo.Get_data_I08(dd.documento_id,dd.nro_linea,'3')
				,dd.DESCRIPCION
				,dd.NRO_LOTE
				,dd.PROP1 AS NRO_PALLET --NRO_PALLET 
				,dd.FECHA_VENCIMIENTO
				,dd.NRO_DESPACHO
				,dd.NRO_PARTIDA
				,dd.UNIDAD_ID 			--sidd.UNIDAD_ID
				,NULL					--sidd.UNIDAD_CONTENEDORA_ID
				,dd.PESO				--sidd.PESO
				,dd.UNIDAD_PESO 		--sidd.UNIDAD_PESO
				,dd.VOLUMEN				--sidd.VOLUMEN
				,dd.UNIDAD_VOLUMEN		--sidd.UNIDAD_VOLUMEN
				,dd.PROP1				--sidd.PROP1
				,dd.prop2
				,dd.prop3
				,dd.LARGO				--sidd.LARGO
				,dd.ALTO 				--sidd.ALTO 
				,dd.ANCHO				--sidd.ANCHO
				,dd.TRACE_BACK_ORDER	--sidd.DOC_BACK_ORDER
				,NULL
				,NULL
				,'P'
				,GETDATE()
				,DD.DOCUMENTO_ID
				,dbo.get_nave_id(dd.documento_id,dd.nro_linea)
				,dbo.get_nave_cod(dd.documento_id,dd.nro_linea)
				,null 	--Flg_movimiento 	
			FROM documento d (nolock)
				inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			WHERE dd.documento_id=@documento_id

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
SET QUOTED_IDENTIFIER OFF
GO

ALTER     PROCEDURE [dbo].[SYS_DEV_I07]
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
	select 
	d.CLIENTE_ID, 
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
	NULL --flg_movimiento
	from documento d 
	where d.documento_id=@documento_id	

	
	insert into sys_dev_det_documento
	select 
	'IM' + CAST(dd.DOCUMENTO_ID AS varchar(100)),
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
	NULL --flg_movimiento
	from det_documento dd
	where dd.documento_id=@documento_id
end
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

ALTER          PROCEDURE [dbo].[SYS_DEV]
@documento_id AS NUMERIC(20,0) output,
@estado	as numeric(2,0) output
AS
DECLARE @doc_Ext AS varchar(100)
DECLARE @td AS varchar(20)
DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
DECLARE @tc AS varchar(15)
DECLARE @status AS varchar(5)
BEGIN
	select @doc_ext=nro_despacho_importacion,@tc=tipo_comprobante_id,@status=status from documento where documento_id=@documento_id
	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @td=tipo_documento_id from sys_int_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
	
IF (@doc_ext <> '' and @doc_ext is not null and @status='D40')
BEGIN
	
	IF (@td='I01' and @estado=1 and @tc='DO')
	BEGIN
	     	 exec sys_dev_I01
		 @doc_ext=@doc_ext
		,@estado=1 
                ,@documento_id=@documento_id
	END --IF

	IF (@td='I01' and @estado=3 and @tc='DO')
	BEGIN
	     	 exec sys_dev_I03
		 @doc_ext=@doc_ext
		,@estado=1 
                ,@documento_id=@documento_id
	END --IF

	IF (@td='I04' and @estado=1 and @tc='PP')
	BEGIN
	     	 exec sys_dev_I04
		 @doc_ext=@doc_ext
		,@estado=1 
                ,@documento_id=@documento_id
	END --IF

	IF (@td is null and @estado=1 and @tc='DE')
	BEGIN
	     	 exec    sys_dev_I08
					 @doc_ext=@doc_ext
					,@estado=@estado 
			        ,@documento_id=@documento_id
	END --IF
	
	SELECT @TD AS [TD]
	SELECT @ESTADO AS [ESTADO]
	SELECT @TC AS [TC]

	IF (@td is null and @estado=1 and @tc='IM')
	BEGIN
	     	 exec sys_dev_I07
		 @doc_ext=@doc_ext
		,@estado=@estado 
                ,@documento_id=@documento_id
	END --IF

END --IF

IF (@td='I04' and @estado=2 and @tc='PP' and @status='D30')
	--Anula el pallet y genera un I06
    BEGIN
	     exec sys_dev_I04_D
		 @doc_ext=@doc_ext
		,@estado=2 
        ,@documento_id=@documento_id
	END --IF

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

ALTER     PROCEDURE [dbo].[SYS_DEV_I01_BULTOS]
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
	
	if @qty=0
      	   BEGIN 	
		insert into sys_dev_documento
		select top 1
		sid.CLIENTE_ID, 
		'I02', 
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
		NULL	--flg_movimiento	
		from sys_int_documento sid
			inner join documento d on (sid.cliente_id=d.cliente_id)
			inner join det_documento dd on(d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2)
		where sid.doc_ext=@doc_ext
	END
	
	insert into sys_dev_det_documento
	select distinct
	sidd.DOC_EXT, 
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
	Isnull(sidd.PROP3,dd.nro_serie), 
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
	NULL	--flg_movimiento	
	from sys_int_documento sid
		inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)
		inner join documento d on (sid.cliente_id=d.cliente_id and sidd.documento_id=d.documento_id)
		inner join det_documento dd on (d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2 and sidd.producto_id = dd.producto_id)
	where sid.doc_ext=@doc_ext 
		and sidd.estado_gt is not null 
		and dd.documento_id=@documento_id
		and sidd.documento_id=@documento_id
end
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

ALTER          PROCEDURE [dbo].[SYS_DEV_BULTOS]
@documento_id AS NUMERIC(20,0) output,
@estado	as numeric(2,0) output
AS
DECLARE @doc_Ext AS varchar(100)
DECLARE @td AS varchar(20)
DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
DECLARE @tc AS varchar(15)
DECLARE @status AS varchar(5)
DECLARE @Cur as cursor
BEGIN
	

	SET @CUR = CURSOR FOR
		select	 isnull(prop2,d.documento_id)
				,tipo_comprobante_id
				,status 
		from	documento d inner join det_documento dd
				on(d.documento_id=dd.documento_id)
		where	d.documento_id=@documento_id
	OPEN @CUR 
	FETCH NEXT FROM @CUR INTO @doc_ext, @tc,@status
	While @@Fetch_Status=0
	begin
		select	@qty=count(*) 
		from	sys_dev_documento 
		where	doc_ext=@doc_ext

		select	@td=tipo_documento_id 
		from	sys_int_documento 
		where	doc_ext=@doc_ext

		select	@nro_lin=max(nro_linea) 
		from	sys_dev_det_documento 
		where doc_ext=@doc_ext
		
		IF (@doc_ext <> '' and @doc_ext is not null and @status='D40')
		BEGIN
			
			IF (@td='I01' and @estado=1 and @tc='DO')
			BEGIN
				 exec SYS_DEV_I01_BULTOS
				 @doc_ext=@doc_ext
				,@estado=1 
				,@documento_id=@documento_id
			END --IF

			IF (@td='I01' and @estado=3 and @tc='DO')
			BEGIN
	     			 exec sys_dev_I03
				 @doc_ext=@doc_ext
				,@estado=1 
						,@documento_id=@documento_id
			END --IF

			IF (@td='I04' and @estado=1 and @tc='PP')
			BEGIN
	     			 exec sys_dev_I04
				 @doc_ext=@doc_ext
				,@estado=1 
						,@documento_id=@documento_id
			END --IF

			IF (@td is null and @estado=1 and @tc='DE')
			BEGIN
	     			exec    sys_dev_I08
							 @doc_ext=@doc_ext
							,@estado=@estado 
							,@documento_id=@documento_id
					break;
			END --IF


			IF (@td is null and @estado=1 and @tc='IM')
			BEGIN
	     			 exec sys_dev_I07
				 @doc_ext=@doc_ext
				,@estado=@estado 
						,@documento_id=@documento_id
			END --IF

		END --IF
		FETCH NEXT FROM @CUR INTO @doc_ext, @tc,@status	
	End
	close @cur
	deallocate @cur
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

ALTER  Procedure [dbo].[Sys_Dev_EgresoE10]
	@pviaje AS varchar(100) output
As
Begin
	Declare @Usuario	as Varchar(30)

	insert into sys_dev_documento
	select
	distinct 
	sid.CLIENTE_ID, 
	CASE WHEN sid.tipo_documento_id='E04' THEN 'E05' WHEN sid.tipo_documento_id='E08' THEN 'E09' ELSE sid.tipo_documento_id END, 
	sid.CPTE_PREFIJO, 
	sid.CPTE_NUMERO, 
	getdate(), --FECHA_CPTE, 
	sid.FECHA_SOLICITUD_CPTE, 
	sid.AGENTE_ID, 
	sid.PESO_TOTAL, 
	sid.UNIDAD_PESO, 
	sid.VOLUMEN_TOTAL, 
	sid.UNIDAD_VOLUMEN, 
	sid.TOTAL_BULTOS, 
	sid.ORDEN_DE_COMPRA, 
	sid.OBSERVACIONES, 
	cast(d.cpte_prefijo as varchar(20)) + cast(d.cpte_numero  as varchar(20)), 
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
	Null --Flg_Movimiento
	from sys_int_documento sid
		left join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
	where sid.codigo_viaje=@pViaje


	insert into sys_dev_det_documento
	select	 d.nro_remito as doc_ext
			,(p.picking_id) as nro_linea
			,dd.cliente_id
			,dd.producto_id
			,dd.cant_solicitada
			,p.cant_confirmada
			,dd.est_merc_id
			,dd.cat_log_id_final
			,null as nro_bulto
			,dd.descripcion
			,dd.nro_lote
			,dd.prop1 as nro_pallet
			,dd.fecha_vencimiento
			,null as nro_despacho
			,dd.nro_partida
			,unidad_id
			,null as unidad_contenedora_id
			,null as peso
			,null as unidad_peso
			,null as volumen
			,null as unidad_volumen
			,Case  When D.Tipo_Comprobante_ID='E10'
				then  	DBO.GetValuesSysIntIA1(d.Documento_id, dd.Nro_linea) 
				Else 	dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1)
			 End As prop1
			,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,2) as prop2
			,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,3) as prop3
			,null as largo
			,null as alto
			,dd.nro_linea as ancho --nro de linea
			,null as doc_back_order
			,null as estado
			,null as fecha_estado
			,'P' as estado_gt
			,getdate() as fecha_estado_gt
			,p.documento_id
			,dbo.Aj_NaveCod_to_Nave_id(p.nave_cod) as nave_id
			,p.nave_cod	
			,Null --Flg_movimiento
	from 	det_documento dd
			inner join documento d on (dd.documento_id=d.documento_id)
			inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
	where
			p.Viaje_id=@pViaje

	Select @Usuario=Usuario_id from #Temp_Usuario_Loggin
	
	update 	picking 
		set 	facturado='1',
			fecha_control_Fac=Getdate(),
			Usuario_Control_fac=@Usuario,
			Terminal_Control_Fac=Host_Name()
	where 	viaje_id=@pViaje

End --Fin Procedure
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

ALTER                         PROCEDURE [dbo].[SYS_DEV_EGRESO]
 @pviaje AS varchar(100) output
AS
	declare @Qty as numeric(10,0)
	declare @ErrorSave int
	declare @AuxNroLinea bigint
	declare @ControlExpedicion char(1)
	declare @TipoComp	as varchar(5)
	declare @Usuario 	as varchar(20)
	declare @count		as smallint
	declare @controla	as char(1)
BEGIN
	begin try

		IF EXISTS (SELECT 1 FROM SYS_DEV_DOCUMENTO WHERE CODIGO_VIAJE = @pviaje)
			RETURN
		--Controlo que el viaje no este cerrado
		select @Qty=count(picking_id) from picking where viaje_id=@pViaje and facturado='1'
		if (@Qty>0) begin
			RAISERROR('El Picking/Viaje ya fue Cerrado!!!!',16,1)
			RETURN 
		end --if

		--Controlo que el viaje tenga todos los picking's cerrados
		set @Qty=0
		select @Qty=count(picking_id) from picking where (fin_picking in ('0','1') or fin_picking is null) and viaje_id=@pViaje
		if (@Qty>0) begin
			RAISERROR('Aun quedan Productos Pendientes por Pickear!!!!',16,1)
			RETURN 	
		end --if

		select	@Controla=isnull(flg_control_picking,'0')
		from	cliente_parametros 
		where	cliente_id=(select distinct cliente_id from picking(nolock) where viaje_id=@pviaje)
		if @controla='1'
		Begin
			SELECT	Distinct
					@count=count(pallet_controlado)
			From	picking p (nolock)
			Where 	P.viaje_id=@pviaje
					And pallet_controlado='0'
			if @count>0
			begin
				raiserror('Aun quedan pallets de picking por controlar',16,1)
				return
			end
		end

		---------------------------------------------------------------------------------------------------------------------
		--Controlo que el viaje este en el camion
		---------------------------------------------------------------------------------------------------------------------
		SELECT @TipoComp=TIPO_DOCUMENTO_ID FROM SYS_INT_DOCUMENTO WHERE CODIGO_VIAJE=@pviaje
		if @TipoComP ='E04'
		Begin

			select 	distinct 
					@ControlExpedicion=isnull(control_expedicion,'0')
			from 	documento d inner join tipo_comprobante tc
					on(d.tipo_comprobante_id=tc.tipo_comprobante_id)
			where	nro_despacho_importacion=ltrim(rtrim(Upper(@pViaje)))

		End
		Else
		Begin
			select @ControlExpedicion=control_expedicion from tipo_comprobante where tipo_comprobante_id=@TipoComp
		End


		set @Qty=0
		--control de expedicion parametrizable.
		SET @Controla=null

		Select	@Controla=isnull(c.flg_control_exp,'0')
		from	picking p inner join cliente_parametros c
				on(p.cliente_id=c.cliente_id)
		where	viaje_id=ltrim(rtrim(upper(@pViaje))) and st_control_exp='0'

		if @controla='1'
		begin
			Select @Qty=count(st_control_exp) from picking where viaje_id=ltrim(rtrim(upper(@pViaje))) and st_control_exp='0'
			if (@Qty>0 and @ControlExpedicion='1') begin
				RAISERROR('Aun quedan Pallets Pendientes de Cargar a Camion!!!!',16,1)
				RETURN 	
			end --if
		end
		--Controlo que no queden en sys_int_det_documento productos pendientes 
		set @Qty=0
		select @Qty=count(dd.doc_ext) 
		from sys_int_det_documento dd 
			inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext)		
			inner join producto prod on (dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id)
		where dd.estado_gt is null and d.codigo_viaje=@pViaje
		if (@Qty>0) begin
			RAISERROR('El Picking/Viaje aun Tiene Productos Pendientes por Procesar!!!!',16,1)
			RETURN 	
		end --if

		If Dbo.GetTipoDocumento(@pViaje)='E10'
		Begin
			Exec Dbo.Sys_Dev_EgresoE10 @pViaje
			Return
		End
		   insert into sys_dev_documento
			select
			distinct 
			sid.CLIENTE_ID, 
			CASE WHEN sid.tipo_documento_id='E04' THEN 'E05' WHEN sid.tipo_documento_id='E08' THEN 'E09' ELSE sid.tipo_documento_id END, 
			sid.CPTE_PREFIJO, 
			sid.CPTE_NUMERO, 
			getdate(), --FECHA_CPTE, 
			sid.FECHA_SOLICITUD_CPTE, 
			sid.AGENTE_ID, 
			sid.PESO_TOTAL, 
			sid.UNIDAD_PESO, 
			sid.VOLUMEN_TOTAL, 
			sid.UNIDAD_VOLUMEN, 
			sid.TOTAL_BULTOS, 
			sid.ORDEN_DE_COMPRA, 
			sid.OBSERVACIONES, 
			cast(d.cpte_prefijo as varchar(20)) + cast(d.cpte_numero  as varchar(20)), 
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
			Null --Flg_Movimiento 
			from	sys_int_documento sid
					left join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
			where	sid.codigo_viaje=@pViaje
					and not exists (select	1 
									from	sys_dev_documento sd 
									where	sd.cliente_id=sid.cliente_id
											and sd.doc_ext=sid.doc_ext)
			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Documento, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END

			insert into sys_dev_det_documento
			select	 d.nro_remito as doc_ext
					,(p.picking_id) as nro_linea
					,dd.cliente_id
					,dd.producto_id
					,dd.cant_solicitada
					,p.cant_confirmada
					,dd.est_merc_id
					,dd.cat_log_id_final
					,null as nro_bulto
					,dd.descripcion
					,dd.nro_lote
					,dd.prop1 as nro_pallet
					,dd.fecha_vencimiento
					,null as nro_despacho
					,dd.nro_partida
					,unidad_id
					,null as unidad_contenedora_id
					,null as peso
					,null as unidad_peso
					,null as volumen
					,null as unidad_volumen
					,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1) as prop1
					,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,2) as prop2
					,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,3) as prop3
					,null as largo
					,null as alto
					,dd.nro_linea as ancho --nro de linea
					,null as doc_back_order
					,null as estado
					,null as fecha_estado
					,'P' as estado_gt
					,getdate() as fecha_estado_gt
					,p.documento_id
					,dbo.Aj_NaveCod_to_Nave_id(p.nave_cod) as nave_id
					,p.nave_cod	
					,Null		--Flg_Movimiento
			from 	det_documento dd
					inner join documento d on (dd.documento_id=d.documento_id)
					inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
					--JOIN AGREGADO PORQUE TREA MAS REGISTROS DADO QUE EXISTE OTRO DOCUMENTO EN LA TABLA DOCUMENTO
					--QUE TIENE EL MISMO 
					INNER JOIN sys_int_documento sid ON (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
			where
					p.Viaje_id=@pViaje
					and not exists (select 1 from sys_dev_det_documento where sys_dev_det_documento.cliente_id = dd.cliente_id and sys_dev_det_documento.doc_Ext = d.nro_remito and sys_dev_det_documento.nro_linea = p.picking_id)

			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Det_Documento, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END
		  
		--Insert los productos que no ingresaron en el documento por falta de Stock
			insert into sys_dev_det_documento
			select dd.doc_ext
			,dd.nro_linea
			,dd.cliente_id
			,dd.producto_id
			,dd.cantidad_solicitada
			,0
			,dd.est_merc_id
			,dd.cat_log_id
			,dd.nro_bulto
			,dd.descripcion
			,dd.nro_lote
			,dd.nro_pallet
			,dd.fecha_vencimiento
			,dd.nro_despacho
			,dd.nro_partida
			,dd.unidad_id
			,dd.unidad_contenedora_id
			,dd.peso
			,dd.unidad_peso
			,dd.volumen
			,dd.unidad_volumen
			,dd.prop1
			,dd.prop2
			,dd.prop3
			,dd.largo
			,dd.alto
			,dd.ancho
			,dd.doc_back_order
			,null
			,null
			,dd.estado_gt
			,getdate()
			,dd.documento_id
			,dd.nave_id
			,dd.nave_cod
			,Null --Flg_Movimiento
			from 
			sys_int_det_documento dd
			inner join sys_int_documento d on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			where cast(dd.doc_ext + dd.producto_id as varchar(400))  not in 
			(select cast(doc_ext + producto_id as varchar(400)) from sys_dev_det_documento)
			and d.codigo_viaje=@pViaje
			and not exists (select 1 from sys_dev_det_documento where sys_dev_det_documento.cliente_id = dd.cliente_id and sys_dev_det_documento.doc_Ext = dd.doc_ext and sys_dev_det_documento.nro_linea = dd.nro_linea)

			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Det_Documento de los Productos Sin Stock, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END
		 	
			exec DBO.PedidoMultiProducto @pViaje

			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error Al Ejecutar la devolucion Pedido MultiProducto, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END

			
			--Si fue todo bien y no salto por error hago el update en facturado
			Select @Usuario=Usuario_id from #Temp_Usuario_Loggin
			
			update 	picking 
				set 	facturado='1',
					fecha_control_Fac=Getdate(),
					Usuario_Control_fac=@Usuario,
					Terminal_Control_Fac=Host_Name()
			where 	viaje_id=@pViaje


			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error Al Realizar la Actualizacion en Cierre de Picking, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END
	end try
	begin catch
		exec usp_RethrowError
	end catch
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

ALTER                         PROCEDURE [dbo].[SYS_DEV_EGRESO_MOSTRADOR]
 @pviaje AS varchar(100) output,
 @DOC_EXT AS varchar(100) output
AS
	declare @Qty as numeric(10,0)
	declare @ErrorSave int
	declare @AuxNroLinea bigint
	declare @ControlExpedicion char(1)
	declare @TipoComp	as varchar(5)
	declare @Usuario 	as varchar(20)
	declare @count		as smallint
	declare @controla	as char(1)
BEGIN
begin try
	--Controlo que el viaje no este cerrado
	select @Qty=count(picking_id) from picking where viaje_id=@pViaje and facturado='1'

	--Controlo que el viaje tenga todos los picking's cerrados
	set @Qty=0
	select @Qty=count(picking_id) from picking where (fin_picking in ('0','1') or fin_picking is null) and viaje_id=@pViaje

	select	@Controla=isnull(flg_control_picking,'0')
	from	cliente_parametros 
	where	cliente_id=(select distinct cliente_id from picking(nolock) where viaje_id=@pviaje)

	---------------------------------------------------------------------------------------------------------------------
	--Controlo que el viaje este en el camion
	---------------------------------------------------------------------------------------------------------------------
	SELECT @TipoComp=TIPO_DOCUMENTO_ID FROM SYS_INT_DOCUMENTO WHERE CODIGO_VIAJE=@pviaje
	if @TipoComP ='E04'
	Begin

		select 	distinct 
				@ControlExpedicion=isnull(control_expedicion,'0')
		from 	documento d inner join tipo_comprobante tc
				on(d.tipo_comprobante_id=tc.tipo_comprobante_id)
		where	nro_despacho_importacion=ltrim(rtrim(Upper(@pViaje)))

	End
	Else
	Begin
		select @ControlExpedicion=control_expedicion from tipo_comprobante where tipo_comprobante_id=@TipoComp
	End


	set @Qty=0

	--Controlo que no queden en sys_int_det_documento productos pendientes 
	set @Qty=0
	select @Qty=count(dd.doc_ext) 
	from sys_int_det_documento dd 
		inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext)		
		inner join producto prod on (dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id)
	where dd.estado_gt is null and d.codigo_viaje=@pViaje

	If Dbo.GetTipoDocumento(@pViaje)='E10'
	Begin
		Exec Dbo.Sys_Dev_EgresoE10 @pViaje
		Return
	End
		INSERT INTO SYS_DEV_DOCUMENTO
		SELECT	DISTINCT 
				SID.CLIENTE_ID, 
				CASE WHEN SID.TIPO_DOCUMENTO_ID='E04' THEN 'E05' WHEN SID.TIPO_DOCUMENTO_ID='E08' THEN 'E09' ELSE SID.TIPO_DOCUMENTO_ID END, 
				SID.CPTE_PREFIJO, 
				SID.CPTE_NUMERO, 
				GETDATE(), --FECHA_CPTE, 
				SID.FECHA_SOLICITUD_CPTE, 
				SID.AGENTE_ID, 
				SID.PESO_TOTAL, 
				SID.UNIDAD_PESO, 
				SID.VOLUMEN_TOTAL, 
				SID.UNIDAD_VOLUMEN, 
				SID.TOTAL_BULTOS, 
				SID.ORDEN_DE_COMPRA, 
				SID.OBSERVACIONES, 
				CAST(D.CPTE_PREFIJO AS VARCHAR(20)) + CAST(D.CPTE_NUMERO  AS VARCHAR(20)), 
				SID.NRO_DESPACHO_IMPORTACION, 
				SID.DOC_EXT, 
				SID.CODIGO_VIAJE, 
				SID.INFO_ADICIONAL_1, 
				SID.INFO_ADICIONAL_2, 
				SID.INFO_ADICIONAL_3, 
   				D.TIPO_COMPROBANTE_ID, 	
				NULL, 
				NULL, 
				'P', 
				GETDATE(),
				NULL --FLG_MOVIMIENTO 
		FROM	SYS_INT_DOCUMENTO SID
				INNER JOIN DOCUMENTOS_E07 E ON(SID.DOC_EXT=E.DOC_EXT)
				LEFT JOIN DOCUMENTO D ON (SID.CLIENTE_ID=D.CLIENTE_ID AND SID.DOC_EXT=D.NRO_REMITO)
		WHERE	SID.DOC_EXT=@DOC_EXT
		if @@rowcount = 0 begin
			RAISERROR('Error al Insertar en Sys_Dev_Documento, No se inserto la cabecera',16,1)
		end
		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error al Insertar en Sys_Dev_Documento, Codigo_Error: %s',16,1,@ErrorSave)
		END

		insert into sys_dev_det_documento
		select	 d.nro_remito as doc_ext
				,(p.picking_id) as nro_linea
				,dd.cliente_id
				,dd.producto_id
				,dd.cant_solicitada
				,p.cant_confirmada
				,dd.est_merc_id
				,dd.cat_log_id_final
				,dd.nro_bulto as nro_bulto
				,dd.descripcion
				,dd.nro_lote
				,dd.prop1 as nro_pallet
				,dd.fecha_vencimiento
				,null as nro_despacho
				,dd.nro_partida
				,unidad_id
				,null as unidad_contenedora_id
				,null as peso
				,null as unidad_peso
				,null as volumen
				,null as unidad_volumen
				,ISNULL(dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1),NULL) as prop1
				,ISNULL(dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,2),NULL) as prop2
				,dd.nro_serie
				,null as largo
				,null as alto
				,dd.nro_linea as ancho --nro de linea
				,null as doc_back_order
				,null as estado
				,null as fecha_estado
				,'P' as estado_gt
				,getdate() as fecha_estado_gt
				,p.documento_id
				,dbo.Aj_NaveCod_to_Nave_id(p.nave_cod) as nave_id
				,p.nave_cod	
				,Null		--Flg_Movimiento
		from 	det_documento dd
				inner join documento d on (dd.documento_id=d.documento_id)
				inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
		where	D.NRO_REMITO=@DOC_EXT

		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error al Insertar en Sys_Dev_Det_Documento, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END
	  
		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error al Insertar en Sys_Dev_Det_Documento de los Productos Sin Stock, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END
	 	
		exec DBO.PedidoMultiProducto @pViaje

		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error Al Ejecutar la devolucion Pedido MultiProducto, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END

		
		--Si fue todo bien y no salto por error hago el update en facturado
		Select @Usuario=Usuario_id from #Temp_Usuario_Loggin
		
		update 	picking 
			set 	facturado='1',
				fecha_control_Fac=Getdate(),
				Usuario_Control_fac=@Usuario,
				Terminal_Control_Fac=Host_Name()
		where 	viaje_id=@pViaje


		IF @@ERROR <> 0 BEGIN
			SET @ErrorSave = @@ERROR
			RAISERROR('Error Al Realizar la Actualizacion en Cierre de Picking, Codigo_Error: %s',16,1,@ErrorSave)
			RETURN
		END
end try
begin catch
	exec usp_RethrowError
end catch

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

ALTER    Procedure [dbo].[Sys_Dev_Transferencia] 
@Doc_Trans_Id as numeric(20,0) Output
As
Begin
	---------------------------------------------------------
	--Para la Funcion.
	---------------------------------------------------------
	Declare @Ejecuta		as Int
	---------------------------------------------------------
	-- Cursor y sus variables.
	---------------------------------------------------------
	Declare @CursorRl		Cursor
	Declare @vRl			as numeric(20,0)
	---------------------------------------------------------
	--Para saber si ya cargue la cabecera
	---------------------------------------------------------
	Declare @Documento		as Int
	---------------------------------------------------------
	--Para el Cabecera.
	---------------------------------------------------------
	Declare @Cliente_id		as varchar(15)
	Declare @Nro_Linea		as numeric(10,0)
	---------------------------------------------------------
	--Para el Detalle
	---------------------------------------------------------	
	Declare @NavAnt			as Varchar(45)
	Declare @NavAct			as varchar(45)
	Declare @NavIdAnt		as Numeric(20,0)
	Declare @NavIdAct		as Numeric(20,0)
	---------------------------------------------------------	

	Set @Documento=0

	Set @CursorRl=Cursor For
		Select 	Rl_Id 
		From 	Rl_Det_Doc_Trans_Posicion
		Where	Doc_Trans_id_Tr=@Doc_Trans_id

	Open @CursorRl

	Fetch Next From @CursorRl into @vRl
	While @@Fetch_Status=0
	Begin 
		Select @Ejecuta=Dbo.Verifica_Cambio_Nave(@vRl)

		If @Ejecuta=1
		Begin
			If @Documento=0
			Begin
				Select 	@Cliente_id=Cliente_Id
				from 	Rl_Det_Doc_Trans_posicion 
				where	Rl_Id=@vRl

				Insert into Sys_Dev_Documento(Cliente_Id,Tipo_Documento_Id,Fecha_Cpte,Doc_Ext,Tipo_Comprobante,Fecha_Estado,Estado_GT,Fecha_Estado_GT, Flg_Movimiento)
				Values (@Cliente_id,'T01',Getdate(),@Doc_Trans_id,null,null,'P',Getdate(), Null)

				Set @Documento=1

			End	--Fin Documento=0

			--Saco la Nave Anterior	
			Select Distinct @NavIdAnt=X.Nave_Id,@NavAnt=X.Nave_Cod
			From(
					Select 	N.Nave_id as Nave_Id
							,N.Nave_Cod as Nave_Cod
					from	rl_det_doc_trans_posicion Rl
							inner join Nave N
							On(Rl.Nave_Anterior=N.Nave_id)
					Where	Rl.Rl_Id=@vRl
					Union All
					Select 	N.Nave_id as Nave_id,
							N.Nave_Cod as Nave_Cod
					From	Rl_Det_Doc_Trans_Posicion Rl
							inner join Posicion P
							On(Rl.Posicion_Anterior=P.Posicion_Id)
							Inner join Nave N
							On(P.Nave_Id=N.Nave_Id)
					Where	Rl.Rl_Id=@vRl
				)As X


			--Saco la Nave Actual
			Select Distinct @NavIdAct=X.Nave_Id,@NavAct=X.Nave_Cod
			From(
					Select 	 Nave_id as Nave_Id
							,N.Nave_Cod as Nave_Cod
					from	rl_det_doc_trans_posicion Rl
							inner join Nave N
							On(Rl.Nave_Actual=N.Nave_id)
					Where	Rl.Rl_Id=@vRl
					Union All
					Select 	 N.Nave_id as Nave_id
							,N.Nave_Cod as Nave_Cod 
					From	Rl_Det_Doc_Trans_Posicion Rl
							inner join Posicion P
							On(Rl.Posicion_Actual=P.Posicion_Id)
							Inner join Nave N
							On(P.Nave_Id=N.Nave_Id)
					Where	Rl.Rl_Id=@vRl
				)As X
		
			Select @Nro_Linea=IsNull(Max(Nro_Linea),0)+1 From Sys_Dev_Det_Documento where Doc_Ext=Cast(@Doc_Trans_id as varchar(20))

			--El Primero (-)
			Insert into Sys_Dev_Det_Documento (	Doc_Ext,Nro_Linea,Cliente_Id,Producto_Id,Cantidad_Solicitada,Cantidad,Est_Merc_Id,Cat_Log_Id,Nro_Bulto,
											Descripcion,Nro_Lote,Nro_Pallet,Fecha_Vencimiento,Nro_Despacho,Unidad_id,Estado_GT,Fecha_Estado_Gt,
											Documento_Id,Nave_Id,Nave_Cod, Flg_Movimiento)
										  (
											Select 	Distinct
													 @Doc_Trans_Id,@Nro_Linea,dd.Cliente_id,dd.Producto_id,(Rl.Cantidad-(Rl.Cantidad*2)),(Rl.Cantidad-(Rl.Cantidad*2))
													,dd.Est_Merc_id,Rl.Cat_Log_id,dd.Nro_Bulto,Prod.Descripcion,dd.Nro_Lote,dd.Prop1
													,dd.Fecha_Vencimiento,dd.Nro_Despacho,dd.Unidad_id,'P',Getdate()
													,dd.Documento_id,@NavIdAnt,@NavAnt, Null
											from	Rl_Det_Doc_Trans_Posicion Rl Inner Join Det_Documento_Transaccion Ddt
													On(Rl.Doc_Trans_Id=Ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=Ddt.Nro_Linea_Trans)
													Inner join Det_Documento Dd
													On(Ddt.Documento_id=Dd.Documento_id And Ddt.Nro_Linea_Doc=Dd.Nro_Linea)
													Inner Join Producto Prod
													On(Dd.Cliente_id=Prod.Cliente_Id And Dd.Producto_id=Prod.Producto_id)
											Where	Rl.Rl_Id=@vRl and Rl.Doc_Trans_Id_Tr=@Doc_Trans_id
											)


			Select @Nro_Linea=IsNull(Max(Nro_Linea),0)+1 From Sys_Dev_Det_Documento where Doc_Ext=Cast(@Doc_Trans_id as varchar(20))


			--El Segundo(+)
			Insert into Sys_Dev_Det_Documento (	Doc_Ext,Nro_Linea,Cliente_Id,Producto_Id,Cantidad_Solicitada,Cantidad,Est_Merc_Id,Cat_Log_Id,Nro_Bulto,
											Descripcion,Nro_Lote,Nro_Pallet,Fecha_Vencimiento,Nro_Despacho,Unidad_id,Estado_GT,Fecha_Estado_Gt,
											Documento_Id,Nave_Id,Nave_Cod, Flg_Movimiento)
										  (
											Select 	Distinct
													 @Doc_Trans_Id,@Nro_Linea,dd.Cliente_id,dd.Producto_id,(Rl.Cantidad),(Rl.Cantidad)
													,dd.Est_Merc_id,Rl.Cat_Log_id,dd.Nro_Bulto,Prod.Descripcion,dd.Nro_Lote,dd.Prop1
													,dd.Fecha_Vencimiento,dd.Nro_Despacho,dd.Unidad_id,'P',Getdate(),dd.Documento_id
													,@NavIdAct,@NavAct, Null
											from	Rl_Det_Doc_Trans_Posicion Rl Inner Join Det_Documento_Transaccion Ddt
													On(Rl.Doc_Trans_Id=Ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=Ddt.Nro_Linea_Trans)
													Inner join Det_Documento Dd
													On(Ddt.Documento_id=Dd.Documento_id And Ddt.Nro_Linea_Doc=Dd.Nro_Linea)
													Inner Join Producto Prod
													On(Dd.Cliente_id=Prod.Cliente_Id And Dd.Producto_id=Prod.Producto_id)
											Where	Rl.Rl_Id=@vRl and Rl.Doc_Trans_Id_Tr=@Doc_Trans_id
											)



		End--@Ejecuta=1
		Fetch Next From @CursorRl into @vRl
	End

	Close @CursorRl
	Deallocate @CursorRl

End --Fin Procedure.
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

ALTER Procedure [dbo].[sys_int_iex]

As
Begin
declare @vSuc						as varchar(20)
declare @vPed						as varchar(100)
declare @vnl						as numeric(20,0)
declare @vProducto_id				as varchar(30)
declare @vQty						as numeric(20,5)
declare @vNroControl				as varchar(100)
declare @vCodViaje					as varchar(100)
declare @vFecha						as varchar(100)
declare @CountReg                   as numeric(20,0)

declare @RsInf	as Cursor
SET NOCOUNT ON;

delete iex

Set @RsInf = Cursor For
	select 
	 d.agente_id 
	,d.doc_ext
	,dd.nro_linea
	,dd.producto_id
	,dd.cantidad_solicitada
	,dd.prop1
	,d.codigo_viaje
	,dbo.fx_DateTimeToAnsi(d.fecha_solicitud_cpte) as fecha 
	from sys_int_documento d
		inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
	where
		d.cliente_id='LEADER PRICE'
		and d.tipo_documento_id in ('E04','E03')
		and dd.estado is null
		and dd.estado_gt='P'
		and d.codigo_viaje in (select distinct ruta from picking p where p.fin_picking='2')
order by d.codigo_viaje desc

Open @RsInf
	Fetch Next From @RsInf into @vSuc,@vPed,@vnl,@vProducto_id,@vQty,@vNroControl,@vCodViaje,@vFecha 

While @@Fetch_Status=0
Begin	
	
	select 
	@CountReg=count(p.producto_id)
	from picking p
		inner join det_documento dd on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
		inner join documento d on (p.documento_id=d.documento_id)
	where p.fin_picking='2' and p.pallet_final is not null and ruta<>'ENVIADO' and
		  p.producto_id=@vProducto_id and d.sucursal_destino=@vSuc and d.nro_remito=@vPed

	if (@CountReg>0) begin
			insert into iex 
			select 
			@vNroControl,
			@vPed,
			@vFecha,
			p.producto_id,
			@vQty, --Cantidad Pedida
			p.cant_confirmada, --Cantidad Pickeada	
			0, --peso
			p.pallet_final,
			@vSuc,
			'LEADER PRICE',
			null
			from picking p
				inner join det_documento dd on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
				inner join documento d on (p.documento_id=d.documento_id)
			where p.fin_picking='2' and p.pallet_final is not null and ruta<>'ENVIADO' and
				  p.producto_id=@vProducto_id and d.sucursal_destino=@vSuc and d.nro_remito=@vPed
			
			update picking set ruta='ENVIADO' where picking_id in (	select p.picking_id		
							from picking p
								inner join det_documento dd on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
								inner join documento d on (p.documento_id=d.documento_id)
							where p.fin_picking='2' and p.pallet_final is not null and ruta<>'ENVIADO' and
								  p.producto_id=@vProducto_id and d.sucursal_destino=@vSuc and d.nro_remito=@vPed)
	end else begin
			insert into iex 
			values ( 
			@vNroControl,
			@vPed,
			@vFecha,
			@vProducto_id,
			@vQty, --Cantidad Pedida			
			0, --Cantidad Pickeada	
			0, --peso
			0,
			@vSuc,
			'LEADER PRICE',
			null)
	
	end --if
	update sys_int_det_documento set estado='INF',fecha_estado=getdate() where cliente_id='LEADER PRICE' and doc_ext=@vPed and Nro_linea=@vnl

	Fetch Next From @RsInf into @vSuc,@vPed,@vnl,@vProducto_id,@vQty,@vNroControl,@vCodViaje,@vFecha 
End	--End While @RsInf.

CLOSE @RsInf
DEALLOCATE @RsInf



Set NoCount Off;
End -- Fin Procedure.
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

ALTER   PROCEDURE [dbo].[Sys_WriteLocking]
@pid 						as	varchar(200) output,
@pError					as varchar(4000) output

AS

BEGIN

	--Borro las sessiones que detecto que no estan activas y la que corre este procedure
	delete Sys_Session_Login where session_id not in (select spid from  master.dbo.sysprocesses)
	delete Sys_Session_Login where session_id in (select @@spid)

	insert into eventslock	
	select distinct 
		@pid,	
		convert (smallint, req_spid) As spid,
		object_name(rsc_objid) As ObjId,
		dbo.Get_data_Session_Login(req_spid,'1') as usuario, 
		dbo.Get_data_Session_Login(req_spid,'2') as nombre_usuario,
		dbo.Get_data_Session_Login(req_spid,'3') as terminal,
		dbo.Get_data_Session_Login(req_spid,'4') as fecha_login,
		dbo.Sys_Obj_Locking(req_spid,'1') as status, 
		dbo.Sys_Obj_Locking(req_spid,'2') as hostname,
		dbo.Sys_Obj_Locking(req_spid,'3') as program_name,
		dbo.Sys_Obj_Locking(req_spid,'4') as cmd,
		dbo.Sys_Obj_Locking(req_spid,'5') as loginname,
		dbo.Sys_Obj_Locking(req_spid,'6') as fecha_lock,
		dbo.Sys_Obj_Locking(req_spid,'7') as dbname,
		getdate() as fecha_registro,
		@pError
	from 	master.dbo.syslockinfo,
		master.dbo.spt_values v,
		master.dbo.spt_values x,
		master.dbo.spt_values u

	where   master.dbo.syslockinfo.rsc_type = v.number
			and v.type = 'LR'
			and master.dbo.syslockinfo.req_status = x.number
			and x.type = 'LS'
			and master.dbo.syslockinfo.req_mode + 1 = u.number
			and u.type = 'L'
			and object_name(rsc_objid) is not null
			and substring (u.name, 1, 8)='X'
			and upper(dbo.Sys_Obj_Locking(req_spid,'7'))='AGUAS_DESA'

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

ALTER  PROCEDURE [dbo].[test_DEFAULT]
@deposito_default varchar(30) OUTPUT
---sirve para averiguar el deposito default ya que no se puede acceder a tablas temporales
---desde una funcion


AS

set @deposito_default=(SELECT top 1    USUARIO_ID
FROM         SYS_USUARIO)
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

ALTER   Procedure [dbo].[Test_VerificaExistencias]
As
Begin
	
	Declare @Qty 	as Float
	
	Create TABLE #temp_existencia (
	clienteid         	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	productoid        	VARCHAR(30)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cantidad          	NUMERIC(20,5) 	NULL,
	nro_serie         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_lote          	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	fecha_vencimiento DATETIME      	NULL,
	nro_despacho     VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_bulto         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_partida       	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	peso              		NUMERIC(20,5) 	NULL,
	volumen           	NUMERIC(20,5) 	NULL,
	tie_in            		CHAR(1)        	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	STORAGE           	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	naveid            	NUMERIC(20,0) 	NULL,
	callecod          	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	calleid           		NUMERIC(20,0) 	NULL,
	columnacod        	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	columnaid         	NUMERIC(20,0) 	NULL,
	nivelcod          	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nivelid           		NUMERIC(20,0) 	NULL,
	categlogid        	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop1             	VARCHAR(100)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop2             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop3             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_id         	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_peso       	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_volumen    VARCHAR(5)     COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	est_merc_id       	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	moneda_id         	VARCHAR(20)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	costo             		NUMERIC(10,3) 	NULL 
	)

	CREATE TABLE #temp_existencia_doc (
	clienteid         	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	productoid        	VARCHAR(30)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cantidad          	NUMERIC(20,5) 	NULL,
	nro_serie         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_lote          	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	fecha_vencimiento DATETIME      	NULL,
	nro_despacho     VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_bulto         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_partida       	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	peso              		NUMERIC(20,5) 	NULL,
	volumen           	NUMERIC(20,5) 	NULL,
	tie_in            		CHAR(1)        	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cantidad_disp     	NUMERIC(20,5) 	NULL,
	code              		CHAR(1)        	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	description       	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cat_log_id        	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop1             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop2             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop3             	VARCHAR(100)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_id         	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_peso       	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_volumen    VARCHAR(5)     COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	est_merc_id       	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	moneda_id         	VARCHAR(20)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	costo             		NUMERIC(10,3) 	NULL,
	orden             	NUMERIC(20,0) 	NULL
	)

	CREATE TABLE #temp_rl_existencia_doc (
	rl_id 			NUMERIC(20,5) NULL
	)
	
	Exec  Funciones_Frontera_Api#VerificaExistencias
			@xCliente_id	='10202',
			@xProducto_id	='10347',
			@xCantidad		= @QTY Output

	Select @QTY as cantidad

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

ALTER  procedure [dbo].[TestCursorDin]
As
Begin
	Declare @xSQL as nvarchar(4000)
	Declare @Cliente_id as varchar(15)
	Declare @Producto_id as varchar(30)

	DECLARE @my_cur CURSOR
    EXEC sp_executesql
          N'SET @my_cur = CURSOR FOR SELECT Cliente_id,Producto_id FROM producto; OPEN @my_cur',
          N'@my_cur cursor OUTPUT', @my_cur OUTPUT

    FETCH NEXT FROM @my_cur into @Cliente_id,@Producto_id
	While @@Fetch_Status=0
	Begin
		Select @Cliente_id as Cliente, @Producto_id as Producto
	    FETCH NEXT FROM @my_cur into @Cliente_id,@Producto_id
	End
	close @my_Cur
	Deallocate @My_cur
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

ALTER PROCEDURE [dbo].[TOLERANCIA] --Control de tolerancia de productos Minima y Maxima
	@CLIENTE_ID		VARCHAR(15),
	@OC				VARCHAR(100),
	@PRODUCTO_ID	VARCHAR(30),
	@TolMax			Float output,
	@TolMin			Float output	
AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
	
	DECLARE @ToleranciaMax		Float
	DECLARE @ToleranciaMin		Float
	DECLARE @DOC_EXT			VARCHAR(100)
	DECLARE @SUCURSAL_ORIGEN	VARCHAR(20)
	DECLARE @qtyBO				Float
	
	SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0), @ToleranciaMin=isnull(TOLERANCIA_MIN,0) from producto where cliente_id=@cliente_id and producto_id=@producto_id
	
	SELECT 	TOP 1
				@DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID
		FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		WHERE 	ORDEN_DE_COMPRA=@OC
				AND PRODUCTO_ID=@PRODUCTO_ID
				AND SD.CLIENTE_ID=@CLIENTE_ID
				and SDD.fecha_estado_gt is null
				and SDD.estado_gt is null
				
	Select 	@qtyBO=sum(cantidad_solicitada)
		from	sys_int_det_documento
		where	doc_ext=@doc_ext
				and fecha_estado_gt is null
				and estado_gt is null
				
	set @ToleranciaMax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)
	set @ToleranciaMin= @qtyBO - ((@qtyBO * @ToleranciaMin)/100)
	
	set @TolMax = @ToleranciaMax
	set @TolMin	= @ToleranciaMin

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

ALTER PROCEDURE [dbo].[TOMA_VH]
@VEHICULO_ID	VARCHAR(50)
AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
	
	DECLARE @USUARIO	VARCHAR(20)
	DECLARE @COUNT	INT
	
	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	
	SELECT 	@COUNT=COUNT(*)
	FROM	RL_USUARIO_VEHICULO
	WHERE	VEHICULO_ID=@VEHICULO_ID

	IF @COUNT>0
	BEGIN
		-- si es mayor a 0 es porque esta tomado por alguien
		DELETE FROM RL_USUARIO_VEHICULO WHERE VEHICULO_ID=@VEHICULO_ID
	END
	DELETE FROM RL_USUARIO_VEHICULO WHERE USUARIO_ID=@USUARIO
	INSERT INTO RL_USUARIO_VEHICULO VALUES (@USUARIO, @VEHICULO_ID, GETDATE())

END --FIN PROCEDURE
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

ALTER         PROCEDURE [dbo].[TRD_ACT_NRO_LINEA_PALLET]
	@Doc_Trans_Id	as Numeric(20,0) output,
	@PalletD		as Varchar(100)
As
Begin
	Declare @Doc_id 		as Numeric(20,0)
	Declare @PalletOrigen	as Varchar(100)
	Declare @PosCodDest		as Varchar(45)
	Declare @Pallet_E		as Varchar(45)
	Declare @NroLinea		as numeric(10,0)

	SELECT 	@Doc_id=DD.Documento_id,@PosCodDest=p.posicion_cod,@Pallet_E=dd.Prop1,
			@NroLinea=dd.nro_linea
	From 	rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt
			on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			inner join det_documento dd
			on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
			left join nave n
			on(n.nave_id=rl.nave_actual)
			left join posicion p
			on(p.posicion_id=rl.posicion_actual)
			left join posicion p2
			on(p2.posicion_id=rl.posicion_anterior)
			left join nave n2
			on(rl.nave_anterior=n2.nave_id)
	Where 	doc_trans_id_tr = @Doc_Trans_Id

	Update 	Det_Documento set Prop1=Ltrim(Rtrim(Upper(@PalletD)))
	where	Documento_id=@Doc_id 
			and nro_linea=@NroLinea

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

ALTER    PROCEDURE [dbo].[TRD_ACT_PALLET]
	@Doc_Trans_Id	as Numeric(20,0) output
As
Begin
		

	EXEC actualiza_pos_picking_desk @Doc_Trans_Id
	if @@error<>0
	Begin
		raiserror('Fallo al ejecutar actualiza_pos_picking_desk Sp.',16,1)
		Return(99)
	End
	Declare @Doc_id 		as Numeric(20,0)
	Declare @PalletOrigen	as Varchar(100)
	Declare @PosCodDest		as Varchar(45)
	Declare @Pallet_E		as Varchar(45)

	SELECT 	@Doc_id=DD.Documento_id,@PosCodDest=p.posicion_cod,@Pallet_E=dd.Prop1
	From 	rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt
			on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			inner join det_documento dd
			on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
			left join nave n
			on(n.nave_id=rl.nave_actual)
			left join posicion p
			on(p.posicion_id=rl.posicion_actual)
			left join posicion p2
			on(p2.posicion_id=rl.posicion_anterior)
			left join nave n2
			on(rl.nave_anterior=n2.nave_id)
	Where 	doc_trans_id_tr = @Doc_Trans_Id

	if @PosCodDest is not null
		Begin
			Select 	@PalletOrigen=dbo.fx_GetPalletByPos(@PosCodDest)
			If @PalletOrigen is not null
				Begin
					Update 	Det_Documento set Prop1=@PalletOrigen 
					where	Documento_id=@Doc_id and Prop1=@Pallet_E
				End
		End
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

ALTER       PROCEDURE [dbo].[TRD_GET_PALLETS_BY_POS]
@POSICION AS VARCHAR(45),
@PRODUCTO AS VARCHAR(30)
AS

BEGIN
	
	DECLARE @EXISTE 	AS INT

	SELECT 	@EXISTE=COUNT(POSICION_ID)
	FROM 	POSICION
	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION)))

	IF @EXISTE=0
		BEGIN
			SELECT 	@EXISTE=COUNT(NAVE_ID)
			FROM 	NAVE
			WHERE	NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION)))

			IF @EXISTE=0
				BEGIN
					RAISERROR('La ubicacion es inexistente',16,1)
					Return
				END
		END	

	SELECT DISTINCT X.*
	FROM(
			SELECT 	DD.PROP1
			FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	P.POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION)))
					--AND DD.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO)))
			
			UNION ALL
	
			SELECT 	DD.PROP1
			FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
			WHERE	N.NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION)))
					--AND DD.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO)))
	) AS X				
	ORDER BY PROP1

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

ALTER         PROCEDURE [dbo].[TRD_GetProdByPallet]
@Pallet as varchar(100)
As
Begin
	SELECT	 DISTINCT
			 DD.PRODUCTO_ID 					
			,PROD.DESCRIPCION					
			,PROD.UNIDAD_ID						
			,SUM(CAST(RL.CANTIDAD AS INT))		AS QTY
			,DD.NRO_LOTE						
			,ISNULL(P.POSICION_COD,N.NAVE_COD)	AS UBICACION
	FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			INNER JOIN PRODUCTO PROD
			ON(DD.PRODUCTO_ID=PROD.PRODUCTO_ID AND DD.CLIENTE_ID=PROD.CLIENTE_ID)
			LEFT JOIN POSICION P
			ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			LEFT JOIN NAVE N
			ON(RL.NAVE_ACTUAL=N.NAVE_ID)
			INNER JOIN DOCUMENTO D
			ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
	WHERE	DD.PROP1=Ltrim(Rtrim(Upper(@Pallet)))
			AND D.STATUS='D40'
	GROUP 
	BY 		DD.PRODUCTO_ID,PROD.DESCRIPCION,P.POSICION_COD,NAVE_COD,DD.NRO_LOTE,PROD.UNIDAD_ID,DD.PROP1,DD.NRO_LINEA

	IF @@ROWCOUNT=0
		BEGIN
			RAISERROR('No hay registros para el pallet ingresado.',16,1)
		END

End
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