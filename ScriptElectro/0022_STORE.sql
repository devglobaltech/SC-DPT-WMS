USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 05:10 p.m.
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

ALTER PROCEDURE [dbo].[EXIST_OCP]
@CLIENTE_ID		  VARCHAR(15),
@ODC			      VARCHAR(100),
@PRODUCTO_ID	  VARCHAR(30),
@loteProveedor  VARCHAR(100),
@partida        VARCHAR(100),
@STATUS			    CHAR(1) output,
@doc_ext        varchar(100) output
AS
BEGIN
	/*
	STATUS=0 -> NO EXISTE
	STATUS=1 -> EXISTE OK
	STATUS=2 -> EXISTE PERO ESTA COMPLETADA.
	*/
	Declare @Control as smallint
	
	select 	@control=count(*) 
	from 	sys_int_documento 
	where 	orden_de_compra=@ODC
			and cliente_id=@cliente_id

	if @control>0
	begin
		set @control=0
    set @doc_ext=null
		select 	@doc_ext=sd.doc_ext
		from	sys_int_documento sd inner join sys_int_det_documento sdd 	on(sd.cliente_id=sdd.cliente_id and sd.doc_ext=sdd.doc_ext )
		where	sd.cliente_id=@cliente_id
				and sdd.producto_id=@producto_id
				and sd.orden_de_compra=@odc
        AND isnull(sdd.nro_lote,'') = @loteProveedor
        and isnull(sdd.NRO_PARTIDA,'') = @partida
				and sdd.fecha_estado_gt is null
				and sdd.estado_gt is null

    if (@doc_ext is not null)
      set @control=1
      
		if @control>0
		begin
			set @status='1'
			return
		end
		else
		begin
			set @status='2'
			raiserror('La orden de compra %s para el cliente %s ya esta finalizada.',16,1,@odc, @cliente_id)	
		end
	End
	else
	begin
		set @status=0
		raiserror('No existe la orden de compra %s para el cliente %s',16,1,@odc, @cliente_id)
		return
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

ALTER PROCEDURE [dbo].[EXIST_ODC]
@CLIENTE_ID	VARCHAR(15),
@ODC			VARCHAR(100),
@STATUS		CHAR(1) output
AS
BEGIN
	/*
	STATUS=0 -> NO EXISTE
	STATUS=1 -> EXISTE OK
	STATUS=2 -> EXISTE PERO ESTA COMPLETADA.
	*/
	Declare @Control as smallint
	
	select 	@control=count(*) 
	from 	sys_int_documento 
	where 	orden_de_compra=@ODC
			and cliente_id=@cliente_id

	if @control>0
	begin
		set @control=0
		select 	@control=count(*)
		from	sys_int_documento sd inner join sys_int_det_documento sdd 	on(sd.cliente_id=sdd.cliente_id and sd.doc_ext=sdd.doc_ext)
		where	sd.cliente_id=@cliente_id
				and sd.orden_de_compra=@odc
				and sdd.fecha_estado_gt is null
				and sdd.estado_gt is null

		if @control>0
		begin
			set @status='1'
			return
		end
		else
		begin
			set @status='2'
			raiserror('La orden de compra %s para el cliente %s ya esta finalizada.',16,1,@odc, @cliente_id)	
		end
	End
	else
	begin
		set @status=0
		raiserror('No existe la orden de compra %s para el cliente %s',16,1,@odc, @cliente_id)
		return
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

ALTER Procedure [dbo].[FacDetalle]
@fDesde		as varchar(20)	output,
@fHasta		as varchar(20)	output,
@Pedido		as varchar(100) output,
@viaje		as varchar(100) output,
@cliente	as varchar(30)	output
As
Begin

	Select	 c.razon_social								[Cod. Cliente]
			,s.sucursal_id								[Cod. Sucursal Destinatario]
			,s.nombre									[Razon Social Destinatario]
			,p.viaje_id									[Cod.Viaje]
			,d.nro_remito								[Pedido]
			,p.producto_id								[Cod. Producto]
			,isnull(dd.prop2,'')						[Lote proveedor]
			,p.descripcion								[Desc. Producto]
			,p.cant_confirmada							[Cant. Confirmada]
			,p.posicion_cod								[Posicion]
			,convert(varchar, p.fecha_inicio,103)+' '+
			 dbo.FxTimebyDetime(p.fecha_inicio)	 		[Fecha Inicio Pick.]
			,convert(varchar, p.fecha_fin, 103)+ ' '+			
			 dbo.FxTimebyDetime(p.fecha_fin)			[Fecha Fin Pick.]
			,su.nombre									[Pickeador]
			,p.pallet_picking							[Pallet Picking]
			,isnull(su2.nombre,'')						[Usuario control picking]
			,convert(varchar,p.fecha_control_exp,103) + ' ' +	
			 dbo.FxTimebyDetime(p.fecha_fin)			[Fecha Control Expedicion]
			,su3.nombre									[Usuario Control Expedicion]
	from	documento d inner join det_documento dd
			on(d.documento_id=dd.documento_id)
			inner join cliente c
			on(c.cliente_id=d.cliente_id)
			inner join sucursal s 
			on(s.cliente_id=d.cliente_id and s.sucursal_id=d.sucursal_destino)
			inner join picking p
			on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			inner join sys_usuario su
			on(su.usuario_id=p.usuario)
			left join sys_usuario su2
			on(su2.usuario_id=p.usuario_control_pick)
			left join sys_usuario su3
			on(su3.usuario_id=p.usuario_control_exp)
	where	((@cliente is null) or(d.cliente_id=@cliente))
			and ((@Pedido is null) or (d.nro_remito=@Pedido))
			and ((@viaje is null) or(p.viaje_id=@viaje))
			and ((@fDesde is null) or(p.fecha_inicio between @fDesde and dateadd(d,1,@fHasta)))
	order by
			d.nro_remito, p.fecha_inicio
End--End Procedure.
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

ALTER Procedure [dbo].[FacTotalizado]
@fDesde		as varchar(20)	output,
@fHasta		as varchar(20)	output,
@Pedido		as varchar(100) output,
@viaje		as varchar(100) output,
@cliente	as varchar(30)	output
As
Begin

	select	 p.viaje_id														as [Viaje / Picking]
			,dbo.date_picking(p.viaje_id,'1')								as [Fecha inicio Pick.]
			,dbo.date_picking(p.viaje_id,'2')								as [Fecha Fin Pick.]
			,ROUND(((SUM(p.CANT_CONFIRMADA)*100)/SUM(p.cantidad)),2)		as [Cumplimiento Pick.]
			,round(((sum(p.cant_confirmada)*100)/x.cantidad_solicitada),2)	as [Cumplimiento Pedido]
			,sum(p.cant_confirmada)											as [Total de Bultos]
	From	picking p (NoLock)
			Inner Join det_documento dd (NoLock) on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
			Inner join documento d(nolock) on(dd.documento_id=d.documento_id)
			inner join rl_sys_cliente_usuario su on(p.cliente_id=su.cliente_id)
			inner join
			(	select	sum(isnull(cantidad_solicitada,0))cantidad_solicitada, codigo_viaje
				from	sys_int_det_documento ss inner join sys_int_documento s 
						on(s.cliente_id=ss.cliente_id and s.doc_ext=ss.doc_ext)
				where	s.tipo_documento_id in (select r.tipo_documento_id from RL_ROL_INT_TIPO_DOCUMENTO R (NoLock) where r.rol_id='ADM')
				group by
						codigo_viaje
			)x on(x.codigo_viaje=p.viaje_id)
	Where	p.fin_picking='2'
			and ((@cliente is null) or(d.cliente_id=@cliente))
			and ((@Pedido is null)	or(d.nro_remito=@Pedido))
			and ((@viaje is null)	or(p.viaje_id=@viaje))
			and ((@fDesde is null)	or(p.fecha_inicio between @fDesde and dateadd(d,1,@fHasta)))
	group by 
			p.viaje_id, x.cantidad_solicitada
	Having  Dbo.Fx_Procesados(p.viaje_id)=0
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

ALTER  PROCEDURE [dbo].[FIN_PICKING]
	@USUARIO 			AS VARCHAR(30),
	@VIAJEID 			AS VARCHAR(100),
	@PRODUCTO_ID		AS VARCHAR(50),
	@POSICION_COD		AS VARCHAR(45),
	@CANT_CONF			AS FLOAT,
	@PALLET_PICKING     AS NUMERIC(20),
	@PALLET				AS VARCHAR(100),
	@RUTA				AS VARCHAR(50),
	@LOTE				AS VARCHAR(100),
	@LOTE_PROVEEDOR		AS VARCHAR(100),
	@NRO_PARTIDA		AS VARCHAR(100),
	@NRO_SERIE			AS VARCHAR(50)
AS

BEGIN
	--DECLARACIONES.
	DECLARE @PICKID 	AS NUMERIC(20,0)
	DECLARE @CANTIDAD 	AS NUMERIC(20,5)
	DECLARE @CANT_CUR 	AS NUMERIC(20,5)	
	DECLARE @DIF 		AS NUMERIC(20,5)
	DECLARE @CONT_DTO 	AS NUMERIC(20,5)
	DECLARE @VCANT 		AS NUMERIC(20,5)
	DECLARE @VINCULACION	AS INT
	DECLARE @ERRORVAR	AS INT
	declare @Qty			as numeric(20,0)
	DECLARE @COUNTPOS	AS INT


	IF LTRIM(RTRIM((@PALLET)))=''
	BEGIN
		SET @PALLET=NULL
	END
	SELECT @VINCULACION=DBO.PICKING_VER_AFECTACION(@USUARIO,@VIAJEID)
	IF @VINCULACION=0
	BEGIN
		RAISERROR('3- Ud. fue desafectado del viaje.',16,1)
		RETURN
	END	

	SELECT 	@CANTIDAD=SUM(P.CANTIDAD)
	FROM 	PICKING P INNER JOIN DET_DOCUMENTO DD 
			ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
	WHERE	USUARIO=LTRIM(RTRIM(UPPER(@USUARIO)))
			AND P.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
			AND POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD )))
			AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
			AND ((@PALLET IS NULL OR @PALLET='') OR(P.PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
			AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
			AND P.FECHA_INICIO IS NOT NULL
			AND P.FECHA_FIN IS NULL
			AND ((@LOTE IS NULL OR @LOTE='') OR (DD.PROP2=@LOTE))
			AND ((@LOTE_PROVEEDOR IS NULL OR @LOTE_PROVEEDOR='') OR (P.NRO_LOTE = @LOTE_PROVEEDOR))
			AND ((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA='') OR (P.NRO_PARTIDA = @NRO_PARTIDA))
			AND ((@NRO_SERIE IS NULL OR @NRO_SERIE = '') OR (P.NRO_SERIE = @NRO_SERIE))
	GROUP BY P.PRODUCTO_ID, POSICION_COD, FECHA_FIN,VIAJE_ID,P.PROP1


	DECLARE PCUR  CURSOR FOR
		SELECT 	P.PICKING_ID, P.CANTIDAD
		FROM 	PICKING P INNER JOIN DET_DOCUMENTO DD 
				ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
		WHERE	USUARIO=LTRIM(RTRIM(UPPER(@USUARIO )))
				AND P.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
				AND P.POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD )))
				AND P.FECHA_FIN IS NULL AND CANT_CONFIRMADA IS NULL
				AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
				AND ((@PALLET IS NULL OR @PALLET='') OR(P.PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
				AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
				AND P.FECHA_INICIO IS NOT NULL
				AND P.FECHA_FIN IS NULL
				AND ((@LOTE IS NULL OR @LOTE='') OR (DD.PROP2=@LOTE))
				AND ((@LOTE_PROVEEDOR IS NULL OR @LOTE_PROVEEDOR='') OR (P.NRO_LOTE = @LOTE_PROVEEDOR))
				AND ((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA='') OR (P.NRO_PARTIDA = @NRO_PARTIDA))
				AND ((@NRO_SERIE IS NULL OR @NRO_SERIE = '') OR (P.NRO_SERIE = @NRO_SERIE))
	OPEN PCUR

	IF @CANTIDAD=@CANT_CONF
		BEGIN
			FETCH NEXT FROM PCUR INTO @PICKID,@CANT_CUR
			WHILE @@FETCH_STATUS = 0
			BEGIN
				UPDATE PICKING SET 	
							FECHA_FIN=GETDATE(),
							CANT_CONFIRMADA=@CANT_CUR,
							PALLET_PICKING= @PALLET_PICKING 
				WHERE	PICKING_ID=@PICKID	

				FETCH NEXT FROM PCUR INTO @PICKID,@CANT_CUR
			END
		END
	ELSE
		BEGIN
			SET @CONT_DTO = 0
			SET @DIF=@CANTIDAD - @CANT_CONF

			FETCH NEXT FROM PCUR INTO @PICKID,@CANT_CUR
			WHILE @@FETCH_STATUS = 0
			BEGIN



				IF  @CONT_DTO=0
					BEGIN
						SET @VCANT = @CANT_CUR - @DIF
						IF @VCANT < 0
							BEGIN
								SET @VCANT=0
							END
						IF @CANT_CUR > @DIF
							BEGIN
								SET @DIF=0
							END
						ELSE
							BEGIN
								SET @DIF= @DIF - @CANT_CUR						
							END
						--Catalina Castillo.Tracker 4741
						--IF @CANT_CUR =@CANT_CONF
							--BEGIN
								UPDATE PICKING SET FECHA_FIN=GETDATE(),	CANT_CONFIRMADA= @VCANT,
											PALLET_PICKING= @PALLET_PICKING 
								WHERE	PICKING_ID=@PICKID 
							--END
						SET @VCANT=0	
						IF @DIF=0
							BEGIN
								SET @CONT_DTO=1
							END
						FETCH NEXT FROM PCUR INTO @PICKID,@CANT_CUR
					END
				ELSE
					BEGIN
					 --Catalina Castillo.Tracker 4741
					  IF @CANT_CUR =@CANT_CONF
						BEGIN
							UPDATE PICKING SET 	
										FECHA_FIN=GETDATE(),
										CANT_CONFIRMADA=@CANT_CUR,
										PALLET_PICKING= @PALLET_PICKING 
							WHERE	PICKING_ID=@PICKID	
						END
						FETCH NEXT FROM PCUR INTO @PICKID,@CANT_CUR
					END				
			END
		END


	SELECT 	@CANTIDAD=COUNT(PICKING_ID)
	FROM	PICKING
	WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(UPPER(RTRIM(@VIAJEID)))


	SELECT 	@DIF=COUNT(PICKING_ID)
	FROM 	PICKING 
	WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(UPPER(RTRIM(@VIAJEID)))
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL


	IF @CANTIDAD=@DIF
		BEGIN
			--FO le agrego esto para que el pedido no desaparezca
			select @Qty=isnull(count(dd.producto_id),0)  	 
			from sys_int_documento d inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			where
			d.codigo_viaje=LTRIM(RTRIM(UPPER(@VIAJEID)))
			and dd.estado_gt is null
			if (@Qty=0) begin
				UPDATE PICKING SET FIN_PICKING='2' WHERE LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
			end --if

		END
	
	SELECT	@COUNTPOS=COUNT(*)
	FROM	POSICION
	WHERE	POSICION_COD=@POSICION_COD
	IF @COUNTPOS=1
	BEGIN
		--Es una posicion.
		Set @CountPos=null
		
		SELECT	@COUNTPOS=COUNT(*)
		FROM	RL_DET_DOC_TRANS_POSICION
		WHERE	POSICION_ACTUAL = (SELECT POSICION_ID FROM POSICION WHERE POSICION_COD=@POSICION_COD)

		If @CountPos=0
		Begin
			update posicion set pos_vacia='1' where posicion_cod=@POSICION_COD
		End
	END
	CLOSE PCUR
	DEALLOCATE PCUR

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

ALTER               Procedure [dbo].[Fin_Picking_Split]
@Usuario 			as varchar(30),
@Viajeid 			as varchar(30),
@Producto_id		as varchar(50),
@Posicion_cod		as varchar(45),
@Cant_conf			as numeric(20,5),
@Pallet_picking     as numeric(20,0),
@Pallet				as varchar(100),
@Ruta				as varchar(50),
@Lote				as varchar(100),
@LOTE_PROVEEDOR		AS VARCHAR(100),
@NRO_PARTIDA		AS VARCHAR(100),
@NRO_SERIE			AS VARCHAR(50)
As
Begin
	
	Declare @Cur			Cursor
	Declare @Cant			Numeric(20,5)
	Declare @PickId			Numeric(20,5)
	Declare @Cantidad		Numeric(20,5)
	Declare @Dif				Numeric(20,5)
	Declare @Vinculacion		Numeric(20,5)
	if ltrim(rtrim(@Pallet))=''
	begin
		Set @Pallet=null
	end

	Select @vinculacion=dbo.picking_ver_afectacion(@usuario,@viajeid)
	If @vinculacion=0
	Begin
		Raiserror('3- ud. fue desafectado del viaje.',16,1)
		Return
	End	

	Set @Cur= Cursor For
		Select 	p.Picking_id, p.Cantidad
		From	Picking p inner join det_documento dd on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
		Where	Usuario			=Ltrim(Rtrim(Upper(@Usuario)))
				And Viaje_id	=Ltrim(Rtrim(Upper(@ViajeId)))
				And p.Producto_id	=Ltrim(Rtrim(Upper(@Producto_id)))
				And Posicion_Cod=Ltrim(Rtrim(Upper(@Posicion_Cod)))
				and ((@pallet is null) or (p.Prop1=Ltrim(Rtrim(Upper(@Pallet)))))
				and ((@lote is null)or(dd.prop2=@lote))
				And Ruta		=Ltrim(Rtrim(Upper(@Ruta)))
				And Fecha_inicio is not null
				And Fecha_Fin is null
			AND ((@LOTE_PROVEEDOR IS NULL OR @LOTE_PROVEEDOR='') OR (P.NRO_LOTE = @LOTE_PROVEEDOR))
			AND ((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA='') OR (P.NRO_PARTIDA = @NRO_PARTIDA))
				--AND ((@NRO_SERIE IS NULL) OR (P.NRO_LOTE = @NRO_SERIE))

	Open @Cur

	Fetch Next From @Cur Into @PickId,@Cant
	While @@Fetch_Status=0
	Begin
					
		If @Cant <= @Cant_conf and @Cant_conf > 0
		Begin
			Update Picking set Cant_Confirmada=@Cant, Fecha_Fin=Getdate(),pallet_picking=@Pallet_picking,NRO_SERIE = @NRO_SERIE Where Picking_id=@PickId
			Set @Cant_conf=@Cant_conf- @Cant
		End
		Else
		Begin
			If @Cant> @Cant_conf and @Cant_conf > 0
			Begin
				Set @Dif= @Cant - @Cant_conf
				
				Update Picking Set Cantidad=@Cant_conf, Cant_Confirmada=@Cant_conf, Fecha_Fin=Getdate(),pallet_picking=@Pallet_picking ,NRO_SERIE = @NRO_SERIE Where Picking_id=@PickId

				Insert into Picking
					Select 	 Documento_id			,Nro_Linea			,Cliente_Id			,Producto_id
							,Viaje_Id				,Tipo_Caja			,Descripcion		,@Dif
							,Nave_Cod				,Posicion_cod		,Ruta				,prop1
							,Null 					,Null				,usuario			,Null		
							,Null					,0					,'0'				,null		
							,'0'					,'0'				,'0'				,'0'
							,'0'					,null				,null				,null
							,null					,null				,null				,null
							,null					,null				,null				,hijo
							,null					,null				,null				,null
							,null					,Remito_Impreso		,Nro_Remito_PF		,ISNULL(PICKING_ID_REF,PICKING_ID)
							,null					,BULTOS_NO_CONTROLADOS					,FLG_PALLET_HOMBRE
							,TRANSF_TERMINADA		,NRO_LOTE			,NRO_PARTIDA		,NULL
					From	Picking
					Where	Picking_id=@PickId
					

				Set @Cant_conf=0
			End
			Else
			Begin
				If @Cant_Conf=0
				Begin
					Update Picking Set Fecha_Inicio=Null, Fecha_Fin=Null, Pallet_Picking=null where picking_id=@PickId
				End
			End
		End	
		Fetch Next From @Cur Into @PickId,@Cant

	End --Fin While.


	select 	@cantidad=count(picking_id)
	from	picking
	where 	LTRIM(RTRIM(UPPER(viaje_id)))=ltrim(upper(rtrim(@viajeid)))


	select 	@dif=count(picking_id)
	from 	picking 
	where 	LTRIM(RTRIM(UPPER(viaje_id)))=ltrim(upper(rtrim(@viajeid)))
			and fecha_inicio is not null
			and fecha_fin is not null
			and pallet_picking is not null
			and usuario is not null
			and cant_confirmada is not null

	if @cantidad=@dif
		begin
			update picking set fin_picking='2' where viaje_id=@viajeid
		end

	Close @Cur
	Deallocate @Cur

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

ALTER PROCEDURE [dbo].[FRONTERA_BUSCADOR_CLIENTDEF]
@CLIENTE	VARCHAR(15) OUTPUT
AS
BEGIN
	DECLARE @COUNT SMALLINT 

	SELECT 	@COUNT=COUNT(*)
	FROM	RL_SYS_CLIENTE_USUARIO
	WHERE 	USUARIO_ID IN(SELECT USUARIO_ID FROM #TEMP_USUARIO_LOGGIN)

	IF @COUNT=1
	BEGIN
		SELECT 	@CLIENTE=CLIENTE_ID
		FROM	RL_SYS_CLIENTE_USUARIO
		WHERE 	USUARIO_ID IN(SELECT USUARIO_ID FROM #TEMP_USUARIO_LOGGIN)
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

/*
	- NUMERO DE PALLET
	- SUCURSAL (CODIGO)
	- CODIGO PRODUCTO
	- CANTIDAD
	- FECHA_PICKING
*/

ALTER PROCEDURE [dbo].[GEN_COT]
@FECHA_PARAM AS VARCHAR(8)
AS
BEGIN
	SELECT	 P.PALLET_FINAL AS [PALLET]
			,S.SUCURSAL_ID AS [SUCURSAL]
			,P.PRODUCTO_ID AS [CODIGO PRODUCTO]
			,SUM(P.CANTIDAD) AS [CANTIDAD]
			,CONVERT(VARCHAR,FECHA_FIN,103) AS [FECHA]
	FROM	PICKING P INNER JOIN DET_DOCUMENTO DD		ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D						ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S						ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_DESTINO)
	WHERE	P.PALLET_FINAL IS NOT NULL
			AND P.PALLET_FINAL=P.PALLET_PICKING
			AND P.PALLET_CERRADO='1'
			AND CONVERT(VARCHAR,FECHA_FIN,103)=CONVERT(VARCHAR,CAST(@FECHA_PARAM AS DATETIME),103) 
	GROUP BY P.PALLET_FINAL, S.SUCURSAL_ID, P.PRODUCTO_ID,CONVERT(VARCHAR,FECHA_FIN,103)

END--FIN PROCEDURE
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

ALTER PROCEDURE [dbo].[GEN_INTERFAZ_EXPEDICION]
@VIAJE	AS VARCHAR(100) OUTPUT
AS
BEGIN
	SELECT	DISTINCT
			D.CLIENTE_ID +';' + S.SUCURSAL_ID + ';' + CAST(P.PALLET_FINAL AS VARCHAR)
	FROM	PICKING P INNER JOIN DET_DOCUMENTO DD ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D	ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S	ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
	WHERE	VIAJE_ID=@VIAJE
			AND PALLET_CERRADO='1'


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

ALTER procedure [dbo].[Gen_Interfaz_VC]
@cliente_id as varchar(20)
as
Begin
	Declare @cViaje			as cursor
	Declare @Viaje_Id		as varchar(100)
	Declare @cProducto		as cursor
	Declare @Producto_id	as varchar(30)
	Declare @Documento_id	as numeric(20,0)
	--
	Declare @Doc_Ext		as varchar(100)
	Declare @qty_sol		as float
	Declare @qty_doc		as float
	Declare @qty_pik		as float
	Declare @StreamLine		as varchar(max)
	Declare @PATH			as varchar(max)
	Declare @Cabecera		as char(1)

	set @Cabecera='0'
	SET @PATH='C:\Gt - Warp\Interfaces VitalCan\' + @cliente_id + '_RPT_DIF_' + CAST(DAY(GETDATE())AS VARCHAR) +'-'+ CAST(MONTH(GETDATE()) AS VARCHAR) +'-' + CAST(YEAR(GETDATE())AS VARCHAR)+'.CSV'

	select	dd.doc_ext,dd.producto_id, sum(dd.cantidad_solicitada) as [Cantidad_Solicitada],
			null as [QTY_DOCUMENTO], Null as[QTY_PICKING], documento_id into #temporal
	from	sys_int_documento d (nolock) inner join sys_int_det_documento dd (nolock)
			on(d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
	where	d.codigo_viaje in(	select	DISTINCT
										viaje_id
								from	picking p inner join documento d on(p.documento_id=d.documento_id)
								where	p.cliente_id=@cliente_id
										AND FACTURADO='1'
										AND FIN_PICKING='2'
										AND isnull(d.observaciones,'')<>'ENVIADO'
								)
	group by
			dd.doc_ext, dd.producto_id,dd.documento_id	
	
	set @cProducto = cursor for
		select producto_id, documento_id from #temporal
		
	open @cProducto
	Fetch Next from @cProducto into @Producto_id, @Documento_id
	While @@Fetch_Status=0
	Begin
		Update #temporal set qty_documento= (	Select	isnull(Sum(Cantidad),0)
												from	Det_documento
												where	documento_id=@Documento_id
														and producto_id=@Producto_id)
		Where	documento_id=@Documento_id and producto_id=@Producto_id

		Update #temporal set qty_picking= (	Select	isnull(Sum(p.cant_confirmada),0)
										from	det_documento dd inner join Picking p on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
										where	dd.documento_id=@Documento_id
												and dd.producto_id=@Producto_id)
		Where	documento_id=@Documento_id and producto_id=@Producto_id

		Fetch Next from @cProducto into @Producto_id, @Documento_id
	End--fin while @cProducto
	close @cproducto
	deallocate @cproducto	
	--Comienzo con los controles de rutina.
	Set @cproducto=cursor for
		select	doc_ext, producto_id,cantidad_solicitada, qty_documento, qty_picking
		from	#temporal
	open @cproducto
	fetch next from @cproducto into @doc_ext, @producto_id, @qty_sol, @qty_doc, @qty_pik
	while @@fetch_status=0
	begin
		if @cabecera='0'
		begin
			Set @StreamLine= 'NRO_REMITO;CODIGO PRODUCTO;CANTIDAD SOLICITADA;CANTIDAD ASIGNADA;CANTIDAD PICKEADA;'
			EXEC sp_AppendToFile @Path, @StreamLine
			Set @Cabecera='1'
		end
		Set @StreamLine=	cast(@doc_ext as varchar) + ';' +
							cast(@producto_id as varchar) + ';' +
							cast(@qty_sol as varchar) + ';' +
							cast(@qty_doc as varchar) + ';' +
							cast(@qty_pik as varchar) + ';'
		if (@qty_sol<>@qty_doc)
		begin
			EXEC sp_AppendToFile @Path, @StreamLine
		end
		else
		begin
			if (@qty_doc<>@qty_pik)
			begin
				EXEC sp_AppendToFile @Path, @StreamLine
			end
		end
		fetch next from @cproducto into @doc_ext, @producto_id, @qty_sol, @qty_doc, @qty_pik
	end --Fin cursor de escritural.
	close @cproducto
	deallocate @cproducto
	Update	documento set observaciones='ENVIADO' 
	where	documento_id in(select distinct documento_id from #temporal)
	drop table #temporal
End--Fin Procedure
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

ALTER        PROCEDURE [dbo].[GET_VALUE_FOR_SEQUENCE]
@SECUENCIA AS VARCHAR(50)OUTPUT,
@VALUE AS NUMERIC(38) OUTPUT
AS

DECLARE @VARAUX AS NUMERIC(38)

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
BEGIN TRANSACTION
		SET @VALUE=0
		SELECT @VARAUX=	VALOR
		FROM SECUENCIA
		WHERE UPPER(NOMBRE)=UPPER(LTRIM(RTRIM(@SECUENCIA)))
		if @VARAUX IS NOT NULL 
			BEGIN
				SET @VALUE=@VARAUX + 1
				UPDATE SECUENCIA SET VALOR=@VALUE WHERE NOMBRE=UPPER(LTRIM(RTRIM(@SECUENCIA)))
			END
		ELSE
			BEGIN
				ROLLBACK TRANSACTION
				RAISERROR ('LA SEQUENCIA NO EXISTE EN LA TABLA. SQLSERVER', 16, 1)
	
			END
COMMIT TRANSACTION	
RETURN
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

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[GenerarNumeroContenedora] 
	-- Add the parameters for the stored procedure here
	@SEQ numeric(20,0) output
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    EXEC GET_VALUE_FOR_SEQUENCE 'PALLET_PICKING', @SEQ OUTPUT
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

ALTER PROCEDURE [dbo].[GET_AGENTESBYVIAJE]
@VIAJE AS VARCHAR(100) OUTPUT
AS
BEGIN

	SELECT 	DISTINCT
			D.SUCURSAL_DESTINO, S.NOMBRE
	FROM	DOCUMENTO D(NOLOCK) INNER JOIN SUCURSAL S (NOLOCK)
			ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
	WHERE	D.NRO_DESPACHO_IMPORTACION=@VIAJE

END -- FIN PROCEDURE
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

ALTER PROCEDURE [dbo].[GET_CANT_STOCK_ENVASES]
@CLIENTE_ID 	AS VARCHAR(15),
@PRODUCTO_ID 	AS VARCHAR(30),
@CANTIDAD		AS FLOAT OUTPUT
AS
BEGIN

	SELECT 	@CANTIDAD=X.A - ISNULL(X.B,0)
	FROM
	(	
		SELECT 	SUM(RL.CANTIDAD)AS A,
				(
						SELECT 	SUM(DD2.CANTIDAD)
						FROM	DET_DOCUMENTO DD2 
								INNER JOIN DOCUMENTO D
								ON(DD2.DOCUMENTO_ID=D.DOCUMENTO_ID)
						WHERE	D.STATUS='D20' AND DD2.PRODUCTO_ID=Ltrim(Rtrim(Upper(@PRODUCTO_ID))) and 
								D.CLIENTE_ID=Ltrim(Rtrim(Upper(@CLIENTE_ID)))
				) AS B
		FROM	RL_DET_DOC_TRANS_POSICION RL
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
				INNER JOIN DET_DOCUMENTO DD
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
		WHERE	DD.PRODUCTO_ID=Ltrim(Rtrim(Upper(@PRODUCTO_ID))) AND RL.CAT_LOG_ID='DISPONIBLE' AND RL.DOC_TRANS_ID_EGR IS NULL 
				AND DOC_TRANS_ID_TR IS NULL and DD.CLIENTE_ID=Ltrim(Rtrim(Upper(@CLIENTE_ID)))
	) AS X

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

-- ESTE PROCEDURE SE ULITIZA CUANDO ES NECESARIO VERIFICAR LA CANTIDAD

ALTER   PROCEDURE [dbo].[GET_CANTIDAD_SOLICITADA]
@DOCUMENTOID NUMERIC(20,0),
@NROLINEA  NUMERIC(10,0)
AS

BEGIN
	SELECT 	CANT_SOLICITADA
	FROM	DET_DOCUMENTO
	WHERE 	DOCUMENTO_ID=@DOCUMENTOID
			AND
			NRO_LINEA=@NROLINEA
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

ALTER    PROCEDURE	[dbo].[Get_CargaDocumento] 
						@DOC_EXT varchar(100) output
AS
BEGIN

	
	SELECT	SD.DOC_EXT
			,SD.CODIGO_VIAJE
			,UPPER(S.NOMBRE) AS SUCURSAL
			,UPPER(SDD.PRODUCTO_ID) AS PRODUCTO_ID
			,SDD.CANTIDAD_SOLICITADA AS CANTIDAD
			,SDD.UNIDAD_ID
			,SDD.DESCRIPCION
		 	,Su.nombre AS USOINTERNOUsuario
	 		,tul.Terminal AS USOINTERNOTerminal
	FROM    SYS_INT_DOCUMENTO SD
			INNER JOIN SYS_INT_DET_DOCUMENTO SDD ON(SD.DOC_EXT=SDD.DOC_EXT) 
			INNER JOIN SUCURSAL S ON(S.SUCURSAL_ID=SD.AGENTE_ID) 
			,#TEMP_USUARIO_LOGGIN TUL 
			INNER JOIN SYS_USUARIO SU ON (TUL.USUARIO_ID = SU.USUARIO_ID)
	WHERE	SD.DOC_EXT = @DOC_EXT
	/*
	GROUP BY SD.DOC_EXT
			,SD.CODIGO_VIAJE
			,SUCURSAL_ID 
			,S.NOMBRE
			,PRODUCTO_ID
			,SDD.CANTIDAD_SOLICITADA
			,SDD.UNIDAD_ID
			,SDD.DESCRIPCION
			,Su.nombre
			,tul.Terminal */

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

ALTER PROCEDURE [dbo].[GET_CLIENTES_BY_USER]
@USER	VARCHAR(30)
AS
BEGIN
	SELECT	UPPER(R.CLIENTE_ID) [CLIENTE_ID], UPPER(C.RAZON_SOCIAL) AS [RAZONSOCIAL]
	FROM	SYS_USUARIO S INNER JOIN RL_SYS_CLIENTE_USUARIO R
			ON(S.USUARIO_ID=R.USUARIO_ID)
			INNER JOIN CLIENTE C
			ON(R.CLIENTE_ID=C.CLIENTE_ID)
	WHERE	R.USUARIO_ID=LTRIM(RTRIM(UPPER(@USER)))
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

ALTER PROCEDURE [dbo].[GET_CLIENTES_FOR_RODC]
AS
BEGIN
	DECLARE @USR	VARCHAR(50)
	
	SELECT @USR=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	SELECT 	CLIENTE_ID
	FROM 	RL_SYS_CLIENTE_USUARIO 
	WHERE	USUARIO_ID=@USR

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

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[Get_Contenedoras]
	-- Add the parameters for the stored procedure here
	@cliente_id	varchar(15),
	@nro_remito varchar(30)	
AS
BEGIN

	SET NOCOUNT ON;

	SELECT	distinct pallet_picking
	FROM	PICKING P INNER JOIN DOCUMENTO D
			ON(D.DOCUMENTO_ID=P.DOCUMENTO_ID)
	WHERE	TIPO_OPERACION_ID='EGR' 
			AND P.FACTURADO='0'
			AND P.ST_CAMION='0'
			AND P.PALLET_CONTROLADO='1'
			AND P.CLIENTE_ID = @cliente_id
			AND D.NRO_REMITO = @nro_remito
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

ALTER PROCEDURE [dbo].[GET_COT]
@FECHA	AS VARCHAR(15)
AS
BEGIN

	SELECT	
			P.PALLET_FINAL, 
			CONVERT(VARCHAR,GETDATE(),103) AS FECHA, 
			S.SUCURSAL_ID,
			P.PRODUCTO_ID, 
			SUM(CANT_CONFIRMADA) AS CANTIDAD,
			d.nro_despacho_importacion
	FROM    PICKING P INNER JOIN DET_DOCUMENTO DD ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_DESTINO)
			INNER JOIN SYS_INT_DOCUMENTO SD ON(SD.CLIENTE_ID=D.CLIENTE_ID AND SD.DOC_EXT=D.NRO_REMITO)
	WHERE	PALLET_FINAL IS NOT NULL
			AND PALLET_CERRADO='1'
			AND CONVERT(VARCHAR,FECHA_SOLICITUD_CPTE,103)=CONVERT(VARCHAR,CAST(@FECHA AS DATETIME),103)
	GROUP BY
			P.PALLET_FINAL, S.SUCURSAL_ID, P.PRODUCTO_ID, D.NRO_DESPACHO_IMPORTACION
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

ALTER PROCEDURE [dbo].[GET_DATA_CONNECTION_RODC]
@IP		AS VARCHAR(13) OUTPUT,
@PORT	AS VARCHAR(5) OUTPUT
AS
BEGIN
	SELECT 	@IP=VALOR 
	FROM 	SYS_PARAMETRO_PROCESO 
	WHERE 	PROCESO_ID='WARP' AND SUBPROCESO_ID='LISTENER'	 AND PARAMETRO_ID='IP_SERVER'

	SELECT 	@PORT=VALOR 
	FROM 	SYS_PARAMETRO_PROCESO 
	WHERE 	PROCESO_ID='WARP' AND SUBPROCESO_ID='LISTENER'	 AND PARAMETRO_ID='PORT'

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

ALTER Procedure [dbo].[Get_Default_Envase]
@Documento_id		as Numeric(20,0),
@Nro_Linea			as Numeric(10,0)
As
Begin
	Declare	@Count 			as Int
	Declare @Cliente_id		as Varchar(15)
	Declare @Producto_id	as Varchar(30)

	Select 	@Cliente_id=Cliente_id, @Producto_id=Producto_id
	from	det_documento
	where	documento_id=@Documento_id
			and nro_linea=@Nro_Linea
	
	SELECT 	NAVE_ID,
			POSICION_ID
	FROM	RL_PRODUCTO_POSICION_PERMITIDA
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID

	IF @@ROWCOUNT =0
	BEGIN
		RAISERROR('No hay ubicacion default para el envase %s .',16,1,@Producto_id)
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

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[GET_DESCRIPCION_UNIDAD_PRODUCTO]
@CLIENTE_ID		VARCHAR(20),
@PRODUCTO_ID	VARCHAR(30),
@DESCRIPCION	VARCHAR(200) OUTPUT,
@UNIDAD			VARCHAR(50)  OUTPUT,
@USA_NROLOTE	VARCHAR(1)   OUTPUT,
@USA_NROPARTIDA VARCHAR(1)   OUTPUT
AS
BEGIN

SELECT @DESCRIPCION = P.DESCRIPCION,@UNIDAD = UM.DESCRIPCION, @USA_NROLOTE = ingLoteProveedor, @USA_NROPARTIDA = ingPartida
FROM PRODUCTO P
INNER JOIN UNIDAD_MEDIDA UM ON(P.UNIDAD_ID = UM.UNIDAD_ID)
WHERE CLIENTE_ID = @CLIENTE_ID
AND PRODUCTO_ID = @PRODUCTO_ID

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

ALTER Procedure [dbo].[Get_Documento_Trans_Lock]
As
Begin

	SELECT 	DD.DOC_TRANS_ID,	U.NOMBRE, DD.TERMINAL, DD.ESTACION_ACTUAL
			,CASE 	DD.TIPO_OPERACION_ID 
					WHEN 'ING' 	THEN 'INGRESO' 
					WHEN 'EGR' 	THEN 'EGRESO'
					WHEN 'TR' 	THEN 'TRANSFERENCIA'
					WHEN 'INV'	THEN 'INVENTARIO'
			END AS TIPO_OPERACION
	FROM 	DOCUMENTO_TRANSACCION DD
			INNER JOIN SYS_USUARIO U
			ON(DD.USUARIO_ID=U.USUARIO_ID)
	WHERE	DD.TR_ACTIVO=1 AND DD.STATUS NOT IN ('T40')
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

ALTER PROCEDURE [dbo].[GET_INFO_INV_INICIAL]
AS
BEGIN
		
		SELECT 	 Count(mob_id) AS [CONTEOS REALIZADOS]
				, POSICION_COD AS [CODIGO DE POSICION]
				, PRODUCTO_ID AS [PRODUCTO ID]
				, NRO_LOTE AS [NRO LOTE]
				,NRO_PALLET AS [NRO PALLET]
				,F_VTO AS [FECHA VENCIMIENTO]
				,QTY AS [CANTIDAD]
				,LOTE_PROVEEDOR AS [LOTE DEL PROVEEDOR]
				,(PRODUCTO_ID + NRO_PALLET + NRO_LOTE + LOTE_PROVEEDOR) AS [KEY]
		FROM	MOB_EXISTENCIA_INICIAL
		GROUP BY 
				POSICION_COD, PRODUCTO_ID, NRO_LOTE, NRO_PALLET,F_VTO,QTY,LOTE_PROVEEDOR


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

ALTER PROCEDURE [dbo].[GET_LOCK_RECEPCION]
AS
BEGIN
	
	SELECT 	L.CLIENTE_ID, L.DOC_EXT, L.USUARIO_ID, U.NOMBRE, L.TERMINAL
			, CONVERT(VARCHAR,FECHA_LOCK,103) + ' ' + CAST(DATEPART(HH,FECHA_LOCK) AS VARCHAR(2)) + ':' 
			  + CAST(DATEPART(MI,FECHA_LOCK) AS VARCHAR(2))
			AS FECHA
	FROM 	SYS_LOCK_RECEPCION L (NOLOCK) INNER JOIN SYS_INT_DOCUMENTO S(NOLOCK)
			ON(L.CLIENTE_ID=S.CLIENTE_ID AND L.DOC_EXT=S.DOC_EXT)
			INNER JOIN SYS_INT_DET_DOCUMENTO SDD
			ON(S.CLIENTE_ID=SDD.CLIENTE_ID AND S.DOC_EXT=SDD.DOC_EXT)
			INNER JOIN SYS_USUARIO U (NOLOCK)
			ON(L.USUARIO_ID=U.USUARIO_ID)
	WHERE	SDD.ESTADO_GT IS NULL AND SDD.FECHA_ESTADO_GT IS NULL
			AND L.LOCK='1'

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

ALTER PROCEDURE [dbo].[GET_MONITORPOSICIONES]
AS
BEGIN
	SET NOCOUNT ON
	SELECT    P.POSICION_ID
			,0 					AS [CHECK]
			,P.POSICION_COD	AS [POSICION]
			,PL.MOTIVO_ID		
			,ML.DESCRIPCION 	AS [DESC_MOTIVO]
			,S.NOMBRE			AS [USER_NAME]
			,PL.TRM_LCK			AS [TERMINAL]
			,PL.F_LCK			AS [FECHA_LOCK]
			,PL.OBS_LCK			AS [OBSERVACIONES]
	FROM 	POSICION P INNER JOIN LOCKEO_POSICION PL
			ON(P.POSICION_ID=PL.POSICION_ID)
			INNER JOIN MOTIVO_LOCKEO ML
			ON(PL.MOTIVO_ID=ML.MOTIVO_ID)
			INNER JOIN SYS_USUARIO S
			ON(PL.USR_LCK=S.USUARIO_ID)
	WHERE	P.POS_LOCKEADA='1'
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

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[GET_nroRemitos]
	-- Add the parameters for the stored procedure here
	@cliente_id	varchar(15)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	SELECT	DISTINCT 
			NRO_REMITO 
	FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
			ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN PICKING P
			ON(DD.DOCUMENTO_ID=P.DOCUMENTO_ID AND DD.NRO_LINEA=P.NRO_LINEA)
	WHERE	TIPO_OPERACION_ID='EGR' 
			AND NRO_REMITO IS NOT NULL
			AND P.FACTURADO='0'
			AND P.ST_CAMION='0'
			AND P.CLIENTE_ID=@cliente_id
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

ALTER   Procedure [dbo].[Get_Picking_Wave]
@Doc_trans_id	as Numeric(20,0) Output
As
Begin
	Declare @Wave 	As Numeric(20,0)

	Select Distinct @Wave=Wave_Id From Sys_Picking_Wave Where Doc_Trans_Id=@Doc_Trans_Id

	Select 
		 	 P.Posicion_cod
			,P.Documento_id
			,P.Nro_Linea
			,P.Cliente_id
			,P.Producto_Id
			,P.Descripcion
			,P.Cantidad
			,DD.Nro_Serie
			,DD.Nro_Bulto
			,DD.Nro_Lote
			,DD.Nro_Despacho
			,DD.Nro_Partida
			,DD.Unidad_Id
			,DD.Fecha_Vencimiento
			,DD.Moneda_id
			,DD.Prop1
			,DD.Prop2
			,DD.Prop3
	From	Picking P Inner Join Det_Documento DD
			On(P.Documento_id=DD.Documento_id And P.Nro_linea=DD.Nro_linea)
			Inner Join Det_Documento_Transaccion DDT
			On(DD.Documento_Id=DDT.Documento_Id And DD.Nro_Linea=DDT.Nro_Linea_Doc)
	Where	DDT.Doc_Trans_Id In(
									Select 	Doc_Trans_Id
									From	Sys_Picking_Wave
									Where	Wave_id=@Wave
									)
	Order By
			P.Posicion_Cod, P.Producto_Id
End;
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

ALTER     Procedure [dbo].[Get_Picking_Wave_Rpt] 
@WaveId	as Numeric(20,0) Output
As
Begin
	Declare 	@Usuario			as varchar(30)
	Declare @FechaImpresion	as Varchar(15)
	Declare @Terminal			as varchar(100)


	Select 	@Usuario=Usuario_id From #Temp_Usuario_Loggin
	Select 	@FechaImpresion=	CAST(DAY(GETDATE()) AS  VARCHAR(2)) 
								+ '/' + CAST(MONTH(GETDATE()) AS VARCHAR(2))
								+ '/' + CAST(YEAR(GETDATE()) AS VARCHAR(4))
	Select  	@Terminal=Host_Name()

	Select 
		 	 P.Posicion_cod --
			,P.Documento_id --
			,P.Nro_Linea
			,P.Cliente_id --
			,P.Producto_Id --
			,P.Descripcion --
			,P.Cantidad --
			,isnull(DD.Nro_Serie,'-') 			as Nro_Serie--
			,isnull(DD.Nro_Bulto,'-')			as Nro_Bulto--
			,isnull(DD.Nro_Lote,'-')			as Nro_Lote--
			,isnull(DD.Nro_Despacho,'-')		as Nro_Despacho--
			,isnull(DD.Nro_Partida,'-')			as Nro_Partida--
			,isnull(DD.Unidad_Id,'-')			as Unidad_Id--
			,isnull(Cast(DD.Fecha_Vencimiento as varchar),'-')	as Fecha_Vencimiento--
			,isnull(DD.Prop1,'-')				as Prop1
			,isnull(DD.Prop2,'-')				as Prop2
			,isnull(DD.Prop3,'-')				as Prop3
			,@Usuario 						as Usuario
			,@FechaImpresion 				as FechaImpresion
			,@Terminal 						as Terminal
			,PW.Wave_Id
	From	Picking P Inner Join Det_Documento DD
			On(P.Documento_id=DD.Documento_id And P.Nro_linea=DD.Nro_linea)
			Inner Join Det_Documento_Transaccion DDT
			On(DD.Documento_Id=DDT.Documento_Id And DD.Nro_Linea=DDT.Nro_Linea_Doc)
			inner Join Sys_Picking_Wave PW
			On(DDT.Doc_trans_id=PW.Doc_trans_id)
	Where	Wave_id=@WaveId
	Order By
			P.Posicion_Cod, p.Documento_id, p.Nro_linea
End;
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

ALTER      Procedure [dbo].[Get_Picking_Wave_Rpt_Cons] 
@WaveId	as Numeric(20,0) Output
As
Begin
	Declare 	@Usuario			as varchar(30)
	Declare @FechaImpresion	as Varchar(15)
	Declare @Terminal			as varchar(100)


	Select 	@Usuario=Usuario_id From #Temp_Usuario_Loggin
	Select 	@FechaImpresion=	CAST(DAY(GETDATE()) AS  VARCHAR(2)) 
								+ '/' + CAST(MONTH(GETDATE()) AS VARCHAR(2))
								+ '/' + CAST(YEAR(GETDATE()) AS VARCHAR(4))
	Select  	@Terminal=Host_Name()

	Select 
		 	 P.Posicion_cod --
			,P.Cliente_id --
			,P.Producto_Id --
			,P.Descripcion --
			,sum(P.Cantidad) 				as Cantidad				--
			,isnull(DD.Nro_Serie,'-') 			as Nro_Serie				--
			,isnull(DD.Nro_Bulto,'-')			as Nro_Bulto				--
			,isnull(DD.Nro_Lote,'-')			as Nro_Lote				--
			,isnull(DD.Nro_Despacho,'-')		as Nro_Despacho		--
			,isnull(DD.Nro_Partida,'-')			as Nro_Partida			--
			,isnull(DD.Unidad_Id,'-')			as Unidad_Id			--
			,isnull(Cast(DD.Fecha_Vencimiento as varchar),'-')	as Fecha_Vencimiento--
			,isnull(DD.Prop1,'-')				as Prop1
			,isnull(DD.Prop2,'-')				as Prop2
			,isnull(DD.Prop3,'-')				as Prop3
			,@Usuario 						as Usuario
			,@FechaImpresion 				as FechaImpresion
			,@Terminal 						as Terminal
			,PW.Wave_Id
	From	Picking P Inner Join Det_Documento DD
			On(P.Documento_id=DD.Documento_id And P.Nro_linea=DD.Nro_linea)
			Inner Join Det_Documento_Transaccion DDT
			On(DD.Documento_Id=DDT.Documento_Id And DD.Nro_Linea=DDT.Nro_Linea_Doc)
			inner Join Sys_Picking_Wave PW
			On(DDT.Doc_trans_id=PW.Doc_trans_id)
	Where	Wave_id=@WaveId
	Group By 
			P.Posicion_Cod, P.CLiente_id, P.Producto_id, P.Descripcion, DD.Nro_Serie, DD.Nro_Bulto, DD.Nro_lote, DD.Nro_Despacho, DD.Nro_Partida,
			DD.Unidad_Id, DD.Fecha_Vencimiento, DD.Prop1, DD.Prop2, DD.Prop3, PW.Wave_Id
	Order By
			P.Posicion_Cod
End;
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

ALTER   procedure [dbo].[Get_PickingByViaje]
	@Viaje_id		as Varchar(100) output
As	
Begin
	Declare @Usuario 	as varchar(20)
	Declare @sString	as varchar (200) 
	
	Select	@Usuario=Usuario_Id
	From	#temp_usuario_loggin

	SELECT 	SP.VIAJE_ID													AS [CODIGO VIAJE]
			,dbo.CLIENTE_VIAJERUTA(SP.VIAJE_ID,SP.RUTA) 				AS [CADENA_CLIENTE]
			,SP.PRODUCTO_ID												AS [PRODUCTO]
			,SP.DESCRIPCION												AS [DESCRIPCION]
			,SUM(SP.CANTIDAD)											AS [CANTIDAD]
			,SP.PROP1													AS [PALLET]
			,SP.POSICION_COD											AS [UBICACION]
			,SP.RUTA													AS [RUTA]
			,SP.TIPO_CAJA												AS [TIPO CAJA]
			,CAST(DAY(Dd.Fecha_Vencimiento)		AS VARCHAR(2)) 
				+'/'+ CAST(MONTH(Dd.Fecha_Vencimiento)	AS VARCHAR(2)) 
				+'/'+CAST(YEAR(Dd.Fecha_Vencimiento)	AS VARCHAR(4)) 	AS [FECHA VTO]
			,SPV.PRIORIDAD												AS [PRIORIDAD]
			,@Usuario													AS [USUARIO]
			,HOST_NAME()												AS [TERMINAL]
			,CAST(DAY(GETDATE())AS VARCHAR(2)) 
			+'/'+ CAST(MONTH(GETDATE()) AS VARCHAR(2)) 
			+'/'+CAST(YEAR(GETDATE()) AS VARCHAR(4)) 					AS [FECHA]
	FROM	PICKING SP (nolock)
			left JOIN PRIORIDAD_VIAJE SPV	(nolock) ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
			INNER JOIN PRODUCTO PROD		(nolock) ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
			INNER JOIN DET_DOCUMENTO DD		(nolock) ON(SP.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DD.NRO_LINEA=SP.NRO_LINEA)
			LEFT JOIN POSICION POS			(nolock) ON(POS.POSICION_COD=SP.POSICION_COD)
			LEFT JOIN NAVE NAV				(nolock) ON(SP.POSICION_COD=NAV.NAVE_COD)
	WHERE	LTRIM(RTRIM(UPPER(SP.VIAJE_ID)))=UPPER(LTRIM(RTRIM(@Viaje_id)))
	GROUP BY	
			 SP.VIAJE_ID ,SP.PRODUCTO_ID
			,SP.DESCRIPCION ,SP.RUTA
			,SP.POSICION_COD ,SP.TIPO_CAJA
			,SP.PROP1 ,PROD.UNIDAD_ID ,DD.FECHA_VENCIMIENTO
			,SPV.PRIORIDAD ,POS.ORDEN_PICKING
	ORDER BY	
			SP.RUTA, CAST(ISNULL(SP.TIPO_CAJA,0) AS NUMERIC(10,1)) DESC,POS.ORDEN_PICKING, SP.POSICION_COD, SP.PRODUCTO_ID
		
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

ALTER       PROCEDURE [dbo].[Get_PickingWaveGen]
@PTransaccion_ID	as Varchar(15) Output
As
Begin

	Declare 	@Usuario 	as varchar(30)

	Select 	@Usuario=Usuario_Id From #Temp_Usuario_Loggin
	SELECT	DISTINCT
			'0' as [check],
			D.CLIENTE_ID,
			(SELECT RAZON_SOCIAL FROM  CLIENTE WHERE CLIENTE_ID=D.CLIENTE_ID) AS [CLIENTE_DESC],
			D.DOCUMENTO_ID,
			DT.TRANSACCION_ID,
			TR.DESCRIPCION AS TRANSACCION,
			DT.DOC_TRANS_ID,
			D.TIPO_COMPROBANTE_ID [TIPO_COMPROBANTE_ID],
			IsNull(D.TIPO_OPERACION_ID, DT.TIPO_OPERACION_ID) [TIPO_OPERACION_ID],
			DT.STATUS AS STATUS_TR,
			WP.WAVE_ID
	FROM 	DET_DOCUMENTO_TRANSACCION DDT
			INNER JOIN DOCUMENTO_TRANSACCION DT 	ON (DT.DOC_TRANS_ID=DDT.DOC_TRANS_ID)
			INNER JOIN DET_DOCUMENTO         DD 			ON DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA
			LEFT JOIN DOCUMENTO               D 				ON DDT.DOCUMENTO_ID=D.DOCUMENTO_ID
			INNER JOIN TRANSACCION              TR 			ON (TR.TRANSACCION_ID = DT.TRANSACCION_ID)
			INNER JOIN RL_TRANSACCION_ESTACION  R1 	ON (DT.ESTACION_ACTUAL = R1.ESTACION_ID AND DT.TRANSACCION_ID = R1.TRANSACCION_ID)
			INNER JOIN SYS_PICKING_WAVE WP			ON(DT.DOC_TRANS_ID=WP.DOC_TRANS_ID)
	WHERE 	1 <> 0  AND DT.ESTACION_ACTUAL =LTRIM(RTRIM(UPPER(@PTransaccion_ID)))
			AND DT.TIPO_OPERACION_ID = 'EGR'
			AND DDT.CLIENTE_ID IN 	(	SELECT 	CLIENTE_ID 
										FROM	CLIENTE
										WHERE  (	SELECT 	CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END
													FROM   	rl_sys_cliente_usuario
													WHERE  	cliente_id = dd.cliente_id
															And    usuario_id =@Usuario) = 1)
	ORDER BY 1,3,4


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

ALTER PROCEDURE [dbo].[Get_PickingWaveProdDesc]
@DOC_TRANS_ID	numeric(20,0) output
As
Begin

	SELECT     P.PRODUCTO_ID
	   ,DESCRIPCION
	   ,CANTIDAD
	   ,PROP1 AS NRO_PALLET
	   ,POSICION_COD AS UBICACION
	   ,RUTA
	FROM    DET_DOCUMENTO_TRANSACCION DDT INNER JOIN PICKING P
	   ON(DDT.DOCUMENTO_ID=P.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=P.NRO_LINEA)
	WHERE    DDT.DOC_TRANS_ID=@DOC_TRANS_ID
	ORDER BY
	   P.RUTA, POSICION_COD 

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

ALTER Procedure [dbo].[Get_Remanente_CrossDock]
@Documento_Id Numeric(20,0)
As
Begin
	SELECT	dd.documento_id, dd.nro_linea, dd.prop1
	FROM 	rl_det_doc_trans_posicion rl
	        	inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
	        	inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			left join nave n on(rl.nave_actual=n.nave_id)
			left join posicion p on(rl.posicion_actual=p.posicion_id)
	WHERE	dd.documento_id=@Documento_id
			and n.pre_ingreso='1'
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

ALTER PROCEDURE [dbo].[GET_TAREAS_INVENTARIO]
@INVENTARIO_ID NUMERIC(20,0)
AS
BEGIN 
	DECLARE @TAREAS AS NUMERIC(20,0)
	DECLARE @CONTEO AS NUMERIC(20,0)
	DECLARE @USUARIO_ID AS VARCHAR(20)
	DECLARE @TAREAS_PENDIENTES AS NUMERIC(20,0)
	DECLARE @MARBETE AS NUMERIC(20,0)
	DECLARE @sql_str AS VARCHAR(MAX)
		
	--SET @USUARIO_ID = 'SGOMEZ'
	SELECT @USUARIO_ID=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	SELECT @TAREAS = QTY_TASK_USER,@CONTEO = NRO_CONTEO FROM INVENTARIO WHERE INVENTARIO_ID = @INVENTARIO_ID
	
	SELECT @TAREAS_PENDIENTES = COUNT(*) 
	FROM RL_DET_CONTEO_USUARIO RL
	INNER JOIN DET_CONTEO DC ON (RL.INVENTARIO_ID = DC.INVENTARIO_ID AND RL.MARBETE = DC.MARBETE)
	WHERE RL.USUARIO_ID = @USUARIO_ID AND RL.INVENTARIO_ID = @INVENTARIO_ID AND RL.FECHA_FIN IS NULL
	AND (
			 (@CONTEO = 1 AND DC.CONTEO1 IS NULL) OR
			 (@CONTEO = 2 AND DC.CONTEO2 IS NULL) OR
			 (@CONTEO = 3 AND DC.CONTEO3 IS NULL))

	SET @TAREAS = @TAREAS - @TAREAS_PENDIENTES
	
	IF @TAREAS > 0
	BEGIN

		SET @sql_str = 'INSERT INTO RL_DET_CONTEO_USUARIO (INVENTARIO_ID, MARBETE,USUARIO_ID,NRO_CONTEO,FECHA_INICIO,FECHA_FIN) '
		SET @sql_str = @sql_str + 'SELECT TOP ' + STR(@TAREAS) 
		SET @sql_str = @sql_str + ' DC.INVENTARIO_ID, DC.MARBETE,'''+ @USUARIO_ID +''' AS USUARIO_ID,' 
		SET @sql_str = @sql_str + STR(@CONTEO)  + ' AS NRO_CONTEO ,GETDATE() AS FECHA_INICIO, NULL AS FECHA_FIN '  
		SET @sql_str = @sql_str + 'FROM DET_CONTEO DC ' 
		SET @sql_str = @sql_str + 'INNER JOIN DET_INVENTARIO DI ON (DC.INVENTARIO_ID = DI.INVENTARIO_ID AND DC.MARBETE = DI.MARBETE) '
		SET @sql_str = @sql_str + 'LEFT JOIN POSICION P ON (P.POSICION_ID = DC.POSICION_ID) '
		SET @sql_str = @sql_str + 'WHERE DC.MARBETE NOT IN '
		SET @sql_str = @sql_str + '(SELECT MARBETE FROM RL_DET_CONTEO_USUARIO '
		SET @sql_str = @sql_str + 'WHERE INVENTARIO_ID = ' + STR(@INVENTARIO_ID)+' AND FECHA_FIN IS NULL'
		SET @sql_str = @sql_str + ' AND NRO_CONTEO = ' + STR(@CONTEO) + ' ) AND DC.INVENTARIO_ID = ' +STR(@INVENTARIO_ID)   
		IF @CONTEO = 1 
		BEGIN
			SET @sql_str = @sql_str + ' AND CONTEO1 IS NULL '
		END
		IF @CONTEO = 2 
		BEGIN
			SET @sql_str = @sql_str + ' AND CONTEO2 IS NULL AND CONTEO1 <> DI.CANT_STOCK_CONT_1 '
		END
		IF @CONTEO = 3 
		BEGIN
			SET @sql_str = @sql_str + ' AND CONTEO3 IS NULL AND CONTEO2 <> CANT_STOCK_CONT_2 '
			SET @sql_str = @sql_str + ' AND CONTEO2 IS NOT NULL '
		END
		SET @sql_str = @sql_str + ' ORDER BY POSICION_COD '
		
		EXECUTE (@sql_str)

	END
	SELECT 
		DC.MARBETE,
		POSICION_COD AS POSICION,
		DC.CLIENTE_ID AS CLIENTE,
		DC.PRODUCTO_ID AS PRODUCTO,
		--MGR 20120312 Se muestra la descripcion del producto
        PR.DESCRIPCION AS DESCRIPCION, 
		UM.DESCRIPCION AS UNIDAD,
		CASE WHEN PR.ingLoteProveedor = '1' THEN DI.NRO_LOTE ELSE '' END AS NRO_LOTE,
		CASE WHEN PR.ingPartida = '1' THEN DI.NRO_PARTIDA ELSE '' END AS NRO_PARTIDA
		FROM RL_DET_CONTEO_USUARIO RL
		INNER JOIN DET_CONTEO DC ON(DC.MARBETE = RL.MARBETE AND DC.INVENTARIO_ID = RL.INVENTARIO_ID) 
		INNER JOIN DET_INVENTARIO DI ON (DI.INVENTARIO_ID = DC.INVENTARIO_ID AND DI.MARBETE = DC.MARBETE)
		INNER JOIN POSICION PS ON(DC.POSICION_ID = PS.POSICION_ID)
		INNER JOIN PRODUCTO PR ON(DC.PRODUCTO_ID = PR.PRODUCTO_ID AND PR.CLIENTE_ID = DC.CLIENTE_ID)
		INNER JOIN UNIDAD_MEDIDA UM ON(UM.UNIDAD_ID = PR.UNIDAD_ID)
		WHERE RL.USUARIO_ID = @USUARIO_ID AND RL.FECHA_FIN IS NULL AND RL.INVENTARIO_ID = @INVENTARIO_ID
			AND (
				 (@CONTEO = 1 AND DC.CONTEO1 IS NULL) OR
				 (@CONTEO = 2 AND DC.CONTEO2 IS NULL) OR
				 (@CONTEO = 3 AND DC.CONTEO3 IS NULL))

		--ORDER BY DC.MARBETE
	UNION ALL
	SELECT 
		DC.MARBETE,
		N.NAVE_COD AS POSICION,
		DC.CLIENTE_ID AS CLIENTE,
		DC.PRODUCTO_ID AS PRODUCTO,
		--MGR 20120312 Se muestra la descripcion del producto
        PR.DESCRIPCION AS DESCRIPCION, 
		UM.DESCRIPCION AS UNIDAD,
		CASE WHEN PR.ingLoteProveedor = '1' THEN DI.NRO_LOTE ELSE '' END AS NRO_LOTE,
		CASE WHEN PR.ingPartida = '1' THEN DI.NRO_PARTIDA ELSE '' END AS NRO_PARTIDA
		FROM RL_DET_CONTEO_USUARIO RL
		INNER JOIN DET_CONTEO DC ON(DC.MARBETE = RL.MARBETE AND DC.INVENTARIO_ID = RL.INVENTARIO_ID) 
		INNER JOIN DET_INVENTARIO DI ON (DI.INVENTARIO_ID = DC.INVENTARIO_ID AND DI.MARBETE = DC.MARBETE)
		INNER JOIN NAVE N ON(DC.NAVE_ID = N.NAVE_ID)
		INNER JOIN PRODUCTO PR ON(DC.PRODUCTO_ID = PR.PRODUCTO_ID AND PR.CLIENTE_ID = DC.CLIENTE_ID)
		INNER JOIN UNIDAD_MEDIDA UM ON(UM.UNIDAD_ID = PR.UNIDAD_ID)
		WHERE RL.USUARIO_ID = @USUARIO_ID AND RL.FECHA_FIN IS NULL AND RL.INVENTARIO_ID = @INVENTARIO_ID
			AND (
				 (@CONTEO = 1 AND DC.CONTEO1 IS NULL) OR
				 (@CONTEO = 2 AND DC.CONTEO2 IS NULL) OR
				 (@CONTEO = 3 AND DC.CONTEO3 IS NULL))

		ORDER BY DC.MARBETE



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

ALTER   PROCEDURE [dbo].[GET_VALUES_FOR_WARPEI]
AS BEGIN

	SET LANGUAGE ESPAÑOL

	SELECT 	PRODUCTO_ID	AS [PRODUCTO], 
			NRO_LOTE	AS [NRO. LOTE], 
			NRO_PALLET	AS [NRO. PALLET],  
			cast(CAST(DAY(F_VTO) AS VARCHAR(2)) + '/' + CAST(MONTH(F_VTO) AS VARCHAR(2)) + '/' +	CAST(YEAR(F_VTO) AS VARCHAR(4)) as varchar(10)) AS [FECHA VENCIMIENTO],
			QTY AS [CANTIDAD],
			LOTE_PROVEEDOR AS [L. PROV.],
			ISNULL(N.NAVE_COD,N2.NAVE_COD) AS [NAVE],
			CNAV.CALLE_COD AS [CALLE],
			CN.COLUMNA_COD	AS [COLUMNA] ,
			NN.NIVEL_COD AS [NIVEL]
	FROM	MOB_EXISTENCIA_INICIAL M
			LEFT JOIN POSICION P
			ON(M.POSICION_COD=P.POSICION_COD)
			LEFT JOIN NIVEL_NAVE NN
			ON(P.NIVEL_ID=NN.NIVEL_ID)
			LEFT JOIN COLUMNA_NAVE CN
			ON(P.COLUMNA_ID=CN.COLUMNA_ID)
			LEFT JOIN CALLE_NAVE CNAV
			ON(P.CALLE_ID=CNAV.CALLE_ID)
			LEFT JOIN NAVE N
			ON(P.NAVE_ID=N.NAVE_ID)
			LEFT JOIN NAVE N2
			ON(M.POSICION_COD=N2.NAVE_COD)
			
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

ALTER PROCEDURE [dbo].[Get_Values_PickingWave]
@PTransaccion_ID	as Varchar(15) Output
As
Begin

	Declare 	@Usuario 	as varchar(30)

	Select 	@Usuario=Usuario_Id From #Temp_Usuario_Loggin

	SELECT	DISTINCT
			'0' as [check],
			D.CLIENTE_ID,
			(SELECT RAZON_SOCIAL FROM  CLIENTE WHERE CLIENTE_ID=D.CLIENTE_ID) AS [CLIENTE_DESC],
			D.DOCUMENTO_ID,
			DT.TRANSACCION_ID,
			TR.DESCRIPCION AS TRANSACCION,
			DT.DOC_TRANS_ID,
			D.TIPO_COMPROBANTE_ID [TIPO_COMPROBANTE_ID],
			IsNull(D.TIPO_OPERACION_ID, DT.TIPO_OPERACION_ID) [TIPO_OPERACION_ID],
			DT.STATUS AS STATUS_TR,
			P.RUTA 
	FROM 	DET_DOCUMENTO_TRANSACCION DDT
			INNER JOIN DOCUMENTO_TRANSACCION DT 	ON (DT.DOC_TRANS_ID=DDT.DOC_TRANS_ID)
			INNER JOIN DET_DOCUMENTO         DD 			ON DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA
			LEFT JOIN DOCUMENTO               D 				ON DDT.DOCUMENTO_ID=D.DOCUMENTO_ID
			INNER JOIN TRANSACCION              TR 			ON (TR.TRANSACCION_ID = DT.TRANSACCION_ID)
			INNER JOIN RL_TRANSACCION_ESTACION  R1 	ON (DT.ESTACION_ACTUAL = R1.ESTACION_ID AND DT.TRANSACCION_ID = R1.TRANSACCION_ID)
			LEFT JOIN PICKING P ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
	WHERE 	1 <> 0  AND DT.ESTACION_ACTUAL =LTRIM(RTRIM(UPPER(@PTransaccion_ID)))
			AND DT.TIPO_OPERACION_ID = 'EGR'
			AND DDT.CLIENTE_ID IN 	(	SELECT 	CLIENTE_ID 
										FROM	CLIENTE
										WHERE  (	SELECT 	CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END
													FROM   	rl_sys_cliente_usuario
													WHERE  	cliente_id = dd.cliente_id
															And    usuario_id =@Usuario) = 1)
			And DT.DOC_TRANS_ID NOT IN(	SELECT	DOC_TRANS_ID FROM SYS_PICKING_WAVE)
	ORDER BY 1,3,4


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

ALTER   PROCEDURE [dbo].[GET_VALUESFORETIBULTOEGR]
AS
BEGIN
	SELECT 	DD.*, C.*, D.*, S.*, DBO.GETPICKINGID(P.VIAJE_ID, P.PRODUCTO_ID, P.POSICION_COD,P.PROP1,P.RUTA) AS ID
	FROM 	#TEMP_ETIQUETA TE (NOLOCK)INNER JOIN DET_DOCUMENTO DD (NOLOCK)
			ON(TE.DOCUMENTO_ID=DD.DOCUMENTO_ID AND TE.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D(NOLOCK)
			ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN CLIENTE C(NOLOCK) 
			ON(D.CLIENTE_ID=C.CLIENTE_ID)
			INNER JOIN SUCURSAL S (NOLOCK)
			ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
			INNER JOIN PICKING P (NOLOCK)
			ON(DD.DOCUMENTO_ID=P.DOCUMENTO_ID AND DD.NRO_LINEA=P.NRO_LINEA)


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

ALTER  PROCEDURE [dbo].[GET_VEHICULOPOSICION]
@VEHICULO_ID	VARCHAR(50) OUTPUT
AS
BEGIN
	SELECT 	RL_ID, R.POSICION_ID, R.NAVE_ID,P.POSICION_COD, N.NAVE_COD,
        CASE 
		WHEN R.NAVE_ID IS NULL THEN 'CON LAYOUT' 
		ELSE 'SIN LAYOUT' 
	END AS SL
	FROM	RL_VEHICULO_POSICION R (NOLOCK)LEFT JOIN POSICION P(NOLOCK)
			ON(R.POSICION_ID=P.POSICION_ID)
			LEFT JOIN NAVE N (NOLOCK)
			ON(R.NAVE_ID=N.NAVE_ID)
	WHERE	VEHICULO_ID=@VEHICULO_ID
	ORDER BY N.NAVE_COD, P.POSICION_COD

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

ALTER   PROCEDURE [dbo].[GETDATAFORPALLET]
@NROPALLET AS VARCHAR(100),
@USUARIO AS VARCHAR(20)
AS
DECLARE @DOCUMENTO_ID AS NUMERIC(20,0)
DECLARE @NRO_LINEA   AS NUMERIC(10,0)
DECLARE @TRANSACCION_ID   AS VARCHAR(15)
DECLARE @TIPO_OPERACION_ID AS VARCHAR(5)
DECLARE @DOC_TRANS_ID AS NUMERIC(20,0)
DECLARE @VCANT AS NUMERIC(20)
DECLARE @ESTACION AS VARCHAR(15)

	BEGIN
		SELECT @DOCUMENTO_ID=X.DOCUMENTO_ID,@NRO_LINEA=NRO_LINEA, @DOC_TRANS_ID=DOC_TRANS_ID
		FROM(
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN NAVE N
					ON(RL.NAVE_ACTUAL = N.NAVE_ID)
			WHERE 	DD.PROP1=UPPER(LTRIM(RTRIM(@NROPALLET))) AND D.STATUS IN ('D30','D35')
					AND N.PRE_INGRESO='1'
		
			UNION ALL
		
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE 	DD.PROP1=UPPER(LTRIM(RTRIM(@NROPALLET))) AND D.STATUS IN ('D30','D35')
					AND	P.INTERMEDIA='1'
		
			UNION ALL
		
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN NAVE N
					ON(N.NAVE_ID=RL.NAVE_ACTUAL)
			WHERE 	DD.PROP1=UPPER(LTRIM(RTRIM(@NROPALLET))) AND D.STATUS IN ('D30','D35')
					AND	N.INTERMEDIA='1'
		) AS X

	END
---Acá validamos que el usuario tenga permiso para tomar el pallet

	IF @@ROWCOUNT =0
		BEGIN
			RAISERROR ('EL PALLET SOLICITADO NO ESTA DISPONIBLE.', 16, 1)
		END

	ELSE
	BEGIN

	SELECT 	@TRANSACCION_ID=DT.TRANSACCION_ID,@TIPO_OPERACION_ID=DT.TIPO_OPERACION_ID,@ESTACION=DT.ESTACION_ACTUAL
	FROM 	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID= DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN DOCUMENTO_TRANSACCION DT
			ON(DDT.DOC_TRANS_ID=DT.DOC_TRANS_ID)
	WHERE 	DD.DOCUMENTO_ID=@DOCUMENTO_ID 
			AND DD.NRO_LINEA=@NRO_LINEA
	
					
	

	SELECT 	@VCANT=COUNT(D.DOCUMENTO_ID)
	FROM 	DET_DOCUMENTO_TRANSACCION DDT
			INNER JOIN DOCUMENTO_TRANSACCION DT ON (DT.DOC_TRANS_ID=DDT.DOC_TRANS_ID)
			INNER JOIN DET_DOCUMENTO         DD ON DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA
			LEFT JOIN DOCUMENTO               D ON DDT.DOCUMENTO_ID=D.DOCUMENTO_ID
			INNER JOIN TRANSACCION              TR ON (TR.TRANSACCION_ID = DT.TRANSACCION_ID)
			INNER JOIN RL_TRANSACCION_ESTACION  R1 ON (DT.ESTACION_ACTUAL = R1.ESTACION_ID AND DT.TRANSACCION_ID = R1.TRANSACCION_ID)
	WHERE 	1 <> 0
			AND DT.ESTACION_ACTUAL IN	(
										SELECT 	RLS.ESTACION_ID
										FROM 	SYS_USUARIO SU INNER JOIN 
												RL_SYS_ROL_TRANS_ESTACION RLS
												ON(SU.ROL_ID=RLS.ROL_ID)
										WHERE 	SU.USUARIO_ID=@USUARIO
												AND RLS.TRANSACCION_ID=@TRANSACCION_ID
										)
			AND DT.TIPO_OPERACION_ID = @TIPO_OPERACION_ID
			AND DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.NRO_LINEA=@NRO_LINEA
			AND DDT.CLIENTE_ID IN (
									SELECT CLIENTE_ID 	
									FROM CLIENTE
									WHERE
											(
												SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END
												FROM   rl_sys_cliente_usuario
												WHERE  cliente_id = dd.cliente_id
												And    usuario_id = @USUARIO
											) = 1
									)


		IF @VCANT=0 --@@ROWCOUNT =0
		BEGIN
			RAISERROR ('NO TIENE PERMISOS SOBRE EL PALLET SELECCIONADO', 16, 1)
		END
		ELSE
		BEGIN 
		SELECT @DOCUMENTO_ID AS DOCUMENTO_ID, @NRO_LINEA AS NRO_LINEA --FROM X
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

ALTER    PROCEDURE [dbo].[GETDATAFORPALLETtest]
@NROPALLET AS VARCHAR(100),
@USUARIO AS VARCHAR(20)
AS
DECLARE @DOCUMENTO_ID AS NUMERIC(20,0)
DECLARE @NRO_LINEA   AS NUMERIC(10,0)
DECLARE @TRANSACCION_ID   AS VARCHAR(15)
DECLARE @TIPO_OPERACION_ID AS VARCHAR(5)
DECLARE @DOC_TRANS_ID AS NUMERIC(20,0)
DECLARE @VCANT AS NUMERIC(20)
DECLARE @ESTACION AS VARCHAR(15)

	BEGIN
		SELECT @DOCUMENTO_ID=X.DOCUMENTO_ID,@NRO_LINEA=NRO_LINEA, @DOC_TRANS_ID=DOC_TRANS_ID
		FROM(
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN NAVE N
					ON(RL.NAVE_ACTUAL = N.NAVE_ID)
			WHERE 	DD.PROP1=@NROPALLET AND D.STATUS='D30'
					AND N.PRE_INGRESO='1'
		
			UNION ALL
		
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE 	DD.PROP1=@NROPALLET AND D.STATUS='D30'
					AND	P.INTERMEDIA='1'
		
			UNION ALL
		
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN NAVE N
					ON(N.NAVE_ID=RL.NAVE_ACTUAL)
			WHERE 	DD.PROP1=@NROPALLET AND D.STATUS='D30'
					AND	N.INTERMEDIA='1'
		) AS X

	END
---Ac  validamos que el usuario tenga permiso para tomar el pallet

	IF @@ROWCOUNT =0
		BEGIN
			RAISERROR ('EL PALLET SOLICITADO NO ESTA DISPONIBLE.', 16, 1)
		END

	ELSE
	BEGIN

	SELECT 	@TRANSACCION_ID=DT.TRANSACCION_ID,@TIPO_OPERACION_ID=DT.TIPO_OPERACION_ID,@ESTACION=DT.ESTACION_ACTUAL
	FROM 	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID= DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN DOCUMENTO_TRANSACCION DT
			ON(DDT.DOC_TRANS_ID=DT.DOC_TRANS_ID)
	WHERE 	DD.DOCUMENTO_ID=@DOCUMENTO_ID 
			AND DD.NRO_LINEA=@NRO_LINEA
	
					
	
	SELECT
	       @VCANT=COUNT(D.DOCUMENTO_ID)
	
	FROM 	DET_DOCUMENTO_TRANSACCION DDT
	      	INNER JOIN DOCUMENTO_TRANSACCION DT ON (DT.DOC_TRANS_ID=DDT.DOC_TRANS_ID)
	      	INNER JOIN DET_DOCUMENTO         DD ON DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA
	      	LEFT JOIN DOCUMENTO               D ON DDT.DOCUMENTO_ID=D.DOCUMENTO_ID
	      	INNER JOIN TRANSACCION              TR ON (TR.TRANSACCION_ID = DT.TRANSACCION_ID)
	      	INNER JOIN RL_TRANSACCION_ESTACION  R1 ON (DT.ESTACION_ACTUAL = R1.ESTACION_ID AND DT.TRANSACCION_ID = R1.TRANSACCION_ID)
	WHERE 	1 <> 0
			AND DT.ESTACION_ACTUAL = @ESTACION --'PRUEBA_1'
			AND DT.TIPO_OPERACION_ID = @TIPO_OPERACION_ID--'ING'
			AND DT.TRANSACCION_ID=@TRANSACCION_ID --'TING_MONO'
			AND DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.NRO_LINEA=@NRO_LINEA
		    AND DDT.CLIENTE_ID IN (
								SELECT CLIENTE_ID 	
								FROM CLIENTE
	                            WHERE
									(SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END
										FROM   rl_sys_cliente_usuario
										WHERE  cliente_id = dd.cliente_id
										And    usuario_id = @USUARIO) = 1)


		IF @VCANT=0 --@@ROWCOUNT =0
		BEGIN
			RAISERROR ('NO TIENE PERMISOS SOBRE EL PALLET SELECCIONADO', 16, 1)
		END
		ELSE
		BEGIN 
		SELECT @DOCUMENTO_ID AS DOCUMENTO_ID, @NRO_LINEA AS NRO_LINEA --FROM X
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

ALTER  PROCEDURE [dbo].[GetPalletByPos]
@Posicion 	as varchar(45),
@PalletOut	as varchar(100) Output
As
Begin

	select 	@PalletOut=dd.prop1
	from	rl_det_doc_trans_posicion rl 
			inner join det_documento_transaccion ddt
			on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			left join nave n
			on(n.nave_id=rl.nave_actual)
			left join posicion p
			on(p.posicion_id=rl.posicion_actual)
			inner join det_documento dd
			on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
	where	p.posicion_cod=Ltrim(Rtrim(Upper(@Posicion)))
			or n.nave_cod=Ltrim(Rtrim(Upper(@Posicion)))


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

ALTER    procedure [dbo].[GetPosByPallet]
@Pallet 	as varchar(100),
@Pos 		as varchar(45) output
As
Begin
	
	
	Select 	Top 1
			@Pos=isnull(p.posicion_cod,n.nave_cod)
	from	rl_det_doc_trans_posicion rl 
			inner join det_documento_transaccion ddt
			on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			left join nave n
			on(n.nave_id=rl.nave_actual)
			left join posicion p
			on(p.posicion_id=rl.posicion_actual)
			inner join det_documento dd
			on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
	where	dd.Prop1=Ltrim(Rtrim(Upper(@Pallet)))


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

ALTER Procedure [dbo].[GetTipoDoc_Sucursal]
	@Cliente_id		as Varchar(15) output,
	@Suc_id		as Varchar(20) output,
	@vTipoDOC		as Varchar(50) output
as
Begin

	Select	@vTipoDOC = tipo_documento_id_F 
	From 	sucursal 
	Where 	cliente_id = @Cliente_id 
			and ltrim(rtrim(upper(Sucursal_id))) =  ltrim(rtrim(upper(@Suc_id)))

	Select @vTipoDOC

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

ALTER PROCEDURE [dbo].[GetValuesForFTP]
AS
BEGIN
	SELECT 	PARAMETRO_ID, VALOR 
	FROM 	SYS_PARAMETRO_PROCESO 
	WHERE 	PROCESO_ID='WARP' AND SUBPROCESO_ID='FTP'
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

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[GRABA_CANT_CONTEO]
@INVENTARIO_ID NUMERIC(20,0),
@MARBETE NUMERIC(20,0),
@CANTIDAD NUMERIC(20,5),
@OBSERVACIONES VARCHAR(2000)
AS
BEGIN
DECLARE @CONTEO AS NUMERIC(20,0)
DECLARE @strSQL AS VARCHAR(MAX)
DECLARE @USUARIO_ID AS VARCHAR(20)
DECLARE @EXISTE AS NUMERIC(20,0)
declare @V_lockgraba AS VARCHAR(1) 

SET XACT_ABORT ON

BEGIN TRY
	BEGIN TRAN

	SELECT @USUARIO_ID=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	SELECT @CONTEO = NRO_CONTEO FROM INVENTARIO WHERE INVENTARIO_ID = @INVENTARIO_ID

	SELECT @EXISTE = COUNT(*) 
	FROM RL_INVENTARIO_USUARIO
	WHERE INVENTARIO_ID = @INVENTARIO_ID AND USUARIO_ID = @USUARIO_ID


	SELECT @V_lockgraba = lockgraba FROM INVENTARIO WHERE INVENTARIO_ID = @INVENTARIO_ID

	IF @V_lockgraba = '1' 
	BEGIN
		RAISERROR ('1- Se a cerrado el ingreso del conteo.',16,1)
		RETURN
	END
		

	IF @EXISTE = 0 
	BEGIN
		RAISERROR ('1- Usted no se encuentra asignado al inventario.',16,1)
		RETURN
	END

	IF (@CONTEO = 1)
	BEGIN 
		UPDATE DET_CONTEO SET CONTEO1 = @CANTIDAD,OBSCONTEO1 = @OBSERVACIONES
		WHERE INVENTARIO_ID = @INVENTARIO_ID AND MARBETE = @MARBETE
	END
	IF (@CONTEO = 2)
	BEGIN 
		UPDATE DET_CONTEO SET CONTEO2 = @CANTIDAD,OBSCONTEO2 =@OBSERVACIONES
		WHERE INVENTARIO_ID = @INVENTARIO_ID AND MARBETE = @MARBETE
	END
	IF (@CONTEO = 3)
	BEGIN 
		UPDATE DET_CONTEO SET CONTEO3 = @CANTIDAD,OBSCONTEO3 =@OBSERVACIONES
		WHERE INVENTARIO_ID = @INVENTARIO_ID AND MARBETE = @MARBETE
	END


		UPDATE RL_DET_CONTEO_USUARIO SET FECHA_FIN = GETDATE()
		WHERE INVENTARIO_ID = @INVENTARIO_ID AND MARBETE = @MARBETE AND USUARIO_ID = @USUARIO_ID

		--ACTUALIZA LA CANTIDAD DE LA POSICION
		EXEC FUNCIONES_INVENTARIO_API#ACT_STOCK @INVENTARIO_ID, @MARBETE
		
	COMMIT
	
END TRY
BEGIN CATCH
IF XACT_STATE() <> 0 ROLLBACK TRAN 
    EXEC usp_RethrowError;
END CATCH	

END
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