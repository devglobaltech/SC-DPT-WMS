IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Fin_Picking_Split]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Fin_Picking_Split]
GO

CREATE               Procedure [dbo].[Fin_Picking_Split]
@Usuario 			as varchar(30),
@Viajeid 			as varchar(100),
@Producto_id		as varchar(50),
@Posicion_cod		as varchar(45),
@Cant_conf			as numeric(20,5),
@Pallet_picking     as numeric(20,0),
@Pallet				as varchar(100),
@Ruta				as varchar(100),
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
				And Ruta=Ltrim(Rtrim(Upper(@Ruta)))
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
				IF (@NRO_SERIE = '')
				BEGIN
					SET @NRO_SERIE = NULL
				END
				
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
							,NULL					,NULL				,NULL				,NULL 
							,NULL					,NULL				,NULL				,NULL 
							,NULL					
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


