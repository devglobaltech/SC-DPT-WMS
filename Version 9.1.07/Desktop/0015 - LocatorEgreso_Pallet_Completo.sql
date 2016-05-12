/****** Object:  StoredProcedure [dbo].[LocatorEgreso_pallet_completo]    Script Date: 05/05/2014 12:42:02 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LocatorEgreso_pallet_completo]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[LocatorEgreso_pallet_completo]
GO

CREATE        Procedure [dbo].[LocatorEgreso_pallet_completo]
	@pDocumento_id 	as Numeric(20,0) Output,
	@pCliente_id	as varchar(15) Output,
	@pViaje_id		as varchar(100) Output
As
Begin 
	declare @Fecha_Vto				as datetime
	declare @OrdenPicking			as numeric(10,0)
	declare @Tipo_Posicion			as varchar(10)
	declare @Codigo_Posicion		as varchar(100)
	declare @Cliente_id				as varchar(15)
	declare @Producto_id			as varchar(30)
	declare @Cantidad				as numeric(20,5)
	declare @Aux					as varchar(50)
	declare @NewProducto			as varchar(30)
	declare @OldProducto			as varchar(30)
	declare @vQtyResto				as numeric(20,5)
	declare @vRl_id					as numeric(20)
	declare @QtySol					as numeric(20,5)
	declare @vNroLinea				as numeric(20)
	declare @NRO_BULTO				as varchar(50)
	declare @NRO_LOTE				as varchar(50)
	declare @EST_MERC_ID			as varchar(15)
	declare @NRO_DESPACHO			as varchar(50)
	declare @NRO_PARTIDA			as varchar(50)
	declare @UNIDAD_ID				as varchar(5)
	declare @PROP1					as varchar(100)
	declare @PROP2					as varchar(100)
	declare @PROP3					as varchar(100)
	declare @DESC					as varchar(200)
	declare @CAT_LOG_ID				as varchar(50)
	declare @id						as numeric(20,0)
	declare @Documento_id 			as Numeric(20,0)
	declare @Saldo					as numeric(20,5)
	declare @TipoSaldo				as varchar(20)
	declare @Doc_Trans 				as numeric(20)
	declare @QtyDetDocumento		as numeric(20)
	declare @vUsuario_id			as varchar(50)
	declare @vTerminal				as varchar(50)
	declare @FLG_PALLET_COMPLETO	as varchar(1)
	declare @RsExist				as cursor
	declare @RsExist_no_pick		as Cursor
	declare @RsExist_pick			as Cursor
	declare @RsActuRL				as Cursor
	declare @row					as int
	declare @file					as varchar(max)
	declare @Crit1					as varchar(30)
	declare @Crit2					as varchar(30)
	declare @Crit3					as varchar(30)
	declare @Fecha_Alta_GTW			as datetime
	declare @nro_serie				as varchar(50)
	declare @NewLoteProveedor		as varchar(100)
	declare @OldLoteProveedor		as varchar(100)
	declare @NewNroPartida			as varchar(100)
	declare @OldNroPartida			as varchar(100)
	declare @NewNroSerie			as varchar(50)
	declare @OldNroSerie			as varchar(50)
	declare @DOCIDPIVOT				as numeric(20,0)
	declare @NROLINEAPIVOT			as numeric(20,0)
	declare @PESOPROPS				as numeric(5,0)
	declare @RSDOCEGR				as cursor
	SET NOCOUNT ON;

	BEGIN TRY
		
		Set @vNroLinea=0
		
		SELECT	@FLG_PALLET_COMPLETO = FLG_PALLET_COMPLETO 
		FROM	CLIENTE_PARAMETROS
		WHERE	CLIENTE_ID = @pCliente_id

		Select	@Crit1=CRITERIO_1, @Crit2=CRITERIO_2, @Crit3=CRITERIO_3
		From	RL_CLIENTE_LOCATOR
		Where	Cliente_id=(select Cliente_id from documento where documento_id=@pDocumento_id)

		if (@Crit1 is null) and (@Crit2 is null) and (@Crit3 is null)
		begin
			--Si todos son nulos entonces x default salgo con orden de picking.
			Set @Crit1='ORDEN_PICKING'
		end
		
		select @Cliente_id = cliente_id from DOCUMENTO where DOCUMENTO_ID = @pDocumento_id

		SET @RSDOCEGR = CURSOR FOR
		  SELECT  DD.DOCUMENTO_ID, DD.NRO_LINEA, P.PESO
		  FROM    DET_DOCUMENTO DD
				  INNER JOIN #SDDPESO P ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
		  WHERE   DD.CLIENTE_ID = @pCliente_id 
				  AND P.CODIGO_VIAJE = @pViaje_id
				  and dd.documento_id=@PDocumento_id
		  ORDER BY 
				  P.PESO DESC, P.DOCUMENTO_ID ASC, P.NRO_LINEA ASC

		OPEN @RSDOCEGR
		FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT, @PESOPROPS
		
		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @QtySol=0
			set @QtySol=dbo.GetQtySol(@DOCIDPIVOT,@NROLINEAPIVOT,@pCliente_id)
			set @vQtyResto=@QtySol	
	          
			IF (@FLG_PALLET_COMPLETO = '1') 
			BEGIN
				Set @RsExist_no_pick = Cursor For
				Select	X.*
				from	(SELECT	 dd.fecha_vencimiento			,isnull(p.orden_picking,99999) as ORDEN_PICKING		,'POS' as ubicacion
								,p.posicion_cod as posicion		,dd.cliente_id										,dd.producto_id as producto
								,rl.cantidad					,rl.rl_id											,dd.NRO_BULTO
								,dd.NRO_LOTE					,RL.EST_MERC_ID										,dd.NRO_DESPACHO
								,dd.NRO_PARTIDA					,dd.UNIDAD_ID										,dd.PROP1
								,dd.PROP2						,dd.PROP3											,dd.DESCRIPCION
								,RL.CAT_LOG_ID					,d.fecha_alta_gtw									,DD.NRO_SERIE
						FROM	rl_det_doc_trans_posicion rl
								inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
								inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
								inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
								inner join posicion p on (rl.posicion_actual=p.posicion_id and isnull(p.bestfit,'0')='1')
								left join  estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
								inner join documento d on(dd.documento_id=d.documento_id)
						WHERE	rl.doc_trans_id_egr is null
								and rl.nro_linea_trans_egr is null
								and rl.disponible='1'
								and isnull(em.disp_egreso,'1')='1'
								and isnull(em.picking,'1')='1'
								and p.pos_lockeada='0' 					
								and cl.disp_egreso='1' 
								and cl.picking='1'
								and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
								and exists (select  1 
											from    det_documento ddegr
											where	ddegr.documento_id = @DOCIDPIVOT 
													and ddegr.nro_linea = @NROLINEAPIVOT
													and ddegr.producto_id = dd.producto_id
													and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
													and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
													and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
											)
								and d.cliente_id = @cliente_id
						UNION
						SELECT	 dd.fecha_vencimiento			,isnull(n.orden_locator,99999) as ORDEN_PICKING		,'NAV' as ubicacion				
								,n.nave_cod as posicion			,dd.cliente_id										,dd.producto_id as producto
								,rl.cantidad					,rl.rl_id											,dd.NRO_BULTO					
								,dd.NRO_LOTE					,RL.EST_MERC_ID										,dd.NRO_DESPACHO
								,dd.NRO_PARTIDA					,dd.UNIDAD_ID										,dd.PROP1						
								,dd.PROP2						,dd.PROP3											,dd.DESCRIPCION
								,RL.CAT_LOG_ID					,d.fecha_alta_gtw									,DD.NRO_SERIE
						FROM	rl_det_doc_trans_posicion rl
								inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
								inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
								inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
								inner join nave n on (rl.nave_actual=n.nave_id)
								left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
								inner join documento d on(dd.documento_id=d.documento_id)
						WHERE	rl.doc_trans_id_egr is null
								and rl.nro_linea_trans_egr is null
								and rl.disponible='1'
								and isnull(em.disp_egreso,'1')='1'
								and isnull(em.picking,'1')='1'
								and rl.cat_log_id<>'TRAN_EGR'
								and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' --and n.picking='1'
								and cl.disp_egreso='1' and cl.picking='1'
								and exists (select  1 
											from    det_documento ddegr
											where	ddegr.documento_id = @DOCIDPIVOT 
													and ddegr.nro_linea = @NROLINEAPIVOT
													and ddegr.producto_id = dd.producto_id
													and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
													and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
													and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
											)
								and d.cliente_id = @cliente_id)X
						order by--order by producto,dd.fecha_vencimiento asc,orden  
								(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
								(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
								(CASE WHEN @Crit1 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
								(CASE WHEN @Crit1 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
								(CASE WHEN @Crit1 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
								(CASE WHEN @Crit1 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
								(CASE WHEN @Crit1 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
								(CASE WHEN @Crit1 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
								(CASE WHEN @Crit1 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
								(CASE WHEN @Crit1 = 'PROP1'				THEN x.PROP1 END),
								(CASE WHEN @Crit1 = 'PROP2'				THEN x.PROP2 END),
								(CASE WHEN @Crit1 = 'PROP3'				THEN x.PROP3 END),
								(CASE WHEN @Crit1 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
								(CASE WHEN @Crit1 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
								 --2
								(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
								(CASE WHEN @Crit2 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
								(CASE WHEN @Crit2 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
								(CASE WHEN @Crit2 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
								(CASE WHEN @Crit2 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
								(CASE WHEN @Crit2 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
								(CASE WHEN @Crit2 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
								(CASE WHEN @Crit2 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
								(CASE WHEN @Crit2 = 'PROP1'				THEN x.PROP1 END),
								(CASE WHEN @Crit2 = 'PROP2'				THEN x.PROP2 END),
								(CASE WHEN @Crit2 = 'PROP3'				THEN x.PROP3 END),
								(CASE WHEN @Crit2 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
								(CASE WHEN @Crit2 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
								--3
								(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
								(CASE WHEN @Crit3 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
								(CASE WHEN @Crit3 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
								(CASE WHEN @Crit3 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
								(CASE WHEN @Crit3 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
								(CASE WHEN @Crit3 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
								(CASE WHEN @Crit3 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
								(CASE WHEN @Crit3 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
								(CASE WHEN @Crit3 = 'PROP1'				THEN x.PROP1 END),
								(CASE WHEN @Crit3 = 'PROP2'				THEN x.PROP2 END),
								(CASE WHEN @Crit3 = 'PROP3'				THEN x.PROP3 END),
								(CASE WHEN @Crit3 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
								(CASE WHEN @Crit3 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END)

				Open @RsExist_no_pick
				
				IF OBJECT_ID('tempdb..#tmp_consumo_locator_egr') IS NULL
				BEGIN
					create table #tmp_consumo_locator_egr (rl_id numeric(20,0), cantidad numeric(20,5));
				END
				ELSE
				BEGIN
					DELETE FROM #tmp_consumo_locator_egr
				END
				
				Fetch Next From @RsExist_no_pick into	@Fecha_Vto,			@OrdenPicking,		@Tipo_Posicion,		@Codigo_Posicion,	@Cliente_id,		@Producto_id,		@Cantidad,			@vRl_id,
														@NRO_BULTO,			@NRO_LOTE,			@EST_MERC_ID,		@NRO_DESPACHO,		@NRO_PARTIDA,		@UNIDAD_ID,			@PROP1,				@PROP2,					
														@PROP3,				@DESC,				@CAT_LOG_ID,		@FECHA_ALTA_GTW,	@NRO_SERIE				
		 
				set @NewProducto=@Producto_id	set @NewLoteProveedor=@nro_lote		set @NewNroPartida=@nro_partida		set @NewNroSerie=@nro_serie	set @OldProducto=''	set @OldLoteProveedor=''
				set @OldNroPartida=''			set @OldNroSerie=''					

				While @@Fetch_Status=0
				Begin	

					--aca asignar si queda resto en vQtyResto y no hay mas registros para el producto anterior
					if (@OldProducto <> '') and (@NewProducto<>@OldProducto or @NewLoteProveedor<>@OldLoteProveedor or @NewNroPartida <> @OldNroPartida or @NewNroSerie<>@OldNroSerie) and (@vQtyResto>0) 
					begin
						
						exec LocatorEgreso_RemanenteDoc @pDocumento_id output,	@pCliente_id output, @pViaje_id output, @NROLINEAPIVOT Output,	@vNroLinea output, 
														@OldProducto output,	@vQtyResto,			@Crit1,				@Crit2,					@Crit3
					end --if (@OldProducto <> '') and (@NewProducto<>@OldProducto or @NewLoteProveedor<>@OldLoteProveedor or @NewNroPartida <> @OldNroPartida or @NewNroSerie<>@OldNroSerie) and (@vQtyResto>0) 

					if (@NewProducto<>@OldProducto or @NewLoteProveedor<>@OldLoteProveedor or @NewNroPartida <> @OldNroPartida or @NewNroSerie<>@OldNroSerie) 
					begin
						set @OldProducto=@NewProducto
						set @OldLoteProveedor=@NewLoteProveedor
						set @OldNroPartida=@NewNroPartida
						set @OldNroSerie=@NewNroSerie
						set @QtySol=dbo.GetQtySol(@pDocumento_id,@NROLINEAPIVOT,@Cliente_id)
						set @vQtyResto=@QtySol
					end --(@NewProducto<>@OldProducto or @NewLoteProveedor<>@OldLoteProveedor or @NewNroPartida <> @OldNroPartida or @NewNroSerie<>@OldNroSerie) 			

					if (@vQtyResto>0) 
					begin   
						if (@vQtyResto>=@Cantidad)
						begin
							set @vNroLinea=@vNroLinea+1
							set @vQtyResto=@vQtyResto-@Cantidad
							
							insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
							values							(@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
										
							--Insert con todas las propiedades en det_documento
							insert into det_documento_aux (	documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
															cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
															unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
										values			  ( @pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
															,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0','1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_Serie)	
															
							insert into #tmp_consumo_locator_egr values (@vRl_id, @Cantidad)	
						end
						else 
						begin
							Set @RsExist_pick = Cursor For
							Select	X.*
							From	(SELECT	 dd.fecha_vencimiento
											,isnull(p.orden_picking,99999) as ORDEN_PICKING
											,'POS' as ubicacion
											,p.posicion_cod as posicion
											,dd.cliente_id
											,dd.producto_id as producto
											,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
											,rl.rl_id
											,dd.NRO_BULTO
											,dd.NRO_LOTE
											,RL.EST_MERC_ID
											,dd.NRO_DESPACHO
											,dd.NRO_PARTIDA
											,dd.UNIDAD_ID
											,dd.PROP1
											,dd.PROP2
											,dd.PROP3
											,dd.DESCRIPCION
											,RL.CAT_LOG_ID
											,D.FECHA_ALTA_GTW
											,dd.nro_serie
									FROM	rl_det_doc_trans_posicion rl
											inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
											inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
											inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
											inner join posicion p on (rl.posicion_actual=p.posicion_id)
											left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
											inner join documento d on(dd.documento_id=d.documento_id)
											left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
									WHERE	rl.doc_trans_id_egr is null
											and rl.nro_linea_trans_egr is null
											and rl.disponible='1'
											and isnull(em.disp_egreso,'1')='1'
											and isnull(em.picking,'1')='1'
											and p.pos_lockeada='0' and p.picking='1'
											and cl.disp_egreso='1' and cl.picking='1'
											and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
											--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id and producto_id =@Producto_id)
											--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
											and exists (select 1 from det_documento ddegr	
													where	ddegr.documento_id = @pDocumento_id
															and ddegr.producto_id = dd.producto_id
															and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
															and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
															and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
											and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
									UNION
									SELECT	 dd.fecha_vencimiento
											,isnull(n.orden_locator,99999) as ORDEN_PICKING
											,'NAV' as ubicacion
											,n.nave_cod as posicion
											,dd.cliente_id
											,dd.producto_id as producto
											,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
											,rl.rl_id
											,dd.NRO_BULTO
											,dd.NRO_LOTE
											,RL.EST_MERC_ID
											,dd.NRO_DESPACHO
											,dd.NRO_PARTIDA
											,dd.UNIDAD_ID
											,dd.PROP1
											,dd.PROP2
											,dd.PROP3
											,dd.DESCRIPCION
											,RL.CAT_LOG_ID
											,D.FECHA_ALTA_GTW
											,dd.nro_serie
									FROM	rl_det_doc_trans_posicion rl
											inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
											inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
											inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
											inner join nave n on (rl.nave_actual=n.nave_id)
											left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
											inner join documento d on(dd.documento_id=d.documento_id)
											left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
									WHERE	rl.doc_trans_id_egr is null
											and rl.nro_linea_trans_egr is null
											and rl.disponible='1'
											and isnull(em.disp_egreso,'1')='1'
											and isnull(em.picking,'1')='1'
											and rl.cat_log_id<>'TRAN_EGR'
											and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
											and cl.disp_egreso='1' and cl.picking='1'
											and exists (select	1 
														from	det_documento ddegr	
														where	ddegr.documento_id = @pDocumento_id
																and ddegr.producto_id = dd.producto_id
																and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
																and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
																and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
											and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
											)X
									order by--order by producto,dd.fecha_vencimiento asc,orden  
											(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
											(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
											(CASE WHEN @Crit1 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
											(CASE WHEN @Crit1 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
											(CASE WHEN @Crit1 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
											(CASE WHEN @Crit1 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
											(CASE WHEN @Crit1 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
											(CASE WHEN @Crit1 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
											(CASE WHEN @Crit1 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
											(CASE WHEN @Crit1 = 'PROP1'				THEN x.PROP1 END),
											(CASE WHEN @Crit1 = 'PROP2'				THEN x.PROP2 END),
											(CASE WHEN @Crit1 = 'PROP3'				THEN x.PROP3 END),
											(CASE WHEN @Crit1 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
											(CASE WHEN @Crit1 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
											 --2
											(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
											(CASE WHEN @Crit2 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
											(CASE WHEN @Crit2 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
											(CASE WHEN @Crit2 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
											(CASE WHEN @Crit2 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
											(CASE WHEN @Crit2 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
											(CASE WHEN @Crit2 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
											(CASE WHEN @Crit2 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
											(CASE WHEN @Crit2 = 'PROP1'				THEN x.PROP1 END),
											(CASE WHEN @Crit2 = 'PROP2'				THEN x.PROP2 END),
											(CASE WHEN @Crit2 = 'PROP3'				THEN x.PROP3 END),
											(CASE WHEN @Crit2 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
											(CASE WHEN @Crit2 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
											--3
											(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
											(CASE WHEN @Crit3 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
											(CASE WHEN @Crit3 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
											(CASE WHEN @Crit3 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
											(CASE WHEN @Crit3 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
											(CASE WHEN @Crit3 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
											(CASE WHEN @Crit3 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
											(CASE WHEN @Crit3 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
											(CASE WHEN @Crit3 = 'PROP1'				THEN x.PROP1 END),
											(CASE WHEN @Crit3 = 'PROP2'				THEN x.PROP2 END),
											(CASE WHEN @Crit3 = 'PROP3'				THEN x.PROP3 END),
											(CASE WHEN @Crit3 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
											(CASE WHEN @Crit3 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END)
													
							Open @RsExist_pick
							Fetch Next From @RsExist_pick into	@Fecha_Vto,	@OrdenPicking,@Tipo_Posicion,@Codigo_Posicion,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@NRO_BULTO,@NRO_LOTE,@EST_MERC_ID,@NRO_DESPACHO,
																@NRO_PARTIDA,@UNIDAD_ID,@PROP1,@PROP2,@PROP3,@DESC,@CAT_LOG_ID,@Fecha_Alta_GTW,@nro_serie
																
							While ((@@Fetch_Status=0) AND (@vQtyResto>0))
							begin
								-- Aca se replica la logica de Pickin=1
								if (@vQtyResto>=@Cantidad) 
								begin 
									set @vNroLinea=@vNroLinea+1
									set @vQtyResto=@vQtyResto-@Cantidad
									insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
												values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
												
									--Insert con todas las propiedades en det_documento
									insert into det_documento_aux (
												documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
												cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
												unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
												values 
												(@pDocumento_id,@vNroLinea
												,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
												,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
												,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)
									insert into #tmp_consumo_locator_egr values (@vRl_id, @Cantidad)	
								end
								else 
								begin
									set @vNroLinea=@vNroLinea+1
									insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
												values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')
									--Insert con todas las propiedades en det_documento
									insert into det_documento_aux (
												documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
												cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
												unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
												values 
												(@pDocumento_id,@vNroLinea
												,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
												,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
												,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
									insert into #tmp_consumo_locator_egr values (@vRl_id, @vQtyResto)	
									set @vQtyResto=0
								end --if (@vQtyResto>=@Cantidad) 
								
								Fetch Next From @RsExist_pick into	@Fecha_Vto,	@OrdenPicking,@Tipo_Posicion,@Codigo_Posicion,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@NRO_BULTO,@NRO_LOTE,@EST_MERC_ID,@NRO_DESPACHO,
																	@NRO_PARTIDA,@UNIDAD_ID,@PROP1,@PROP2,@PROP3,@DESC,@CAT_LOG_ID,@Fecha_Alta_GTW,@nro_serie
							end -- End While ((@@Fetch_Status=0) AND (@vQtyResto>0))

							set @vQtyResto=0
						end --ifelse (@vQtyResto>=@Cantidad)
					end --if (@vQtyResto>0) 

					Fetch Next From @RsExist_no_pick into	@Fecha_Vto,			@OrdenPicking,		@Tipo_Posicion,		@Codigo_Posicion,
															@Cliente_id,		@Producto_id,		@Cantidad,			@vRl_id,
															@NRO_BULTO,			@NRO_LOTE,			@EST_MERC_ID,		@NRO_DESPACHO,		
															@NRO_PARTIDA,		@UNIDAD_ID,			@PROP1,				@PROP2,					
															@PROP3,				@DESC,				@CAT_LOG_ID,		@Fecha_Alta_GTW,
															@nro_serie
					set @NewProducto=@Producto_id
				End	--End @@Fetch_Status=0 - @RsExist_no_pick.


				if (@vQtyResto>0) begin
					-------------------------------------------------------------------------------------------------------------
					-- para contemplar el caso de que no hayan mas registros en el cursor @RsExist_no_pick y quedan remanente sin asignar
					-------------------------------------------------------------------------------------------------------------
					exec LocatorEgreso_RemanenteDoc @pDocumento_id, @pCliente_id, @pViaje_id, @NROLINEAPIVOT output, @vNroLinea output, @Producto_id,
													@vQtyResto, @Crit1, @Crit2, @Crit3
				end --if (@vQtyResto>0) begin

				CLOSE @RsExist_no_pick
				DEALLOCATE @RsExist_no_pick
		

			END
			FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT, @PESOPROPS		
		END
		
		CLOSE @RSDOCEGR
		DEALLOCATE @RSDOCEGR

		--GUARDO SERIES INICIALES
		SELECT DISTINCT NRO_SERIE INTO #TMPSERIES FROM DET_DOCUMENTO WHERE DOCUMENTO_ID = @pDocumento_id

		--Borro det_documento y lo vuelvo a insertar con las nuevas propiedades
		delete det_documento where documento_id=@pDocumento_id
		insert into det_documento select 	DOCUMENTO_ID,	ROW_NUMBER()OVER(ORDER BY NRO_LINEA ASC),	CLIENTE_ID,
											PRODUCTO_ID,	CANTIDAD,	NRO_SERIE,	NRO_SERIE_PADRE,	EST_MERC_ID,
											CAT_LOG_ID,		NRO_BULTO,	DESCRIPCION,	NRO_LOTE,	FECHA_VENCIMIENTO,
											NRO_DESPACHO,	NRO_PARTIDA,	UNIDAD_ID,	PESO,	UNIDAD_PESO,	VOLUMEN,
											UNIDAD_VOLUMEN,	BUSC_INDIVIDUAL,	TIE_IN,	NRO_TIE_IN_PADRE,	NRO_TIE_IN,
											ITEM_OK,	CAT_LOG_ID_FINAL,	MONEDA_ID,	COSTO,	PROP1,	PROP2,
											PROP3,	LARGO,	ALTO,	ANCHO,	VOLUMEN_UNITARIO,	PESO_UNITARIO,
											CANT_SOLICITADA,	TRACE_BACK_ORDER
									from	det_documento_aux 
									where	documento_id=@pDocumento_id
									
		------CONTROLO QUE SERIES FUERON OBLIGATORIAS Y CUALES NO.
		UPDATE	DET_DOCUMENTO
		SET		NRO_SERIE = NULL
		WHERE	DOCUMENTO_ID = @pDocumento_id
				AND NOT EXISTS (SELECT 1 FROM #TMPSERIES WHERE NRO_SERIE = DET_DOCUMENTO.NRO_SERIE)

		update documento set status='D20' where documento_id=@pDocumento_id
		Exec Asigna_Tratamiento#Asigna_Tratamiento_EGR @pDocumento_id

		select distinct @Doc_Trans=doc_trans_id from det_documento_transaccion where documento_id=@pDocumento_id

		--Hago la reserva en RL
		Set @RsActuRL = Cursor For 
			select	[id],documento_id,Nro_Linea,Cliente_id,Producto_id,Cantidad,rl_id,saldo,tipo 
			from	consumo_locator_egr 
			where	procesado='N' and Documento_id=@pDocumento_id

		Open @RsActuRL
		Fetch Next From @RsActuRL into 	@id,@Documento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Saldo,@TipoSaldo

		While @@Fetch_Status=0
		Begin
			if (@Saldo=0) begin
			
				update	rl_det_doc_trans_posicion 
				set		doc_trans_id_egr=@Doc_Trans, nro_linea_trans_egr=@vNroLinea,disponible='0'
						,cat_log_id='TRAN_EGR',nave_anterior=nave_actual,posicion_anterior=posicion_actual
						,nave_actual='2',posicion_actual=null 
				where rl_id=@vRl_id
															
				update consumo_locator_egr set procesado='S' where [id]=@id
			end --if (@Saldo=0)

			if (@Saldo>0) begin
				insert into rl_det_doc_trans_posicion (doc_trans_id,nro_linea_trans,posicion_anterior,posicion_actual,cantidad,tipo_movimiento_id,
																	ultima_estacion,ultima_secuencia,nave_anterior,nave_actual,documento_id,nro_linea,
																	disponible,doc_trans_id_egr,nro_linea_trans_egr,doc_trans_id_tr,nro_linea_trans_tr,
																	cliente_id,cat_log_id,cat_log_id_final,est_merc_id)
							  select doc_trans_id,nro_linea_trans,posicion_anterior,posicion_actual,@Saldo,tipo_movimiento_id,
										ultima_estacion,ultima_secuencia,nave_anterior,nave_actual,documento_id,nro_linea,
										disponible,doc_trans_id_egr,nro_linea_trans_egr,doc_trans_id_tr,nro_linea_trans_tr,
										cliente_id,cat_log_id,cat_log_id_final,est_merc_id
							  from rl_det_doc_trans_posicion 
							  where rl_id=@vRl_id 	
				update rl_det_doc_trans_posicion set cantidad=@Cantidad,doc_trans_id_egr=@Doc_Trans, nro_linea_trans_egr=@vNroLinea,disponible='0'
																,cat_log_id='TRAN_EGR',nave_anterior=nave_actual,posicion_anterior=posicion_actual
																,nave_actual='2',posicion_actual=null where rl_id=@vRl_id
				update consumo_locator_egr set procesado='S' where [id]=@id
			end --if (@Saldo>0)	

			Fetch Next From @RsActuRL into 	@id,@Documento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Saldo,@TipoSaldo
		End	--End While @RsActuRL.

		CLOSE @RsActuRL
		DEALLOCATE @RsActuRL

		--Si no hay existencia de ningun producto del documento lo borro para que no quede solo cabecera
		select @QtyDetDocumento=count(documento_id) from det_documento where documento_id=@pDocumento_id
		
		if (@QtyDetDocumento=0) 
		begin
			delete documento where documento_id=@pDocumento_id 
		end 
		else 
		begin
			select @vUsuario_id=usuario_id, @vTerminal=Terminal from #temp_usuario_loggin
			insert into docxviajesprocesados values (@pViaje_id,@pDocumento_id,'P',getdate(),@vUsuario_id,@vTerminal)
		end --ifelse (@QtyDetDocumento=0) 

		Set NoCount Off

	END TRY
	BEGIN CATCH
		 EXEC usp_RethrowError
	END CATCH
END--FIN PROCEDURE.
GO


