/****** Object:  StoredProcedure [dbo].[LocatorEgreso_RemanenteDoc]    Script Date: 07/10/2014 16:32:20 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LocatorEgreso_RemanenteDoc]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[LocatorEgreso_RemanenteDoc]
GO

CREATE        Procedure [dbo].[LocatorEgreso_RemanenteDoc]
@pDocumento_id 	as Numeric(20,0)	Output,
@pCliente_id	as varchar(15)		Output,
@pViaje_id		as varchar(100)		Output,
@pLineaDoc		as Numeric(20,0)	Output,
@vNroLinea		as Numeric(20,0)	Output,
@pProducto_id   as varchar(100)		Output,
@pCantRem		as Numeric(20,0)	Output,
@Crit1			as varchar(30)		Output,
@Crit2			as varchar(30)		Output,
@Crit3			as varchar(30)		Output			

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
declare @Fecha_Alta_GTW			as datetime
declare @RsRem			        as Cursor
declare @auxErr					as varchar(4000)
declare @nro_serie				as varchar(50)


SET NOCOUNT ON;

		set @vQtyResto = @pCantRem  
		set @QtySol = @pCantRem  
		--set @vNroLinea=0
		Set @RsRem = Cursor For
			Select	X.*
			From	(
				SELECT	 dd.fecha_vencimiento
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
						--and dd.producto_id =@pProducto_id
						and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
						and exists (select	1 
									from	det_documento ddegr	
									where	ddegr.documento_id = @pDocumento_id
											and ddegr.nro_linea= @pLineaDoc
											and dd.CLIENTE_ID=ddegr.CLIENTE_ID
											and ddegr.producto_id = dd.producto_id
											and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
											and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
											and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
											and ((ddegr.cat_log_id is null)OR(ddegr.cat_log_id=rl.cat_log_id))
											and ((ddegr.est_merc_id is null)OR(ddegr.est_merc_id=rl.est_merc_id))
									)
						--and d.cliente_id = @cliente_id
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
						--and dd.producto_id =@pProducto_id
						and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
						and exists (select	1 
									from	det_documento ddegr	
									where	ddegr.documento_id = @pDocumento_id
											and ddegr.nro_linea= @pLineaDoc
											and dd.CLIENTE_ID=ddegr.CLIENTE_ID
											and ddegr.producto_id = dd.producto_id
											and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
											and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
											and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
											and ((ddegr.cat_log_id is null)OR(ddegr.cat_log_id=rl.cat_log_id))
											and ((ddegr.est_merc_id is null)OR(ddegr.est_merc_id=rl.est_merc_id))
									)
						--and d.cliente_id = @cliente_id
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
								
	Open @RsRem
	Fetch Next From @RsRem into
											@Fecha_Vto,
											@OrdenPicking,
											@Tipo_Posicion,
											@Codigo_Posicion,
											@Cliente_id,
											@Producto_id,
											@Cantidad,
											@vRl_id,
											@NRO_BULTO,
											@NRO_LOTE,				
											@EST_MERC_ID,			
											@NRO_DESPACHO,		
											@NRO_PARTIDA,			
											@UNIDAD_ID,			
											@PROP1,					
											@PROP2,					
											@PROP3,
											@DESC,
											@CAT_LOG_ID,
											@Fecha_Alta_GTW,
											@nro_serie
	While ((@@Fetch_Status=0) AND (@vQtyResto>0))
	begin --While Picking = 1
	-- Aca se replica la logica de Pickin=1
			if (@vQtyResto>=@Cantidad) 
				begin -- (@vQtyResto>=@Cantidad) 
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
			else begin
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
							,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_Serie)	
				insert into #tmp_consumo_locator_egr values (@vRl_id, @vQtyResto)	
				set @vQtyResto=0
			end --if (@vQtyResto>=@Cantidad) 
			Fetch Next From @RsRem into	@Fecha_Vto,
												@OrdenPicking,
												@Tipo_Posicion,
												@Codigo_Posicion,
												@Cliente_id,
												@Producto_id,
												@Cantidad,
												@vRl_id,
												@NRO_BULTO,
												@NRO_LOTE,				
												@EST_MERC_ID,			
												@NRO_DESPACHO,		
												@NRO_PARTIDA,			
												@UNIDAD_ID,			
												@PROP1,					
												@PROP2,					
												@PROP3,
												@DESC,
												@CAT_LOG_ID,
												@Fecha_Alta_GTW,
												@nro_serie
		end -- End While Picking = 1
CLOSE @RsRem
DEALLOCATE @RsRem

--if @vQtyResto > 0 begin
--	set @auxErr = 'No se pudo asignar del producto ' + @pProducto_id + ', la cantidad total solicitada, para completar falta la cantidad de ' + convert(varchar,convert(int,@vQtyResto)) + ' unidades. '
--	RAISERROR (@auxErr,16,1)
--end --if


Set NoCount Off;
END

GO


