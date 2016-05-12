/****** Object:  StoredProcedure [dbo].[RPT_PREPICKING_MULTIPLE_BEST_FIT]    Script Date: 03/04/2015 12:20:51 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RPT_PREPICKING_MULTIPLE_BEST_FIT]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[RPT_PREPICKING_MULTIPLE_BEST_FIT]
GO

CREATE PROCEDURE [dbo].[RPT_PREPICKING_MULTIPLE_BEST_FIT]
	@CLIENTE_ID		VARCHAR(20),
	@PRODUCTO_ID	VARCHAR(30),
	@QTY_SOLICITADA	NUMERIC(20,5),
	@NRO_LOTE		VARCHAR(50),
	@NRO_PARTIDA	VARCHAR(50),
	@NRO_SERIE		VARCHAR(100),
	@EST_MERC_ID	VARCHAR(50),
	@CAT_LOG_ID		VARCHAR(50),
	@QTY_RET		NUMERIC(20,5)	OUTPUT
AS
BEGIN
	declare @Fecha_Vto				as datetime
	declare @OrdenPicking			as numeric(10,0)
	declare @Tipo_Posicion			as varchar(10)
	declare @Codigo_Posicion		as varchar(100)
	declare @Cantidad				as numeric(20,5)
	declare @Aux					as varchar(50)
	declare @NewProducto			as varchar(30)
	declare @OldProducto			as varchar(30)
	declare @vQtyResto				as numeric(20,5)
	declare @vRl_id					as numeric(20)
	declare @QtySol					as numeric(20,5)
	declare @vNroLinea				as numeric(20)
	declare @NRO_BULTO				as varchar(50)
	declare @NRO_DESPACHO			as varchar(50)
	declare @UNIDAD_ID				as varchar(5)
	declare @PROP1					as varchar(100)
	declare @PROP2					as varchar(100)
	declare @PROP3					as varchar(100)
	declare @DESC					as varchar(200)
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
	declare @QTYPallet				as Numeric(20,5)
	declare @AsignaPallet			as varchar(2)
	declare @Oldprop1				as varchar(100)
	declare @Asignado				as numeric(20,5)
	declare @miPrint				as varchar(1000)
	declare @rl_id					as numeric(20,0)
	declare @rl_cant				as numeric(20,5)
	
	SET NOCOUNT ON;

	Select	@Crit1=CRITERIO_1, @Crit2=CRITERIO_2, @Crit3=CRITERIO_3
	From	RL_CLIENTE_LOCATOR
	Where	Cliente_id=@CLIENTE_ID

	if (@Crit1 is null) and (@Crit2 is null) and (@Crit3 is null)
	begin
		--Si todos son nulos entonces x default salgo con orden de picking.
		Set @Crit1='ORDEN_PICKING'
	end
	
	Set @RsExist_no_pick = Cursor For
	Select	 X.FECHA_VENCIMIENTO	,X.ORDEN_PICKING		,X.UBICACION,		X.POSICION
			,X.CLIENTE_ID			,X.PRODUCTO				,SUM(X.CANTIDAD)
			,X.NRO_BULTO			,X.NRO_LOTE				,X.EST_MERC_ID		,X.NRO_DESPACHO
			,X.NRO_PARTIDA			,X.UNIDAD_ID			,X.PROP1
			,X.PROP2				,X.PROP3				,X.DESCRIPCION
			,X.CAT_LOG_ID			,X.FECHA_ALTA_GTW			
	from	(SELECT	 dd.fecha_vencimiento			,isnull(p.orden_bestfit,99999) as ORDEN_PICKING		,'POS' as ubicacion
					,p.posicion_cod as posicion		,dd.cliente_id										,dd.producto_id as producto
					,rl.cantidad					,dd.NRO_BULTO
					,dd.NRO_LOTE					,RL.EST_MERC_ID										,dd.NRO_DESPACHO
					,dd.NRO_PARTIDA					,dd.UNIDAD_ID										,dd.PROP1
					,dd.PROP2						,dd.PROP3											,dd.DESCRIPCION
					,RL.CAT_LOG_ID					,d.fecha_alta_gtw									,DD.NRO_SERIE
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join posicion p on (rl.posicion_actual=p.posicion_id and isnull(p.bestfit,'0')='1' and isnull(p.picking,'0')='0')
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
					and dd.cliente_id=@cliente_id
					and dd.producto_id=@producto_id
					and ((isnull(@NRO_LOTE,'')='') or (DD.nro_lote = @NRO_LOTE))
					and ((isnull(@NRO_PARTIDA,'')='') or (DD.nro_partida = @NRO_PARTIDA))
					and ((isnull(@NRO_SERIE,'')='') or (DD.nro_serie = @NRO_SERIE))
					and ((isnull(@CAT_LOG_ID,'')='') or (RL.CAT_LOG_ID = @CAT_LOG_ID))
					and ((isnull(@EST_MERC_ID,'')='') or (RL.EST_MERC_ID = @EST_MERC_ID))
					)X
			GROUP BY
					X.fecha_vencimiento		,X.ORDEN_PICKING		,x.ubicacion		,x.cliente_id			
					,x.producto				,X.NRO_BULTO			,X.NRO_LOTE			,x.EST_MERC_ID							
					,x.NRO_DESPACHO			,X.NRO_PARTIDA			,X.UNIDAD_ID		,x.PROP1
					,X.PROP2				,x.PROP3				,x.DESCRIPCION		,x.posicion
					,X.CAT_LOG_ID			,x.fecha_alta_gtw											
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

	IF OBJECT_ID('tempdb..#tmp_consumo_locator_egr2') IS NULL
	BEGIN
		create table #tmp_consumo_locator_egr2 (rl_id numeric(20,0), cantidad numeric(20,5));
	END
	ELSE
	BEGIN
		DELETE FROM #tmp_consumo_locator_egr2
	END
	
	Open @RsExist_no_pick	
	
	Fetch Next From @RsExist_no_pick into	@Fecha_Vto,			@OrdenPicking,		@Tipo_Posicion,		@Codigo_Posicion,	@Cliente_id,		@Producto_id,		@Cantidad,			--@vRl_id,
											@NRO_BULTO,			@NRO_LOTE,			@EST_MERC_ID,		@NRO_DESPACHO,		@NRO_PARTIDA,		@UNIDAD_ID,			@PROP1,				
											@PROP2,				@PROP3,				@DESC,				@CAT_LOG_ID,		@FECHA_ALTA_GTW

	While @@Fetch_Status=0
	Begin
		SET @QTYPallet=0;
		--VALIDO LA CANTIDAD TOTAL DEL PALLET.
		SELECT	@QTYPallet =SUM(RL.CANTIDAD)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	DD.CLIENTE_ID=@Cliente_id
				AND DD.PRODUCTO_ID=@Producto_id
				AND DD.PROP1=@PROP1
				AND RL.DISPONIBLE='1'
				AND RL.DOC_TRANS_ID_EGR IS NULL
				AND RL.DOC_TRANS_ID_TR IS NULL
				AND RL.RL_ID NOT IN(SELECT RL_ID FROM #tmp_consumo_locator_egr2)
	
		IF @QTY_SOLICITADA>=@QTYPallet AND @QTY_SOLICITADA>0
		BEGIN
		
			insert into #tmp_consumo_locator_egr2
			SELECT	rl.rl_id, rl.cantidad
			FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			WHERE	DD.CLIENTE_ID=@Cliente_id
					AND DD.PRODUCTO_ID=@Producto_id
					AND DD.PROP1=@PROP1	
					AND RL.DOC_TRANS_ID_EGR IS NULL
					AND RL.DOC_TRANS_ID_TR IS NULL
					AND RL.RL_ID NOT IN(SELECT RL_ID FROM #tmp_consumo_locator_egr2)	
					
			insert into #tmp_consumo_locator_egr2 values(@rl_id,@rl_cant);
			IF @@ROWCOUNT>0
			BEGIN
				SET @QTY_SOLICITADA=@QTY_SOLICITADA - @QTYPallet
			END
		END
		Fetch Next From @RsExist_no_pick into	@Fecha_Vto,			@OrdenPicking,		@Tipo_Posicion,		@Codigo_Posicion,	@Cliente_id,		@Producto_id,		@Cantidad,			--@vRl_id,
												@NRO_BULTO,			@NRO_LOTE,			@EST_MERC_ID,		@NRO_DESPACHO,		@NRO_PARTIDA,		@UNIDAD_ID,			@PROP1,				
												@PROP2,				@PROP3,				@DESC,				@CAT_LOG_ID,		@FECHA_ALTA_GTW
	end
	close @RsExist_no_pick;
	deallocate @RsExist_no_pick;
	
	SELECT	@QTY_RET=SUM(CANTIDAD)
	FROM	#tmp_consumo_locator_egr2
	
	IF @QTY_RET IS NULL
	BEGIN
		SET @QTY_RET=0;
	END
	
END	
GO


