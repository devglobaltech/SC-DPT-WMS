
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 05:41 p.m.
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

ALTER    PROCEDURE [dbo].[Estacion_GetProductos_Egr]
@Picking_id		Numeric(20,0) 	Output,
@Tipo			Numeric(1,0)	Output
As
Begin
	Declare @Producto_id	as varchar(30)
	Declare @Cliente_id		as varchar(15)
	Declare @Documento_id	as Numeric(20,0)
	Declare @Nro_linea		as Numeric(10,0)
	declare @Nro_Lote		as varchar(100)
	declare @Nro_Partida	as varchar(100)

	Select 	 @Producto_id	= Producto_id
			,@Cliente_id	= Cliente_id
			,@Documento_Id	= Documento_id
			,@Nro_linea		= Nro_linea
			,@Nro_Lote		= Nro_Lote
			,@Nro_Partida	= Nro_Partida
	From	Picking (nolock)
	Where 	Picking_id		= @Picking_id


	If @Tipo=0
	Begin
		
--		Select	@Nro_Partida=Nro_partida
--		From	Det_Documento (nolock)
--		Where	Documento_id=@Documento_id
--				and Nro_linea=@Nro_Linea

		SELECT
				 dd.cliente_id				As CLIENTE_ID
				,dd.producto_id 			As PRODUCTO_ID
				,dd.DESCRIPCION				As DESCRIPCION
				,rl.cantidad				AS CANTIDAD
				,dd.NRO_BULTO				AS NRO_BULTO
				,dd.NRO_LOTE				AS NRO_LOTE
				,RL.EST_MERC_ID				AS EST_MERC_ID
				,dd.NRO_DESPACHO			AS NRO_DESPACHO
				,dd.NRO_PARTIDA				AS NRO_PARTIDA
				,dd.UNIDAD_ID				AS UNIDAD_ID
				,dd.PROP1					AS NRO_PALLET
				,dd.PROP2					AS PROP2
				,dd.PROP3					AS PROP3
				,RL.CAT_LOG_ID				AS CAT_LOG_ID
				,dd.fecha_vencimiento		AS FECHA_VENCIMIENTO
				,'POS' 						AS UBICACION
				,p.posicion_cod 			AS POSICION
				,isnull(p.orden_picking,999)AS ORDEN
				,rl.rl_id					AS RL_ID
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join posicion p (nolock) on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')
	
		UNION 
		SELECT
				 dd.cliente_id
				,dd.producto_id as Producto_Id
				,dd.DESCRIPCION
				,rl.cantidad
				,dd.NRO_BULTO
				,dd.NRO_LOTE
				,RL.EST_MERC_ID
				,dd.NRO_DESPACHO
				,dd.NRO_PARTIDA
				,dd.UNIDAD_ID
				,dd.PROP1
				,dd.PROP2
				,dd.PROP3
				,RL.CAT_LOG_ID
				,dd.fecha_vencimiento
				,'NAV' as ubicacion
				,n.nave_cod as posicion
				,isnull(n.orden_locator,999) as orden
				,rl.rl_id
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join nave n (nolock) on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')

	End
	Else
	Begin
		SELECT
				 dd.cliente_id				As CLIENTE_ID
				,dd.producto_id 			As PRODUCTO_ID
				,dd.DESCRIPCION				As DESCRIPCION
				,rl.cantidad				AS CANTIDAD
				,dd.NRO_BULTO				AS NRO_BULTO
				,dd.NRO_LOTE				AS NRO_LOTE
				,RL.EST_MERC_ID				AS EST_MERC_ID
				,dd.NRO_DESPACHO			AS NRO_DESPACHO
				,dd.NRO_PARTIDA				AS NRO_PARTIDA
				,dd.UNIDAD_ID				AS UNIDAD_ID
				,dd.PROP1					AS NRO_PALLET
				,dd.PROP2					AS PROP2
				,dd.PROP3					AS PROP3
				,RL.CAT_LOG_ID				AS CAT_LOG_ID
				,dd.fecha_vencimiento		AS FECHA_VENCIMIENTO
				,'POS' 						AS UBICACION
				,p.posicion_cod 			AS POSICION
				,isnull(p.orden_picking,999)AS ORDEN
				,rl.rl_id					AS RL_ID
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join posicion p (nolock) on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')
		UNION 
		SELECT
				 dd.cliente_id
				,dd.producto_id as Producto_Id
				,dd.DESCRIPCION
				,rl.cantidad
				,dd.NRO_BULTO
				,dd.NRO_LOTE
				,RL.EST_MERC_ID
				,dd.NRO_DESPACHO
				,dd.NRO_PARTIDA
				,dd.UNIDAD_ID
				,dd.PROP1
				,dd.PROP2
				,dd.PROP3
				,RL.CAT_LOG_ID
				,dd.fecha_vencimiento
				,'NAV' as ubicacion
				,n.nave_cod as posicion
				,isnull(n.orden_locator,999) as orden
				,rl.rl_id
		FROM 	rl_det_doc_trans_posicion rl (nolock)
				inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
				inner join nave n (nolock) on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
				left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
		WHERE
				rl.doc_trans_id_egr is null
				and rl.nro_linea_trans_egr is null
				and rl.disponible='1'
				and isnull(em.disp_egreso,'1')='1'
				and isnull(em.picking,'1')='1'
				and rl.cat_log_id<>'TRAN_EGR'
				and dd.producto_id	=@Producto_id
				and dd.cliente_id	=@Cliente_id
				and ISNULL(dd.Nro_Partida,'')	=ISNULL(@Nro_Partida,'')
				and ISNULL(dd.nro_lote,'') = ISNULL(@nro_lote,'')

	End

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

ALTER PROCEDURE  [dbo].[Estacion_Picking_Cont_PickinCorrecto] 
@DocumentoId	Numeric(20,0),--Documento de egreso
@ProductoId	Varchar(30),
@UltimoPickinId	Numeric(20,0),
@DifCantidad			Numeric(20,5),
@CantidadTotalxProducto Numeric(20,5)
AS
Begin
	set xact_abort on
	-----------------------------------------------------------------------------
	--Declaracion de Variables.
	-----------------------------------------------------------------------------
	Declare @OldRl_Id			as Numeric(20,0)
	Declare @Old_Doc_Trans_Id	as Numeric(20,0)
	Declare @QtyPicking			as Float
	Declare @QtyRl				as Float
	Declare @Documento_Id		as Numeric(20,0)
	Declare @Nro_Linea			as Numeric(10,0)
	Declare @PreEgrId			as Numeric(20,0)
	Declare @Doc_Trans_IdEgr	as Numeric(20,0)
	Declare @Nro_Linea_TransEgr	as Numeric(10,0)
	Declare @Documento_IdNew	as Numeric(20,0)
	Declare @Nro_LineaNew		as Numeric(10,0)
	Declare @Dif				as Float
	Declare @MaxLinea			as Numeric(10,0)
	Declare @Doc_Trans_Id		as Numeric(20,0)
	Declare @MaxLineaDDT		as Numeric(10,0)
	Declare @SplitRl			as Numeric(20,0)
	Declare @Producto_IdC		as Varchar(30)
	Declare @Cliente_IdC		as Varchar(15)
	Declare @Cat_log_Id_Final	as Varchar(50)
	-----------------------------------------------------------------------------
	Declare @NRO_SERIE			as varchar(50)
	Declare @NRO_SERIE_PADRE	as varchar(50)
	Declare @EST_MERC_ID		as varchar(15)
	Declare @CAT_LOG_ID			as varchar(15)
	Declare @NRO_BULTO			as varchar(50)
	Declare @DESCRIPCION		as varchar(200)
	Declare @NRO_LOTE			as varchar(50)
	Declare @FECHA_VENCIMIENTO	as datetime
	Declare @NRO_DESPACHO		as varchar(50)
	Declare @NRO_PARTIDA		as varchar(50)
	Declare @UNIDAD_ID			as varchar(5)
	Declare @PESO				as numeric(20,5)
	Declare @UNIDAD_PESO		as varchar(5)
	Declare @VOLUMEN			as numeric(20,5)
	Declare @UNIDAD_VOLUMEN		as varchar(5)
	Declare @BUSC_INDIVIDUAL	as varchar(1)
	Declare @TIE_IN				as varchar(1)
	Declare @NRO_TIE_IN			as varchar(100)
	Declare @ITEM_OK			as varchar(1)
	Declare @MONEDA_ID			as varchar(20)
	Declare @COSTO				as numeric(20,3)
	Declare @PROP1				as varchar(100)
	Declare @PROP2				as varchar(100)
	Declare @PROP3				as varchar(100)
	Declare @LARGO				as numeric(10,3)
	Declare @ALTO				as numeric(10,3)
	Declare @ANCHO				as numeric(10,3)
	Declare @VOLUMEN_UNITARIO	as varchar(1)
	Declare @PESO_UNITARIO		as varchar(1)
	Declare @CANT_SOLICITADA	as numeric(20,5)	
	-----------------------------------------------------------------------------
	Declare @PALLET_HOMBRE		AS CHAR(1)
	Declare @Transf				as char(1)

	--Variables Catalina
	Declare @PALLET_PICKING     AS NUMERIC(20)
	Declare @RUTA				AS VARCHAR(50)
	Declare @FECHAINICIO		AS DATETIME
	Declare @USUARIO			AS VARCHAR(30)
	Declare @PICKING_ID_REF		AS NUMERIC(20,0)
	Declare @CANTIDAD_OldRl_id	AS NUMERIC(20,5)
	Declare @PosicionActual_Old	AS NUMERIC(20,0)
	--Declare @CantidadTotalxProducto		AS NUMERIC(20,5)
    Declare @CantidadTotalaCorregir		AS NUMERIC(20,5)
	Declare @DifTotal as Float
	Declare @Cantidad AS NUMERIC(20,5)
	Declare @Picking_Id as NUMERIC(20,0)
	Declare @NewRl_Id	as	Numeric(20,0)

	SET @CantidadTotalaCorregir=0
	
	DECLARE Picking_Cursor CURSOR FOR
	SELECT  Cantidad,Picking_Id,Nro_Linea From Picking  Where DOCUMENTO_ID=@DocumentoId AND PRODUCTO_ID=@ProductoId
		AND PICKING_ID<>@UltimoPickinId AND CANT_CONFIRMADA IS NULL
		ORDER BY Cantidad DESC

	
	OPEN Picking_Cursor
	FETCH NEXT FROM Picking_Cursor INTO @Cantidad, @Picking_Id,@Nro_Linea
	
	WHILE @@FETCH_STATUS = 0 AND @CantidadTotalaCorregir<>@CantidadTotalxProducto
	BEGIN	
	
	
	IF @Cantidad = @DifCantidad
	BEGIN
	
		Select	 @Documento_Id	=Documento_id
			,@Nro_Linea 	=Nro_linea
			,@PALLET_PICKING = PALLET_PICKING
			,@RUTA= RUTA
			,@FECHAINICIO=FECHA_INICIO
			,@USUARIO=USUARIO
			,@PICKING_ID_REF = ISNULL(PICKING_ID_REF,0)
			,@ProductoId = PRODUCTO_ID
	From	Picking
	Where	Picking_Id		=@Picking_Id
	
	
	Select 	@OldRl_Id=Rl.Rl_Id,@Old_Doc_Trans_Id=ddt.DOC_TRANS_ID
			From	Rl_Det_Doc_Trans_posicion Rl
					Inner join Det_Documento_Transaccion ddt
					On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
					Inner Join Det_Documento dd
					on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
			Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
		
		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea
			
	
		Update 	 Rl_Det_Doc_Trans_posicion 
			Set 	 Disponible				='1'
					,Doc_Trans_Id_Egr		=null
					,Nro_Linea_Trans_Egr	=null
					,Posicion_Actual		=Posicion_Anterior
					,Posicion_Anterior		=Null
					,Nave_Actual			=Nave_Anterior
					,Nave_Anterior			=1
					,Cat_log_id				=@Cat_log_Id_Final
			Where	Rl_Id					=@OldRl_Id
		
		Delete from PICKING where DOCUMENTO_ID=@Documento_id and PICKING_ID=@Picking_Id
		Delete from DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@Documento_id AND NRO_LINEA_DOC=@Nro_Linea
		Delete from DET_DOCUMENTO WHERE DOCUMENTO_ID=@Documento_id AND NRO_LINEA=@Nro_Linea
		
	END
		IF @Cantidad > @DifCantidad
		BEGIN
			Set @Dif= @Cantidad - @DifCantidad--50-30=20


		Select	 @Documento_Id	=Documento_id
			,@Nro_Linea 	=Nro_linea
			,@PALLET_PICKING = PALLET_PICKING
			,@RUTA= RUTA
			,@FECHAINICIO=FECHA_INICIO
			,@USUARIO=USUARIO
			,@PICKING_ID_REF = ISNULL(PICKING_ID_REF,0)
			,@ProductoId = PRODUCTO_ID
		From	Picking
		Where	Picking_Id		=@Picking_Id
	
		--Obtengo la Rl Anterior.
		Select 	@OldRl_Id=Rl.Rl_Id
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea

		--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
		Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
				,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@OldRl_id
		
		--Actualizo la cantidad en la linea original de det_documento.	
		Update Det_Documento Set Cantidad=@Dif where Documento_Id=@Documento_id And Nro_Linea=@Nro_linea
		
		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea

		--Inserto en Rl el sobrante
		Insert into Rl_Det_Doc_Trans_Posicion
		Select 	 Doc_Trans_id
				,Nro_Linea_Trans
				,NULL
				,Posicion_Anterior
				,@DifCantidad	--Cantidad
				,Tipo_movimiento_Id
				,Ultima_Estacion
				,Ultima_Secuencia
				,'1'
				,NULL
				,Documento_id
				,Nro_Linea
				,'1'
				,NULL
				,NULL
				,Doc_Trans_Id_Tr
				,Nro_Linea_Trans_Tr
				,Cliente_id
				,@Cat_log_Id_Final
				,@Cat_log_Id_Final
				,Est_Merc_Id
		From	Rl_Det_Doc_Trans_Posicion
		Where	Rl_Id=@OldRl_Id
		
		
		--Actualizo la cantidad en Rl
		Update Rl_det_doc_Trans_Posicion Set Cantidad=@Dif where Rl_id=@OldRl_Id
		Update Picking Set Cantidad=@Dif Where Picking_id=@Picking_id

		
		Update 	Consumo_Locator_Egr 
		Set 	Cantidad= Cantidad ,
				saldo 	= (Saldo + (Cantidad))
		Where	Documento_id=Documento_id
				and Nro_linea=@Nro_linea
	 END
	 IF @Cantidad < @DifCantidad
		BEGIN
			Select	 @Documento_Id	=Documento_id
					,@Nro_Linea 	=Nro_linea
					,@PALLET_PICKING = PALLET_PICKING
					,@RUTA= RUTA
					,@FECHAINICIO=FECHA_INICIO
					,@USUARIO=USUARIO
					,@PICKING_ID_REF = ISNULL(PICKING_ID_REF,0)
					,@ProductoId = PRODUCTO_ID
			From	Picking
			Where	Picking_Id		=@Picking_Id
			
			
			Select 	@OldRl_Id=Rl.Rl_Id,@Old_Doc_Trans_Id=ddt.DOC_TRANS_ID
					From	Rl_Det_Doc_Trans_posicion Rl
							Inner join Det_Documento_Transaccion ddt
							On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
							Inner Join Det_Documento dd
							on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
					Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
				
				Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea
					
			
				Update 	 Rl_Det_Doc_Trans_posicion 
					Set 	 Disponible				='1'
							,Doc_Trans_Id_Egr		=null
							,Nro_Linea_Trans_Egr	=null
							,Posicion_Actual		=Posicion_Anterior
							,Posicion_Anterior		=Null
							,Nave_Actual			=Nave_Anterior
							,Nave_Anterior			=1
							,Cat_log_id				=@Cat_log_Id_Final
					Where	Rl_Id					=@OldRl_Id
				
				Delete from PICKING where DOCUMENTO_ID=@Documento_id and PICKING_ID=@Picking_Id
				Delete from DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@Documento_id AND NRO_LINEA_DOC=@Nro_Linea
				Delete from DET_DOCUMENTO WHERE DOCUMENTO_ID=@Documento_id AND NRO_LINEA=@Nro_Linea

		END
		
		SELECT @CantidadTotalaCorregir=SUM(CANTIDAD) FROM PICKING WHERE DOCUMENTO_ID=@DocumentoId AND PRODUCTO_ID=@ProductoId
		
	
	
	FETCH NEXT FROM Picking_Cursor INTO  @Cantidad, @Picking_Id,@Nro_Linea
	END	
	--COMMIT TRANSACTION
	CLOSE Picking_Cursor
	DEALLOCATE Picking_Cursor
	
	

	If @@Error<>0
	Begin
		raiserror('Se produjo un error inesperado.',16,1)
		return
	End
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

ALTER PROCEDURE  [dbo].[Estacion_Picking_ActNroLinea_Cont] 
@NewRl_Id		Numeric(20,0) Output,
@Picking_Id		Numeric(20,0) Output,
@Cantidad		Numeric(20,5) Output
AS
Begin
	set xact_abort on
	-----------------------------------------------------------------------------
	--Declaracion de Variables.
	-----------------------------------------------------------------------------
	Declare @OldRl_Id			as Numeric(20,0)
	Declare @QtyPicking			as Float
	Declare @QtyRl				as Float
	Declare @Documento_Id		as Numeric(20,0)
	Declare @Nro_Linea			as Numeric(10,0)
	Declare @PreEgrId			as Numeric(20,0)
	Declare @Doc_Trans_IdEgr	as Numeric(20,0)
	Declare @Nro_Linea_TransEgr	as Numeric(10,0)
	Declare @Documento_IdNew	as Numeric(20,0)
	Declare @Nro_LineaNew		as Numeric(10,0)
	Declare @Dif				as Float
	Declare @MaxLinea			as Numeric(10,0)
	Declare @Doc_Trans_Id		as Numeric(20,0)
	Declare @MaxLineaDDT		as Numeric(10,0)
	Declare @SplitRl			as Numeric(20,0)
	Declare @Producto_IdC		as Varchar(30)
	Declare @Cliente_IdC		as Varchar(15)
	Declare @Cat_log_Id_Final	as Varchar(50)
	-----------------------------------------------------------------------------
	Declare @NRO_SERIE			as varchar(50)
	Declare @NRO_SERIE_PADRE	as varchar(50)
	Declare @EST_MERC_ID		as varchar(15)
	Declare @CAT_LOG_ID			as varchar(15)
	Declare @NRO_BULTO			as varchar(50)
	Declare @DESCRIPCION		as varchar(200)
	Declare @NRO_LOTE			as varchar(50)
	Declare @FECHA_VENCIMIENTO	as datetime
	Declare @NRO_DESPACHO		as varchar(50)
	Declare @NRO_PARTIDA		as varchar(50)
	Declare @UNIDAD_ID			as varchar(5)
	Declare @PESO				as numeric(20,5)
	Declare @UNIDAD_PESO		as varchar(5)
	Declare @VOLUMEN			as numeric(20,5)
	Declare @UNIDAD_VOLUMEN		as varchar(5)
	Declare @BUSC_INDIVIDUAL	as varchar(1)
	Declare @TIE_IN				as varchar(1)
	Declare @NRO_TIE_IN			as varchar(100)
	Declare @ITEM_OK			as varchar(1)
	Declare @MONEDA_ID			as varchar(20)
	Declare @COSTO				as numeric(20,3)
	Declare @PROP1				as varchar(100)
	Declare @PROP2				as varchar(100)
	Declare @PROP3				as varchar(100)
	Declare @LARGO				as numeric(10,3)
	Declare @ALTO				as numeric(10,3)
	Declare @ANCHO				as numeric(10,3)
	Declare @VOLUMEN_UNITARIO	as varchar(1)
	Declare @PESO_UNITARIO		as varchar(1)
	Declare @CANT_SOLICITADA	as numeric(20,5)	
	-----------------------------------------------------------------------------
	Declare @PALLET_HOMBRE		AS CHAR(1)
	Declare @Transf				as char(1)

	--Variables Catalina
	Declare @PALLET_PICKING     AS NUMERIC(20)
	Declare @RUTA				AS VARCHAR(50)
	Declare @FECHAINICIO		AS DATETIME
	Declare @USUARIO			AS VARCHAR(30)
	Declare @PICKING_ID_REF		AS NUMERIC(20,0)
	Declare @CANTIDAD_OldRl_id	AS NUMERIC(20,5)
	Declare @PosicionActual_Old	AS NUMERIC(20,0)
	Declare @CantidadTotalxProducto		AS NUMERIC(20,5)
    Declare @CantidadTotalaCorregir		AS NUMERIC(20,5)
	Declare @ProductoId as VARCHAR(30)
	Declare @DifTotal as Float
	Declare @UltimoPickinId		as Numeric(20,0)	

	--Obtengo las Cantidades.
	Select @QtyPicking=Cantidad from picking where picking_id=@Picking_Id
	Select @QtyRl= Cantidad From Rl_Det_Doc_Trans_Posicion Where Rl_Id=@NewRl_Id
	--Se agrega esta validación cuando las cantidades a pickear son mayores
	
	IF (@Cantidad<>@QtyPicking) AND (@Cantidad=@QtyRl OR @Cantidad<@QtyRl)
		BEGIN
		SELECT @QtyPicking =@Cantidad
		END
	

	--Estos valores me van a servir mas adelante.
	Select	 @Documento_Id	=Documento_id
			,@Nro_Linea 	=Nro_linea
			,@PALLET_PICKING = PALLET_PICKING
			,@RUTA= RUTA
			,@FECHAINICIO=FECHA_INICIO
			,@USUARIO=USUARIO
			,@PICKING_ID_REF = ISNULL(PICKING_ID_REF,0)
			,@ProductoId = PRODUCTO_ID
	From	Picking
	Where	Picking_Id		=@Picking_Id

	select	@PALLET_HOMBRE=flg_pallet_hombre
	from	cliente_parametros c inner join documento d
			on(c.cliente_id=d.cliente_id)
	where	d.documento_id=@Documento_Id

	--Saco la nave de preegreso.
	Select	@PreEgrId=Nave_Id
	From	Nave
	Where	Pre_Egreso='1'
	
	Select @CantidadTotalxProducto = (SUM(CANTIDAD)-SUM(ISNULL(CANT_CONFIRMADA,0)))  from PICKING where 
		DOCUMENTO_ID=@Documento_Id and PRODUCTO_ID=@ProductoId

	--Obtengo el Nuevo Documento y numero de linea para Updetear.
	Select 	 Distinct
			 @Documento_idNew	=dd.Documento_Id
			,@Nro_lineaNew		=dd.Nro_Linea
			,@PosicionActual_Old = Posicion_Actual
	From	Rl_Det_Doc_Trans_posicion Rl
			Inner join Det_Documento_Transaccion ddt
			On(Rl.Doc_Trans_id=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=ddt.Nro_Linea_Trans)
			Inner Join Det_Documento dd
			on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
	Where	Rl.Rl_id=@NewRl_Id
	
	If (@QtyPicking = @QtyRL)
	Begin
			--Obtengo la Rl Anterior.
			Select 	@OldRl_Id=Rl.Rl_Id
			From	Rl_Det_Doc_Trans_posicion Rl
					Inner join Det_Documento_Transaccion ddt
					On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
					Inner Join Det_Documento dd
					on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
			Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
			
			Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea
			
			--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
			Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
					,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
					,@CANTIDAD_OldRl_id = CANTIDAD
			From	Rl_Det_Doc_Trans_posicion
			Where	Rl_Id=@OldRl_id

			--Restauro la rl Anterior

			IF @PICKING_ID_REF = 0 
			BEGIN
			Update 	 Rl_Det_Doc_Trans_posicion 
			Set 	 Disponible				='1'
					,Doc_Trans_Id_Egr		=null
					,Nro_Linea_Trans_Egr	=null
					,Posicion_Actual		=Posicion_Anterior
					,Posicion_Anterior		=Null
					,Nave_Actual			=Nave_Anterior
					,Nave_Anterior			=1
					,Cat_log_id				=@Cat_log_Id_Final
			Where	Rl_Id					=@OldRl_Id

		 END
		ELSE
			BEGIN
				Update 	 Rl_Det_Doc_Trans_posicion 
			Set 	 CANTIDAD = @CANTIDAD_OldRl_id-@QtyPicking
			Where	Rl_Id =@OldRl_Id

			END
		
		
			--Consumo la Nueva Rl
			Update	Rl_Det_Doc_Trans_Posicion 
			Set 	 Disponible='0'
					,Posicion_Anterior=Posicion_Actual
					,Posicion_Actual=Null
					,Nave_Anterior=Nave_Actual
					,Nave_Actual=@PreEgrId
					,Doc_Trans_id_Egr=@Doc_Trans_IdEgr
					,Nro_Linea_Trans_Egr=@Nro_Linea_TransEgr
					,Cat_log_Id='TRAN_EGR'
			Where	Rl_id=@NewRl_Id

			--Saco los valores de la Nueva linea de det_documento
			Select	  @NRO_SERIE				=Nro_Serie
					, @NRO_SERIE_PADRE			=Nro_Serie_Padre
					, @EST_MERC_ID				=Est_Merc_Id
					, @CAT_LOG_ID				=Cat_log_id
					, @NRO_BULTO				=Nro_Bulto
					, @DESCRIPCION				=Descripcion
					, @NRO_LOTE					=Nro_Lote
					, @FECHA_VENCIMIENTO		=Fecha_Vencimiento
					, @NRO_DESPACHO				=Nro_Despacho
					, @NRO_PARTIDA				=Nro_Partida
					, @UNIDAD_ID				=Unidad_Id
					, @PESO						=Peso
					, @UNIDAD_PESO				=Unidad_Peso
					, @VOLUMEN					=Volumen
					, @UNIDAD_VOLUMEN			=Unidad_Volumen
					, @BUSC_INDIVIDUAL			=Busc_Individual
					, @TIE_IN					=Tie_In
					, @NRO_TIE_IN				=Nro_Tie_In
					, @ITEM_OK					=Item_Ok
					--, @CAT_LOG_ID_FINAL			=Cat_Log_Id_Final
					, @MONEDA_ID				=Moneda_id
					, @COSTO					=Costo
					, @PROP1					=Prop1
					, @PROP2					=Prop2
					, @PROP3					=Prop3
					, @LARGO					=largo
					, @ALTO						=Alto
					, @ANCHO					=Ancho
					, @VOLUMEN_UNITARIO			=Volumen_Unitario
					, @PESO_UNITARIO			=Peso_Unitario
					, @CANT_SOLICITADA			=Cant_Solicitada
			FROM 	DET_DOCUMENTO				
			Where	Documento_Id=@Documento_idNew
					And Nro_linea=@Nro_LineaNew

			--Actualizo Det_Documento
			Update Det_Documento
			Set
					  Nro_Serie			=@NRO_SERIE				
					, Nro_Serie_padre	=@NRO_SERIE_PADRE		
					, Est_Merc_Id		=@EST_MERC_ID			
					, Cat_log_id		= 'TRAN_EGR'				
					, Nro_Bulto			=@NRO_BULTO				
					, Descripcion		=@DESCRIPCION			
					, Nro_Lote			=@NRO_LOTE				
					, Fecha_Vencimiento	=@FECHA_VENCIMIENTO		
					, Nro_Despacho		=@NRO_DESPACHO			
					, nro_partida		=@NRO_PARTIDA			
					, Unidad_id			=@UNIDAD_ID				
					, Peso				=@PESO					
					, Unidad_Peso		=@UNIDAD_PESO			
					, Volumen			=@VOLUMEN				
					, Unidad_Volumen	=@UNIDAD_VOLUMEN			
					, busc_individual	=@BUSC_INDIVIDUAL		
					, tie_in			=@TIE_IN					
					, Nro_Tie_in		=@NRO_TIE_IN				
					, Item_ok			=@ITEM_OK				
					--, Cat_log_Id_Final	=@CAT_LOG_ID_FINAL		
					, Moneda_id			=@MONEDA_ID				
					, Costo				=@COSTO					
					, Prop1				=@PROP1					
					, Prop2				=@PROP2					
					, Prop3				=@PROP3					
					, Largo				=@LARGO					
					, Alto				=@ALTO					
					, Ancho				=@ANCHO					
					, Volumen_Unitario	=@VOLUMEN_UNITARIO		
					, Peso_Unitario		=@PESO_UNITARIO		
					--, Cant_solicitada	=ISNULL(@CANT_SOLICITADA,CANTIDAD)
					, CANTIDAD = @QtyPicking
			Where	Documento_id=@Documento_id
					And Nro_Linea=@Nro_Linea

			--Elimino la Linea de Picking
			Delete From Picking Where Picking_Id=@Picking_Id

			--Inserto la Nueva linea de Picking.
			INSERT INTO PICKING 
			SELECT 	 DISTINCT
					 DD.DOCUMENTO_ID
					,DD.NRO_LINEA
					,DD.CLIENTE_ID
					,DD.PRODUCTO_ID 
					,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
					,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
					,P.DESCRIPCION
					,@QtyPicking--DD.CANTIDAD
					,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
					,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
					,@RUTA
					,DD.PROP1
					,@FECHAINICIO AS FECHA_INICIO
					,NULL AS FECHA_FIN
					,@USUARIO AS USUARIO
					,NULL AS CANT_CONFIRMADA
					,@PALLET_PICKING AS PALLET_PICKING
					,0 	  AS SALTO_PICKING
					,'0'  AS PALLET_CONTROLADO
					,NULL AS USUARIO_CONTROL_PICKING
					,'0'  AS ST_ETIQUETAS
					,'0'  AS ST_CAMION
					,'0'  AS FACTURADO
					,'0'  AS FIN_PICKING
					,'0'  AS ST_CONTROL_EXP
					,NULL AS FECHA_CONTROL_PALLET
					,NULL AS TERMINAL_CONTROL_PALLET
					,NULL AS FECHA_CONTROL_EXP
					,NULL AS USUARIO_CONTROL_EXP
					,NULL AS TERMINAL_CONTROL_EXPEDICION
					,NULL AS FECHA_CONTROL_FAC
					,NULL AS USUARIO_CONTROL_FAC
					,NULL AS TERMINAL_CONTROL_FAC
					,NULL AS VEHICULO_ID
					,NULL AS PALLET_COMPLETO
					,NULL AS HIJO
					,NULL AS QTY_CONTROLADO
					,NULL AS PALLET_FINAL
					,NULL AS PALLET_CERRADO
					,NULL AS USUARIO_PF
					,NULL AS TERMINAL_PF
					,'0'  AS REMITO_IMPRESO
					,NULL AS NRO_REMITO_PF
					,NULL AS PICKING_ID_REF
					,NULL AS BULTOS_CONTROLADOS
					,NULL AS BULTOS_NO_CONTROLADOS
					,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE --CAMBIAR
					,0	  AS TRANSF_TERMINANDA	--CAMBIAR
					,DD.NRO_LOTE,DD.NRO_PARTIDA,NULL
			FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
					ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
					INNER JOIN PRODUCTO P
					ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
					LEFT JOIN POSICION POS
					ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
					LEFT JOIN NAVE N2
					ON(POS.NAVE_ID=N2.NAVE_ID)
			WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
					And dd.Nro_linea=@Nro_Linea

			SELECT @UltimoPickinId = SCOPE_IDENTITY() 

			Select 	@Cliente_IdC= Cliente_Id,
					@Producto_idC= Producto_Id
			From	Det_Documento 
			Where	Documento_id=@Documento_id
					And Nro_Linea=@Nro_Linea

			Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QtyPicking,@NewRl_Id,0,2,GETDATE(),'S')

		SELECT @CantidadTotalaCorregir = (SUM(CANTIDAD)-SUM(ISNULL(CANT_CONFIRMADA,0))) 
			FROM PICKING WHERE DOCUMENTO_ID=@Documento_id AND PRODUCTO_ID=@Productoid
		IF @CantidadTotalaCorregir>@CantidadTotalxProducto
			BEGIN
				Select @DifTotal = @CantidadTotalaCorregir-@CantidadTotalxProducto
				exec dbo.Estacion_Picking_Cont_PickinCorrecto @Documento_id,@Producto_idC,@UltimoPickinId,@DifTotal,@CantidadTotalxProducto
				
			END
		
		
	End--Fin Picking=Rl 1er. caso

	If (@QtyPicking < @QtyRL)
	Begin	
		Set @Dif= @QtyRL - @QtyPicking

		--Obtengo la Rl Anterior.
		Select 	@OldRl_Id=Rl.Rl_Id
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
			
		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea

		--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
		Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
				,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
				,@CANTIDAD_OldRl_id = CANTIDAD
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@OldRl_id
		
		--Spliteo la Rl.
		Insert into Rl_Det_Doc_Trans_Posicion
		Select 	 Doc_Trans_id
				,Nro_Linea_Trans
				,Posicion_Anterior
				,Posicion_Actual
				,@Dif	--Cantidad
				,Tipo_movimiento_Id
				,Ultima_Estacion
				,Ultima_Secuencia
				,Nave_Anterior
				,Nave_Actual
				,Documento_id
				,Nro_Linea
				,Disponible
				,Doc_Trans_id_Egr
				,Nro_Linea_Trans_Egr
				,Doc_Trans_Id_Tr
				,Nro_Linea_Trans_Tr
				,Cliente_id
				,Cat_log_Id
				,Cat_Log_Id_Final
				,Est_Merc_Id
		From	Rl_Det_Doc_Trans_Posicion
		Where	Rl_Id=@NewRl_id

		--Consumo la Rl.
		Update	Rl_Det_Doc_Trans_Posicion 
		Set 	 Disponible='0'
				,Cantidad=@QtyPicking
				,Posicion_Anterior=Posicion_Actual
				,Posicion_Actual=Null
				,Nave_Anterior=Nave_Actual
				,Nave_Actual=@PreEgrId
				,Doc_Trans_id_Egr=@Doc_Trans_IdEgr
				,Nro_Linea_Trans_Egr=@Nro_Linea_TransEgr
				,Cat_log_Id='TRAN_EGR'
		Where	Rl_id=@NewRl_Id

		--Restauro la rl Anterior.
		IF @PICKING_ID_REF = 0 
			BEGIN
		Update 	 Rl_Det_Doc_Trans_posicion 
		Set 	 Disponible				='1'
				,Doc_Trans_Id_Egr		=null
				,Nro_Linea_Trans_Egr	=null
				,Posicion_Actual		=Posicion_Anterior
				,Posicion_Anterior		=Null
				,Nave_Actual			=Nave_Anterior
				,Nave_Anterior			='1'
				,Cat_log_id				=@Cat_log_Id_Final
		Where	Rl_Id					=@OldRl_Id

		END
		ELSE
			BEGIN
			--ACTUALIZO EL REGISTRO QUE FUE TOMADO EN LA PRIMERA TAREA, ACTUALIZANDO SOLO LA CANTIDAD
			Update 	 Rl_Det_Doc_Trans_posicion 
			Set 	 CANTIDAD = @CANTIDAD_OldRl_id-@QtyPicking
			Where	Rl_Id =@OldRl_Id
			
			--ACTUALIZO EL REGISTRO QUE TENGA STOCK DISPONIBLE RL_DET_DOC_TRANS_POSICION CON LO QUE QUEDA PARA QUE EL STOCK QUEDE CORRECTO
				Insert into Rl_Det_Doc_Trans_Posicion
				Select 	 Doc_Trans_id
						,Nro_Linea_Trans
						,null
						,Posicion_Anterior
						,@QtyPicking	--Cantidad
						,Tipo_movimiento_Id
						,Ultima_Estacion
						,Ultima_Secuencia
						,'1'
						,Nave_Anterior
						,Documento_id
						,Nro_Linea
						,'1'
						,null
						,null
						,Doc_Trans_Id_Tr
						,Nro_Linea_Trans_Tr
						,Cliente_id
						,@Cat_log_Id_Final
						,Cat_Log_Id_Final
						,Est_Merc_Id
				From	Rl_Det_Doc_Trans_Posicion
				Where	Rl_Id=@OldRl_Id

		END
		
		--Saco los valores de la Nueva linea de det_documento.
		Select	  @NRO_SERIE				=Nro_Serie
				, @NRO_SERIE_PADRE			=Nro_Serie_Padre
				, @EST_MERC_ID				=Est_Merc_Id
				, @CAT_LOG_ID				=Cat_log_id
				, @NRO_BULTO				=Nro_Bulto
				, @DESCRIPCION				=Descripcion
				, @NRO_LOTE					=Nro_Lote
				, @FECHA_VENCIMIENTO		=Fecha_Vencimiento
				, @NRO_DESPACHO				=Nro_Despacho
				, @NRO_PARTIDA				=Nro_Partida
				, @UNIDAD_ID				=Unidad_Id
				, @PESO						=Peso
				, @UNIDAD_PESO				=Unidad_Peso
				, @VOLUMEN					=Volumen
				, @UNIDAD_VOLUMEN			=Unidad_Volumen
				, @BUSC_INDIVIDUAL			=Busc_Individual
				, @TIE_IN					=Tie_In
				, @NRO_TIE_IN				=Nro_Tie_In
				, @ITEM_OK					=Item_Ok
				--, @CAT_LOG_ID_FINAL			=Cat_Log_Id_Final
				, @MONEDA_ID				=Moneda_id
				, @COSTO					=Costo
				, @PROP1					=Prop1
				, @PROP2					=Prop2
				, @PROP3					=Prop3
				, @LARGO					=largo
				, @ALTO						=Alto
				, @ANCHO					=Ancho
				, @VOLUMEN_UNITARIO			=Volumen_Unitario
				, @PESO_UNITARIO			=Peso_Unitario
				, @CANT_SOLICITADA			=Cant_Solicitada
		FROM 	DET_DOCUMENTO				
		Where	Documento_Id=@Documento_idNew
				And Nro_linea=@Nro_LineaNew

		--Actualizo Det_Documento
		Update Det_Documento
		Set
				  Nro_Serie			=@NRO_SERIE				
				, Nro_Serie_padre	=@NRO_SERIE_PADRE		
				, Est_Merc_Id		=@EST_MERC_ID			
				, Cat_log_id		='TRAN_EGR'				
				, Nro_Bulto			=@NRO_BULTO				
				, Descripcion		=@DESCRIPCION			
				, Nro_Lote			=@NRO_LOTE				
				, Fecha_Vencimiento	=@FECHA_VENCIMIENTO		
				, Nro_Despacho		=@NRO_DESPACHO			
				, nro_partida		=@NRO_PARTIDA			
				, Unidad_id			=@UNIDAD_ID				
				, Peso				=@PESO					
				, Unidad_Peso		=@UNIDAD_PESO			
				, Volumen			=@VOLUMEN				
				, Unidad_Volumen	=@UNIDAD_VOLUMEN			
				, busc_individual	=@BUSC_INDIVIDUAL		
				, tie_in			=@TIE_IN					
				, Nro_Tie_in		=@NRO_TIE_IN				
				, Item_ok			=@ITEM_OK				
				--, Cat_log_Id_Final	=@CAT_LOG_ID_FINAL		
				, Moneda_id			=@MONEDA_ID				
				, Costo				=@COSTO					
				, Prop1				=@PROP1					
				, Prop2				=@PROP2					
				, Prop3				=@PROP3					
				, Largo				=@LARGO					
				, Alto				=@ALTO					
				, Ancho				=@ANCHO					
				, Volumen_Unitario	=@VOLUMEN_UNITARIO		
				, Peso_Unitario		=@PESO_UNITARIO		
				, CANTIDAD			=@QtyPicking
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		--Elimino la Linea de Picking
		Delete From Picking Where Picking_Id=@Picking_Id

		--Inserto la Nueva linea de Picking.
		INSERT INTO PICKING 
		SELECT 	 DISTINCT
				 DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,DD.CLIENTE_ID
				,DD.PRODUCTO_ID 
				,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
				,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
				,P.DESCRIPCION
				,@QtyPicking--DD.CANTIDAD
				,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
				,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
				,@RUTA
				,DD.PROP1
				,@FECHAINICIO AS FECHA_INICIO
				,NULL AS FECHA_FIN
				,@USUARIO AS USUARIO
				,NULL AS CANT_CONFIRMADA
				,@PALLET_PICKING AS PALLET_PICKING
				,0 	  AS SALTO_PICKING
				,'0'  AS PALLET_CONTROLADO
				,NULL AS USUARIO_CONTROL_PICKING
				,'0'  AS ST_ETIQUETAS
				,'0'  AS ST_CAMION
				,'0'  AS FACTURADO
				,'0'  AS FIN_PICKING
				,'0'  AS ST_CONTROL_EXP
				,NULL AS FECHA_CONTROL_PALLET
				,NULL AS TERMINAL_CONTROL_PALLET
				,NULL AS FECHA_CONTROL_EXP
				,NULL AS USUARIO_CONTROL_EXP
				,NULL AS TERMINAL_CONTROL_EXPEDICION
				,NULL AS FECHA_CONTROL_FAC
				,NULL AS USUARIO_CONTROL_FAC
				,NULL AS TERMINAL_CONTROL_FAC
				,NULL AS VEHICULO_ID
				,NULL AS PALLET_COMPLETO
				,NULL AS HIJO
				,NULL AS QTY_CONTROLADO
				,NULL AS PALLET_FINAL
				,NULL AS PALLET_CERRADO
				,NULL AS USUARIO_PF
				,NULL AS TERMINAL_PF
				,'0'  AS REMITO_IMPRESO
				,NULL AS NRO_REMITO_PF
				,NULL AS PICKING_ID_REF
				,NULL AS BULTOS_CONTROLADOS
				,NULL AS BULTOS_NO_CONTROLADOS
				,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
				,0	  AS TRANSF_TERMINANDA
				,DD.NRO_LOTE,DD.NRO_PARTIDA,NULL
		FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
				ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN PRODUCTO P
				ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
				LEFT JOIN NAVE N
				ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
				LEFT JOIN POSICION POS
				ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
				LEFT JOIN NAVE N2
				ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				And dd.Nro_linea=@Nro_Linea
				
		SELECT @UltimoPickinId = SCOPE_IDENTITY() 

		Select 	@Cliente_IdC= Cliente_Id,
				@Producto_idC= Producto_Id
		From	Det_Documento 
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QtyPicking,@NewRl_Id,0,2,GETDATE(),'S')
		
		SELECT @CantidadTotalaCorregir = (SUM(CANTIDAD)-SUM(ISNULL(CANT_CONFIRMADA,0))) 
			 FROM PICKING WHERE DOCUMENTO_ID=@Documento_id AND PRODUCTO_ID=@Productoid
		IF @CantidadTotalaCorregir>@CantidadTotalxProducto
			BEGIN
				Select @DifTotal = @CantidadTotalaCorregir-@CantidadTotalxProducto
				exec dbo.Estacion_Picking_Cont_PickinCorrecto @Documento_id,@Producto_idC,@UltimoPickinId,@DifTotal,@CantidadTotalxProducto
				
			END

	End --Fin @QtyPicking < @QtyRL 2do. Caso.

	If (@QtyPicking > @QtyRL)	
	Begin
		Set @Dif= @QtyPicking - @QtyRL

		--Obtengo la Rl Anterior.
		Select 	@OldRl_Id=Rl.Rl_Id
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea

		--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
		Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
				,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@OldRl_id
		
		--Actualizo la cantidad en la linea original de det_documento.	
		Update Det_Documento Set Cantidad=@Dif, Cant_Solicitada=@Dif where Documento_Id=@Documento_id And Nro_Linea=@Nro_linea

		--Ya tengo el Nuevo Nro_Linea Para el Split	
		Select @MaxLinea=Max(Nro_linea) + 1 From Det_Documento Where Documento_Id=@Documento_id

		--Hago El Split de la linea de Det_Documento.
		Insert into Det_documento
		Select	Documento_Id, @MaxLinea, Cliente_Id, Producto_Id, @QtyRL,	Nro_Serie, Nro_Serie_Padre, Est_Merc_Id, Cat_Log_Id, Nro_Bulto,
				Descripcion, Nro_Lote, Fecha_Vencimiento, Nro_Despacho, Nro_Partida, Unidad_Id, Peso, Unidad_Peso, Volumen, Unidad_Volumen,
				Busc_Individual, Tie_In, Nro_Tie_In_Padre, Nro_Tie_in, Item_Ok, Cat_log_Id_Final, Moneda_Id, Costo, Prop1, Prop2, Prop3,
				Largo, Alto, Ancho, Volumen_unitario, Peso_Unitario, Cant_Solicitada, Trace_Back_Order
		From 	Det_Documento
		Where	Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Select @MaxLineaDDT=Max(Nro_linea_doc) + 1 From Det_Documento_Transaccion Where Documento_Id=@Documento_id

		--Saco el documento de Transaccion para poder hacer la insercion de DDT
		Select @Doc_Trans_Id=Doc_Trans_id From Det_Documento_Transaccion Where Documento_id=@Documento_id and Nro_Linea_doc=@Nro_Linea

		--Inserto en Det_Documento_Transaccion.	

		Insert Into Det_Documento_Transaccion
		Select 	 Doc_Trans_Id
				,@MaxLineaDDT
				,@Documento_id
				,@MaxLinea
				,Motivo_id
				,Est_Merc_Id
				,Cliente_Id
				,Cat_Log_Id
				,Item_Ok
				,Movimiento_Pendiente
				,Doc_Trans_ID_Ref
				,Nro_Linea_Trans_Ref
		From	Det_Documento_Transaccion
		Where	Documento_Id=@Documento_id
				And Nro_linea_Doc=@Nro_linea

		Update Rl_det_doc_Trans_Posicion Set Cantidad=@QtyPicking - @QtyRL where Rl_id=@OldRl_Id
		
		--Consumo la Rl.
		Update	Rl_Det_Doc_Trans_Posicion 
		Set 	 Disponible='0'
				,Posicion_Anterior=Posicion_Actual
				,Posicion_Actual=Null
				,Nave_Anterior=Nave_Actual
				,Nave_Actual=@PreEgrId
				,Doc_Trans_id_Egr=@Doc_Trans_IdEgr
				,Nro_Linea_Trans_Egr=@MaxLineaDDT
				,Cat_log_Id='TRAN_EGR'
		Where	Rl_id=@NewRl_Id

		--Debo Hacer el Split de la Linea de Rl Anterior.
		Insert into Rl_Det_Doc_Trans_Posicion
		Select 	 Doc_Trans_id
				,Nro_Linea_Trans
				,Posicion_Anterior
				,Posicion_Actual
				,@Dif	--Cantidad
				,Tipo_movimiento_Id
				,Ultima_Estacion
				,Ultima_Secuencia
				,Nave_Anterior
				,Nave_Actual
				,Documento_id
				,Nro_Linea
				,Disponible
				,Doc_Trans_id_Egr
				,Nro_Linea_Trans_Egr
				,Doc_Trans_Id_Tr
				,Nro_Linea_Trans_Tr
				,Cliente_id
				,Cat_log_Id
				,Cat_Log_Id_Final
				,Est_Merc_Id
		From	Rl_Det_Doc_Trans_Posicion
		Where	Rl_Id=@OldRl_Id

		--Necesario para saber q rl debo liberar.
		Select @SplitRl=Scope_Identity()

		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea

		--RL NUEVA LIBERADA
		Update 	 Rl_Det_Doc_Trans_posicion 
		Set 	 Disponible				='1'
				,Cantidad				=@QtyRL
				,Doc_Trans_Id_Egr		=null
				,Nro_Linea_Trans_Egr	=null
				,Posicion_Actual		=Posicion_Anterior
				,Posicion_Anterior		=Null
				,Nave_Actual			=Nave_Anterior
				,Nave_Anterior			='1'
				,Cat_log_id				=@Cat_log_Id_Final
		Where	Rl_Id					=@SplitRl
		
		Update Picking Set Cantidad=@Dif Where Picking_id=@Picking_id

		--Inserto la Nueva linea de Picking.
		INSERT INTO PICKING 
		SELECT 	 DISTINCT
				 DD.DOCUMENTO_ID
				,DD.NRO_LINEA
				,DD.CLIENTE_ID
				,DD.PRODUCTO_ID 
				,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
				,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
				,P.DESCRIPCION
				,@QtyPicking--DD.CANTIDAD
				,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
				,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
				,@RUTA
				,DD.PROP1
				,@FECHAINICIO AS FECHA_INICIO
				,NULL AS FECHA_FIN
				,@USUARIO AS USUARIO
				,NULL AS CANT_CONFIRMADA
				,@PALLET_PICKING AS PALLET_PICKING
				,0 	  AS SALTO_PICKING
				,'0'  AS PALLET_CONTROLADO
				,NULL AS USUARIO_CONTROL_PICKING
				,'0'  AS ST_ETIQUETAS
				,'0'  AS ST_CAMION
				,'0'  AS FACTURADO
				,'0'  AS FIN_PICKING
				,'0'  AS ST_CONTROL_EXP
				,NULL AS FECHA_CONTROL_PALLET
				,NULL AS TERMINAL_CONTROL_PALLET
				,NULL AS FECHA_CONTROL_EXP
				,NULL AS USUARIO_CONTROL_EXP
				,NULL AS TERMINAL_CONTROL_EXPEDICION
				,NULL AS FECHA_CONTROL_FAC
				,NULL AS USUARIO_CONTROL_FAC
				,NULL AS TERMINAL_CONTROL_FAC
				,NULL AS VEHICULO_ID
				,NULL AS PALLET_COMPLETO
				,NULL AS HIJO
				,NULL AS QTY_CONTROLADO
				,NULL AS PALLET_FINAL
				,NULL AS PALLET_CERRADO
				,NULL AS USUARIO_PF
				,NULL AS TERMINAL_PF
				,'0'  AS REMITO_IMPRESO
				,NULL AS NRO_REMITO_PF
				,NULL AS PICKING_ID_REF
				,NULL AS BULTOS_CONTROLADOS
				,NULL AS BULTOS_NO_CONTROLADOS
				,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
				,0	  AS TRANSF_TERMINANDA
				,DD.NRO_LOTE,DD.NRO_PARTIDA,NULL
		FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
				ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN PRODUCTO P
				ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
				LEFT JOIN NAVE N
				ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
				LEFT JOIN POSICION POS
				ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
				LEFT JOIN NAVE N2
				ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				And dd.Nro_linea=@MaxLinea		

		Update 	Consumo_Locator_Egr 
		Set 	Cantidad= @QtyPicking - @QtyRl ,
				saldo 	= (Saldo + (@QtyPicking - @QtyRl))
		Where	Documento_id=Documento_id
				and Nro_linea=@Nro_linea

		Select 	@Cliente_IdC= Cliente_Id,
				@Producto_idC= Producto_Id
		From	Det_Documento 
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @MaxLinea, @Cliente_IdC, @Producto_idC, @QtyRl, @NewRl_Id, 0, 2, GETDATE(),'S')

	End -- Fin 	If (@QtyPicking > @QtyRL) 3er. Caso.


	If @@Error<>0
	Begin
		raiserror('Se produjo un error inesperado.',16,1)
		return
	End
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

ALTER  Procedure [dbo].[PedidoMultiProducto]
@pViaje_id			as varchar(100) Output
As
Begin
declare @RsProd			as Cursor
declare @Cliente_id		as varchar(30)
declare @Doc_ext			as varchar(100)
declare @Producto_id		as varchar(100)
declare @Nave_id			as numeric(30,0)
declare @Nave_Cod			as varchar(100)
declare @QtyPick			as numeric(30,5)
declare @Nro_Linea		as numeric(20,0)
declare @QtySol			as numeric(30,5)

create table #Temp_PedMulti (
	Cliente_id			varchar(15) 	COLLATE SQL_Latin1_General_CP1_CI_AS,
	Doc_ext				varchar(100) 	COLLATE SQL_Latin1_General_CP1_CI_AS,
	Producto_id			varchar(30)		COLLATE SQL_Latin1_General_CP1_CI_AS
)

create table #Temp_PedMultiPickeados (
	Id_Temp				numeric(20,0) IDENTITY (1, 1) NOT NULL,
	Cliente_id			varchar(15)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	Doc_ext				varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,
	Producto_id			varchar(30)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	Nave_id				numeric(20,0),
	Nave_Cod				varchar(15)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	QtyPend				numeric(30,5),
	Est_Merc_id			varchar(50)		COLLATE SQL_Latin1_General_CP1_CI_AS,
	cat_log_id			varchar(50)		COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_Bulto			varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Descripcion			varchar(500)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_Lote				varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_pallet			varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Fecha_Vto			datetime,	
	Nro_Despacho		varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Nro_Partida			varchar(100)	COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Unidad_id			varchar(5)		COLLATE SQL_Latin1_General_CP1_CI_AS,	
	Documento_id		numeric(20,0)	
)
	
insert into #Temp_PedMulti
						select dd.cliente_id,dd.doc_ext,dd.producto_id
						from sys_int_documento d inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
						where
						d.codigo_viaje=@pViaje_id
						group by dd.cliente_id,dd.doc_ext,dd.producto_id
						having count(dd.producto_id)>1

insert into #Temp_PedMultiPickeados
							select dd.cliente_id,dd.doc_ext,dd.producto_id,dd.nave_id,dd.nave_cod,sum(dd.cantidad) 
									,dd.Est_Merc_id,dd.cat_log_id,dd.Nro_Bulto,dd.Descripcion,dd.Nro_Lote,dd.Nro_pallet,dd.Fecha_Vencimiento
									,dd.Nro_Despacho,dd.Nro_Partida,dd.Unidad_id,dd.Documento_id
							from sys_dev_det_documento dd inner join #Temp_PedMulti t on (dd.cliente_id=t.cliente_id and dd.doc_ext=t.doc_ext and dd.producto_id=t.producto_id)
							group by dd.cliente_id,dd.doc_ext,dd.producto_id,dd.nave_id,dd.nave_cod
										,dd.Est_Merc_id,dd.cat_log_id,dd.Nro_Bulto,dd.Descripcion,dd.Nro_Lote,dd.Nro_pallet,dd.Fecha_Vencimiento
										,dd.Nro_Despacho,dd.Nro_Partida,dd.Unidad_id,dd.Documento_id
							having sum(dd.cantidad) > 0 

Set @RsProd = Cursor For
						select dd.cliente_id,dd.doc_ext,dd.producto_id,dd.nro_linea,dd.cantidad_solicitada
						from sys_int_documento d inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
							  inner join #Temp_PedMulti t on (dd.cliente_id=t.cliente_id and dd.doc_ext=t.doc_ext and dd.producto_id=t.producto_id)
						where
						d.codigo_viaje=@pViaje_id

Open @RsProd
Fetch Next From @RsProd into @Cliente_id,@Doc_ext,@Producto_id,@Nro_Linea,@QtySol
While @@Fetch_Status=0 Begin
	delete sys_dev_det_documento where cliente_id=@Cliente_id and doc_ext=@Doc_ext and producto_id=@Producto_id	and estado_gt<>'PP'
	exec dbo.GetPutQtyMultiProd @Cliente_id,@Doc_ext,@Producto_id,@Nro_Linea,@QtySol
	Fetch Next From @RsProd into @Cliente_id,@Doc_ext,@Producto_id,@Nro_Linea,@QtySol
end --while rsProd

CLOSE @RsProd
DEALLOCATE @RsProd

drop table #Temp_PedMulti
drop table #Temp_PedMultiPickeados


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

ALTER    Procedure [dbo].[Picking_Act_Flag] 
@Viaje_Id 	as Varchar(100) Output
As
Begin
	Declare @Cantidad 	as Int
	Declare @Dif			as Int
	declare @Qty			as numeric(20,0)

	select 	@cantidad=count(picking_id)
	from	picking
	where 	LTRIM(RTRIM(UPPER(viaje_id)))=ltrim(upper(rtrim(@Viaje_Id)))


	select 	@dif=count(picking_id)
	from 	picking 
	where 	LTRIM(RTRIM(UPPER(viaje_id)))=ltrim(upper(rtrim(@Viaje_Id)))
			and cant_confirmada is not null

	if @cantidad=@dif begin

			--FO le agrego esto para que el pedido no desaparezca
			select @Qty=isnull(count(dd.producto_id),0)  	 
			from sys_int_documento d inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			where
			d.codigo_viaje=@Viaje_Id
			and dd.estado_gt is null
			if (@Qty=0) begin
				update picking set fin_picking='2' where viaje_id=@Viaje_Id
			end --if
	

	end
	Else
		Begin
			Update Picking set Fin_Picking=(	Select	Min(isnull(Fin_Picking,0)) 
											From 	Picking 
											where 	Viaje_Id=@Viaje_id) 
			Where viaje_Id=ltrim(rtrim(upper(@viaje_id)))
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
SET QUOTED_IDENTIFIER OFF
GO

ALTER   PROCEDURE [dbo].[PICKING_COMPLETADO]
@USUARIO AS VARCHAR(20),
@VIAJE_ID AS VARCHAR(30)
AS
BEGIN
	SELECT 		PRODUCTO_ID AS Cod_Producto,
				SUM(CANTIDAD)AS Cantidad,
				POSICION_COD as Posicion,
				PROP1 AS Pallet,
				PALLET_PICKING
	FROM 		PICKING
	WHERE 		--USUARIO = LTRIM(RTRIM(UPPER(@USUARIO)))
				--AND
				VIAJE_ID= RTRIM(UPPER(LTRIM(@VIAJE_ID)))
				AND FECHA_INICIO IS NOT NULL
				AND FECHA_FIN IS NOT NULL
	GROUP	BY
				PRODUCTO_ID,
				CANTIDAD,
				POSICION_COD,
				PROP1,
				PALLET_PICKING
	BEGIN
		IF @@ROWCOUNT = 0
			RAISERROR ('No hay Finalizados para mostrar', 16, 1)
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