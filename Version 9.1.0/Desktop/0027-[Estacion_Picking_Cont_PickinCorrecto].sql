
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Estacion_Picking_Cont_PickinCorrecto]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Estacion_Picking_Cont_PickinCorrecto]
GO

CREATE PROCEDURE  [dbo].[Estacion_Picking_Cont_PickinCorrecto] 
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
	Declare @RUTA				AS VARCHAR(100)
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
