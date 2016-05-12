
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 04:07 p.m.
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

ALTER PROCEDURE  [dbo].[Estacion_Picking_ActNroLinea] 
@NewRl_Id		Numeric(20,0) Output,
@Picking_Id		Numeric(20,0) Output
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

	--Obtengo las Cantidades.
	Select @QtyPicking=Cantidad from picking where picking_id=@Picking_Id
	Select @QtyRl= Cantidad From Rl_Det_Doc_Trans_Posicion Where Rl_Id=@NewRl_Id
	
	--Verifico que al momento de hacer el cambio no este tomada la tarea de picking
	If Dbo.Picking_inProcess(@Picking_Id)=1
	Begin
		Raiserror('La tarea de Picking ya fue asignada. No es posible realizar el cambio.',16,1);
		return
	End
	
	--Estos valores me van a servir mas adelante.
	Select	 @Documento_Id	=Documento_id
			,@Nro_Linea 	=Nro_linea
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

	--Obtengo el Nuevo Documento y numero de linea para Updetear.
	Select 	 Distinct
			 @Documento_idNew	=dd.Documento_Id
			,@Nro_lineaNew		=dd.Nro_Linea
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
			From	Rl_Det_Doc_Trans_posicion
			Where	Rl_Id=@OldRl_id

			--Restauro la rl Anterior
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
					, Cant_solicitada	=ISNULL(@CANT_SOLICITADA,CANTIDAD)
			Where	Documento_id=@Documento_id
					And Nro_Linea=@Nro_Linea

			--Elimino la Linea de Picking
			Delete From Picking Where Picking_Id=@Picking_Id

			--Inserto la Nueva linea de Picking.
select * from picking
			INSERT INTO PICKING 
			SELECT 	 DISTINCT
					 DD.DOCUMENTO_ID
					,DD.NRO_LINEA
					,DD.CLIENTE_ID
					,DD.PRODUCTO_ID 
					,ISNULL(LTRIM(RTRIM(D.NRO_DESPACHO_IMPORTACION)),LTRIM(RTRIM(DD.DOCUMENTO_ID))) AS VIAJE
					,ISNULL(P.TIPO_CONTENEDORA,'0') --'TIPO_CAJA' AS TIPO_CAJA --
					,P.DESCRIPCION
					,DD.CANTIDAD
					,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
					,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
					,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
					,DD.PROP1
					,NULL AS FECHA_INICIO
					,NULL AS FECHA_FIN
					,NULL AS USUARIO
					,NULL AS CANT_CONFIRMADA
					,NULL AS PALLET_PICKING
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
					,NULL AS NRO_LOTE
					,NULL AS NRO_PARTIDA
					,NULL AS NRO_SERIE
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

			Select 	@Cliente_IdC= Cliente_Id,
					@Producto_idC= Producto_Id
			From	Det_Documento 
			Where	Documento_id=@Documento_id
					And Nro_Linea=@Nro_Linea

			Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QtyPicking,@NewRl_Id,0,2,GETDATE(),'S')

	
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
				, Cant_solicitada	=ISNULL(@CANT_SOLICITADA,CANTIDAD)
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
				,DD.CANTIDAD
				,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
				,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
				,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
				,DD.PROP1
				,NULL AS FECHA_INICIO
				,NULL AS FECHA_FIN
				,NULL AS USUARIO
				,NULL AS CANT_CONFIRMADA
				,NULL AS PALLET_PICKING
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
				,NULL AS NRO_LOTE
				,NULL AS NRO_PARTIDA
				,NULL AS NRO_SERIE
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

		Select 	@Cliente_IdC= Cliente_Id,
				@Producto_idC= Producto_Id
		From	Det_Documento 
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QtyPicking,@NewRl_Id,0,2,GETDATE(),'S')

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
				,DD.CANTIDAD
				,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
				,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
				,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
				,DD.PROP1
				,NULL AS FECHA_INICIO
				,NULL AS FECHA_FIN
				,NULL AS USUARIO
				,NULL AS CANT_CONFIRMADA
				,NULL AS PALLET_PICKING
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
				,NULL AS NRO_LOTE
				,NULL AS NRO_PARTIDA
				,NULL AS NRO_SERIE
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

ALTER        PROCEDURE [dbo].[EtiquetaBulto]
AS

BEGIN
	select '63' as iddoc
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

ALTER  PROCEDURE [dbo].[FIN_PICKING_CONTENEDORA]
	@USUARIO 			AS VARCHAR(30),
	@VIAJEID 			AS VARCHAR(30),
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
						UPDATE PICKING SET FECHA_FIN=GETDATE(),	CANT_CONFIRMADA= @VCANT,
									PALLET_PICKING= @PALLET_PICKING 
						WHERE	PICKING_ID=@PICKID
						SET @VCANT=0	
						IF @DIF=0
							BEGIN
								SET @CONT_DTO=1
							END
						FETCH NEXT FROM PCUR INTO @PICKID,@CANT_CUR
					END
				ELSE
					BEGIN

						
						UPDATE PICKING SET 	
									FECHA_FIN=GETDATE(),
									CANT_CONFIRMADA=@CANT_CUR,
									PALLET_PICKING= @PALLET_PICKING 
						WHERE	PICKING_ID=@PICKID	
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