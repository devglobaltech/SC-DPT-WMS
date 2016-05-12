
/****** Object:  StoredProcedure [dbo].[Estacion_Picking_ActNroLinea]    Script Date: 07/28/2015 12:26:55 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Estacion_Picking_ActNroLinea]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Estacion_Picking_ActNroLinea]
GO

CREATE PROCEDURE  [dbo].[Estacion_Picking_ActNroLinea]
  @NewRl_Id			Numeric(20,0) Output,
  @Picking_Id		Numeric(20,0) Output
AS
Begin
	set xact_abort on
	-----------------------------------------------------------------------------
	--Declaracion de Variables.
	-----------------------------------------------------------------------------
	Declare @OldRl_Id			    as Numeric(20,0)
	Declare @QtyPicking			    as Float
	Declare @QtyRl				    as Float
	Declare @Documento_Id		    as Numeric(20,0)
	Declare @Nro_Linea			    as Numeric(10,0)
	Declare @PreEgrId			    as Numeric(20,0)
	Declare @Doc_Trans_IdEgr		as Numeric(20,0)
	Declare @Nro_Linea_TransEgr		as Numeric(10,0)
	Declare @Documento_IdNew		as Numeric(20,0)
	Declare @Nro_LineaNew		    as Numeric(10,0)
	Declare @Dif				    as Float
	Declare @MaxLinea			    as Numeric(10,0)
	Declare @Doc_Trans_Id		    as Numeric(20,0)
	Declare @MaxLineaDDT		    as Numeric(10,0)
	Declare @SplitRl			    as Numeric(20,0)
	Declare @Producto_IdC		    as Varchar(30)
	Declare @Cliente_IdC		    as Varchar(15)
	Declare @Cat_log_Id_Final		as Varchar(50)
	-----------------------------------------------------------------------------
	Declare @NRO_SERIE			    as varchar(50)
	Declare @NRO_SERIE_PADRE		as varchar(50)
	Declare @EST_MERC_ID		    as varchar(15)
	Declare @CAT_LOG_ID			    as varchar(15)
	Declare @NRO_BULTO			    as varchar(50)
	Declare @DESCRIPCION		    as varchar(200)
	Declare @NRO_LOTE			    as varchar(50)
	Declare @FECHA_VENCIMIENTO		as datetime
	Declare @NRO_DESPACHO		    as varchar(50)
	Declare @NRO_PARTIDA		    as varchar(50)
	Declare @UNIDAD_ID			    as varchar(5)
	Declare @PESO				    as numeric(20,5)
	Declare @UNIDAD_PESO		    as varchar(5)
	Declare @VOLUMEN			    as numeric(20,5)
	Declare @UNIDAD_VOLUMEN			as varchar(5)
	Declare @BUSC_INDIVIDUAL		as varchar(1)
	Declare @TIE_IN				    as varchar(1)
	Declare @NRO_TIE_IN			    as varchar(100)
	Declare @ITEM_OK			    as varchar(1)
	Declare @MONEDA_ID			    as varchar(20)
	Declare @COSTO				    as numeric(20,3)
	Declare @PROP1				    as varchar(100)
	Declare @PROP2				    as varchar(100)
	Declare @PROP3				    as varchar(100)
	Declare @LARGO				    as numeric(10,3)
	Declare @ALTO				    as numeric(10,3)
	Declare @ANCHO				    as numeric(10,3)
	Declare @VOLUMEN_UNITARIO		as varchar(1)
	Declare @PESO_UNITARIO			as varchar(1)
	Declare @CANT_SOLICITADA		as numeric(20,5)	
	-----------------------------------------------------------------------------
	Declare @PALLET_HOMBRE			as CHAR(1)
	Declare @Transf				    as char(1)
	Declare @Ruta					as varchar(100)
	
	Declare @QtyPaux			    as Float
	Declare @QtyPaux2			    as Float
	Declare @Dif2				    as Float
	Declare @QtyRL2				    as Float
	Declare @NewRl					Numeric(20,0)
	Declare @CantRl					Numeric(20,0)
	Declare @PosPick				Numeric(20,0)
	Declare @NavePick				Numeric(20,0)
	Declare @PosPickV				VARCHAR(45)
	Declare @NavePickV				VARCHAR(15)
	Declare @TIENE_LAYOUT			as CHAR(1)

	--Obtengo las Cantidades.
	Select @QtyPicking=Cantidad from picking where picking_id=@Picking_Id
	Select @QtyRl=Cantidad From Rl_Det_Doc_Trans_Posicion Where Rl_Id=@NewRl_Id
	
	SELECT @PosPickV = POSICION_COD FROM PICKING WHERE PICKING_ID=@PICKING_ID
	SELECT @NavePickV = NAVE_COD FROM PICKING WHERE PICKING_ID=@PICKING_ID
	
	SELECT @TIENE_LAYOUT = NAVE_TIENE_LAYOUT FROM NAVE WHERE NAVE_COD = @NavePickV
	
	IF (@TIENE_LAYOUT = 1)
	BEGIN
		SELECT @PosPick = POSICION_ID FROM POSICION WHERE POSICION_COD = @PosPickV
		SET @NavePick = NULL
	END ELSE
	BEGIN
		SELECT @NavePick = NAVE_ID FROM NAVE WHERE NAVE_COD = @NavePickV
		SET @PosPick = NULL
	END
		
	--Verifico que al momento de hacer el cambio no este tomada la tarea de picking
	If Dbo.Picking_inProcess(@Picking_Id)=1
	Begin
		Raiserror('La tarea de Picking ya fue asignada. No es posible realizar el cambio.',16,1);
		return
	End
	
	--Estos valores me van a servir mas adelante.
	Select	 @Documento_Id	=Documento_id
			 ,@Nro_Linea 	=Nro_linea
	From	 Picking
	Where	 Picking_Id	=@Picking_Id

	select	@PALLET_HOMBRE=flg_pallet_hombre
	from	cliente_parametros c inner join documento d
			on(c.cliente_id=d.cliente_id)
	where	d.documento_id=@Documento_Id

	--Saco la nave de preegreso.
	Select	@PreEgrId=Nave_Id
	From	Nave
	Where	Pre_Egreso='1'

	--Obtengo el Nuevo Documento y numero de linea para Updetear.
	Select 	Distinct
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
			From	  Rl_Det_Doc_Trans_posicion Rl
					    Inner join Det_Documento_Transaccion ddt
					    On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
					    Inner Join Det_Documento dd
					    on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
			Where   dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
			
			Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea
			
			--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
			Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
					    ,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
			From	  Rl_Det_Doc_Trans_posicion
			Where	  Rl_Id=@OldRl_id

			--Restauro la rl Anterior
			Update 	 Rl_Det_Doc_Trans_posicion 
			Set 	 Disponible				  ='1'
					  ,Doc_Trans_Id_Egr		=null
					  ,Nro_Linea_Trans_Egr=null
					  ,Posicion_Actual		=Posicion_Anterior
					  ,Posicion_Anterior	=Null
					  ,Nave_Actual			  =Nave_Anterior
					  ,Nave_Anterior			=1
					  ,Cat_log_id				  =@Cat_log_Id_Final
			Where	Rl_Id					      =@OldRl_Id
			
			--Consumo la Nueva Rl
			Update	Rl_Det_Doc_Trans_Posicion 
			Set 	 Disponible='0'
            ,Posicion_Anterior  =Posicion_Actual
            ,Posicion_Actual    =Null
            ,Nave_Anterior      =Nave_Actual
            ,Nave_Actual        =@PreEgrId
            ,Doc_Trans_id_Egr   =@Doc_Trans_IdEgr
            ,Nro_Linea_Trans_Egr=@Nro_Linea_TransEgr
            ,Cat_log_Id='TRAN_EGR'
			Where	Rl_id=@NewRl_Id

			--Saco los valores de la Nueva linea de det_documento
			Select	  @NRO_SERIE				=Nro_Serie
					, @NRO_SERIE_PADRE	=Nro_Serie_Padre
					, @EST_MERC_ID			=Est_Merc_Id
					, @CAT_LOG_ID				=Cat_log_id
					, @NRO_BULTO				=Nro_Bulto
					, @DESCRIPCION			=Descripcion
					, @NRO_LOTE					=Nro_Lote
					, @FECHA_VENCIMIENTO=Fecha_Vencimiento
					, @NRO_DESPACHO			=Nro_Despacho
					, @NRO_PARTIDA			=Nro_Partida
					, @UNIDAD_ID				=Unidad_Id
					, @PESO						  =Peso
					, @UNIDAD_PESO			=Unidad_Peso
					, @VOLUMEN					=Volumen
					, @UNIDAD_VOLUMEN		=Unidad_Volumen
					, @BUSC_INDIVIDUAL	=Busc_Individual
					, @TIE_IN					  =Tie_In
					, @NRO_TIE_IN				=Nro_Tie_In
					, @ITEM_OK					=Item_Ok
					, @MONEDA_ID				=Moneda_id
					, @COSTO					  =Costo
					, @PROP1					  =Prop1
					, @PROP2					  =Prop2
					, @PROP3					  =Prop3
					, @LARGO					  =largo
					, @ALTO						  =Alto
					, @ANCHO					  =Ancho
					, @VOLUMEN_UNITARIO	=Volumen_Unitario
					, @PESO_UNITARIO		=Peso_Unitario
					, @CANT_SOLICITADA	=Cant_Solicitada
			FROM	DET_DOCUMENTO				
			Where	Documento_Id=@Documento_idNew
					And Nro_linea=@Nro_LineaNew

			--Actualizo Det_Documento
			Update  Det_Documento
			Set       Nro_Serie			    =@NRO_SERIE				
					, Nro_Serie_padre	  =@NRO_SERIE_PADRE		
					, Est_Merc_Id		    =@EST_MERC_ID			
					, Cat_log_id		    ='TRAN_EGR'				
					, Nro_Bulto			    =@NRO_BULTO				
					, Descripcion		    =@DESCRIPCION			
					, Nro_Lote			    =@NRO_LOTE				
					, Fecha_Vencimiento	=@FECHA_VENCIMIENTO		
					, Nro_Despacho		  =@NRO_DESPACHO			
					, nro_partida		    =@NRO_PARTIDA			
					, Unidad_id			    =@UNIDAD_ID				
					, Peso				      =@PESO					
					, Unidad_Peso		    =@UNIDAD_PESO			
					, Volumen			      =@VOLUMEN				
					, Unidad_Volumen	  =@UNIDAD_VOLUMEN			
					, busc_individual	  =@BUSC_INDIVIDUAL		
					, tie_in			      =@TIE_IN					
					, Nro_Tie_in		    =@NRO_TIE_IN				
					, Item_ok			      =@ITEM_OK				
					, Moneda_id			    =@MONEDA_ID				
					, Costo				      =@COSTO					
					, Prop1				      =@PROP1					
					, Prop2				      =@PROP2					
					, Prop3				      =@PROP3					
					, Largo				      =@LARGO					
					, Alto				      =@ALTO					
					, Ancho			  	    =@ANCHO					
					, Volumen_Unitario	=@VOLUMEN_UNITARIO		
					, Peso_Unitario		  =@PESO_UNITARIO		
					, Cant_solicitada	  =ISNULL(@CANT_SOLICITADA,CANTIDAD)
			Where	Documento_id=@Documento_id
					And Nro_Linea=@Nro_Linea
					
			--Elimino la Linea de Picking
			select @Ruta=RUTA from PICKING where  documento_id=@Documento_Id and NRO_LINEA=@Nro_Linea;
			
			Delete From Picking Where Picking_Id=@Picking_Id
			SELECT * FROM PICKING
			--Inserto la Nueva linea de Picking.
			INSERT INTO PICKING 
			SELECT 	DISTINCT
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
					,@Ruta
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
					,DD.NRO_LOTE    AS NRO_LOTE
					,DD.NRO_PARTIDA AS NRO_PARTIDA
					,DD.NRO_SERIE	AS NRO_SERIE
					,null			AS ESTADO
					,NULL			AS NRO_UCDESCONSOLIDACION
					,null			as fecha_desconsolidacion
					,null			as usuario_desconsolidacion
					,null			as terminal_desconsolidacion
					,null			as nro_ucempaquetado
					,null			as ucempaquetado_medidas
					,null			as fecha_ucempaquetado
					,null			as ucempaquetado_peso
			FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD   ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
					INNER JOIN PRODUCTO P                     ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT  ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL   ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
					LEFT JOIN NAVE N                          ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
					LEFT JOIN POSICION POS                    ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
					LEFT JOIN NAVE N2                         ON(POS.NAVE_ID=N2.NAVE_ID)
			WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
					And dd.Nro_linea=@Nro_Linea
					
			Update PICKING set TIPO_CAJA=0 where documento_id=@Documento_Id and NRO_LINEA=@Nro_Linea and LTRIM(rtrim(tipo_caja))='';

			Select	@Cliente_IdC= Cliente_Id,
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

		--Obtengo la Rl Anterior QUE PUEDE O NO TENER PARTE CONFIRMADA EN PICKING
		--PRIMERO VERIFICO SI HAY MAS DE UNA
		
		Select 	@CantRl=COUNT(*)
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
		
		if (@CantRl > 1) BEGIN

		/*Select 	@OldRl_Id=Rl.Rl_Id, @QtyRL2=rl.CANTIDAD
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea 
				AND (RL.NAVE_ANTERIOR = @NavePick) OR (RL.POSICION_ANTERIOR = @PosPick)
		*/
			RAISERROR('No es posible realizar más de un cambio de ubicación',16,1)
			RETURN		
		END ELSE
		BEGIN
		
		Select 	@OldRl_Id=Rl.Rl_Id, @QtyRL2=rl.CANTIDAD
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
		
		END
		
		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea
		
		--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
		Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
				,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@OldRl_id
				
		--OBTENGO LA CANTIDAD CONFIRMADA EN PICKING VS LA CANTIDAD QUE QUIERO CAMBIAR DE LUGAR
		
		SELECT 	@QTYPAUX=SUM(ISNULL(P.CANT_CONFIRMADA,0))
		FROM	PICKING P
				INNER JOIN DET_DOCUMENTO DD ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND P.NRO_LINEA = DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
		
		SELECT 	@QTYPAUX2=SUM(ISNULL(P.CANTIDAD,0))
		FROM	PICKING P
				INNER JOIN DET_DOCUMENTO DD ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND P.NRO_LINEA = DD.NRO_LINEA)
		WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
		
		--SI SON DISTINTAS QUIERE DECIR QUE YA SE PICKEO PARTE, 
		--POR LO QUE CREO UNA NUEVA RL CON LA CANTIDAD PICKEADA Y LIBERO LA OTRA
		IF (@QTYPAUX <> @QTYPAUX2)
		BEGIN
		
			INSERT INTO RL_DET_DOC_TRANS_POSICION
			SELECT 	 DOC_TRANS_ID
					,NRO_LINEA_TRANS
					,POSICION_ANTERIOR
					,POSICION_ACTUAL
					,@QTYPAUX	--CANTIDAD
					,TIPO_MOVIMIENTO_ID
					,ULTIMA_ESTACION
					,ULTIMA_SECUENCIA
					,NAVE_ANTERIOR
					,NAVE_ACTUAL
					,DOCUMENTO_ID
					,NRO_LINEA
					,DISPONIBLE
					,DOC_TRANS_ID_EGR
					,NRO_LINEA_TRANS_EGR
					,DOC_TRANS_ID_TR
					,NRO_LINEA_TRANS_TR
					,CLIENTE_ID
					,CAT_LOG_ID
					,CAT_LOG_ID_FINAL
					,EST_MERC_ID
			FROM	RL_DET_DOC_TRANS_POSICION
			WHERE	RL_ID=@OldRl_Id		
			
			SET @NewRl= Scope_identity()
			
			UPDATE 	 RL_DET_DOC_TRANS_POSICION 
			SET 	 DISPONIBLE				='1'
					,CANTIDAD				= @QTYPAUX2 - @QTYPAUX
					,DOC_TRANS_ID_EGR		=NULL
					,NRO_LINEA_TRANS_EGR	=NULL
					,POSICION_ACTUAL		=POSICION_ANTERIOR
					,POSICION_ANTERIOR		=NULL
					,NAVE_ACTUAL			=NAVE_ANTERIOR
					,NAVE_ANTERIOR			='1'
					,CAT_LOG_ID				=@CAT_LOG_ID_FINAL
			WHERE	RL_ID					=@OLDRL_ID			
		
		END
		
		--Spliteo la Rl NUEVA
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
		select @Ruta=RUTA from PICKING where  documento_id=@Documento_Id and NRO_LINEA=@Nro_Linea;
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
				,DD.CANTIDAD - @QTYPAUX
				,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS NAVE
				,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS POSICION
				,@Ruta--ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.NRO_REMITO)),LTRIM(RTRIM(D.DOCUMENTO_ID))))
				,DD.PROP1
				,NULL			AS FECHA_INICIO
				,NULL			AS FECHA_FIN
				,NULL			AS USUARIO
				,NULL			AS CANT_CONFIRMADA
				,NULL			AS PALLET_PICKING
				,0 				AS SALTO_PICKING
				,'0'			AS PALLET_CONTROLADO
				,NULL			AS USUARIO_CONTROL_PICKING
				,'0'			AS ST_ETIQUETAS
				,'0'			AS ST_CAMION
				,'0'			AS FACTURADO
				,'0'			AS FIN_PICKING
				,'0'			AS ST_CONTROL_EXP
				,NULL			AS FECHA_CONTROL_PALLET
				,NULL			AS TERMINAL_CONTROL_PALLET
				,NULL			AS FECHA_CONTROL_EXP
				,NULL			AS USUARIO_CONTROL_EXP
				,NULL			AS TERMINAL_CONTROL_EXPEDICION
				,NULL			AS FECHA_CONTROL_FAC
				,NULL			AS USUARIO_CONTROL_FAC
				,NULL			AS TERMINAL_CONTROL_FAC
				,NULL			AS VEHICULO_ID
				,NULL			AS PALLET_COMPLETO
				,NULL			AS HIJO
				,NULL			AS QTY_CONTROLADO
				,NULL			AS PALLET_FINAL
				,NULL			AS PALLET_CERRADO
				,NULL			AS USUARIO_PF
				,NULL			AS TERMINAL_PF
				,'0'			AS REMITO_IMPRESO
				,NULL			AS NRO_REMITO_PF
				,NULL			AS PICKING_ID_REF
				,NULL			AS BULTOS_CONTROLADOS
				,NULL			AS BULTOS_NO_CONTROLADOS
				,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE
				,0				AS TRANSF_TERMINANDA
				,DD.NRO_LOTE	AS NRO_LOTE
				,DD.NRO_PARTIDA AS NRO_PARTIDA
				,DD.NRO_SERIE	AS NRO_SERIE
				,null			AS ESTADO
				,NULL			AS NRO_UCDESCONSOLIDACION
				,null			as fecha_desconsolidacion
				,null			as usuario_desconsolidacion
				,null			as terminal_desconsolidacion
				,null			as nro_ucempaquetado
				,null			as ucempaquetado_medidas
				,null			as fecha_ucempaquetado
				,null			as ucempaquetado_peso				
		FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD     ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN PRODUCTO P                       ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT    ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL     ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
				LEFT JOIN NAVE N                            ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
				LEFT JOIN POSICION POS                      ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
				LEFT JOIN NAVE N2                           ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				And dd.Nro_linea=@Nro_Linea
				AND RL.RL_ID = @NewRl_Id

		Update PICKING set TIPO_CAJA=0 where documento_id=@Documento_Id and NRO_LINEA=@Nro_Linea and LTRIM(rtrim(tipo_caja))='';

		Select 	@Cliente_IdC= Cliente_Id,
				@Producto_idC= Producto_Id
		From	Det_Documento 
		Where	Documento_id=@Documento_id
				And Nro_Linea=@Nro_Linea

		Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea
		
		IF (@QTYPAUX <> @QTYPAUX2)
		BEGIN
		Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QTYPAUX ,@NewRl,0,2,GETDATE(),'S')		
		END		

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
		
		select @Ruta=RUTA from PICKING where documento_id=@Documento_Id and NRO_LINEA=@Nro_Linea
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
            ,DD.NRO_LOTE AS NRO_LOTE
            ,DD.NRO_PARTIDA AS NRO_PARTIDA
            ,DD.NRO_SERIE AS NRO_SERIE
			,'0'			AS ESTADO
			,NULL			AS NRO_UCDESCONSOLIDACION
			,null			as fecha_desconsolidacion
			,null			as usuario_desconsolidacion
			,null			as terminal_desconsolidacion
			,null			as nro_ucempaquetado
			,null			as ucempaquetado_medidas
			,null			as fecha_ucempaquetado
			,null			as ucempaquetado_peso	            
		FROM	  DOCUMENTO D INNER JOIN DET_DOCUMENTO DD     ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
            INNER JOIN PRODUCTO P                       ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
            INNER JOIN DET_DOCUMENTO_TRANSACCION DDT    ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
            INNER JOIN RL_DET_DOC_TRANS_POSICION RL     ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
            LEFT JOIN NAVE N                            ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
            LEFT JOIN POSICION POS                      ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
            LEFT JOIN NAVE N2                           ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				    And dd.Nro_linea=@MaxLinea		
		
		Update PICKING set TIPO_CAJA=0 where documento_id=@Documento_Id and NRO_LINEA=@Nro_Linea and LTRIM(rtrim(tipo_caja))='';

		Update 	Consumo_Locator_Egr 
		Set 	  Cantidad= @QtyPicking - @QtyRl ,
				    saldo 	= (Saldo + (@QtyPicking - @QtyRl))
		Where	  Documento_id=Documento_id
				    and Nro_linea=@Nro_linea

		Select 	@Cliente_IdC= Cliente_Id,
				    @Producto_idC= Producto_Id
		From	  Det_Documento 
		Where	  Documento_id=@Documento_id
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


