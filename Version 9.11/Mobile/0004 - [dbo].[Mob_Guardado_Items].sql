/****** Object:  StoredProcedure [dbo].[Mob_Guardado_Items]    Script Date: 07/04/2013 17:09:40 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Guardado_Items]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Guardado_Items]
GO

CREATE PROCEDURE [dbo].[Mob_Guardado_Items] 
@Documento_ID	numeric(20,0),
@Nro_linea		numeric(10,0),
@Cantidad		numeric(20,5),
@Posicion_Cod	varchar(45),
@Producto_id	varchar(30)=null
AS
begin
	set xact_abort on
	set nocount on
	Declare @Qty			as numeric(20,5)
	Declare @QtyVar			as varchar(max)
	Declare @CurRl			as Cursor
	Declare @RlID			as Numeric(20,0)
	Declare @QtyRL			as numeric(20,5)
	Declare @NewPos			as Numeric(20,0)
	Declare @NewNave		as Numeric(20,0)
	Declare @NewRl			as Numeric(20,0)
	Declare @FContenedora	as varchar(1);
	Declare @Prod			as varchar(50);
	Declare @Cur			as Cursor;
	Declare @vDoc			as Numeric(20,0)
	Declare @vcant			as Numeric(20,5)
	Declare @vLin			as Numeric(10,0)
	Declare @Vrl			as Numeric(20,0)

	SELECT	@fcontenedora=isnull(p.flg_contenedora,'0'),@Prod=dd.producto_id
	FROM	DET_DOCUMENTO DD inner join producto p	on(dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
	WHERE	DD.DOCUMENTO_ID=@Documento_ID
			AND DD.NRO_LINEA=@Nro_linea;
			
	if @fcontenedora = '1' begin 
		--1. Valido que la cantidad ingresada sea menor o igual a la cantidad disponible para guardar.
		SELECT	@QTY=ISNULL(SUM(RL.CANTIDAD),0)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	DD.DOCUMENTO_ID=@Documento_ID
				AND DD.NRO_LINEA=@Nro_linea
				AND RL.NAVE_ACTUAL='1'

		IF @CANTIDAD > @QTY
		BEGIN
			Set @qtyvar=cast(@qty as varchar)
			RAISERROR('La cantidad a ubicar excede el maximo pendiente de guardado. Maximo a ubicar %s',16,1,@qtyvar)
			return
		END
		--1.1 Obtengo los Id.
		SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@Posicion_Cod) AS INT)
		SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@Posicion_Cod) AS INT)
	
		IF (@NEWNAVE IS NULL) AND (@NEWPOS IS NULL)
		BEGIN
			RAISERROR('LA UBICACON ESTABLECIDA ES INVALIDA %s',16,1,@Posicion_Cod)
			RETURN
		END
		--2. Ubico la mercaderia sin tener que hacer un split de la rl. :)
		if @cantidad=@qty
		begin
			update	rl_det_doc_trans_posicion set nave_anterior=nave_actual,nave_actual=@NEWNAVE,posicion_actual=@NEWPOS
			FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL						ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
			WHERE	DD.DOCUMENTO_ID=@Documento_ID
					AND DD.NRO_LINEA=@Nro_linea
					AND RL.NAVE_ACTUAL='1'
		end
		--3. Ubicacion de la mercaderia con split :(
		if @Cantidad<@Qty
		begin
			set @CurRl = Cursor for
				SELECT	RL.RL_ID, RL.CANTIDAD
				FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
						INNER JOIN RL_DET_DOC_TRANS_POSICION RL						ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
				WHERE	DD.DOCUMENTO_ID=@Documento_ID
						AND DD.NRO_LINEA=@Nro_linea
						AND RL.NAVE_ACTUAL='1';
						
			open @CurRl
			Fetch Next From @CurRl into @RlID, @QtyRl
			while @@Fetch_Status=0
			begin
				if(@QtyRL<=@Cantidad)
				begin
					update	rl_det_doc_trans_posicion set nave_anterior=nave_actual,nave_actual=@NEWNAVE,posicion_actual=@NEWPOS
					WHERE	rl_id=@RlId
					
					Set @Cantidad=@Cantidad-@QtyRL	
				end
				if(@QtyRl>@Cantidad)
				Begin
					--Split de la rl
					INSERT INTO RL_DET_DOC_TRANS_POSICION
					SELECT	DOC_TRANS_ID, NRO_LINEA_TRANS,POSICION_ANTERIOR,POSICION_ACTUAL, (CANTIDAD-@Cantidad),
							TIPO_MOVIMIENTO_ID, ULTIMA_ESTACION, ULTIMA_SECUENCIA, NAVE_ANTERIOR, NAVE_ACTUAL, DOCUMENTO_ID,
							NRO_LINEA, DISPONIBLE,DOC_TRANS_ID_EGR,NRO_LINEA_TRANS_EGR, DOC_TRANS_ID_TR, NRO_LINEA_TRANS_TR,
							CLIENTE_ID, CAT_LOG_ID,CAT_LOG_ID_FINAL, EST_MERC_ID
					FROM	RL_DET_DOC_TRANS_POSICION
					WHERE	RL_ID=@RlID

					set @NewRl=scope_identity()

					update	rl_det_doc_trans_posicion set nave_anterior=nave_actual,nave_actual=@NEWNAVE,posicion_actual=@NEWPOS,cantidad=@Cantidad
					WHERE	rl_id=@RlId
					
					Set @Cantidad=0
				End
				If @Cantidad=0
				begin
					break
				end
				Fetch Next From @CurRl into @RlID, @QtyRl
			End--Fin Del While
			close @CurRl
			Deallocate @CurRl
		end
	end --Fin Caso contenedoras='1'.
	else
	begin
		/*
			Este segmento queda para que se pueda guardar mas de una linea, en el caso de que el ingreso no sea por contenedora.
			Para ello como no se cuantos numeros de linea estan involucrados debo generar un cursor. Recorrerlo y prorratear las
			cantidades segun corresponda.
		*/
		SELECT	@QTY=ISNULL(SUM(RL.CANTIDAD),0)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	DD.DOCUMENTO_ID=@Documento_ID
				AND DD.PRODUCTO_ID=@Prod
				AND RL.NAVE_ACTUAL='1'

		IF @CANTIDAD > @QTY
		BEGIN
			Set @qtyvar=cast(@qty as varchar)
			RAISERROR('La cantidad a ubicar excede el maximo pendiente de guardado. Maximo a ubicar %s',16,1,@qtyvar)
			return
		END

		SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@Posicion_Cod) AS INT)
		SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@Posicion_Cod) AS INT)
	
		IF (@NEWNAVE IS NULL) AND (@NEWPOS IS NULL)
		BEGIN
			RAISERROR('LA UBICACON ESTABLECIDA ES INVALIDA %s',16,1,@Posicion_Cod)
			RETURN
		END		
		
		if @Cantidad= @qty
		Begin
			--Este es un caso de ubicacion total del Sku, aca no hace falta abrir un cursor sino que el usuario ubica el total del producto
			--del documento.
			update	rl_det_doc_trans_posicion set nave_anterior=nave_actual,nave_actual=@NEWNAVE,posicion_actual=@NEWPOS
			FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL						ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
			WHERE	DD.DOCUMENTO_ID=@Documento_ID
					AND DD.PRODUCTO_ID=@Prod
					AND RL.NAVE_ACTUAL='1';		
		End
			
		Set @Cur=cursor for
			SELECT	RL.RL_ID
			FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
			WHERE	DD.DOCUMENTO_ID=@Documento_ID
					AND DD.PRODUCTO_ID=@Prod
					AND RL.NAVE_ACTUAL='1'		
		Open @Cur
		Fetch @cur into @VRL
		While @@Fetch_Status=0
		Begin
			
			--1. Obtengo la cantidad de la linea.
			SELECT	@vcant=ISNULL(SUM(RL.CANTIDAD),0)
			FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
			WHERE	RL.RL_ID=@VRL
					AND RL.NAVE_ACTUAL='1';
			
			--2. Si la cantidad ingresada es mayor o igual a la linea, ubico.
			if @CANTIDAD>=@vcant begin
				print('Primera Comparacion.');
				update	rl_det_doc_trans_posicion set nave_anterior=nave_actual,nave_actual=@NEWNAVE,posicion_actual=@NEWPOS
				FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
						INNER JOIN RL_DET_DOC_TRANS_POSICION RL						ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
				WHERE	rl.rl_id=@Vrl
						AND RL.NAVE_ACTUAL='1';
						
				Set @cantidad=@cantidad - @vcant;
			end
			else
			begin 
				
				--3. Si la cantidad ingresada es menor a la de la linea, ubico y spliteo.
				if @Cantidad<@vcant begin
					print('RL: ' + cast(@vrl as varchar))
					INSERT INTO RL_DET_DOC_TRANS_POSICION
					SELECT	DOC_TRANS_ID, NRO_LINEA_TRANS,POSICION_ANTERIOR,POSICION_ACTUAL, (CANTIDAD-@Cantidad),
							TIPO_MOVIMIENTO_ID, ULTIMA_ESTACION, ULTIMA_SECUENCIA, NAVE_ANTERIOR, NAVE_ACTUAL, DOCUMENTO_ID,
							NRO_LINEA, DISPONIBLE,DOC_TRANS_ID_EGR,NRO_LINEA_TRANS_EGR, DOC_TRANS_ID_TR, NRO_LINEA_TRANS_TR,
							CLIENTE_ID, CAT_LOG_ID,CAT_LOG_ID_FINAL, EST_MERC_ID
					FROM	RL_DET_DOC_TRANS_POSICION
					WHERE	RL_ID=@Vrl;

					set @NewRl=scope_identity()

					update	rl_det_doc_trans_posicion set nave_anterior=nave_actual,nave_actual=@NEWNAVE,posicion_actual=@NEWPOS,cantidad=@Cantidad
					WHERE	rl_id=@Vrl;		
					
					Set @cantidad=@cantidad - @vcant;
				end
			end
			if @cantidad<=0 begin
				break;
			end 
			Fetch @cur into @VRL

		End
		Close @Cur
		Deallocate @Cur;				
	
	end-- Fin caso Contenedoras='0'
end





GO


