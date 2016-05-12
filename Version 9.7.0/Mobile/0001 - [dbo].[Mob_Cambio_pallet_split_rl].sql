IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Cambio_pallet_split_rl]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Cambio_pallet_split_rl]
GO

Create procedure [dbo].[Mob_Cambio_pallet_split_rl]
	@rl_id		numeric(20,0),
	@cantidad	numeric(20,5),
	@new_rl		numeric(20,0)	Output,
	@pError		char(1)			Output
As
Begin
	declare		@cant_rl	Numeric(20,5)
	Begin Try

		Select @Cant_rl=cantidad from rl_det_doc_trans_posicion where rl_id=@rl_id;
		
		If(@Cant_rl<@Cantidad)
		begin
			--raiserror('La Rl indicada tiene una cantidad menor a la cantidad a splitear',16,1);	
			SET @New_Rl=@rl_id
			return
		end;
		
		IF (@Cant_rl<>@Cantidad)
		BEGIN 
			insert into rl_det_doc_trans_posicion(
				DOC_TRANS_ID,	NRO_LINEA_TRANS,	POSICION_ANTERIOR,	POSICION_ACTUAL,
				CANTIDAD,		TIPO_MOVIMIENTO_ID,	ULTIMA_ESTACION,	ULTIMA_SECUENCIA,	NAVE_ANTERIOR,
				NAVE_ACTUAL,	DOCUMENTO_ID,		NRO_LINEA,			DISPONIBLE,			DOC_TRANS_ID_EGR,
				NRO_LINEA_TRANS_EGR,				DOC_TRANS_ID_TR,	NRO_LINEA_TRANS_TR,	CLIENTE_ID,
				CAT_LOG_ID,		CAT_LOG_ID_FINAL,	EST_MERC_ID
			)	
			select	DOC_TRANS_ID,		NRO_LINEA_TRANS,	POSICION_ANTERIOR,
					POSICION_ACTUAL,	@Cantidad,			TIPO_MOVIMIENTO_ID,
					ULTIMA_ESTACION,	ULTIMA_SECUENCIA,	NAVE_ANTERIOR,
					NAVE_ACTUAL,		DOCUMENTO_ID,		NRO_LINEA,
					DISPONIBLE,			DOC_TRANS_ID_EGR,	NRO_LINEA_TRANS_EGR,
					DOC_TRANS_ID_TR,	NRO_LINEA_TRANS_TR,	CLIENTE_ID,
					CAT_LOG_ID,			CAT_LOG_ID_FINAL,	EST_MERC_ID
			From	Rl_det_doc_trans_posicion
			where	rl_id=@rl_id;
			
			Set @New_Rl=SCOPE_IDENTITY();

			Update	Rl_Det_Doc_Trans_posicion 
			Set		Cantidad=Cantidad-@Cantidad
			where	rl_id=@rl_id;
		END
		ELSE
		BEGIN
			SET @New_Rl=@rl_id
		END
		Set @pError='0'
	End Try
	Begin Catch
		Set @pError='1'
	End Catch
End
