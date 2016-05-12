
/****** Object:  StoredProcedure [dbo].[Actualiza_Det_documento_Historicos]    Script Date: 12/18/2013 15:21:24 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Actualiza_Det_documento_Historicos]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Actualiza_Det_documento_Historicos]
GO

/****** Object:  StoredProcedure [dbo].[Actualiza_Det_documento_Historicos]    Script Date: 12/18/2013 15:21:24 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE                  Procedure [dbo].[Actualiza_Det_documento_Historicos]
@Doc_Trans_id	as numeric(20,0) output
As
Begin

	Declare @Cantidad 	as Float
	Declare @Documento_id	as numeric(20,0)
	Declare @Nro_linea	as numeric(10,0)
	Declare @Nro_Linea_tr	as numeric(10,0)
	Declare @Diferencia	as float
	Declare @CurACDD	Cursor
	--nuevo para probar
	declare @DocTrO		as numeric(20,0)
	declare @NroLineaTrO	as numeric(10,0)
	declare @ControlRl	as int
	Declare @Rl_Hist	as Numeric(20,0)
	
	--Abro un cursor con los datos que necesito.
	Set @CurACDD= Cursor for
		Select 	dd.Documento_id,dd.Nro_linea,ddt.Nro_linea_trans
		From	Det_Documento dd 
				inner join Det_Documento_transaccion ddt
				on(dd.Documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
		Where	ddt.Doc_Trans_id=@Doc_trans_id
	
	Open @CurACDD

	Fetch Next From @CurACDD into @Documento_id,@Nro_Linea,@Nro_linea_tr
	While @@Fetch_Status=0
	Begin
		--saco las diferencias
		--Aca hago un Sum porque la linea podria estar spliteada. Si no lo hago no funca.
		Select 	@Cantidad=isnull(SUM(Cant_confirmada),0),@Diferencia=isnull(SUM(Cantidad),0) - isnull(SUM(Cant_Confirmada),0)
		From 	Picking
		Where	Documento_id=@Documento_id
				and Nro_Linea=@Nro_Linea

		If @Diferencia > 0 
		Begin
			--Hago los ajustes necesarios.
			Update 	Det_Documento set Cantidad=@Cantidad where Documento_ID=@Documento_id and Nro_linea=@Nro_linea
		
			Update 	Rl_Det_Doc_Tr_Pos_Hist set Cantidad=Cantidad - @Diferencia where Doc_trans_Id_Egr=@Doc_Trans_id and Nro_Linea_Trans_Egr=@Nro_Linea_tr
			
			--Obtengo El documento de transaccion y la linea con la que ingreso.
			Select	@DocTrO=doc_trans_id,@NroLineaTrO=nro_linea_trans
			from	rl_det_doc_trans_posicion
			where	doc_trans_id_egr=@Doc_Trans_id and nro_linea_trans_egr=@Nro_linea_tr
			
			--Si me devuelve un null lo saco del historico
			IF (@DocTrO Is Null) And (@NroLineaTrO Is Null)
			Begin
				Select	@DocTrO=doc_trans_id,@NroLineaTrO=nro_linea_trans
				from	rl_det_doc_tr_pos_hist
				where	doc_trans_id_egr=@Doc_Trans_id and nro_linea_trans_egr=@Nro_linea_tr
			End
			
			Select 	@ControlRl=Count(rl_id)
			from	rl_det_doc_trans_posicion
			where	doc_trans_id=@DocTrO and nro_linea_trans=@NroLineaTrO
					and nave_actual=2
					and Doc_Trans_Id_egr=@Doc_Trans_id
					And Nro_Linea_Trans_Egr=@Nro_linea_tr

			if @ControlRl> 0
			Begin
				Insert into Rl_Det_Doc_Trans_Posicion
				select 	 top 1
						 doc_trans_id
						,nro_linea_trans
						,NULL 
						,posicion_anterior
						,@Diferencia as cantidad
						,tipo_movimiento_id
						,Ultima_estacion
						,Ultima_Secuencia
						,1 
						,Nave_anterior
						,documento_id
						,nro_linea
						,1
						,null as doc_trans_id_egr
						,null as nro_linea_trans_egr
						,doc_trans_id_tr
						,nro_linea_trans_tr
						,cliente_id
						,'DIF_INV' as cat_log_id
						,'DISPONIBLE' cat_log_id_final
						,est_merc_id
				from	rl_det_doc_trans_posicion
				where	Doc_trans_id=@DocTrO and nro_linea_trans=@NroLineaTrO
						And Nave_Actual=2
						and Doc_Trans_Id_egr=@Doc_Trans_id
						And Nro_Linea_Trans_Egr=@Nro_linea_tr
			End
			else
			Begin
				--hago el insert.
				Insert into rl_det_doc_trans_posicion
				select 	 top 1
						 doc_trans_id
						,nro_linea_trans
						,NULL
						,POSICION_ANTERIOR
						,@Diferencia as cantidad
						,tipo_movimiento_id
						,Ultima_estacion
						,Ultima_Secuencia
						,1
						,NAVE_ANTERIOR
						,documento_id
						,nro_linea
						,1
						,null as doc_trans_id_egr
						,null as nro_linea_trans_egr
						,doc_trans_id_tr
						,nro_linea_trans_tr
						,cliente_id
						,'DIF_INV' as cat_log_id
						,'DISPONIBLE' cat_log_id_final
						,est_merc_id
				from	rl_det_doc_trans_posicion
				where	Doc_trans_id=@DocTrO and nro_linea_trans=@NroLineaTrO
						and nave_actual=2
						and Doc_Trans_Id_egr=@Doc_Trans_id
						And Nro_Linea_Trans_Egr=@Nro_linea_tr
				
				If @@rowcount=0 --por si no inserto una goma, Lo inserto de historico
				Begin
					--Hasta aca veniamos bien, ahora no tengo la rl. tengo que levantarla de los historicos.
					--basicamente por q el doc trans y nro. linea se consumio por completo.
					select @rl_hist=max(rl_id) from rl_det_doc_tr_pos_hist where doc_trans_id=@DocTrO and nro_linea_trans=@NroLineaTrO
					--cambio por dfernandez
									and Doc_Trans_Id_egr=@Doc_Trans_id	And Nro_Linea_Trans_Egr=@Nro_linea_tr
					Insert into rl_det_doc_trans_posicion
					select 	 top 1
							 doc_trans_id
							,nro_linea_trans
							,NULL
							,POSICION_ANTERIOR
							,@Diferencia as cantidad
							,tipo_movimiento_id
							,Ultima_estacion
							,Ultima_Secuencia
							,1
							,NAVE_ANTERIOR
							,documento_id
							,nro_linea
							,1
							,null as doc_trans_id_egr
							,null as nro_linea_trans_egr
							,doc_trans_id_tr
							,nro_linea_trans_tr
							,cliente_id
							,'DIF_INV' as cat_log_id
							,'DISPONIBLE' cat_log_id_final
							,est_merc_id
					from	rl_det_doc_tr_pos_hist
					where	Doc_trans_id=@DocTrO and nro_linea_trans=@NroLineaTrO
							and rl_id=@RL_HIST
							and Doc_Trans_Id_egr=@Doc_Trans_id
							And Nro_Linea_Trans_Egr=@Nro_linea_tr
				End
			end
		End
		Set @DocTrO	=Null
		Set @NroLineaTrO=Null
		Set @Cantidad	=Null
		Set @Diferencia	=Null

		Fetch Next From @CurACDD into @Documento_id,@Nro_Linea,@Nro_linea_tr
	End
	Close @CurACDD
	Deallocate @CurACDD
End

GO


