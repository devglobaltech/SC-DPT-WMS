/****** Object:  StoredProcedure [dbo].[TRD_ACT_PALLET]    Script Date: 10/10/2014 12:14:24 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TRD_ACT_PALLET]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[TRD_ACT_PALLET]
GO


CREATE    PROCEDURE [dbo].[TRD_ACT_PALLET]
	@Doc_Trans_Id	as Numeric(20,0) output
As
Begin
	Declare @Doc_id 		as Numeric(20,0)
	Declare @PalletOrigen	as Varchar(100)
	Declare @PosCodDest		as Varchar(45)
	Declare @Pallet_E		as Varchar(45)		

	EXEC actualiza_pos_picking_desk @Doc_Trans_Id
	if @@error<>0
	Begin
		raiserror('Fallo al ejecutar actualiza_pos_picking_desk Sp.',16,1)
		Return(99)
	End

	/*
	SELECT 	@Doc_id=DD.Documento_id,@PosCodDest=p.posicion_cod,@Pallet_E=dd.Prop1
	From 	rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt
			on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			inner join det_documento dd
			on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
			left join nave n
			on(n.nave_id=rl.nave_actual)
			left join posicion p
			on(p.posicion_id=rl.posicion_actual)
			left join posicion p2
			on(p2.posicion_id=rl.posicion_anterior)
			left join nave n2
			on(rl.nave_anterior=n2.nave_id)
	Where 	doc_trans_id_tr = @Doc_Trans_Id

	if @PosCodDest is not null
	Begin
		Select 	@PalletOrigen=dbo.fx_GetPalletByPos(@PosCodDest)
		If @PalletOrigen is not null
			Begin
				Update 	Det_Documento set Prop1=@PalletOrigen 
				where	Documento_id=@Doc_id and Prop1=@Pallet_E
		End
	End
	*/
End

GO


