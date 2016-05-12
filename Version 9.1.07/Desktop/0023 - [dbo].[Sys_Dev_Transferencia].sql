
/****** Object:  StoredProcedure [dbo].[Sys_Dev_Transferencia]    Script Date: 05/20/2014 16:00:35 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_Dev_Transferencia]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Sys_Dev_Transferencia]
GO

CREATE    Procedure [dbo].[Sys_Dev_Transferencia] 
@Doc_Trans_Id as numeric(20,0) Output
As
Begin
	---------------------------------------------------------
	--Para la Funcion.
	---------------------------------------------------------
	Declare @Ejecuta		as Int
	---------------------------------------------------------
	-- Cursor y sus variables.
	---------------------------------------------------------
	Declare @CursorRl		Cursor
	Declare @vRl			as numeric(20,0)
	---------------------------------------------------------
	--Para saber si ya cargue la cabecera
	---------------------------------------------------------
	Declare @Documento		as Int
	---------------------------------------------------------
	--Para el Cabecera.
	---------------------------------------------------------
	Declare @Cliente_id		as varchar(15)
	Declare @Nro_Linea		as numeric(10,0)
	---------------------------------------------------------
	--Para el Detalle
	---------------------------------------------------------	
	Declare @NavAnt			as Varchar(45)
	Declare @NavAct			as varchar(45)
	Declare @NavIdAnt		as Numeric(20,0)
	Declare @NavIdAct		as Numeric(20,0)
	---------------------------------------------------------	

	Set @Documento=0

	Set @CursorRl=Cursor For
		Select 	Rl_Id 
		From 	Rl_Det_Doc_Trans_Posicion
		Where	Doc_Trans_id_Tr=@Doc_Trans_id

	Open @CursorRl

	Fetch Next From @CursorRl into @vRl
	While @@Fetch_Status=0
	Begin 
		Select @Ejecuta=Dbo.Verifica_Cambio_Nave(@vRl)

		If @Ejecuta=1
		Begin
			If @Documento=0
			Begin
				Select 	@Cliente_id=Cliente_Id
				from 	Rl_Det_Doc_Trans_posicion 
				where	Rl_Id=@vRl

				Insert into Sys_Dev_Documento(Cliente_Id,Tipo_Documento_Id,Fecha_Cpte,Doc_Ext,Tipo_Comprobante,Fecha_Estado,Estado_GT,Fecha_Estado_GT, Flg_Movimiento)
				Values (@Cliente_id,'T01',Getdate(),'TRANS-'+Cast(@Doc_Trans_id as varchar(20)),null,null,'P',Getdate(), Null)

				Set @Documento=1

			End	--Fin Documento=0

			--Saco la Nave Anterior	
			Select Distinct @NavIdAnt=X.Nave_Id,@NavAnt=X.Nave_Cod
			From(
					Select 	N.Nave_id as Nave_Id
							,N.Nave_Cod as Nave_Cod
					from	rl_det_doc_trans_posicion Rl
							inner join Nave N
							On(Rl.Nave_Anterior=N.Nave_id)
					Where	Rl.Rl_Id=@vRl
					Union All
					Select 	N.Nave_id as Nave_id,
							N.Nave_Cod as Nave_Cod
					From	Rl_Det_Doc_Trans_Posicion Rl
							inner join Posicion P
							On(Rl.Posicion_Anterior=P.Posicion_Id)
							Inner join Nave N
							On(P.Nave_Id=N.Nave_Id)
					Where	Rl.Rl_Id=@vRl
				)As X


			--Saco la Nave Actual
			Select Distinct @NavIdAct=X.Nave_Id,@NavAct=X.Nave_Cod
			From(
					Select 	 Nave_id as Nave_Id
							,N.Nave_Cod as Nave_Cod
					from	rl_det_doc_trans_posicion Rl
							inner join Nave N
							On(Rl.Nave_Actual=N.Nave_id)
					Where	Rl.Rl_Id=@vRl
					Union All
					Select 	 N.Nave_id as Nave_id
							,N.Nave_Cod as Nave_Cod 
					From	Rl_Det_Doc_Trans_Posicion Rl
							inner join Posicion P
							On(Rl.Posicion_Actual=P.Posicion_Id)
							Inner join Nave N
							On(P.Nave_Id=N.Nave_Id)
					Where	Rl.Rl_Id=@vRl
				)As X
		
			Select @Nro_Linea=IsNull(Max(Nro_Linea),0)+1 From Sys_Dev_Det_Documento where Doc_Ext='TRANS-'+Cast(@Doc_Trans_id as varchar(20))

			--El Primero (-)
			Insert into Sys_Dev_Det_Documento (	Doc_Ext,Nro_Linea,Cliente_Id,Producto_Id,Cantidad_Solicitada,Cantidad,Est_Merc_Id,Cat_Log_Id,Nro_Bulto,
												Descripcion,Nro_Lote,Nro_Pallet,Fecha_Vencimiento,Nro_Despacho,Unidad_id,Estado_GT,Fecha_Estado_Gt,
												Documento_Id,Nave_Id,Nave_Cod, Flg_Movimiento,nro_partida,PROP3)
										  (
												Select 	Distinct
														'TRANS-'+Cast(@Doc_Trans_id as varchar(20)),@Nro_Linea,dd.Cliente_id,dd.Producto_id,(Rl.Cantidad-(Rl.Cantidad*2)),(Rl.Cantidad-(Rl.Cantidad*2))
														,dd.Est_Merc_id,Rl.Cat_Log_id,dd.Nro_Bulto,Prod.Descripcion,dd.Nro_Lote,dd.Prop1
														,dd.Fecha_Vencimiento,dd.Nro_Despacho,dd.Unidad_id,'P',Getdate()
														,dd.Documento_id,@NavIdAnt,@NavAnt, Null,Dd.NRO_PARTIDA,Dd.NRO_SERIE
												from	Rl_Det_Doc_Trans_Posicion Rl Inner Join Det_Documento_Transaccion Ddt
														On(Rl.Doc_Trans_Id=Ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=Ddt.Nro_Linea_Trans)
														Inner join Det_Documento Dd
														On(Ddt.Documento_id=Dd.Documento_id And Ddt.Nro_Linea_Doc=Dd.Nro_Linea)
														Inner Join Producto Prod
														On(Dd.Cliente_id=Prod.Cliente_Id And Dd.Producto_id=Prod.Producto_id)
												Where	Rl.Rl_Id=@vRl and Rl.Doc_Trans_Id_Tr=@Doc_Trans_id
											)


			Select @Nro_Linea=IsNull(Max(Nro_Linea),0)+1 From Sys_Dev_Det_Documento where Doc_Ext='TRANS-'+Cast(@Doc_Trans_id as varchar(20))


			--El Segundo(+)
			Insert into Sys_Dev_Det_Documento (	Doc_Ext,Nro_Linea,Cliente_Id,Producto_Id,Cantidad_Solicitada,Cantidad,Est_Merc_Id,Cat_Log_Id,Nro_Bulto,
												Descripcion,Nro_Lote,Nro_Pallet,Fecha_Vencimiento,Nro_Despacho,Unidad_id,Estado_GT,Fecha_Estado_Gt,
												Documento_Id,Nave_Id,Nave_Cod, Flg_Movimiento,NRO_PARTIDA,PROP3)
										  (
												Select 	Distinct
														'TRANS-'+Cast(@Doc_Trans_id as varchar(20)),@Nro_Linea,dd.Cliente_id,dd.Producto_id,(Rl.Cantidad),(Rl.Cantidad)
														,dd.Est_Merc_id,Rl.Cat_Log_id,dd.Nro_Bulto,Prod.Descripcion,dd.Nro_Lote,dd.Prop1
														,dd.Fecha_Vencimiento,dd.Nro_Despacho,dd.Unidad_id,'P',Getdate(),dd.Documento_id
														,@NavIdAct,@NavAct, Null,Dd.NRO_PARTIDA,Dd.NRO_SERIE
												from	Rl_Det_Doc_Trans_Posicion Rl Inner Join Det_Documento_Transaccion Ddt
														On(Rl.Doc_Trans_Id=Ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=Ddt.Nro_Linea_Trans)
														Inner join Det_Documento Dd
														On(Ddt.Documento_id=Dd.Documento_id And Ddt.Nro_Linea_Doc=Dd.Nro_Linea)
														Inner Join Producto Prod
														On(Dd.Cliente_id=Prod.Cliente_Id And Dd.Producto_id=Prod.Producto_id)
												Where	Rl.Rl_Id=@vRl and Rl.Doc_Trans_Id_Tr=@Doc_Trans_id
											)



		End--@Ejecuta=1
		Fetch Next From @CursorRl into @vRl
	End

	Close @CursorRl
	Deallocate @CursorRl

End --Fin Procedure.

GO


