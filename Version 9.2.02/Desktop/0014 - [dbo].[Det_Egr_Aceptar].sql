
/****** Object:  StoredProcedure [dbo].[Det_Egr_Aceptar]    Script Date: 07/23/2014 11:46:38 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Det_Egr_Aceptar]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Det_Egr_Aceptar]
GO

/****** Object:  StoredProcedure [dbo].[Det_Egr_Aceptar]    Script Date: 07/23/2014 11:46:38 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*#14082008#*/ 
CREATE                   Procedure [dbo].[Det_Egr_Aceptar]
@pDocumento_id 	as Numeric(20,0) Output
As
Begin
	declare @Cliente_id			as varchar(15)
	declare @Producto_id		as varchar(30)
	declare @Cantidad			as numeric(20,5)
	declare @vRl_id				as numeric(20)
	declare @vNroLinea			as numeric(20)
	declare @id					as numeric(20,0)
	declare @Documento_id 		as Numeric(20,0)
	declare @Saldo				as float
	declare @TipoSaldo			as varchar(20)
	declare @Doc_Trans 			as numeric(20)
	declare @vUsuario_id		as varchar(50)
	declare @vTerminal			as varchar(50)
	declare @vDocTrId			as numeric(20,0)
	declare @RsActuRL			as Cursor
	declare @DTCur				as Cursor
	declare @TipoCompId			as varchar(15)
	declare @ControlViaje		as int
	declare @CodigoViaje		as Varchar(100)
	declare @ControlDD			as float
	declare @ControlCl			as float
	declare @controlRl			as float
	--SET NOCOUNT ON;
	--================================================================
	--Comentar esto.
	--================================================================
	/*
	CREATE TABLE #temp_usuario_loggin (
		usuario_id            			VARCHAR(20)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		terminal              			VARCHAR(100)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		fecha_loggin          		DATETIME     ,
		session_id            			VARCHAR(60)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		rol_id                			VARCHAR(5)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		emplazamiento_default 	VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		deposito_default      		VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
	)

	exec FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN 'USER'
	--================================================================
	*/
	select	@ControlDD=sum(cantidad)
	from	det_documento 
	where 	documento_id=@pDocumento_id

	select	@ControlCl=sum(cantidad)
	from	consumo_locator_egr 
	where 	documento_id=@pDocumento_id

	if @ControlDD<>@ControlCl
	Begin
		raiserror('Hay productos en el documento que no tienen existencias, por favor verifiquelo',16,1)
		return
	End
	
	Select 	@TipoCompId=Tipo_Comprobante_Id from Documento Where Documento_Id=@pDocumento_id	

	Select  @CodigoViaje= Nro_Despacho_Importacion from documento where Documento_Id=@pDocumento_id

	Select 	@ControlViaje=Count(sidd.Doc_Ext)
	From	Sys_Dev_Documento Sid Inner Join Sys_Dev_Det_Documento Sidd
			On(sid.cliente_id=Sidd.cliente_id and sid.Doc_Ext=Sidd.Doc_Ext)
	Where	Sid.Codigo_Viaje=@CodigoViaje

	If @ControlViaje>0
	Begin
		raiserror('El N° Viaje %s ya fue utilizado, utilice otro codigo.', 16,1,@CodigoViaje)
		return
	End
	update documento set status='D20' where documento_id=@pDocumento_id
	Exec Asigna_Tratamiento#Asigna_Tratamiento_EGR @pDocumento_id
	
	--Actualmente se modifica porque el documento podria tener asignado mas de un tratamiento, ergo hay mas de un Doc_Trans_Id
	Set @DTCur=Cursor For
		Select 	distinct doc_trans_id 
		From 	det_documento_transaccion 
		Where 	documento_id=@pDocumento_id;
	

	Open @DTCur
	
	fetch Next from @DTCur Into @vDocTrId
	While @@Fetch_Status=0
	Begin
		--Hago la reserva en RL
		Set @RsActuRL = Cursor For 
			select 	c.[id]
					,c.documento_id
					,ddt.Nro_Linea_trans
					,c.Cliente_id
					,c.Producto_id
					,c.Cantidad
					,c.rl_id
					,c.saldo
					,c.tipo 
			from 	consumo_locator_egr c
					Inner join Det_Documento_Transaccion DDT
					On(c.Documento_id=DDT.Documento_id and c.Nro_Linea=DDT.Nro_linea_Doc)
			where 	DDT.Doc_Trans_Id=@vDocTrId

	
		Open @RsActuRL
		Fetch Next From @RsActuRL into 	@id,
										@Documento_id,
										@vNroLinea,
										@Cliente_id,
										@Producto_id,
										@Cantidad,
										@vRl_id,
										@Saldo,
										@TipoSaldo
	
		While @@Fetch_Status=0
		Begin
			--Controlo las rl porque desconfio de la concurrencia.
			select 	@ControlRl=Count(rl_id)
			from	rl_det_doc_trans_posicion
			where 	rl_id=@vRl_id
					and doc_trans_id_egr is null
					and nro_linea_trans_egr is null
			if @ControlRl=0
			begin
				DECLARE @cantaux VARCHAR(100)
				SET @cantaux=CAST(@Cantidad AS VARCHAR(100))
				raiserror('El producto %s por cantidad %s, fue utilizado por favor verifique este producto.',16,1,@Producto_id,@cantaux)
				--raiserror('El producto %s por cantidad %d, fue utilizado por favor verifique este producto.',16,1,@Producto_id,@Cantidad)
				return
			end
			if (@Saldo=0) begin
				update rl_det_doc_trans_posicion 
					set	  doc_trans_id_egr=@vDocTrId
						, nro_linea_trans_egr=@vNroLinea
						,disponible='0'
						,cat_log_id='TRAN_EGR'
						,nave_anterior=nave_actual
						,posicion_anterior=posicion_actual
						,nave_actual='2'
						,posicion_actual=null 
				where rl_id=@vRl_id
	
				update consumo_locator_egr set procesado='S' where [id]=@id
			end --if	
		
			if (@Saldo>0) begin

				insert into 	rl_det_doc_trans_posicion (
						doc_trans_id
						,nro_linea_trans
						,posicion_anterior
						,posicion_actual
						,cantidad
						,tipo_movimiento_id
						,ultima_estacion
						,ultima_secuencia
						,nave_anterior
						,nave_actual
						,documento_id
						,nro_linea
						,disponible
						,doc_trans_id_egr
						,nro_linea_trans_egr
						,doc_trans_id_tr
						,nro_linea_trans_tr
						,cliente_id
						,cat_log_id
						,cat_log_id_final
						,est_merc_id)
					select 	doc_trans_id	
						,nro_linea_trans
						,posicion_anterior	
						,posicion_actual		
						,@Saldo		
						,tipo_movimiento_id
						,ultima_estacion	
						,ultima_secuencia	
						,nave_anterior		
						,nave_actual		
						,documento_id
						,nro_linea		
						,disponible			
						,doc_trans_id_egr	
						,nro_linea_trans_egr
						,doc_trans_id_tr
						,nro_linea_trans_tr
						,cliente_id			
						,cat_log_id			
						,cat_log_id_final
						,est_merc_id
					from 	rl_det_doc_trans_posicion 
					where 	rl_id=@vRl_id 	

				update	rl_det_doc_trans_posicion 
				set 	cantidad=@Cantidad
					,doc_trans_id_egr=@vDocTrId
					,nro_linea_trans_egr=@vNroLinea
					,disponible='0'
					,cat_log_id='TRAN_EGR'
					,nave_anterior=nave_actual
					,posicion_anterior=posicion_actual
					,nave_actual='2'
					,posicion_actual=null 
				where rl_id=@vRl_id

				update consumo_locator_egr set procesado='S' where [id]=@id
			end --if	
		
			Fetch Next From @RsActuRL into 
												@id,
												@Documento_id,
												@vNroLinea,
												@Cliente_id,
												@Producto_id,
												@Cantidad,
												@vRl_id,
												@Saldo,
												@TipoSaldo
		End	--End While @RsActuRL.
		CLOSE @RsActuRL
		DEALLOCATE @RsActuRL

		Fetch Next from @DTCur into @vDocTrId
	End
	Close @DTCur
	Deallocate @DTCur
	
	Exec dbo.INGRESA_PICKING @pDocumento_id
	If (@TipoCompId='E03') or(@TipoCompId='E10')
	Begin
		Exec Dbo.Det_Egr_IngresaSysInt @pDocumento_id

		update det_documento set Cant_solicitada=cantidad where documento_id=@pDocumento_id and cant_solicitada=null
	End

	--Set NoCount Off;
End -- Fin Procedure.

GO


