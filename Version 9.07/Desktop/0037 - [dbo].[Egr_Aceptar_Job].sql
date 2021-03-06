
ALTER           Procedure [dbo].[Egr_Aceptar_Job]
@Doc_Trans_id		as Numeric(20,0)
--@TipoOperacion		as varchar(5)
As
Begin
	Set xact_abort on
	Declare @TransId				as Varchar(15)
	Declare @RsFlg					Cursor
	Declare @bMandatorios			as char(1)
	Declare @bVMandatorios			as char(1)
	Declare @TipoOp					as Int
	Declare @iOrden					as Int
	Declare @PreEgr					as Char(1)
	Declare @EstacionActual			as varchar(15)
	-----------------------------------------------
	-- Sirven para el fetch del cursor de salida --
	-----------------------------------------------
	Declare @transaccion_id 		varchar(15)
	Declare @estacion_id 			varchar(15)
	Declare @orden 					numeric(3,0)
	Declare @r_informacion_id 		varchar(5)  
	Declare @r_impresion_id 		varchar(5)  
	Declare @categ_stock_id 		varchar(15)  
	Declare @determina_ubicacion 	varchar(1)
	Declare @ubicacion_autom 		varchar(1) 
	Declare @actualiza_stock 		varchar(1) 
	Declare @fin 					varchar(1) 
	Declare @nave_default 			numeric(20,0)
	Declare @cancelar_transaccion 	varchar(1)
	Declare @rollback_transaccion 	varchar(1)
	Declare @cola_trabajo 			varchar(1)
	Declare @deposito_id 			varchar(15)
	Declare @ubicacion_obligatoria 	varchar(1)
	Declare @inv_crear 				varchar(1)  
	Declare @inv_contar 			varchar(1)  
	Declare @inv_adm 				varchar(1)  
	Declare @serie_egr 				varchar(1)
	Declare @actualiza_cabecera 	varchar(1)
	Declare @imprimir_remito		varchar(1)
	Declare @imprimir_rem_anexo 	varchar(1)
	Declare @codigo_barras 			varchar(1)
	Declare @categoria_logica 		varchar(1)
	Declare @cant_solicitada 		varchar(1) 
	Declare @ControlStatus			varchar(3)

	print('[Egr_Aceptar_Job]=> Inicio.')
 	CREATE TABLE #temp_saldos_catlog (
 	cliente_id     VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	producto_id    VARCHAR(30)    	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	cat_log_id     VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	cantidad       NUMERIC(20,5) 	NOT NULL,
 	categ_stock_id VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
 	est_merc_id    VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 	)
 
 	
 	CREATE TABLE #temp_saldos_stock (
 	cliente_id  VARCHAR(15)    		COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	producto_id VARCHAR(30)    		COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	cant_tr_ing NUMERIC(20,5) 		NULL,
 	cant_stock  NUMERIC(20,5) 		NULL,
 	cant_tr_egr NUMERIC(20,5) 		NULL
 	)
-- 
 	CREATE TABLE #temp_usuario_loggin (
 	usuario_id            VARCHAR(20)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	terminal              VARCHAR(100)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	fecha_loggin          DATETIME,
 	session_id            VARCHAR(60)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	rol_id                VARCHAR(5)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
 	emplazamiento_default VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
 	deposito_default      VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
 	) 
-- 	
 	Exec Funciones_Loggin_Api#Registra_Usuario_Loggin 'ADMIN'
	--- fin de comentar 

	Select	@ControlStatus=status
	from	Documento_Transaccion
	Where	Doc_Trans_id=@Doc_Trans_id

	If @ControlStatus='T40'
	Begin
		Raiserror('El Documento ya fue Finalizado',16,1)
		Return
	End
	if @ControlStatus is null
	Begin
		Raiserror('No existe el documento de transaccion',16,1)
		Return
	End

	Set @bMandatorios='1'

	Exec Egr_CollectData @Doc_Trans_id			=@Doc_Trans_id 	Output
						,@Transaccion_id 		=@TransId 		Output
						,@RsFlag		 		=@RsFlg 		Output
						,@blnMandatorios 		=@bMandatorios 	Output
						,@blnVerboseMandatorios	=@bVMandatorios Output
						,@TipOp					=@TipoOp 		Output
						,@iOrdenEstacion		=@iOrden		Output

	Fetch Next from @RsFlg into  
							 @transaccion_id	
							,@estacion_id 	
							,@orden 			
							,@r_informacion_id
							,@r_impresion_id 	
							,@categ_stock_id 
							,@determina_ubicacion
							,@ubicacion_autom 		
							,@actualiza_stock 		
							,@fin 					
							,@nave_default 			
							,@cancelar_transaccion 	
							,@rollback_transaccion 	
							,@cola_trabajo 			
							,@deposito_id 			
							,@ubicacion_obligatoria 	
							,@inv_crear 				
							,@inv_contar 			
							,@inv_adm 				
							,@serie_egr 				
							,@actualiza_cabecera 	
							,@imprimir_remito		
							,@imprimir_rem_anexo 	
							,@codigo_barras 			
							,@categoria_logica 		
							,@cant_solicitada 		

	Select 	@EstacionActual=Estacion_Actual
	from	Documento_Transaccion
	where	doc_trans_id=@Doc_Trans_id

	Exec Egr_PasarDoc
					 @EstacionActual
					,@TransId 		
					,@Doc_Trans_id 	
					,1				
					,'EGR'			
					,@transaccion_id	
					,@estacion_id 	
					,@orden 			
					,@r_informacion_id
					,@r_impresion_id 	
					,@categ_stock_id 
					,@determina_ubicacion
					,@ubicacion_autom 		
					,@actualiza_stock 		
					,@fin 					
					,@nave_default 			
					,@cancelar_transaccion 	
					,@rollback_transaccion 	
					,@cola_trabajo 			
					,@deposito_id 			
					,@ubicacion_obligatoria 	
					,@inv_crear 				
					,@inv_contar 			
					,@inv_adm 				
					,@serie_egr 				
					,@actualiza_cabecera 	
					,@imprimir_remito		
					,@imprimir_rem_anexo 	
					,@codigo_barras 			
					,@categoria_logica 		
					,@cant_solicitada 		


	Update 	DOCUMENTO_TRANSACCION
	Set 	TR_ACTIVO = 0,
			TR_ACTIVO_ID = Null,
			SESSION_ID = Null,
			FECHA_CAMBIO_TR = Null
	WHERE 	DOC_TRANS_ID =@Doc_Trans_id

	Exec Actualiza_Det_Documento_Historicos @Doc_Trans_id

	Close 		@RsFlg
	Deallocate 	@RsFlg
End --Fin Procedure.
