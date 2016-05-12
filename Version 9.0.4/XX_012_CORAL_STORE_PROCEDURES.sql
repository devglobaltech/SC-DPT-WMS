
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 04:20 p.m.
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

ALTER    Procedure [dbo].[Funciones_Loggin_Api#Registra_Usuario_Loggin]
@Usuario as varchar(30)
As
Begin

	Declare @Session_Id				as varchar(100)
	Declare @Terminal				as varchar(100)
	Declare @emplazamiento_default	as varchar(15)
	Declare @deposito_default		as varchar(15)
	Declare	@RolId					as varchar(5)


	SELECT 	@Session_Id=USER_NAME(),
	       		@Terminal=HOST_NAME()

	SELECT 	@emplazamiento_default=emplazamiento_default,
	       	@deposito_default=deposito_default
	FROM   	sys_perfil_usuario
	WHERE  	usuario_id =@Usuario

	SELECT 	@RolId=rol_id
	FROM   	sys_usuario 
	WHERE  	usuario_id =@Usuario

	INSERT INTO #temp_usuario_loggin 
	            (usuario_id,
	             terminal,
	             fecha_loggin,
	             session_id,
	             rol_id,
	             emplazamiento_default,
	             deposito_default)
	Values
	            ( @Usuario
	             ,@Terminal
	             ,GETDATE()
	             ,@Session_Id 
	             ,@RolId
	             ,@emplazamiento_default
	             ,@deposito_default)
	set @usuario = rtrim(ltrim(upper(@Usuario)))

	Exec dbo.Registra_Sys_Session_Login @Usuario, @Terminal , 1

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

ALTER   Procedure [dbo].[Funciones_Movimiento_api#Actualizar_Reglas_Pendientes]
@Doc_Trans_Id 	as Numeric(20,0),
@Regla			as Varchar(5),
@Opcion			as Int
As
Begin
	Declare @vTotal as Int
	
	If ltrim(rtrim(Upper(@Regla)))='RM_1'
	Begin
		If @Opcion=1
		Begin
			delete 	pendiente_doc_trans
			where 	doc_trans_id =@doc_trans_id
					and regla_id = 'RM_1'
		End
		Else
		Begin
			select @vTotal=isnull(count(regla_id),0)
			from pendiente_doc_trans
			Where doc_trans_id =@Doc_Trans_id
			and regla_id = 'RM_1'
			
			if @vTotal=0
			Begin
				Insert Into PENDIENTE_DOC_TRANS(
				        DOC_TRANS_ID,
				        TIPO_REGLA,
				        REGLA_ID,
				        NRO_LINEA)
				 Values (
				        @Doc_trans_id
				        ,'MOV'
				        ,'RM_1'
				        ,1
				     )
			End
		End--Fin Else
	End
	Else
	Begin
		if @Opcion=1
		Begin
			delete 	pendiente_doc_trans
			where 	doc_trans_id =@doc_trans_id
					and regla_id = 'RU_1'
		End
	End --Fin Else
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

ALTER    Procedure [dbo].[Funciones_Movimiento_Api#Realizar_Movimiento]
@RlId as numeric(20,0),
@Secuencia_Realizada as int
As
Begin

	Declare @DocTransID			as numeric(20,0)
	Declare @NroLineaT			as numeric(10,0)
	Declare @TipoOperacion		as varchar(5)
	Declare @Posicion_Actual	as numeric(20,0)
	Declare @Posicion_anterior 	as numeric(20,0)
	Declare @Tipo_Movimiento	as varchar(5)
	Declare @max_secuencia		as int
	Declare @vCountInt			as Int
	Declare @ppos_lock			as varchar(1)
	Declare @plock_tipo_op		as varchar(5)
	Declare @plock_doc_trans_id as numeric(20,0)
	Declare @PosVacia			as varchar(1)
	Declare @Flag				as Int


	select 	@DocTransID=dt.doc_trans_id,@NroLineaT=rl.nro_linea_trans,@TipoOperacion=dt.tipo_operacion_id
	from 	rl_det_doc_trans_posicion rl,documento_transaccion dt
	where 	rl_id = @rlid and isnull(isnull(rl.doc_trans_id_egr,rl.doc_trans_id_tr),rl.doc_trans_id)=dt.doc_trans_id

	Update rl_det_doc_trans_posicion set ultima_secuencia =1	where rl_id =@RlId
	Update det_documento_transaccion set movimiento_pendiente='1' where doc_trans_id =@doctransid     and nro_linea_trans=@nrolineat
	Update documento_transaccion Set it_mover = 1	WHERE doc_trans_id=@DocTransID

	select @Posicion_Actual=posicion_actual, @Posicion_anterior=posicion_anterior, @Tipo_Movimiento=tipo_movimiento_id
	from rl_det_doc_trans_posicion
	where rl_id =@RlId

	if @@RowCount>0
	Begin
		If @Tipo_Movimiento is not null
		Begin
			    SELECT 	@max_secuencia=Max(secuencia)  FROM det_tipo_movimiento
			    Where 	tipo_movimiento_id =1
		End
		Else
		Begin
			    SELECT 	@max_secuencia=Max(secuencia)  FROM det_tipo_movimiento
			    Where 	tipo_movimiento_id is null
		End
		
		if @Posicion_Actual is not null
		Begin
			SELECT 	@vCountInt=Count(rl_id) 
			FROM 	rl_det_doc_trans_posicion rl
			Where 	posicion_actual = @Posicion_Actual
					and ultima_secuencia IS NULL and rl.doc_trans_id_egr =@DocTransID
			
			if @vCountInt=0
			Begin
				SELECT 	@ppos_lock=pos_lockeada,@plock_tipo_op=lck_tipo_operacion,@plock_doc_trans_id=lck_doc_trans_id
				From 	posicion
				WHERE 	posicion_id = @Posicion_Actual
				
				If @plock_tipo_op = @tipooperacion  And @plock_doc_trans_id = @doctransid And @max_secuencia = @Secuencia_Realizada
				Begin
					Update posicion	SET pos_vacia=0, pos_lockeada=0, LCK_TIPO_OPERACION=NULL, LCK_USUARIO_ID=NULL, LCK_DOC_TRANS_ID=NULL,LCK_OBS = Null
					WHERE posicion_id = @posicion_actual
				End
			End
		End
		
		if @Posicion_Anterior is not null
		Begin
			SELECT	@vCountInt=Count(rl_id)
			FROM 	rl_det_doc_trans_posicion rl
			Where 	posicion_actual = @posicion_anterior
			      	and ultima_secuencia IS NULL and rl.doc_trans_id_egr =@DocTransID
			
			If @vCountInt=0
			Begin
				If @posicion_anterior is not null
				Begin
					SELECT 	@ppos_lock=pos_lockeada,@plock_tipo_op=lck_tipo_operacion,@plock_doc_trans_id=lck_doc_trans_id,@PosVacia=pos_vacia
					From posicion
					WHERE posicion_id =@posicion_anterior
				End
				Else
				Begin
					SELECT 	@ppos_lock=pos_lockeada,@plock_tipo_op=lck_tipo_operacion,@plock_doc_trans_id=lck_doc_trans_id,@PosVacia=pos_vacia
					From posicion
					WHERE posicion_id is null
				End
				
				If @plock_tipo_op = @tipooperacion  And @plock_doc_trans_id = @doctransid And @max_secuencia = @Secuencia_Realizada
				Begin
					Update posicion SET pos_lockeada=0, LCK_TIPO_OPERACION = NULL, LCK_USUARIO_ID = NULL, LCK_DOC_TRANS_ID=NULL, LCK_OBS = Null
	                WHERE posicion_id = @posicion_anterior
					
					select @vCountInt=sum(isnull(cantidad,0))
					from rl_det_doc_trans_posicion rl
					where posicion_actual=@posicion_anterior
					
					if @vCountInt>0
					Begin
						Set @Flag=0
					End
					Else
						Set @Flag=1				
					End

					if @posvacia=0 and @Flag=1
					Begin
						Update posicion Set pos_vacia = 1  WHERE posicion_id =@posicion_anterior
					End
				End				
			End
		End
	End 
--End --Fin Procedure.
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

ALTER    Procedure [dbo].[Funciones_movimiento_api#Tareas_Post_Movimiento]
@Doc_trans_id	as numeric(20,0)
As
Begin
	Declare @TipoOperacion 	as varchar(5)
	Declare @Estacion 		as varchar(20)

	Select @TipoOperacion=tipo_operacion_id from documento_transaccion  where doc_trans_id =@Doc_trans_Id

	if @TipoOperacion='ING'
	Begin
		SELECT 	@Estacion=min(estacion)
		FROM  	det_tipo_movimiento t1, RL_DET_DOC_TRANS_POSICION t2,
		      	det_documento_transaccion t3,det_documento t4
		WHERE 	t1.tipo_movimiento_id = t2.tipo_movimiento_id 
				and t3.nro_linea_trans = t2.nro_linea_trans 
				and t4.documento_id = t3.documento_id AND  t4.nro_linea = t3.nro_linea_doc 
				and t2.doc_trans_id =@Doc_trans_id 
				AND t2.ultima_estacion < t1.estacion AND
				((ultima_secuencia IS NOT NULL and ultima_secuencia < secuencia) OR
				(ultima_secuencia IS NULL)  ) AND t3.doc_trans_id =@Doc_trans_id
				and t2.doc_trans_id=t3.doc_Trans_id

		UPDATE RL_DET_DOC_TRANS_POSICION
		SET  ultima_estacion =  @Estacion
			,ultima_secuencia = NULL 
			,tipo_movimiento_ID=null 
		where doc_trans_id=@Doc_trans_id

	End
	
	if @TipoOperacion='EGR'
	Begin
		SELECT 	@Estacion=min(estacion)
		FROM  	det_tipo_movimiento t1, RL_DET_DOC_TRANS_POSICION t2,
		      	det_documento_transaccion t3,det_documento t4
		WHERE 	t1.tipo_movimiento_id = t2.tipo_movimiento_id 
				and t3.nro_linea_trans = t2.nro_linea_trans 
				and t4.documento_id = t3.documento_id AND  t4.nro_linea = t3.nro_linea_doc 
				and t2.doc_trans_id =@Doc_trans_id 
				AND t2.ultima_estacion < t1.estacion AND
				((ultima_secuencia IS NOT NULL and ultima_secuencia < secuencia) OR
				(ultima_secuencia IS NULL)  ) AND t3.doc_trans_id =@Doc_trans_id
				and t2.doc_trans_id=t3.doc_Trans_id	
				and t2.doc_trans_id_egr=t3.doc_Trans_id

		UPDATE RL_DET_DOC_TRANS_POSICION
		SET  ultima_estacion =  @Estacion
			,ultima_secuencia = NULL 
			,tipo_movimiento_ID=null 
		where doc_trans_id_egr=@Doc_trans_id

	End
		
	If @TipoOperacion='TR'
	Begin
		SELECT 	@Estacion=min(estacion)
		FROM  	det_tipo_movimiento t1, RL_DET_DOC_TRANS_POSICION t2,
		      	det_documento_transaccion t3,det_documento t4
		WHERE 	t1.tipo_movimiento_id = t2.tipo_movimiento_id 
				and t3.nro_linea_trans = t2.nro_linea_trans 
				and t4.documento_id = t3.documento_id AND  t4.nro_linea = t3.nro_linea_doc 
				and t2.doc_trans_id =@Doc_trans_id 
				AND t2.ultima_estacion < t1.estacion AND
				((ultima_secuencia IS NOT NULL and ultima_secuencia < secuencia) OR
				(ultima_secuencia IS NULL)  ) AND t3.doc_trans_id =@Doc_trans_id
				and t2.doc_trans_id=t3.doc_Trans_id	
				and t2.doc_trans_id_tr=t3.doc_Trans_id

		UPDATE RL_DET_DOC_TRANS_POSICION
		SET  ultima_estacion =  @Estacion
			,ultima_secuencia = NULL 
			,tipo_movimiento_ID=null 
		where doc_trans_id_tr=@Doc_trans_id

	End

	--Exec Funciones_Movimiento_api#Actualizar_Reglas_Pendientes @Doc_trans_id,'RM_1',1
	UPDATE documento_transaccion SET it_mover=0 Where doc_trans_id=@Doc_trans_id

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

ALTER    Procedure [dbo].[Funciones_Movimiento_api#Tareas_pre_movimiento]
	@Doc_trans_id as numeric(20,0)
As
Begin

	Declare @Rl_Id 		as Numeric(20,0)
	Declare @Nav_Ant 	as Numeric(20,0)
	Declare @Pos_Ant	as Numeric(20,0)
	Declare @Pos_Act	as Numeric(20,0)
	Declare @Nav_Act	as Numeric(20,0)
	
	Declare Tpm Cursor For
	SELECT 	 rl_id 
			--,nave_anterior
			--,posicion_anterior
			--,posicion_actual
			--,nave_actual
	FROM  	RL_DET_DOC_TRANS_POSICION rl 
	WHERE 	rl.doc_trans_id_egr = @Doc_Trans_id

	Open Tpm
	Fetch Next From Tpm into @Rl_Id
	While @@Fetch_Status=0
	Begin
		Update RL_DET_DOC_TRANS_POSICION SET tipo_movimiento_id = '1',ultima_estacion = 'A',ultima_secuencia = Null
		Where rl_id =@Rl_Id
	
		Fetch Next From Tpm into @Rl_Id
	End
	Close Tpm
	Deallocate Tpm

	update documento_transaccion set est_mov_actual = 'A' where doc_trans_id =@doc_trans_id

	--EXEC Funciones_Movimiento_api#Actualizar_Reglas_Pendientes @Doc_trans_id, 'RM_1', 0
    -- EXEC Funciones_Movimiento_api#Actualizar_Reglas_Pendientes @Doc_trans_id, 'RU_1', 1

End --Fin procedure
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

ALTER  procedure [dbo].[Funciones_Saldo_Api#Actualizar_Saldos_CatLog1]
As
Begin

	Declare @FSAASC 	Cursor
	Declare @Cliente	as varchar(15)
	Declare @Producto	as varchar(30)
	Declare @Saldo		as Float
	Declare @CatLogID	as varchar(50)
	Declare @EstMercId	as varchar(15)

	Set @FSAASC= Cursor for
		select 	t2.cliente_id,
				t2.producto_id,
				(t2.cantidad - isnull(t1.cantidad, 0)) as saldo,
				t2.cat_log_id,
				t2.est_merc_id
		from 	#temp_saldos_catlog t1,
				#temp_saldos_catlog t2
		where 	t1.categ_stock_id = 'TRAN_EGR'
				and t2.categ_stock_id = 'STOCK'
				and t2.cat_log_id = t1.cat_log_id
				and t2.est_merc_id = t1.est_merc_id
				and t2.cliente_id = t1.cliente_id
				and t2.producto_id = t1.producto_id

	Open @FSAASC
	Fetch Next From @FSAASC into @Cliente,@Producto,@Saldo,@CatLogId,@EstMercId
	While @@Fetch_Status=0
	Begin

		UPDATE 	#TEMP_SALDOS_CATLOG SET cantidad = @Saldo
		WHERE 	cliente_id = @Cliente
				And producto_id =@Producto
				And categ_stock_id = 'STOCK'
				And cat_log_id = @CatLogId
				And est_merc_id =@EstMercId
		
		Fetch Next From @FSAASC into @Cliente,@Producto,@Saldo,@CatLogId,@EstMercId
	End
	Close 		@FSAASC
	Deallocate 	@FSAASC
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

ALTER  procedure [dbo].[Funciones_Saldo_api#Actualizar_Saldos_CatLog2]
@Cliente_id		as varchar(15),
@Producto_id	as varchar(30)
As
Begin

	declare @Cliente	as varchar(15)
	declare @producto	as varchar(30)
	declare @Qty		as float
	declare @EstMercId	as varchar(15)
	declare @FSAASC2	Cursor

	Set @FSAASC2= Cursor For
		SELECT 	
				dd.cliente_id,					dd.producto_id,
				Sum(rl.cantidad) AS Cantidad,	rl.est_merc_id
		FROM 	det_documento_transaccion ddt,	det_documento dd,
				documento d, categoria_logica cl,rl_det_doc_trans_posicion rl
		WHERE 	dd.documento_id  = d.documento_id
				And d.status = 'D30'						And rl.cliente_id = cl.cliente_id
				And rl.cat_log_id = cl.cat_log_id			And cl.categ_stock_id = 'TRAN_EGR'
				And ddt.documento_id = dd.documento_id		And ddt.nro_linea_doc = dd.nro_linea
				And rl.doc_trans_id_egr = ddt.doc_trans_id	And rl.nro_linea_trans_egr = ddt.nro_linea_trans
				And dd.cliente_id = @Cliente_id				And dd.producto_id = @Producto_id
		GROUP BY 	dd.cliente_id,				dd.producto_id,
					cl.cat_log_id,				cl.categ_stock_id,
					rl.est_merc_id

	Open @FSAASC2
	Fetch Next From @FSAASC2 into @Cliente,@Producto,@Qty,@EstMercId
	While @@Fetch_Status=0
	Begin
		UPDATE 	#TEMP_SALDOS_CATLOG SET cantidad = cantidad + @Qty
		WHERE 	cliente_id 	= @Cliente
				And producto_id 	= @Producto
				And categ_stock_id 	= 'STOCK'
				And cat_log_id 		= 'DISPONIBLE'

		Fetch Next From @FSAASC2 into @Cliente,@Producto,@Qty,@EstMercId
	End

	Close		@FSAASC2
	Deallocate	@FSAASC2
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

ALTER  procedure [dbo].[Funciones_Saldo_Api#Actualizar_Saldos_STOCK1]
As
Begin
	Update #TEMP_SALDOS_STOCK Set CANT_STOCK = CANT_STOCK  - CANT_TR_EGR
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

ALTER    Procedure [dbo].[Funciones_Saldo_Api#Actualizar_Saldos_STOCK2]
@Cliente_id 	as varchar(15),
@Producto_Id	as varchar(30)
As
Begin
	
	declare @FSAASS2	Cursor
	declare @ClienteID	as varchar(15)
	declare @productoid as varchar(30)
	declare @acumulador	as float
	declare @cantidad	as float
	
	
	Set @FSAASS2= Cursor Forward_only for
		SELECT 	dd.cliente_id,
				dd.producto_id,
				Sum(IsNull(dd.cantidad, 0)) AS cantidad
		FROM 	det_documento_transaccion ddt,
				det_documento dd,
				documento d,
				categoria_logica cl
		WHERE 	dd.documento_id = d.documento_id
				And d.status = 'D30'
				And dd.cliente_id = cl.cliente_id
				And dd.cat_log_id = cl.cat_log_id
				AND cl.categ_stock_id = 'TRAN_EGR'
				AND ddt.documento_id = dd.documento_id
				AND ddt.nro_linea_doc = dd.nro_linea
				And dd.cliente_id =@Cliente_id
				And dd.producto_id = @Producto_Id
				And Exists (SELECT rl_id FROM rl_det_doc_trans_posicion rl
		WHERE 	rl.doc_trans_id_egr=ddt.doc_trans_id
				And rl.nro_linea_trans_egr=ddt.nro_linea_trans)
		GROUP BY dd.cliente_id, dd.producto_id, cl.categ_stock_id

	set @Acumulador=0

	Open @FSAASS2
	Fetch Next From @FSAASS2 into @ClienteId,@ProductoId,@Cantidad
	While @@Fetch_Status=0
	Begin
		set @Acumulador= @Acumulador + @Cantidad
		Fetch Next From @FSAASS2 into @ClienteId,@ProductoId,@Cantidad		
	End

	Update 	#TEMP_SALDOS_STOCK Set CANT_STOCK = CANT_STOCK - @Acumulador
	WHERE 	cliente_id = @ClienteId
			And producto_id = @productoid
	
	Close 		@FSAASS2
	Deallocate 	@FSAASS2
End	--Fin Procedure.
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

ALTER  procedure [dbo].[Funciones_Saldo_Api#Insertar_Lista_ProdCatLogING]
@Cliente_Id 	as varchar(15),
@Producto_id	as Varchar(30)
As
Begin

	truncate table #temp_saldos_catlog

	INSERT INTO #TEMP_SALDOS_CATLOG (CLIENTE_ID, PRODUCTO_ID, CAT_LOG_ID, CATEG_STOCK_ID, CANTIDAD, EST_MERC_ID)
		(	SELECT DISTINCT 	
					DD.CLIENTE_ID,	DD.PRODUCTO_ID,	dd.CAT_LOG_ID_FINAL AS CATEGORIA_LOGICA,
					CL.CATEG_STOCK_ID,	SUM(RL.CANTIDAD),	RL.EST_MERC_ID
			FROM 	RL_DET_DOC_TRANS_POSICION RL,
					DET_DOCUMENTO_TRANSACCION DDT,
					DET_DOCUMENTO DD,
					CATEGORIA_LOGICA CL
			WHERE 	RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID
					AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS
					AND DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID
					AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC
					AND RL.CLIENTE_ID = CL.CLIENTE_ID
					AND RL.CAT_LOG_ID = CL.CAT_LOG_ID
					AND CL.CATEG_STOCK_ID <> 'TRAN_EGR'
					AND DD.CLIENTE_ID = @Cliente_Id
					AND DD.PRODUCTO_ID = @Producto_id
			GROUP BY 	dd.cliente_id,
						dd.producto_id,
						DD.cat_log_id_final,
						cl.categ_stock_id,
						rl.est_merc_id
		)

	INSERT INTO #TEMP_SALDOS_CATLOG(CLIENTE_ID,PRODUCTO_ID,CAT_LOG_ID,CATEG_STOCK_ID,CANTIDAD,EST_MERC_ID)
	(	SELECT 	DISTINCT 
				DD.CLIENTE_ID,
				DD.PRODUCTO_ID,
				DD.CAT_LOG_ID_FINAL CATEGORIA_LOGICA,
				CL.CATEG_STOCK_ID,
				SUM(RL.CANTIDAD),
				DD.EST_MERC_ID
		FROM 	RL_DET_DOC_TRANS_POSICION RL,
				DET_DOCUMENTO DD,
				CATEGORIA_LOGICA CL
		WHERE 	RL.DOCUMENTO_ID = DD.DOCUMENTO_ID
				AND RL.NRO_LINEA = DD.NRO_LINEA
				AND RL.CLIENTE_ID = CL.CLIENTE_ID
				AND RL.CAT_LOG_ID = CL.CAT_LOG_ID
				AND DD.CLIENTE_ID = @Cliente_id
				AND DD.PRODUCTO_ID = @Producto_id
				AND NOT EXISTS (
								SELECT 	*
								FROM	#TEMP_SALDOS_CATLOG T
								WHERE 	T.CLIENTE_ID = DD.CLIENTE_ID
										AND T.PRODUCTO_ID = DD.PRODUCTO_ID
										AND T.CAT_LOG_ID = DD.CAT_LOG_ID
										AND T.CATEG_STOCK_ID = CL.CATEG_STOCK_ID
										AND T.EST_MERC_ID = DD.EST_MERC_ID
								)
		GROUP BY DD.CLIENTE_ID,
				DD.PRODUCTO_ID,
				DD.CAT_LOG_ID_FINAL,
				CL.CATEG_STOCK_ID,
				DD.EST_MERC_ID
	)

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

ALTER  procedure [dbo].[Funciones_Saldo_api#Insertar_Lista_ProdCatLogEGR]
@Cliente_id		as varchar(15),
@Producto_id	as Varchar(30)
As
Begin
	
	Declare @StrSql1	as nvarchar(3000)
	Declare @StrSql2	as nvarchar(1000)
	Declare @StrSql3	as nvarchar(1000)
	Declare @StrSql3b	as nvarchar(1000)
	Declare @StrSql4	as nvarchar(1000)
	Declare @strSQLW	as nvarchar(500)
	Declare @xSQL		as nvarchar(4000)

	
	
	Set @StrSql1 = 'INSERT INTO #TEMP_SALDOS_CATLOG (cliente_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      producto_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      cat_log_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      categ_stock_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      cantidad,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      est_merc_id)'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + ' (SELECT cliente_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         producto_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         cat_log_id_final,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         categ_stock_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         Sum(cantidad) AS CANTIDAD,+ CHAR(13)'
	Set @StrSql1 = @StrSql1 + '         est_merc_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '    FROM'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '  (SELECT dd.cliente_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          Sum(dd.cantidad) AS cantidad,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          dd.est_merc_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '     FROM documento d,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          det_documento dd,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          categoria_logica cl'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '    WHERE dd.documento_id = d.documento_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND dd.cliente_id = cl.cliente_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND dd.cat_log_id = cl.cat_log_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND d.status = ' + CHAR(39) + 'D20' + CHAR(39) + CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND cl.categ_stock_id = ' + CHAR(39) + 'TRAN_EGR' + CHAR(39) + CHAR(13)
	
	     	 Set   @StrSql2 = ' GROUP BY dd.cliente_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.est_merc_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + ' UNION ALL'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '   SELECT dd.cliente_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          Sum(dd.cantidad),'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.est_merc_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '     FROM documento d,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          det_documento dd,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          categoria_logica cl,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          det_documento_transaccion ddt,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          documento_Transaccion dt'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '    WHERE ddt.documento_id = dd.documento_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND ddt.nro_linea_doc = dd.nro_linea'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dd.documento_id = d.documento_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dd.cliente_id = cl.cliente_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dd.cat_log_id = cl.cat_log_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dt.doc_trans_id = ddt.doc_trans_id'+ CHAR(13)
	
	      	   Set @StrSql3 = '      AND d.status = ' + CHAR(39) + 'D30' + CHAR(39) + CHAR(13)
	Set @StrSql3 = @StrSql3 + '      AND dt.status = ' + CHAR(39) + 'T10' + CHAR(39) + CHAR(13)
	Set @StrSql3 = @StrSql3 + '      AND cl.categ_stock_id = ' + CHAR(39) + 'TRAN_EGR' + CHAR(39) + CHAR(13)
	Set @StrSql3 = @StrSql3 + '      and not EXISTS (SELECT rl_id' + CHAR(13)
	Set @StrSql3 = @StrSql3 + '                        FROM rl_det_doc_trans_posicion rl'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                       WHERE rl.doc_trans_id_egr = ddt.doc_trans_id'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                         AND rl.nro_linea_trans_egr = ddt.nro_linea_trans)'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                    GROUP BY dd.cliente_id,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             dd.producto_id,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             dd.est_merc_id'+ CHAR(13)
	
	            Set @StrSql3b = ' UNION ALL' + CHAR(13)
	Set @StrSql3b = @StrSql3b + '    SELECT dd.cliente_id,' + CHAR(13)
	Set @StrSql3b = @StrSql3b + '           dd.producto_id,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           Sum(RL.cantidad),'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           rl.est_merc_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      FROM documento d,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           det_documento dd,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           categoria_logica cl,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           det_documento_transaccion ddt,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           rl_det_doc_trans_posicion rl'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '    WHERE rl.cliente_id = cl.cliente_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND rl.cat_log_id = cl.cat_log_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND rl.doc_trans_id_egr = ddt.doc_trans_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND rl.nro_linea_trans_egr = ddt.nro_linea_trans'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND ddt.documento_id = dd.documento_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND ddt.nro_linea_doc = dd.nro_linea'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND dd.documento_id = d.documento_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND d.status = ' + CHAR(39) + 'D30' + CHAR(39) + CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND cl.categ_stock_id = ' + CHAR(39) + 'TRAN_EGR' + CHAR(39) + CHAR(13)
	
	           Set @StrSql4 = ' GROUP BY dd.cliente_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          rl.est_merc_id'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + ' ) T1'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + ' GROUP BY cliente_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          producto_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          cat_log_id_final,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          categ_stock_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          est_merc_id'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + ' )'+ CHAR(13)
	
	If @Cliente_id IS NOT NULL 
	Begin
		Set @strSQLW = @strSQLW + '      AND dd.cliente_id = ltrim(rtrim(Upper(' + CHAR(39) + @CLiente_id + CHAR(39) +')))' + CHAR(13)
	End
	/*
	Else
	Begin
		'ORIGINAL @StrSql1 = @StrSql1 + '      AND funciones_generales_api.ClienteEnUsuario(dd.cliente_id, ' + fg.GetUsuarioActivo + ') = 1'
		@StrSql1 = @StrSql1 + '     AND '
		@StrSql1 = @StrSql1 + '        (SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END'
		@StrSql1 = @StrSql1 + '         FROM   rl_sys_cliente_usuario'
		@StrSql1 = @StrSql1 + '         WHERE  cliente_id = dd.cliente_id'
		@StrSql1 = @StrSql1 + '         And    usuario_id = '' + UCase(Trim(fg.GetUsuarioActivo)) + '') = 1'
		
		'ORIGINAL @StrSql2 = @StrSql2 + '      AND funciones_generales_api.ClienteEnUsuario(dd.cliente_id, ' + fg.GetUsuarioActivo + ') = 1'
		@StrSql2 = @StrSql2 + '     AND '
		@StrSql2 = @StrSql2 + '        (SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END'
		@StrSql2 = @StrSql2 + '         FROM   rl_sys_cliente_usuario'
		@StrSql2 = @StrSql2 + '         WHERE  cliente_id = dd.cliente_id'
		@StrSql2 = @StrSql2 + '         And    usuario_id = '' + UCase(Trim(fg.GetUsuarioActivo)) + '') = 1'
		
		'ORIGINAL @StrSql3b = @StrSql3b + '      AND funciones_generales_api.ClienteEnUsuario(dd.cliente_id, ' + fg.GetUsuarioActivo + ') = 1'
		@strSQLW = @strSQLW + '     AND '
		@strSQLW = @strSQLW + '        (SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END'
		@strSQLW = @strSQLW + '         FROM   rl_sys_cliente_usuario'
		@strSQLW = @strSQLW + '         WHERE  cliente_id = dd.cliente_id'
		@strSQLW = @strSQLW + '         And    usuario_id = '' + UCase(Trim(fg.GetUsuarioActivo)) + '') = '1''
		
		P_CLIENTE_ID = '1'
		@strSQLW = @strSQLW + '      AND '1' = '' + UCase(Trim(P_CLIENTE_ID)) + '''
	End*/
	
	If @Producto_id is not null
	Begin
		Set @strSQLW = @strSQLW + '      AND dd.producto_id= ltrim(rtrim(upper(' + CHAR(39) + @Producto_id + CHAR(39) +')))'
	End
	Else
	Begin
		set @Producto_id = '1'
		Set @strSQLW = @strSQLW + '      AND ' + CHAR(39) + '1' + CHAR(39) +' = ' + CHAR(39) + @Producto_id + CHAR(39)
	End 
	set @xSQL=@StrSql1 + @strSQLW + @StrSql2 + @strSQLW + @StrSql3 + @StrSql3b + @strSQLW + @StrSql4

	Exec sp_Executesql @xSQL

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

ALTER  Procedure [dbo].[Funciones_Saldo_Api#Generar_Saldos_CategLog]
@Cliente_id		as varchar(15),
@Producto_id	as varchar(30)
As
Begin

	Declare @ErrCod	as Int

	exec @ErrCod=Funciones_Saldo_Api#Insertar_Lista_ProdCatLogING @Cliente_id, @Producto_id
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Insertar_Lista_ProdCatLogING',16,1)
		Return
	End
	Exec @ErrCod=Funciones_Saldo_Api#Insertar_Lista_ProdCatLogEGR @Cliente_id, @Producto_id
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Insertar_Lista_ProdCatLogEGR',16,1)
		Return
	End
	Exec @ErrCod=Funciones_Saldo_Api#Actualizar_Saldos_CatLog1
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_CatLog1',16,1)
		Return
	End
	Exec @ErrCod=Funciones_Saldo_Api#Actualizar_Saldos_CatLog2 @Cliente_id, @Producto_id
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_CatLog2',16,1)
		Return
	End
    
	delete from #temp_saldos_catlog where cantidad=0

End	--fin procedure.
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

ALTER   Procedure [dbo].[Funciones_Saldo_Api#Insertar_Lista_Productos]
@ClienteId		as varchar(15),
@ProductoId		as varchar(30)
As
Begin
	
	declare @xSQL as Nvarchar(2000)	

	Truncate Table #temp_saldos_stock

	set @xSQL=' INSERT INTO #TEMP_SALDOS_STOCK ( ' + CHAR(13)
	set @xSQL= @xSQL +'        cliente_id,' + CHAR(13)
	set @xSQL= @xSQL +'        producto_id,' + CHAR(13)
	set @xSQL= @xSQL +'        cant_tr_ing,' + CHAR(13)
	set @xSQL= @xSQL +'        cant_stock,' + CHAR(13)
	set @xSQL= @xSQL +'        cant_tr_egr)' + CHAR(13)
	set @xSQL= @xSQL +' (SELECT DISTINCT p.cliente_id As Cliente,' + CHAR(13)
	set @xSQL= @xSQL +'        p.producto_id As Producto,' + CHAR(13)
	set @xSQL= @xSQL +'        0,' + CHAR(13)
	set @xSQL= @xSQL +'        0,' + CHAR(13)
	set @xSQL= @xSQL +'        0' + CHAR(13)
	set @xSQL= @xSQL +'        FROM producto As p ' + CHAR(13)
	set @xSQL= @xSQL +' WHERE 1 <> 0 ' + CHAR(13)

	if @clienteId is not null
	Begin
	    set @xSQL= @xSQL +'        AND p.cliente_id = ltrim(rtrim(upper(' + CHAR(39) + @Clienteid + CHAR(39) + ')))' + CHAR(13)
	End
    Else
	Begin
		set @ClienteId='1'
	    set @xSQL= @xSQL +'        AND ' + CHAR(39) + '1' + CHAR(39)+'=' + CHAR(39) + @Clienteid + CHAR(39) + CHAR(13)
	End

	if @ProductoId is not null
	Begin
	    set @xSQL= @xSQL +'        AND p.producto_id =rtrim(ltrim(upper(' + CHAR(39) + @Productoid + CHAR(39) +')))' + CHAR(13)
	End
	Else
	Begin
		set @ProductoId='1'
	    set @xSQL= @xSQL +'        AND ' + CHAR(39) + '1' +CHAR(39) +'=' + CHAR(39) + @Productoid + CHAR(39) + CHAR(13)
    End
	set @xSQL= @xSQL + ')' + CHAR(13)

	EXEC sp_executesql @xSQL

End--Fin Procedure.
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

ALTER      Procedure [dbo].[Funciones_Saldo_api#Obtener_Saldo_TR_ING_Act_Cantidad]
@Cliente_id		as Varchar(15),
@Producto_id	as Varchar(30),
@Caso			as varchar(20)

As
Begin

	declare @xSQL 			as nvarchar(400)
	declare @Acumulador		as Float
	declare @Campo			as nvarchar(50)
	declare @cliente 		as varchar(50)
	declare @productoid 	as varchar(50)
	declare @cantidad 		as float

	if @Cliente_id is null or ltrim(rtrim(@Cliente_id))=''
	Begin
		raiserror('El argumento Cliente no puede ser nulo',16,1)
	End
	if @Producto_id is null or ltrim(rtrim(@Producto_id))=''
	Begin
		raiserror('El argumento Producto no puede ser nulo',16,1)
	End

	Declare pCursorFS Cursor for
		SELECT 	dd.cliente_id,
				dd.producto_id,
				Sum(IsNull(rl.cantidad, 0)) As cantidad
		from 	rl_det_doc_trans_posicion rl, det_documento dd, categoria_logica cl
		where	rl.documento_id = dd.documento_id
				and rl.nro_linea = dd.nro_linea
				and rl.cliente_id=cl.cliente_id
				and rl.cat_log_id=cl.cat_log_id
				and cl.categ_stock_id = 'TRAN_ING'
				and dd.cliente_id = @cliente_id
				and dd.producto_id = @producto_id
		GROUP BY dd.cliente_id ,dd.producto_id , cl.categ_stock_id
		UNION all
		SELECT 	dd.cliente_id,
				dd.producto_id,
				Sum(IsNull(rl.cantidad, 0)) AS cantidad
		FROM 	rl_det_doc_trans_posicion rl,det_documento_transaccion  ddt , det_documento dd, categoria_logica cl
		WHERE 	rl.doc_trans_id = ddt.doc_trans_id AND rl.nro_linea_trans = ddt.nro_linea_trans
				and ddt.documento_id=dd.documento_id  and ddt.nro_linea_doc = dd.nro_linea
				and rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id
				and cl.categ_stock_id = 'TRAN_ING'
				And dd.cliente_id = @Cliente_id
				And dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id

	open pCursorFS

	If @Caso='TR_ING'
	Begin
		Set @Campo='cant_tr_ing'
	End
	Else
	Begin
		if @Caso='TR_EGR'
		Begin
			Set @Campo='cant_tr_egr'
		End
		Else
		Begin
			Set @Campo='cant_stock'
		End
	End

	Set @Acumulador=0

	FETCH NEXT FROM pCursorFS into @cliente,@producto_id,@cantidad
	While @@Fetch_Status=0
	Begin
		Set @Acumulador= @Acumulador + @cantidad
		FETCH NEXT FROM pCursorFS into @cliente,@producto_id,@cantidad
	End
	If @Acumulador > 0 
	Begin
		set @xSQL=N' Update 	#TEMP_SALDOS_STOCK Set ' + @campo + '= ' + @campo + ' + ' + cast(@Acumulador as varchar) +	' Where 	Cliente_id =' + char(39) + @cliente + char(39) +' And producto_id = ' + char(39) + @producto_id + char(39)
		EXEC sp_executesql @xSQL
	End



	Close pCursorFS
	Deallocate pCursorFS
End --Fin Picking
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

ALTER    Procedure [dbo].[Funciones_Saldo_api#Obtener_Saldo_TR_EGR_Act_Cant]
@Cliente_Id 	as varchar(15),
@Producto_id	as varchar(30),
@Caso			as varchar(10)
As
Begin
	
	declare @FSATREGR cursor
	declare @xSQL 			as nvarchar(400)
	declare @Acumulador		as Float
	declare @Campo			as nvarchar(50)
	declare @cliente 		as varchar(50)
	declare @productoid 	as varchar(50)
	declare @cantidad 		as float
	
	if @Cliente_id is null or ltrim(rtrim(@Cliente_id))=''
	Begin
		raiserror('El argumento Cliente no puede ser nulo',16,1)
		return
	End
	if @Producto_id is null or ltrim(rtrim(@Producto_id))=''
	Begin
		raiserror('El argumento Producto no puede ser nulo',16,1)
		return
	End

	Set	@FSATREGR = Cursor Forward_Only Static for
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(dd.cantidad, 0)) as cantidad
		from 	det_documento dd,
				documento d,
				categoria_logica cl
		where 	dd.documento_id  = d.documento_id
				and d.status = 'D20'
				and dd.cliente_id = cl.cliente_id
				and cl.cliente_id = dd.cliente_id
				and dd.cat_log_id = cl.cat_log_id
				and cl.categ_stock_id = 'TRAN_EGR'
				and dd.cliente_id = @Cliente_Id
				and dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id
		union all
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(dd.cantidad, 0)) as cantidad
		from  	det_documento_transaccion ddt,
				det_documento dd,
				documento_transaccion dt,
				categoria_logica cl
		where 	ddt.cliente_id = cl.cliente_id
				and ddt.cat_log_id = cl.cat_log_id
				and cl.cliente_id = dd.cliente_id
				and cl.categ_stock_id = 'TRAN_EGR'
				and ddt.documento_id = dd.documento_id
				and ddt.nro_linea_doc = dd.nro_linea
				and ddt.doc_trans_id = dt.doc_trans_id
				and dt.status = 'T10'
				and not exists (select 	rl_id 
								from 	rl_det_doc_trans_posicion rl
								where 	rl.doc_trans_id_egr = ddt.doc_trans_id
										and rl.nro_linea_trans_egr = ddt.nro_linea_trans)
				and dd.cliente_id = @Cliente_id
				and dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id
		union all
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(dd.cantidad, 0)) as cantidad
		from  	det_documento_transaccion ddt,
				det_documento dd,
				rl_det_doc_trans_posicion rl,
				documento d,
				categoria_logica cl
		where 	dd.documento_id = d.documento_id
				and rl.cat_log_id = cl.cat_log_id
				and d.status = 'D30'
				and rl.cliente_id = cl.cliente_id
				and cl.cliente_id = dd.cliente_id
				and cl.categ_stock_id = 'TRAN_EGR'
				and ddt.documento_id = dd.documento_id
				and ddt.nro_linea_doc = dd.nro_linea
				and rl.doc_trans_id_egr = ddt.doc_trans_id
				and rl.nro_linea_trans_egr = ddt.nro_linea_trans
				and dd.cliente_id = @Cliente_id
				and dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id

	If @Caso='TR_ING'
	Begin
		Set @Campo='cant_tr_ing'
	End
	Else
	Begin
		if @Caso='TR_EGR'
		Begin
			Set @Campo='cant_tr_egr'
		End
		Else
		Begin
			Set @Campo='cant_stock'
		End
	End

	open @FSATREGR

	Set @Acumulador=0

	FETCH NEXT FROM @FSATREGR into @cliente,@producto_id,@cantidad
	While @@Fetch_Status=0
	Begin
		Set @Acumulador= @Acumulador + @cantidad
		FETCH NEXT FROM @FSATREGR into @cliente,@producto_id,@cantidad
	End
	If @Acumulador > 0 
	Begin
		set @xSQL=N' Update 	#TEMP_SALDOS_STOCK Set ' + @campo + '= ' + @campo + ' + ' + Cast(@Acumulador as varchar) +	' Where 	Cliente_id =' + char(39) + @cliente + char(39) +' And producto_id = ' + char(39) + @producto_id + char(39)

		EXEC sp_executesql @xSQL

	End



	Close @FSATREGR
	Deallocate @FSATREGR

End --Fin Del Procedure.
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

ALTER     procedure [dbo].[Funciones_Saldo_Api#Obtener_Saldo_Stock_Actualizar_Cantidades]
@Cliente_Id		as varchar(15),
@Producto_Id	as varchar(30),
@Caso			as varchar(10)
As
Begin
	
	Declare @FSAOST 		cursor
	declare @xSQL 			as nvarchar(400)
	declare @Acumulador		as Float
	declare @Campo			as nvarchar(50)
	declare @cliente 		as varchar(50)
	declare @productoid 	as varchar(50)
	declare @cantidad 		as float
	
	if @Cliente_id is null or ltrim(rtrim(@Cliente_id))=''
	Begin
		raiserror('El argumento Cliente no puede ser nulo',16,1)
		return
	End
	if @Producto_id is null or ltrim(rtrim(@Producto_id))=''
	Begin
		raiserror('El argumento Producto no puede ser nulo',16,1)
		return
	End

	Set @FSAOST= Cursor Forward_Only For
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(rl.cantidad, 0)) as cantidad
		from 	rl_det_doc_trans_posicion rl
				inner join det_documento_transaccion ddt
				on (ddt.doc_trans_id = rl.doc_trans_id	and ddt.nro_linea_trans =rl.nro_linea_trans)
				inner join det_documento dd
				on (dd.documento_id = ddt.documento_id	and dd.nro_linea = ddt.nro_linea_doc)
				inner join categoria_logica cl
				on (cl.cliente_id = rl.cliente_id and cl.cat_log_id = rl.cat_log_id	and cl.categ_stock_id = 'stock')
		where
				dd.cliente_id = @Cliente_Id
				and dd.producto_id = @Producto_Id
		group by dd.cliente_id, dd.producto_id
		order by dd.cliente_id, dd.producto_id

	If @Caso='TR_ING'
	Begin
		Set @Campo='cant_tr_ing'
	End
	Else
	Begin
		if @Caso='TR_EGR'
		Begin
			Set @Campo='cant_tr_egr'
		End
		Else
		Begin
			Set @Campo='cant_stock'
		End
	End
	
	Open @FSAOST

	Set @Acumulador=0

	FETCH NEXT FROM @FSAOST into @cliente,@producto_id,@cantidad
	While @@Fetch_Status=0
	Begin
		Set @Acumulador= @Acumulador + @cantidad
		FETCH NEXT FROM @FSAOST into @cliente,@producto_id,@cantidad
	End
	If @Acumulador > 0 
	Begin
		set @xSQL=N' Update 	#TEMP_SALDOS_STOCK Set ' + @campo + '= ' + @campo + ' + ' + cast(@Acumulador as varchar) +	' Where 	Cliente_id =' + char(39) + @cliente + char(39) +' And producto_id = ' + char(39) + @producto_id + char(39)
		EXEC sp_executesql @xSQL
	End



	Close @FSAOST
	Deallocate @FSAOST

End--Fin Procedure.
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

ALTER  Procedure [dbo].[Funciones_Saldo_Api#Generar_Saldos_Stock]
@Cliente_Id 	as varchar(15),
@Producto_Id	as varchar(30)
As
Begin
	
	Declare @ErrorCod	as int

	Exec @ErrorCod=Funciones_Saldo_Api#Insertar_Lista_Productos @Cliente_ID,@Producto_ID
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Insertar_Lista_Productos.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_api#Obtener_Saldo_TR_ING_Act_Cantidad @Cliente_ID,@Producto_ID,'TR_ING'
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_api#Obtener_Saldo_TR_ING_Act_Cantidad.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_api#Obtener_Saldo_TR_EGR_Act_Cant @Cliente_ID,@Producto_ID,'TR_EGR'
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_api#Obtener_Saldo_TR_EGR_Act_Cant.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_Api#Obtener_Saldo_Stock_Actualizar_Cantidades @Cliente_ID,@Producto_ID,'STOCK'
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Obtener_Saldo_Stock_Actualizar_Cantidades.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_Api#Actualizar_Saldos_STOCK1
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_STOCK1.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_Api#Actualizar_Saldos_STOCK2 @Cliente_Id, @Producto_ID
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_STOCK2.',16,1)
		Return
	End
	
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

ALTER       Procedure [dbo].[Funciones_Stock_Api#Documento_A_Egreso]
				 @pDocumentoID numeric(20,0)
As
Begin

	Declare @vCategoriaStockID varchar(8)
	Declare @Fecha_Actual datetime
	Declare @pUsuarioID varchar(20)
	Declare @Nro_Linea numeric(10,0)
	Declare @Cliente_Id varchar(15)
	Declare @Producto_Id varchar(30)
	Declare @Cantidad numeric(20,5)
	Declare @Tipo_Operacion_Id varchar(5)
	Declare @Nro_Serie varchar(50)
	Declare @Nro_Lote varchar(50)
	Declare @Fecha_Vencimiento datetime
	Declare @Nro_Partida varchar(50)
	Declare @Nro_Despacho varchar(50)
	Declare @vNaveID numeric(20,0)
	Declare @vHPOS numeric(20,0)

	Set @vCategoriaStockID = 'TRAN_EGR'
	Set @Fecha_Actual = GetDate()

	Select @pUsuarioID = usuario_id	From #temp_usuario_loggin
	
	Declare PCUR Cursor For
		Select  dd.nro_linea,
				dd.cliente_id,
				dd.producto_id,
				dd.cantidad,
				d.tipo_operacion_id,
				dd.nro_serie,
				dd.nro_lote,
				dd.fecha_vencimiento,
				dd.nro_partida,
				dd.nro_despacho
		From det_documento dd,
		   documento d
		Where d.documento_id = dd.documento_id
		    And d.documento_id = @pDocumentoID

	Open PCUR
	Fetch Next From PCUR Into @Nro_Linea, @Cliente_Id, @Producto_Id
							, @Cantidad, @Tipo_Operacion_Id, @Nro_Serie
							, @Nro_Lote, @Fecha_Vencimiento, @Nro_Partida
							, @Nro_Despacho
	While @@Fetch_Status = 0
	Begin

		Exec Historico_Producto_Api#InsertRecord 
								@vHPOS 
                                ,@Fecha_Actual
                                ,Null
                                ,@Cantidad
                                ,@Tipo_Operacion_Id
                                ,Null 
                                ,@vNaveID
								,@pDocumentoID
								,@Nro_Linea 
								,@pUsuarioID
                                ,'+'
                                ,@Cliente_Id 
                                ,@Producto_Id
								,@Nro_Serie
								,@Nro_Lote
								,@Fecha_Vencimiento
								,@Nro_Partida
								,@Nro_Despacho

		Update det_documento
			Set cat_log_id = (Select cat_log_id
		                         From categoria_stock cs
		                         Inner Join categoria_logica cl
		                         On cl.categ_stock_id = cs.categ_stock_id
		                         Where cs.categ_stock_id = 'TRAN_EGR'
		                         And cliente_id = @Cliente_Id
							)
		Where documento_id = @pDocumentoID 
			And nro_linea = @Nro_Linea

		Update documento
		Set status = 'D20'
		Where documento_id = @pDocumentoID


		Fetch Next From PCUR Into @Nro_Linea, @Cliente_Id, @Producto_Id
								, @Cantidad, @Tipo_Operacion_Id, @Nro_Serie
								, @Nro_Lote, @Fecha_Vencimiento, @Nro_Partida
								, @Nro_Despacho
	End

	Close PCUR
	Deallocate PCUR



	Exec dbo.Funciones_Historico_Api#Actualizar_HistSaldos_STOCK @pDocumentoID, Null, Null
	Exec dbo.Funciones_Historico_Api#Actualizar_HistSaldos_CatLog @pDocumentoID, Null, Null

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

/*
	- NUMERO DE PALLET
	- SUCURSAL (CODIGO)
	- CODIGO PRODUCTO
	- CANTIDAD
	- FECHA_PICKING
*/

ALTER PROCEDURE [dbo].[GEN_COT]
@FECHA_PARAM AS VARCHAR(8)
AS
BEGIN
	SELECT	 P.PALLET_FINAL AS [PALLET]
			,S.SUCURSAL_ID AS [SUCURSAL]
			,P.PRODUCTO_ID AS [CODIGO PRODUCTO]
			,SUM(P.CANTIDAD) AS [CANTIDAD]
			,CONVERT(VARCHAR,FECHA_FIN,103) AS [FECHA]
	FROM	PICKING P INNER JOIN DET_DOCUMENTO DD		ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D						ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S						ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_DESTINO)
	WHERE	P.PALLET_FINAL IS NOT NULL
			AND P.PALLET_FINAL=P.PALLET_PICKING
			AND P.PALLET_CERRADO='1'
			AND CONVERT(VARCHAR,FECHA_FIN,103)=CONVERT(VARCHAR,CAST(@FECHA_PARAM AS DATETIME),103) 
	GROUP BY P.PALLET_FINAL, S.SUCURSAL_ID, P.PRODUCTO_ID,CONVERT(VARCHAR,FECHA_FIN,103)

END--FIN PROCEDURE
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

ALTER PROCEDURE [dbo].[GEN_INTERFAZ_EXPEDICION]
@VIAJE	AS VARCHAR(100) OUTPUT
AS
BEGIN
	SELECT	DISTINCT
			D.CLIENTE_ID +';' + S.SUCURSAL_ID + ';' + CAST(P.PALLET_FINAL AS VARCHAR)
	FROM	PICKING P INNER JOIN DET_DOCUMENTO DD ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D	ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S	ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
	WHERE	VIAJE_ID=@VIAJE
			AND PALLET_CERRADO='1'


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

ALTER procedure [dbo].[Gen_Interfaz_VC]
@cliente_id as varchar(20)
as
Begin
	Declare @cViaje			as cursor
	Declare @Viaje_Id		as varchar(100)
	Declare @cProducto		as cursor
	Declare @Producto_id	as varchar(30)
	Declare @Documento_id	as numeric(20,0)
	--
	Declare @Doc_Ext		as varchar(100)
	Declare @qty_sol		as float
	Declare @qty_doc		as float
	Declare @qty_pik		as float
	Declare @StreamLine		as varchar(max)
	Declare @PATH			as varchar(max)
	Declare @Cabecera		as char(1)

	set @Cabecera='0'
	SET @PATH='C:\Gt - Warp\Interfaces VitalCan\' + @cliente_id + '_RPT_DIF_' + CAST(DAY(GETDATE())AS VARCHAR) +'-'+ CAST(MONTH(GETDATE()) AS VARCHAR) +'-' + CAST(YEAR(GETDATE())AS VARCHAR)+'.CSV'

	select	dd.doc_ext,dd.producto_id, sum(dd.cantidad_solicitada) as [Cantidad_Solicitada],
			null as [QTY_DOCUMENTO], Null as[QTY_PICKING], documento_id into #temporal
	from	sys_int_documento d (nolock) inner join sys_int_det_documento dd (nolock)
			on(d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
	where	d.codigo_viaje in(	select	DISTINCT
										viaje_id
								from	picking p inner join documento d on(p.documento_id=d.documento_id)
								where	p.cliente_id=@cliente_id
										AND FACTURADO='1'
										AND FIN_PICKING='2'
										AND isnull(d.observaciones,'')<>'ENVIADO'
								)
	group by
			dd.doc_ext, dd.producto_id,dd.documento_id	
	
	set @cProducto = cursor for
		select producto_id, documento_id from #temporal
		
	open @cProducto
	Fetch Next from @cProducto into @Producto_id, @Documento_id
	While @@Fetch_Status=0
	Begin
		Update #temporal set qty_documento= (	Select	isnull(Sum(Cantidad),0)
												from	Det_documento
												where	documento_id=@Documento_id
														and producto_id=@Producto_id)
		Where	documento_id=@Documento_id and producto_id=@Producto_id

		Update #temporal set qty_picking= (	Select	isnull(Sum(p.cant_confirmada),0)
										from	det_documento dd inner join Picking p on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
										where	dd.documento_id=@Documento_id
												and dd.producto_id=@Producto_id)
		Where	documento_id=@Documento_id and producto_id=@Producto_id

		Fetch Next from @cProducto into @Producto_id, @Documento_id
	End--fin while @cProducto
	close @cproducto
	deallocate @cproducto	
	--Comienzo con los controles de rutina.
	Set @cproducto=cursor for
		select	doc_ext, producto_id,cantidad_solicitada, qty_documento, qty_picking
		from	#temporal
	open @cproducto
	fetch next from @cproducto into @doc_ext, @producto_id, @qty_sol, @qty_doc, @qty_pik
	while @@fetch_status=0
	begin
		if @cabecera='0'
		begin
			Set @StreamLine= 'NRO_REMITO;CODIGO PRODUCTO;CANTIDAD SOLICITADA;CANTIDAD ASIGNADA;CANTIDAD PICKEADA;'
			EXEC sp_AppendToFile @Path, @StreamLine
			Set @Cabecera='1'
		end
		Set @StreamLine=	cast(@doc_ext as varchar) + ';' +
							cast(@producto_id as varchar) + ';' +
							cast(@qty_sol as varchar) + ';' +
							cast(@qty_doc as varchar) + ';' +
							cast(@qty_pik as varchar) + ';'
		if (@qty_sol<>@qty_doc)
		begin
			EXEC sp_AppendToFile @Path, @StreamLine
		end
		else
		begin
			if (@qty_doc<>@qty_pik)
			begin
				EXEC sp_AppendToFile @Path, @StreamLine
			end
		end
		fetch next from @cproducto into @doc_ext, @producto_id, @qty_sol, @qty_doc, @qty_pik
	end --Fin cursor de escritural.
	close @cproducto
	deallocate @cproducto
	Update	documento set observaciones='ENVIADO' 
	where	documento_id in(select distinct documento_id from #temporal)
	drop table #temporal
End--Fin Procedure
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

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[GenerarNumeroContenedora] 
	-- Add the parameters for the stored procedure here
	@SEQ numeric(20,0) output
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    EXEC GET_VALUE_FOR_SEQUENCE 'PALLET_PICKING', @SEQ OUTPUT
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