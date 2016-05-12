USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 04:48 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from DESARROLLO_906 to WMS_ELECTRO_906_MATCH'
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

ALTER  procedure [dbo].[Act_Comprobante_RODC]
@fecha			varchar(20) output,
@Odc			varchar(50) output,
@transportista	varchar(50) output,
@Vehiculo		varchar(50) output,
@remito		varchar(30) Output
as
begin
	set xact_abort on
	begin transaction
	update 	documento set  nro_remito=@remito, observaciones='Transportista: ' + @transportista
	where	fecha_cpte between 	convert(datetime,@fecha,103) and dateadd(dd,1,convert(datetime,@fecha,103))
			and nro_despacho_importacion in(	select 	doc_ext
												from	sys_int_documento
												where	orden_de_compra=ltrim(rtrim(upper(@odc)))
											)

	update 	sys_dev_det_documento set prop3=@remito
	Where	doc_ext in(select 	doc_ext
						from	sys_dev_documento
						where	orden_de_compra=ltrim(rtrim(upper(@odc))) )

	commit transaction	


end
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

ALTER PROCEDURE [dbo].[ACT_POS_VH]
AS
BEGIN --START PROC. ANONIMO.
	SET XACT_ABORT ON;
	BEGIN TRANSACTION;

	DECLARE @CUR_POS	CURSOR;
	DECLARE @POS		NUMERIC(20,0);
	DECLARE @CUR_VH		CURSOR;
	DECLARE @VH			VARCHAR(20);
	DECLARE @COUNT		SMALLINT;
	
	SET @CUR_POS= CURSOR FOR
		SELECT	POSICION_ID 
		FROM	POSICION 
		WHERE	PICKING='1';

	OPEN @CUR_POS;
	FETCH NEXT FROM @CUR_POS INTO @POS;
	WHILE @@FETCH_STATUS=0
	BEGIN --START WHILE POS.

		SET @CUR_VH=CURSOR FOR
			SELECT	VEHICULO_ID 
			FROM	VEHICULO_PICKING;

		OPEN @CUR_VH
		FETCH NEXT FROM @CUR_VH INTO @VH;
		WHILE @@FETCH_STATUS=0
		BEGIN
			SELECT	@COUNT=COUNT(*)
			FROM	RL_VEHICULO_POSICION
			WHERE	VEHICULO_ID=@VH
					AND POSICION_ID=@POS
			IF @COUNT=0
			BEGIN
				INSERT INTO RL_VEHICULO_POSICION (VEHICULO_ID, POSICION_ID)
				VALUES(@VH, @POS);
			END
			FETCH NEXT FROM @CUR_VH INTO @VH;
		END
		CLOSE @CUR_VH;
		DEALLOCATE @CUR_VH;
		
		FETCH NEXT FROM @CUR_POS INTO @POS;
	END-- END WHILE POS.
	COMMIT TRANSACTION
END-- FIN PROC. ANONIMO.
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

ALTER PROCEDURE [dbo].[ACTUALIZA_FLAGS]
AS
BEGIN
	
	DECLARE @TCUR		CURSOR
	DECLARE @VIAJE_ID	AS VARCHAR(100)

	SET @TCUR= CURSOR FOR
		SELECT 	DISTINCT VIAJE_ID
		FROM	PICKING

	OPEN @TCUR
	
	FETCH NEXT FROM @TCUR INTO @VIAJE_ID
	WHILE @@FETCH_STATUS=0
	BEGIN
		
		EXEC PICKING_ACT_FLAG @VIAJE_ID
		
		FETCH NEXT FROM @TCUR INTO @VIAJE_ID
	END
	CLOSE @TCUR
	DEALLOCATE @TCUR
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

ALTER    PROCEDURE [dbo].[ACTUALIZA_POS_PICKING_TR]
	@POSICION_O AS VARCHAR(45),
	@POSICION_D AS VARCHAR(45),
	@PALLET		AS VARCHAR(100)
AS
BEGIN
	DECLARE @NAVE_COD 	AS VARCHAR(15)

	SELECT 	@NAVE_COD=N.NAVE_COD
	FROM 	POSICION P INNER JOIN NAVE N
			ON(P.NAVE_ID=N.NAVE_ID)
	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_D)))

	UPDATE 	PICKING SET 
						NAVE_COD=ISNULL(@NAVE_COD,@POSICION_D),
						POSICION_COD=@POSICION_D,
						SALTO_PICKING=0
	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
			AND FECHA_FIN IS NULL
			AND CANT_CONFIRMADA IS NULL
			AND PROP1=LTRIM(RTRIM(UPPER(@PALLET)))
					
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

ALTER  PROCEDURE [dbo].[ACTUALIZA_POS_PICKING_DESK]
	@DOC_TRANS_ID AS NUMERIC(20,0)output
As
Begin
	Declare @PosOrigen 	as varchar(45)
	Declare @PosDestino as varchar(45)
	Declare @Pallet		as varchar(100)

	Declare Cur_actPosPickDesk cursor for
		Select 	isnull(p.posicion_cod,n.nave_cod) as anterior,
				isnull(p2.posicion_cod,n2.nave_cod)as destino,
				dd.prop1 as pallet
		From 	rl_det_doc_trans_posicion rl
				left join nave n
				on(rl.nave_anterior=n.nave_id)
				left join nave n2
				on(rl.nave_actual=n2.nave_id)
				left join posicion p
				on(rl.posicion_anterior=p.posicion_id)
				left join posicion p2
				on(rl.posicion_actual=p2.posicion_id)
				--con esto saco el pallet que se esta moviendo
				inner join det_documento_transaccion ddt
				on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd
				on(ddt.documento_id=dd.documento_id and ddt.nro_linea_doc=dd.nro_linea)
		Where	rl.doc_trans_id_tr=@DOC_TRANS_ID

	Open Cur_actPosPickDesk

	Fetch Next from Cur_actPosPickDesk into @PosOrigen,@PosDestino,@Pallet
	while @@Fetch_Status=0
	Begin
		exec Actualiza_pos_picking_Tr @PosOrigen,@PosDestino,@Pallet
		if @@Error<>0
		Begin
			Raiserror('Ocurrio un error al actualizar la tabla de Picking',16,1)
			Break
		End
		Fetch Next from Cur_actPosPickDesk into @PosOrigen,@PosDestino,@Pallet
	End
	Close Cur_actPosPickDesk
	Deallocate Cur_actPosPickDesk
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

ALTER PROCEDURE [dbo].[ACTUALIZA_RELACION_PICKING]
@VIAJEID			VARCHAR(100),
@PRODUCTO_ID		VARCHAR(30),
@POSICION_COD		VARCHAR(50),
@PALLET			VARCHAR(100),
@RUTA				VARCHAR(100),
@ID					NUMERIC(20,0)
AS
BEGIN
	
	UPDATE 	PICKING SET HIJO=@ID
	WHERE	VIAJE_ID=@VIAJEID
			AND PRODUCTO_ID=@PRODUCTO_ID
			AND POSICION_COD=@POSICION_COD
			AND PROP1=@PALLET
			AND RUTA=@RUTA
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

ALTER PROCEDURE [dbo].[ACTUALIZAR_CONTROL_APF]
@EAN		VARCHAR(50),
@SUCURSAL	VARCHAR(30),
@PALLET		VARCHAR(100)
AS
BEGIN
	SET XACT_ABORT ON
	DECLARE @PRODUCTO		VARCHAR(30)
	DECLARE @PICKING_ID		NUMERIC (20,0)
	DECLARE @CURPICK		CURSOR
	DECLARE @QTY_CONTROL	FLOAT
	DECLARE @QTY_CONFIRMADA	FLOAT
	--POR EL EAN TENGO EL CODIGO PRODUCTO.
	SELECT	@PRODUCTO=PRODUCTO_ID
	FROM	RL_PRODUCTO_CODIGOS 
	WHERE	CLIENTE_ID='LEADER PRICE' AND CODIGO=@EAN

	IF @PRODUCTO IS NULL
	BEGIN
		INSERT INTO EAN_NO_VALIDO VALUES(@EAN)
		--RAISERROR('NO SE ENCONTRO EL CODIGO EAN %s',16,1,@EAN)
		RETURN
	END
	SET @CURPICK=CURSOR FOR
		SELECT	PICKING_ID 
		FROM	PICKING P INNER JOIN DET_DOCUMENTO DD ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
				INNER JOIN DOCUMENTO D ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
				INNER JOIN SUCURSAL S ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_DESTINO)
		WHERE	P.PRODUCTO_ID=@PRODUCTO
				AND S.SUCURSAL_ID=@SUCURSAL
				AND P.VIAJE_ID='20090630'
	OPEN @CURPICK
	FETCH NEXT FROM @CURPICK INTO @PICKING_ID
	WHILE @@FETCH_STATUS=0
	BEGIN
		SELECT	@QTY_CONFIRMADA=CANT_CONFIRMADA, @QTY_CONTROL=ISNULL(QTY_CONTROLADO,0)
		FROM	PICKING 
		WHERE	PICKING_ID=@PICKING_ID
		SELECT @PICKING_ID
		IF @QTY_CONFIRMADA>ISNULL(@QTY_CONTROL,0)
		BEGIN
			UPDATE PICKING SET QTY_CONTROLADO=ISNULL(QTY_CONTROLADO,0)+1, PALLET_FINAL=@PALLET, PALLET_CERRADO='1' WHERE PICKING_ID=@PICKING_ID
			IF @@ROWCOUNT>0
			BEGIN
				INSERT INTO AFP_MANUAL VALUES(@PICKING_ID)
			END
		END
		FETCH NEXT FROM @CURPICK INTO @PICKING_ID
	END --FIN CICLO CURSOR
	CLOSE @CURPICK
	DEALLOCATE @CURPICK
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

ALTER     PROCEDURE [dbo].[ACTUALIZAR_HISTORICOS_X_MOV]
	@P_RL_POS_ID 	AS NUMERIC(20,0),
	@USUARIO 		AS VARCHAR(30)	
AS
BEGIN
	DECLARE @POSICION_ACTUAL	AS NUMERIC(20,0)
	DECLARE @NAVE_ACTUAL		AS NUMERIC(20,0)
	DECLARE @POSICION_ANTERIOR	AS NUMERIC(20,0)
	DECLARE @NAVE_ANTERIOR		AS NUMERIC(20,0)
	DECLARE @CANTIDAD			AS NUMERIC(20,5)
	DECLARE @DOCUMENTO_ID		AS NUMERIC(20,0)
	DECLARE @NRO_LINEA_DOC		AS NUMERIC(10,0)
	DECLARE @DOC_TRANS_ID_EGR	AS NUMERIC(20,0)
	DECLARE @CLIENTE_ID			AS VARCHAR(15)
	DECLARE @PRODUCTO_ID		AS VARCHAR(30)
	DECLARE @NRO_SERIE			AS VARCHAR(50)
	DECLARE @NRO_LOTE			AS VARCHAR(50)
	DECLARE @FECHA_VENCIMIENTO 	AS DATETIME
	DECLARE @NRO_PARTIDA		AS VARCHAR(50)
	DECLARE @NRO_DESPACHO		AS VARCHAR(50)
	DECLARE @CODIGO				AS VARCHAR(10)
	DECLARE @CODIGO2			AS VARCHAR(10)
	

	DECLARE PCUR3 CURSOR FOR
		SELECT RL.POSICION_ACTUAL, RL.NAVE_ACTUAL, RL.POSICION_ANTERIOR,
		       RL.NAVE_ANTERIOR, RL.CANTIDAD, DDT.DOCUMENTO_ID, DDT.NRO_LINEA_DOC,
		       RL.DOC_TRANS_ID_EGR, DD.CLIENTE_ID, DD.PRODUCTO_ID,DD.NRO_SERIE,
		       DD.NRO_LOTE, DD.FECHA_VENCIMIENTO, DD.NRO_PARTIDA,DD.NRO_DESPACHO
		FROM  RL_DET_DOC_TRANS_POSICION RL,
		       DET_DOCUMENTO_TRANSACCION DDT,
		       DET_DOCUMENTO_TRANSACCION DDT2,
		       DET_DOCUMENTO DD
		WHERE DDT2.DOCUMENTO_ID = DD.DOCUMENTO_ID
		       AND DDT2.NRO_LINEA_DOC = DD.NRO_LINEA
		       AND RL.DOC_TRANS_ID = DDT2.DOC_TRANS_ID
		       AND RL.NRO_LINEA_TRANS = DDT2.NRO_LINEA_TRANS
		       AND ISNULL(RL.DOC_TRANS_ID_TR, ISNULL(RL.DOC_TRANS_ID_EGR, RL.DOC_TRANS_ID)) = DDT.DOC_TRANS_ID
		       AND ISNULL(RL.NRO_LINEA_TRANS_TR, ISNULL(RL.NRO_LINEA_TRANS_EGR, RL.NRO_LINEA_TRANS)) = DDT.NRO_LINEA_TRANS
		       AND RL.RL_ID = @P_RL_POS_ID
	OPEN PCUR3
	FETCH NEXT FROM PCUR3 INTO    @POSICION_ACTUAL
								,@NAVE_ACTUAL
								,@POSICION_ANTERIOR
								,@NAVE_ANTERIOR
								,@CANTIDAD
								,@DOCUMENTO_ID
								,@NRO_LINEA_DOC
								,@DOC_TRANS_ID_EGR
								,@CLIENTE_ID
								,@PRODUCTO_ID
								,@NRO_SERIE
								,@NRO_LOTE
								,@FECHA_VENCIMIENTO
								,@NRO_PARTIDA
								,@NRO_DESPACHO
	WHILE @@FETCH_STATUS=0
		BEGIN
			SET @CODIGO='TR'
			SET @CODIGO2='+'
			/*
			INSERT INTO HISTORICO_PRODUCTO 
			VALUES(	 GETDATE(),@POSICION_ANTERIOR,@CANTIDAD,@CODIGO
					,NULL,@NAVE_ANTERIOR,@DOCUMENTO_ID,@NRO_LINEA_DOC
					,@USUARIO,'-',@CLIENTE_ID,@PRODUCTO_ID
					,@NRO_SERIE,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_PARTIDA
					,@NRO_DESPACHO)

			INSERT INTO HISTORICO_PRODUCTO 
			VALUES(	GETDATE(), @POSICION_ACTUAL,@CANTIDAD,@CODIGO,NULL,@NAVE_ACTUAL,
					@DOCUMENTO_ID,@NRO_LINEA_DOC,@USUARIO,@CODIGO2,@CLIENTE_ID,	
					@PRODUCTO_ID,@NRO_SERIE,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_PARTIDA,
					@NRO_DESPACHO)
			*/
			INSERT INTO HISTORICO_POSICION
			VALUES(	@POSICION_ANTERIOR,'EGR',GETDATE(),NULL,@CANTIDAD,@DOCUMENTO_ID,@NRO_LINEA_DOC,@USUARIO,
					@NAVE_ANTERIOR,@CLIENTE_ID,@PRODUCTO_ID,@NRO_SERIE,@NRO_LOTE,@FECHA_VENCIMIENTO,
					@NRO_PARTIDA,@NRO_DESPACHO)
			
			INSERT INTO HISTORICO_POSICION
			VALUES(	@POSICION_ACTUAL,'ING',GETDATE(),NULL,@CANTIDAD,@DOCUMENTO_ID,@NRO_LINEA_DOC,@USUARIO,
					@NAVE_ACTUAL,@CLIENTE_ID,@PRODUCTO_ID,@NRO_SERIE,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_PARTIDA,
					@NRO_DESPACHO)

			FETCH NEXT FROM PCUR3 INTO    @POSICION_ACTUAL
										,@NAVE_ACTUAL
										,@POSICION_ANTERIOR
										,@NAVE_ANTERIOR
										,@CANTIDAD
										,@DOCUMENTO_ID
										,@NRO_LINEA_DOC
										,@DOC_TRANS_ID_EGR
										,@CLIENTE_ID
										,@PRODUCTO_ID
										,@NRO_SERIE
										,@NRO_LOTE
										,@FECHA_VENCIMIENTO
										,@NRO_PARTIDA
										,@NRO_DESPACHO
		
		END
	CLOSE PCUR3
	DEALLOCATE PCUR3

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

ALTER    procedure [dbo].[ActualizarGMDev]
As
Begin
	Set nocount on
	Set xact_abort on
	Declare @IsLock			Int

	Select 	@IsLock=Count(*) 
	from 	master.dbo.syslockinfo 
	where 	rsc_objid=(	Select 	ID from 	sysobjects where name='GM_DEV_DOCUMENTO')
	If @IsLock>0
	Begin
		Insert into Sys_Auditoria_Dev_Gm(F_Ejecucion, Estado,Observaciones) Values(Getdate(),'E','Se encontro la tabla GM_DEV_DOCUMENTO lockeada')
		Return
	End

	Set @Islock=null

	Select 	@IsLock=Count(*) 
	from 	master.dbo.syslockinfo 
	where 	rsc_objid=(	Select 	ID from 	sysobjects where name='GM_DEV_DET_DOCUMENTO')
	If @IsLock>0
	Begin
		Insert into Sys_Auditoria_Dev_Gm(F_Ejecucion, Estado,Observaciones) Values(Getdate(),'E','Se encontro la tabla GM_DEV_DET_DOCUMENTO lockeada')
		Return
	End

	Select * into #detalle  FROM sys_dev_det_documento where flg_movimiento is null
	
	select * into #cabecera from sys_dev_documento c  where exists (select 1 from #detalle d  where c.cliente_id = d.cliente_id and c.doc_ext = d.doc_ext)
								and flg_movimiento is null

	Begin Transaction

	--Cabecera
	Insert into Gm_Dev_Documento
	Select 	 Cliente_Id
			,Tipo_Documento_Id
			,Cpte_Prefijo
			,Cpte_Numero
			,Fecha_Cpte
			,Fecha_Solicitud_Cpte
			,Agente_Id
			,Peso_Total
			,Unidad_peso
			,Volumen_Total
			,Unidad_Volumen
			,Total_Bultos
			,Orden_De_Compra
			,Observaciones
			,Nro_Remito
			,Nro_Despacho_Importacion
			,Doc_Ext
			,Codigo_Viaje
			,Info_Adicional_1
			,Info_Adicional_2
			,Info_Adicional_3
			,Tipo_Comprobante
			,Estado
			,Fecha_Estado
			,Estado_Gt
			,Fecha_Estado_Gt
			,Getdate()
	from	#cabecera


	--Detalle
	Insert into Gm_Dev_Det_Documento
	Select	 Doc_Ext
			,Nro_Linea
			,Cliente_id
			,Producto_Id
			,Cantidad_Solicitada
			,Cantidad
			,Est_Merc_Id
			,Cat_Log_id
			,Nro_Bulto
			,Descripcion
			,nro_lote
			,nro_pallet
			,fecha_vencimiento
			,nro_despacho
			,nro_partida
			,unidad_id
			,unidad_contenedora_id
			,peso
			,unidad_peso
			,volumen
			,Unidad_Volumen
			,Prop1
			,Prop2
			,Prop3
			,Largo
			,Alto
			,Ancho
			,Doc_Back_Order
			,Estado
			,Fecha_Estado
			,Estado_Gt
			,Fecha_estado_gt
			,documento_id
			,nave_id
			,nave_cod
			,Getdate()
	From	#detalle

	update 	Sys_Dev_Det_documento set flg_movimiento=1 
	from 	Sys_Dev_Det_documento s,#Detalle d
	where 	S.Cliente_Id=d.Cliente_Id and S.Doc_Ext=D.Doc_Ext and s.nro_linea=d.nro_linea

	update 	Sys_Dev_documento set flg_movimiento=1 
	from 	Sys_Dev_documento s,#cabecera d
	where 	S.Cliente_Id=d.Cliente_Id and S.Doc_Ext=D.Doc_Ext
			
	Commit Transaction
	Drop table #detalle
	drop table #cabecera

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

ALTER  PROCEDURE [dbo].[Ajuste_Qty]
@pCliente_id		varchar(100)  output,
@pTipo_Documento	varchar(100)  output,
@pDoc_ext		varchar(100)  output,
@pproducto_id		varchar(100)  output,
@pcantidad		numeric(20,5) output,
@pest_merc_id		varchar(100)  output,
@pcat_log_id		varchar(100)  output,
@pdescripcion		varchar(100)  output,
@pnro_lote		varchar(100)  output,
@pnro_pallet		varchar(100)  output,
@pfecha_vencimiento	datetime      output,
@pnro_despacho		varchar(100)  output,
@pnro_partida		varchar(100)  output,
@punidad_id		varchar(100)  output,
@pnave_id		varchar(100)  output,
@pnave_cod		varchar(100)  output,
@pSigno			varchar(100)  output
AS
--SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
BEGIN
--BEGIN TRANSACTION
	if @pSigno='RESTAR' BEGIN
		set @pcantidad=(@pcantidad * -1)
	END --IF
	insert into sys_dev_documento (
		cliente_id,
		tipo_documento_id,
		fecha_cpte,
		Doc_ext, 
		estado_gt,
		fecha_estado_gt,
		Flg_Movimiento)
	values (
		@pCliente_id,
		@pTipo_Documento,
		getdate(), --Fecha_Cpte
		'AJ_QTY' + CAST(@pDoc_ext AS varchar(100)),
		'P', --Estado_GT
		getdate(), -- Fecha_Estado_GT
		Null --Flg_Movimiento
		 )

	IF @@ERROR <> 0 BEGIN
		RAISERROR('Error al Registrar el Ajuste por Cantidad: CABECERA',16,1)
		RETURN --PARA QUENO SIGA EJECUTANDO CODIGO
	END
	
	insert into sys_dev_det_documento (
		 doc_ext
		,nro_linea
		,cliente_id
		,producto_id
		,cantidad_solicitada
		,cantidad
		,est_merc_id
		,cat_log_id
		,descripcion
		,nro_lote
		,nro_pallet
		,fecha_vencimiento
		,nro_despacho
		,nro_partida
		,unidad_id
		,prop1
		,estado_gt
		,fecha_estado_gt
		,nave_id
		,nave_cod
		,Flg_Movimiento)
	values(
		'AJ_QTY' + CAST(@pDoc_ext AS varchar(100)),
		1, --nro_linea
		@pCliente_id,
		@pproducto_id,
		@pcantidad,
		@pcantidad,
		@pest_merc_id,
		@pcat_log_id,
		@pdescripcion,
		@pnro_lote,
		@pnro_pallet,
		@pfecha_vencimiento,
		@pnro_despacho,
		@pnro_partida,
		@punidad_id,
		@pTipo_Documento,		
		'P', --estado_gt
		getdate(), --fecha_estado_gt
		dbo.Aj_NaveCod_to_Nave_id(@pnave_cod),
		@pnave_cod,
		Null --Flg_Movimiento
		)	

	IF @@ERROR <> 0 BEGIN
		RAISERROR('Error al Registrar el Ajuste por Cantidad',16,1)
		RETURN
	END


/*
IF @@ERROR <> 0 BEGIN
--	ROLLBACK TRANSACTION
	RAISERROR('Error al Registrar el Cambio de Categoria Logica',16,1)
END ELSE BEGIN
	COMMIT TRANSACTION
END --IF
*/

END --PROCEDURE
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

ALTER      PROCEDURE [dbo].[Am_funciones_estacion_api#Actualiza_Estado]
	@Doc_Trans_Id	as Numeric(20,0),
	@TipoOperacion	as Varchar(10),
	@pCur_ActEst	CURSOR VARYING OUTPUT
As
Begin
	Declare @vStatus		as varchar(3)
	Declare @vTr_Activo 	as varchar(1)
	Declare @v1Usuario_ID 	as varchar(20)
	Declare @v1Terminal 	as varchar(20)
	Declare @vUsuario_Id 	as varchar(30)
	Declare @vTerminal		as varchar(20)
	Declare @Session_Id		as varchar(60)

	SELECT	@vStatus=DT.Status,	@vTr_Activo=DT.TR_ACTIVO, @v1Usuario_ID=DT.USUARIO_ID, @v1Terminal=DT.TERMINAL
	FROM  	DOCUMENTO_TRANSACCION DT
	WHERE 	DT.DOC_TRANS_ID=@Doc_Trans_Id

	SELECT  @vUsuario_Id=TUL.USUARIO_ID, @vTerminal=TUL.TERMINAL, @Session_Id=session_id
	FROM	#TEMP_USUARIO_LOGGIN TUL
		
	If @vTr_Activo='0'
	----------------------------------------------------------------------------------------------------------------
	Begin
		--Abre el Cursor de Salida
		Set @pCur_ActEst= Cursor Forward_Only Static for
		select '' tr_activo,''status,''usuario,''terminal from documento_transaccion where doc_trans_id =@doc_trans_id

		update 	documento_transaccion set tr_activo='0',tr_activo_id=null,session_id=null,fecha_cambio_tr=null
		where 	usuario_id = @vusuario_id and terminal = @vterminal	and tr_activo = '1'

		update documento_transaccion set tr_activo = '1',tr_activo_id = (select tr_activo_id
												                         from sys_tr_activo_motivo
												                         where tipo_operacion_id=@tipooperacion)                 
			   ,USUARIO_ID = @vUsuario_ID,TERMINAL =@vTerminal,SESSION_ID =@Session_Id,FECHA_CAMBIO_TR =Getdate()
		WHERE DOC_TRANS_ID=@Doc_Trans_Id
		open @pCur_ActEst
	End --Fin If @vTr_Activo='0'
	----------------------------------------------------------------------------------------------------------------
	If @vTr_Activo = '1' And upper(lTrim(rtrim(@vStatus))) = 'T10' And @v1Usuario_ID = @vUsuario_ID And @v1Terminal = @vterminal
	----------------------------------------------------------------------------------------------------------------	
	Begin
		Set @pCur_ActEst= Cursor Forward_Only Static for
		select '' tr_activo,''status,''usuario,''terminal  FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID = @Doc_Trans_Id

		Update 	DOCUMENTO_TRANSACCION SET TR_ACTIVO = '0',TR_ACTIVO_ID = Null,	SESSION_ID = Null,	FECHA_CAMBIO_TR = Null
		WHERE 	Usuario_Id = @vUsuario_id	AND TERMINAL =@vTerminal AND TR_ACTIVO = '1'
				      
		UPDATE DOCUMENTO_TRANSACCION
		     SET 	TR_ACTIVO = '1',TR_ACTIVO_ID = ( SELECT TR_ACTIVO_ID
							                         From SYS_TR_ACTIVO_MOTIVO
							                         WHERE TIPO_OPERACION_ID =@TipoOperacion)
		         	,USUARIO_ID =@vUsuario_id,TERMINAL =@vTerminal,SESSION_ID =@Session_id,FECHA_CAMBIO_TR =GetDate()
		WHERE DOC_TRANS_ID =@Doc_Trans_Id
		Open @pCur_ActEst
	End
	----------------------------------------------------------------------------------------------------------------	
	If @vTr_Activo = '1' Or Upper(lTrim(rTrim(@vStatus))) <> 'T10'
	----------------------------------------------------------------------------------------------------------------	
	Begin
		Set @pCur_ActEst= Cursor Forward_Only Static for
		SELECT 	DT.TR_ACTIVO,
				DT.STATUS,
				SU.NOMBRE AS USUARIO,
				TERMINAL
		FROM 	DOCUMENTO_TRANSACCION DT
				INNER JOIN SYS_USUARIO SU ON (SU.USUARIO_ID = DT.USUARIO_ID)
		WHERE 	DT.DOC_TRANS_ID=@Doc_Trans_Id
		Open @pCur_ActEst

		RAISERROR ('El documento esta siendo procesado por otro Usuario. ',16,1)
		return(99)
    End
	----------------------------------------------------------------------------------------------------------------	
End	--Fin Procedure
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

ALTER  Procedure [dbo].[Am_Funciones_Estacion_api#Comprobar_ReglaMovimiento]
@DocTransID 	as numeric(20,0)
As
Begin
	Declare @nueva_estacion 	as varchar(5)


	select 	@nueva_estacion=max(ultima_estacion)
	from 	rl_det_doc_trans_posicion
	where 	doc_trans_id =@DocTransID

	If @nueva_estacion is not null
	Begin
		Exec Funciones_Movimiento_api#Actualizar_Reglas_Pendientes @DocTransID,'RM_1', 0
	End

End --Fin Procedure
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

ALTER  Procedure [dbo].[Am_Funciones_Estacion_Api#Comprobar_Si_debe_ubicar]
@TransaccionID 	As varchar(15),
@DocTransID 	As numeric(20,0), 
@EstacionID 	As varchar(15),
@OrdenEstacion 	As int
As
Begin
	
	Declare @UObligatoria	as Char(1)

	select 	@UObligatoria=ubicacion_obligatoria
	from 	rl_transaccion_estacion
	where 	transaccion_id  =@TransaccionID
			and estacion_id =@EstacionID
			and orden =@OrdenEstacion

	if @UObligatoria is not null and @UObligatoria='1'
	Begin
		SELECT regla_id FROM pendiente_doc_trans WHERE doc_trans_id =@DocTransID AND regla_id = 'RU_1'
		if @@RowCount > 0
		Begin
			Insert Into PENDIENTE_DOC_TRANS(
				        DOC_TRANS_ID,
				        TIPO_REGLA,
				        REGLA_ID,
				        NRO_LINEA)
			 Values (
				         @DocTransID
				        ,'UBIC'
				        ,'RU_1'
				        ,1
				     )
		End
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

ALTER    Procedure [dbo].[Am_Funciones_Estacion_Api#DocID_A_DocTrID]
					@PDocId numeric(20,0)
As
Begin
	Declare @Doc_Trans_Id numeric(20,0)
	Declare @nro_linea_trans numeric(10,0)
	Declare @nro_linea_doc numeric(10,0)

	Declare	PCUR Cursor For
		Select doc_trans_id,
			nro_linea_trans,
			nro_linea_doc
		From det_documento_transaccion 
		Where documento_id = @PDocId 

	Open PCUR
	Fetch Next From PCUR Into @Doc_Trans_Id, @nro_linea_trans, @nro_linea_doc
	While @@Fetch_Status = 0
		Begin

			Update rl_det_doc_trans_posicion
			Set 	 	doc_trans_id = @Doc_Trans_Id
					,documento_id = Null
					,nro_linea_trans =@nro_linea_trans
					,nro_linea = Null
			Where 	documento_id = @PDocId
					And nro_linea = @nro_linea_doc

			Fetch Next From PCUR Into @Doc_Trans_Id, @nro_linea_trans, @nro_linea_doc
		End

	Close PCUR
	DEALLOCATE PCUR

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

ALTER    procedure [dbo].[Am_Funciones_Estacion_API#GetNextEstacion]
@TransaccionId 	as varchar(15),
@pOrden 		as int,
@Doc_trans_id	as Numeric(20,0),
@Retorno		as Varchar(100) output
As
Begin
	Declare @Salida 		as int
	Declare @vTransaccionId as Varchar(15)
	Declare @vEstacion 		as varchar(15)
	Declare @Orden			as Int
	Declare @StrEstacion	as Varchar(100)

	Declare @Trans			as varchar(15)
	Declare @Station 		as Varchar(15)
	Declare @iOrden			as Int

	Set	@salida = 0
	------------------------------------------------------------------------------------------------------
	select
			@vtransaccionid	= transaccion_id,
			@vestacion		= estacion_actual,
			@orden			= orden_estacion
	from 	documento_transaccion
	where 	doc_trans_id = @Doc_trans_id 
			and transaccion_id = ltrim(rtrim(upper(@TransaccionId)))
			and (status = 'T10' or status = 'T20')
	------------------------------------------------------------------------------------------------------
	Declare Pcur Cursor For
		select
				rte.TRANSACCION_ID,
				rte.estacion_id,
				rte.orden
		FROM 	rl_transaccion_estacion rte
		WHERE 	rte.TRANSACCION_ID = @vtransaccionid
		ORDER BY TRANSACCION_ID, orden

	Open Pcur
	Fetch Next From Pcur into @Trans,@Station,@iOrden
	While @@Fetch_Status=0
	Begin
        set @strEstacion = @Station + '|' + Cast(@iOrden as Varchar(10))
		If @Salida=1 and @iOrden > @orden
		Begin
			Break
		End
		
		If ltrim(rtrim(upper(@Station)))=ltrim(rtrim(upper(@vEstacion)))
		Begin
			Set @Salida=1
		End

		Fetch Next From Pcur into @Trans,@Station,@iOrden
	End --Fin While

    If @salida = 0
	Begin
        Set @strEstacion = ''
    End 
	Close 	Pcur
	Deallocate Pcur
	Set @Retorno= @StrEstacion

End --Fin del procedure.
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

ALTER   PROCEDURE [dbo].[Am_Funciones_Estacion_api#GetRsPendientesForDoc]
	@Doc_Trans_id as Numeric(20,0),
	@Cur_GetRsPendientesForDoc CURSOR VARYING OUTPUT
As
Begin

	Set @Cur_GetRsPendientesForDoc	= Cursor Forward_Only Static For
	
	SELECT 	pdt.tipo_regla, 'DATOS ADICIONALES' AS Descripcion, pdt.doc_trans_id, PDT.REGLA_ID
			,(	SELECT 	(tabla + '/' + campo) 
				FROM  	det_regla_informacion dri
			    WHERE 	dri.r_informacion_id = PDT.REGLA_ID and dri.nro_linea = pdt.nro_linea
			) AS DescID
	FROM 	PENDIENTE_doc_trans pdt 
	WHERE 	pdt.doc_trans_id = @Doc_Trans_id AND pdt.tipo_regla = 'INF'
	UNION ALL
	--'************MOVIMIENTO********************************
	SELECT 	pdt.tipo_regla, 'MOVIMIENTOS' AS Descripcion,
			pdt.doc_trans_id, PDT.REGLA_ID, 'REGLA DE MOVIMIENTOS'
	FROM 	PENDIENTE_doc_trans pdt
	WHERE 	pdt.doc_trans_id = @Doc_Trans_id
			AND pdt.tipo_regla = 'MOV'
	UNION ALL
	
	--'******IMPRESION*****************************************
	SELECT 	pdt.tipo_regla, 'REPORTES'  AS Descripcion, pdt.doc_trans_id,
			PDT.REGLA_ID, (	SELECT descripcion 
							FROM det_regla_impresion rimp
							WHERE 	rimp.regla_impresion_id = PDT.REGLA_ID and pdt.nro_linea = rimp.nro_linea
							) AS DescID
	FROM 	PENDIENTE_doc_trans pdt
	WHERE 	pdt.doc_trans_id =@Doc_Trans_id 
			AND pdt.tipo_regla = 'IMP'
	UNION ALL
	--'******UBICACION*****************************************
	SELECT 	pdt.tipo_regla, 'UBICACION' AS Descripcion, pdt.doc_trans_id,
			PDT.REGLA_ID, 'REGLA DE UBICACION'
	FROM 	PENDIENTE_doc_trans pdt
	WHERE 	pdt.doc_trans_id = @Doc_Trans_id 
			AND pdt.tipo_regla = ' UBIC'
	UNION ALL
	--'******Transferencia*****************************************
	SELECT 	pdt.tipo_regla, 'TRANSFERENCIA' AS Descripcion, pdt.doc_trans_id,
			PDT.REGLA_ID, 'REGLA DE TRANSFERENCIA'
	FROM 	PENDIENTE_doc_trans pdt
	WHERE 	pdt.doc_trans_id = @Doc_Trans_id 
			AND pdt.tipo_regla = 'TR'
	--'******INVENTARIO****************************************
	UNION ALL
	SELECT 	pdt.tipo_regla, 'INVENTARIO' AS Descripcion, pdt.doc_trans_id, PDT.REGLA_ID,
			CASE
			   when PDT.REGLA_ID = 'RINV1' then 'CONFIGURAR INVENTARIO'
			   when PDT.REGLA_ID = 'RINV2' then 'INGRESAR CONTEOS'
			   when PDT.REGLA_ID = 'RINV3' then 'CERRAR INVENTARIO'
			END
	FROM 	PENDIENTE_doc_trans pdt
	WHERE 	pdt.doc_trans_id = @Doc_Trans_id
			AND pdt.tipo_regla = 'INV'
	
	--'******CARGA DE NUMEROS DE SERIE AL EGRESO*********************
	UNION ALL
	SELECT 	pdt.tipo_regla , 'DATOS ADICIONALES' AS Descripcion, pdt.doc_trans_id, PDT.REGLA_ID,
			CASE
			   when PDT.REGLA_ID = 'SER_1' then 'CARGA DE NUMEROS DE SERIE'
			END
	FROM 	PENDIENTE_doc_trans pdt
	WHERE 	pdt.doc_trans_id = @Doc_Trans_id 
			AND pdt.tipo_regla = 'SER'
	--'-------------------------------------
	ORDER BY 2

	Open @Cur_GetRsPendientesForDoc

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

ALTER   Procedure [dbo].[Am_Funciones_Estacion_Api#UpdateEstacionActual]
@TransaccionID 	as varchar(15), 
@DocTransID 	as numeric(20,0), 
@EstacionID 	as varchar(15), 
@OrdenEstacion 	as Int, 
@Final 			as int
As
Begin

	if @Final=0
	Begin
		Update 	 DOCUMENTO_TRANSACCION
		Set 	 ESTACION_ACTUAL = @EstacionID
				,ORDEN_ESTACION =@OrdenEstacion
				,EST_MOV_ACTUAL = (	select Max(ultima_estacion) 
									from RL_DET_DOC_TRANS_POSICION
									WHERE doc_trans_id = @DocTransID)
				,IT_MOVER = 0
		Where DOC_TRANS_ID = @DocTransID
	
		Exec Am_Funciones_Estacion_Api#Comprobar_si_Debe_Ubicar @TransaccionID,@DocTransID,@EstacionID,@OrdenEstacion

		Exec Ent_Documento_Api#Set_Status_Documento_por_Tr @DocTransID
	End
	if @Final=2
	Begin

		Update DOCUMENTO_TRANSACCION SET ESTACION_ACTUAL=null, STATUS='T40',FECHA_FIN_GTW =Getdate() WHERE DOC_TRANS_ID=@DocTransID

		Exec Ent_Documento_Api#Set_Status_Documento_por_TR @DocTransID

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

ALTER     procedure [dbo].[Am_Funciones_Estacion_Api#UpdateEstacionActual_Stock]
@Doc_trans_id	as numeric(20,0),
@Flag 			as Int
As
Begin
	
	Declare @vRlId	as numeric(20,0)
	
	If @Flag=1
	Begin
		Declare CurRL Cursor For
			select rl_id from rl_det_doc_trans_posicion where doc_trans_id = @doc_trans_id 

		Open CurRl

		Fetch Next From CurRl into @vRlId
        While @@Fetch_Status=0
		Begin
			Exec Funciones_Historicos_Api#Actualizar_Historicos_X_Mov @vRlId
			Fetch Next From CurRl into @vRlId
		End
        
		UPDATE 	RL_DET_DOC_TRANS_POSICION SET CAT_LOG_ID=CAT_LOG_ID_FINAL,DISPONIBLE = '1' WHERE DOC_TRANS_ID=@Doc_trans_id
		UPDATE 	DOCUMENTO_TRANSACCION SET status='T20' WHERE 	doc_trans_id=@Doc_trans_id

		Exec Funciones_Historico_Api#Enviar_RL_a_Historico @Doc_trans_id, 'ING'
       		Exec Funciones_Historico_Api#Actualizar_HistSaldos_STOCK Null, DocTransID, Null
        	--Exec Funciones_Historico_Api#Actualizar_HistSaldos_CatLog Null, DocTransID, Null

		Close CurRl
		Deallocate CurRl
	End

	If @Flag=2
	Begin
	
		Declare CurRL Cursor For
			SELECT rl_id FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID_EGR =@Doc_Trans_Id
	
		Open CurRL

		Fetch Next From CurRL Into @vRlId
        While @@Fetch_Status=0
		Begin
			Exec Funciones_Historicos_Api#Actualizar_Historicos_X_Mov @vRlId	
			Fetch Next From CurRL Into @vRlId
		End

		Exec Funciones_Historico_Api#Enviar_RL_a_Historico  @Doc_trans_id, 'EGR'
        
        Exec Funciones_Estacion_Api#BorrarDocTREgreso @Doc_trans_id

		UPDATE DOCUMENTO_TRANSACCION SET status = 'T20' WHERE doc_trans_id=@Doc_trans_id

       -- Exec Funciones_Historico_Api#Actualizar_HistSaldos_STOCK Null, @Doc_Trans_Id, Null
        --Exec Funciones_Historico_Api#Actualizar_HistSaldos_CatLog Null, @Doc_Trans_Id, Null

		Close CurRl
		Deallocate CurRl
	End

	If @Flag=3
	Begin
		Declare CurRL Cursor For
			select rl_id from rl_det_doc_trans_posicion	where doc_trans_id_tr =@Doc_Trans_Id

		Open CurRl

		Fetch Next From CurRL Into @vRlId
        While @@Fetch_Status=0
		Begin
			Exec Funciones_Historicos_Api#Actualizar_Historicos_X_Mov @vRlId
			Fetch Next From CurRL Into @vRlId
        End

		UPDATE RL_DET_DOC_TRANS_POSICION SET DISPONIBLE = '1' WHERE DOC_TRANS_ID_TR =@Doc_Trans_Id
		Exec Funciones_Historico_Api#Enviar_RL_a_Historico @Doc_trans_id, 'TR'

		Close CurRl
		Deallocate CurRl

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

ALTER   Procedure [dbo].[Am_Funciones_Estacion_Api#UpdateStatusDoc]
					@DocumentoId numeric(20,0)
					, @StrStatus varchar(3)
As
Begin

	Update documento
	Set Status = Upper(LTrim(RTrim(@StrStatus)))
	WHERE documento_id = @DocumentoId 

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

ALTER    Procedure [dbo].[Am_funciones_estacion2_api#GetDataForTR]
@Estacion_ID 		as Varchar(15),
@Transaccion_ID		as Varchar(15),
@Orden				as Numeric(3,0),
@PCur_GetDataForTR	CURSOR VARYING OUTPUT
As
Begin
	Set @PCur_GetDataForTR=Cursor Forward_Only Static For
	Select 	transaccion_id	
		,estacion_id 	
		,orden 			
		,r_informacion_id
		,r_impresion_id 	
		,categ_stock_id 
		,determina_ubicacion
		,ubicacion_autom 		
		,actualiza_stock 		
		,fin 					
		,nave_default 			
		,cancelar_transaccion 	
		,rollback_transaccion 	
		,cola_trabajo 			
		,deposito_id 			
		,ubicacion_obligatoria 	
		,inv_crear 				
		,inv_contar 			
		,inv_adm 				
		,serie_egr 				
		,actualiza_cabecera 	
		,imprimir_remito		
		,imprimir_rem_anexo 	
		,codigo_barras 			
		,categoria_logica 		
		,cant_solicitada 		
	From  	rl_transaccion_estacion
	Where 	transaccion_id =Ltrim(Rtrim(Upper(@Transaccion_ID)))
	     	and estacion_id=Ltrim(Rtrim(Upper(@Estacion_ID)))
		    and orden=Ltrim(Rtrim(Upper(@Orden)))
	Open @PCur_GetDataForTR

End --Fin Procedure
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

ALTER          Procedure [dbo].[Asigna_Tratamiento#Asigna_Tratamiento_EGR]
 							@P_Doc_Id as numeric(20,0)
As
Begin
	Declare @xSTatus varchar(10)
    Declare @VTipoDoc varchar(15)
	Declare @VTipoOp varchar(15)
	Declare @VNroLinea numeric(10,0)
	Declare @VEstMerc varchar(15)
	Declare @VClienteID varchar(15)
	Declare @VCatLog varchar(50)
	Declare @VItemOk varchar(5)
	Declare @VTransId varchar(15)
	Declare @VCant numeric(10,0)
	Declare @VStation varchar(15)
	Declare @VNroLineaTrans numeric(10,0)
	Declare @VSeq numeric(20,0)
	Declare @xSQL varchar(200)


	SELECT @xStatus = STATUS, @VTipoDoc = TIPO_COMPROBANTE_ID,@VTipoOp = TIPO_OPERACION_ID 
	FROM DOCUMENTO WHERE DOCUMENTO_ID= @P_DOC_ID

	If @xStatus <> 'D20'
		Begin	
			Raiserror ('El documento no esta en D20.',16,1)
			Return    
		End

	DECLARE	PCUR CURSOR FOR
		--Obtiene el tratamiento de cada producto, a nivel particular o el default
		SELECT  DD.NRO_LINEA,
				DD.EST_MERC_ID,
				DD.CLIENTE_ID,
				DD.CAT_LOG_ID,
				DD.ITEM_OK,
				ISNULL((   SELECT TRANSACCION_ID 
							From RL_PRODUCTO_TRATAMIENTO 
							Where cliente_id = dd.cliente_id 
								AND TIPO_OPERACION_ID=D.TIPO_OPERACION_ID 
								AND TIPO_COMPROBANTE_ID=D.TIPO_COMPROBANTE_ID 
								AND PRODUCTO_ID=DD.PRODUCTO_ID)
				,P.EGRESO 
				) AS TRANSACCION_ID
		From DET_DOCUMENTO DD INNER JOIN PRODUCTO P 
								On DD.CLIENTE_ID= P.CLIENTE_ID AND 
								DD.PRODUCTO_ID = P.PRODUCTO_ID INNER JOIN DOCUMENTO D 
								On DD.DOCUMENTO_ID=D.DOCUMENTO_ID 
		Where dd.documento_id = @P_Doc_Id
		ORDER BY TRANSACCION_ID,DD.NRO_LINEA 
		
	Open PCUR
	Fetch Next From PCUR Into @VNroLinea, @VEstMerc, @VClienteID
							, @VCatLog, @VItemOk, @VTransId

	While @@Fetch_Status = 0
	Begin
        If @VTransId = ''
			Begin            
				Raiserror ('NO PUEDE CONTINUAR CON ESTE DOCUMENTO HASTA QUE CARGUE LOS TRATAMIENTO EN EL MAESTRO DE PRODUCTO.',16,1)
				Return    
			End

      Select @VCant = Count(DT.doc_trans_id)
		From DET_DOCUMENTO_TRANSACCION DDT INNER JOIN DOCUMENTO_TRANSACCION DT
        On DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID
        Where DDT.DOCUMENTO_ID = @P_Doc_Id AND DT.TRANSACCION_ID = @VTransId
        
        If @VCant = 0 
			Begin

				Select @VStation = ESTACION_ID
				From RL_TRANSACCION_ESTACION
				Where TRANSACCION_ID = @VTransId AND ORDEN = 1
            
				Set @xSql = @VTRANSID + '-' + cast(@P_DOC_ID as varchar(20))
				Exec dbo.DOCUMENTO_TRANSACCION_API#InsertRecord 
						0, @xSql, @VTransId, @VStation , 'T10'
						, 'A', Null, '0', 1, @VTipoOp , Null, '0' 
						, Null, Null, Null, Null, Null, Null, Null, @VSeq Output

				Set @VNroLineaTrans = 1
            
				Exec Det_Documento_Transaccion_Api#InsertRecord 
						@VSeq, @VNroLineaTrans 
						, @P_Doc_Id, @VNroLinea , null, @VEstMerc  
						, @vClienteID, @VCatLog , '0', '0'
			End
        Else
			Begin
				Select  @VNroLineaTrans = Max(NRO_LINEA_TRANS)+1
				From DET_DOCUMENTO_TRANSACCION 
				Where DOCUMENTO_ID = @P_Doc_Id AND DOC_TRANS_ID = @VSeq

				If @VNroLineaTrans is null
					Begin
						Set @VNroLineaTrans = 1
					End

				Exec Det_Documento_Transaccion_Api#InsertRecord @VSeq, @VNroLineaTrans , @P_Doc_Id, @VNroLinea , NULL, @VEstMerc  
																, @vClienteID, @VCatLog , '0', '0'
			End

		Fetch Next From PCUR Into @VNroLinea, @VEstMerc, @VClienteID
								, @VCatLog, @VItemOk, @VTransId
	End

	Close PCUR
	DEALLOCATE PCUR

	Exec Am_Funciones_Estacion_Api#UpdateStatusDoc @P_Doc_Id, 'D30'
	Exec Am_Funciones_Estacion_Api#DocID_A_DocTrID @P_Doc_Id

End
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