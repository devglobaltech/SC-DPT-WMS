USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 05:22 p.m.
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

ALTER  PROCEDURE [dbo].[DeleteAllDoc]
AS


	DECLARE @DOC_TRANS_ID AS NUMERIC(20)
	DECLARE @DOCUMENTO_ID AS NUMERIC(20)

	DECLARE CUR_DET_DOC_TRANS CURSOR FOR
	SELECT DOC_TRANS_ID, DOCUMENTO_ID
	FROM DET_DOCUMENTO_TRANSACCION
        OPEN CUR_DET_DOC_TRANS
	
	FETCH NEXT FROM CUR_DET_DOC_TRANS
	INTO @DOC_TRANS_ID, @DOCUMENTO_ID
	    WHILE @@FETCH_STATUS = 0
		BEGIN

		DELETE FROM DET_DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	
		DELETE FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM HISTORICO_PRODUCTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM HISTORICO_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	
		DELETE FROM DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
		
	   FETCH NEXT FROM CUR_DET_DOC_TRANS
	   INTO @DOC_TRANS_ID, @DOCUMENTO_ID
	END
        CLOSE CUR_DET_DOC_TRANS
        DEALLOCATE CUR_DET_DOC_TRANS
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

ALTER    PROCEDURE [dbo].[DELETEDOC]
@DOCUMENTO_ID AS NUMERIC(20)
AS

DECLARE @DOC_TRANS_ID AS NUMERIC(20)
BEGIN TRANSACTION
BEGIN
	
	SELECT @DOC_TRANS_ID= DOC_TRANS_ID 
	FROM DET_DOCUMENTO_TRANSACCION
	WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DET_DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID=@DOC_TRANS_ID

	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM HISTORICO_PRODUCTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM HISTORICO_POSICION WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	DELETE FROM DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

END 
COMMIT TRANSACTION

/*
DELETEDOC
@DOCUMENTO_ID=18
*/
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

ALTER    PROCEDURE [dbo].[DELETEDOC_EGR]
@DOCUMENTO_ID	NUMERIC(20,0)
AS
BEGIN
	SET XACT_ABORT ON
	DECLARE @DOC_TRANS_ID NUMERIC(20,0)
	
	SELECT @DOC_TRANS_ID=DOC_TRANS_ID FROM DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE DOC_TRANS_ID_EGR=@DOC_TRANS_ID
	DELETE FROM DET_DOCUMENTO_TRANSACCION WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	DELETE FROM DOCUMENTO_TRANSACCION WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	DELETE FROM DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
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

ALTER Procedure [dbo].[DeleteNroLinea]
@Documento_Id	Numeric(20,0)Output,
@Nro_linea		Numeric(10,0)Output
As
Begin
	
	Delete from Det_Documento_Aux 	where Documento_id=@Documento_id and Nro_Linea=@Nro_Linea
	Delete from Consumo_Locator_Egr where Documento_id=@Documento_id and Nro_Linea=@Nro_Linea

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

ALTER  PROCEDURE [dbo].[DeletePickerMan]
@viaje_id 	varchar(100) output,
@usuario_id 	varchar(100) output
AS
BEGIN
	 DELETE RL_VIAJE_USUARIO WHERE VIAJE_ID=@viaje_id AND USUARIO_ID=@usuario_id
	 
	--Limpio las tareas tomadas por el usuario
	update picking set fecha_inicio=null,usuario=null,pallet_picking=null
	where
	   usuario=@usuario_id and fecha_fin is null and cant_confirmada is null
	   and viaje_id=@viaje_id
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

ALTER    PROCEDURE [dbo].[DESK_FIN_TRANSFERENCIA]
	@Doc_trans_Id 	NUMERIC(20,0) OUTPUT

AS
BEGIN
	DECLARE @IORDEN 			AS NUMERIC(3,0)
	DECLARE @STATION 			AS VARCHAR(15)
	DECLARE @TRANSACCION_ID 	AS VARCHAR(15)
	DECLARE @STATUS			AS VARCHAR(3)
	DECLARE @FLG_FIN			AS CHAR(1)
	DECLARE @FLG_ACT_STOCK	AS CHAR(1)
	DECLARE @NEXT_STATION 	AS VARCHAR(15)
	DECLARE @NEXT_ORDEN		AS VARCHAR(15)
	DECLARE @USUARIO 			VARCHAR(20)

	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	--OBTENGO EL ORDEN DE LA ESTACION.
	SELECT 	@IORDEN=DBO.GETORDENESTACIONFORDOCTRID(@Doc_trans_Id)

	SELECT 	@STATION=ESTACION_ACTUAL,@TRANSACCION_ID=TRANSACCION_ID,
			@STATUS=STATUS
	FROM  	DOCUMENTO_TRANSACCION
	WHERE 	DOC_TRANS_ID=@Doc_trans_Id


	SELECT 	@FLG_FIN=FIN, @FLG_ACT_STOCK=ACTUALIZA_STOCK
	FROM  	RL_TRANSACCION_ESTACION
	WHERE 	TRANSACCION_ID 	=@TRANSACCION_ID
	     	AND ESTACION_ID	=@STATION
	     	AND ORDEN		=@IORDEN

	EXEC DBO.UPDATEESTACIONACTUAL_STOCK_TRANS	@DOC_TRANS_ID=@Doc_trans_Id, @USUARIO=@USUARIO

	EXEC DBO.UPDATEESTACIONACTUAL  @DOC_TRANS_ID=@Doc_trans_Id


END --FIN DEL PROCEDURE
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

ALTER         Procedure [dbo].[Det_Documento_Api#InsertRecord]
	@P_Documento_Id numeric(20,0)
	,@P_Nro_Linea numeric(10,0)
	,@P_Cliente_Id varchar(15)
	,@P_Producto_Id varchar(30)
	,@P_Cantidad numeric(20,5)
	,@P_Nro_Serie varchar(50)
	,@P_Nro_Serie_Padre varchar(50)
	,@P_Est_Merc_Id varchar(15)
	,@P_Cat_Log_Id varchar(50)
	,@P_Nro_Bulto varchar(50)
	,@P_Descripcion varchar(200)
	,@P_Nro_Lote varchar(50)
	,@P_Fecha_Vencimiento datetime
	,@P_Nro_Despacho varchar(50)
	,@P_Nro_Partida varchar(50)
	,@P_Unidad_Id varchar(5)
	,@P_Peso numeric(20,5)
	,@P_Unidad_Peso varchar(5)
	,@P_Volumen numeric(20,5)
	,@P_Unidad_Volumen varchar(5)
	,@P_Busc_Individual varchar(1)
	,@P_Tie_In varchar(1)
	,@P_Nro_Tie_In_Padre varchar(100)
	,@P_Nro_Tie_In varchar(100)
	,@P_Item_Ok varchar(1)
	,@P_Moneda_Id varchar(20)
	,@P_Costo numeric(10,3)
	,@P_Cat_Log_Id_Final varchar(50)
	,@P_Prop1 varchar(100)
	,@P_Prop2 varchar(100)
	,@P_Prop3 varchar(100)
	,@P_Largo numeric(10,3)
	,@P_Alto numeric(10,3)
	,@P_Ancho numeric(10,3)
	,@P_Volumen_Unitario varchar(1)
	,@P_Peso_Unitario varchar(1)
	,@P_Cant_Solicitada numeric(20,5)
As
Begin
	
	Declare @VTipoDoc varchar(5)
	Declare @vCATLOG varchar(15)
	Declare @vlote numeric(1,0)
	Declare @vpallet numeric(1,0)
	Declare @Secuencia varchar(30)


	Select @P_NRO_LINEA = isnull(Max(nro_linea),0) + 1
	From det_documento
	Where documento_id = @P_DOCUMENTO_ID

	IF (@P_NRO_LINEA IS NULL) SET @P_NRO_LINEA = 1

	Select @VTipoDoc = TIPO_OPERACION_ID 
	From DOCUMENTO 
	Where DOCUMENTO_ID = @P_DOCUMENTO_ID

	If @VTipoDoc is not null
		Begin
			If @VTipoDoc = 'ING'
				Begin
					Select @vlote = lote_automatico  , @vpallet = pallet_automatico 
					From producto 
					Where cliente_id = Upper(LTrim(RTrim(@P_Cliente_Id)))
					AND producto_id = Upper(LTrim(RTrim(@P_Producto_Id)))
					
					If (@vlote = 1) And ((@P_Nro_Lote is null) Or (@P_Nro_Lote = ''))
						Begin
							Set @Secuencia = 'NROLOTE_SEQ'
							exec dbo.GET_VALUE_FOR_SEQUENCE @Secuencia, @P_Nro_Lote	
							--Set @P_Nro_Lote = dbo.GET_VALUE_FOR_SEQUENCE(@Secuencia)	
						End
					If (@vpallet = 1) And ((@P_Prop1 is null) Or (@P_Prop1 = ''))
						Begin
							Set @Secuencia = 'NROPALLET_SEQ'
							exec dbo.GET_VALUE_FOR_SEQUENCE @Secuencia, @P_Prop1
							--Set @P_Prop1 = dbo.GET_VALUE_FOR_SEQUENCE(@Secuencia)		
						End
					
					if @P_Nro_Partida is null 
						begin
							Set @Secuencia = 'NRO_PARTIDA'
							Exec dbo.GET_VALUE_FOR_SEQUENCE @Secuencia, @P_Nro_Partida
						end
	               
					Select @vCatLog = ISNULL(RL.cat_log_id, P.ING_CAT_LOG_ID) 
					From PRODUCTO P 
						Inner JOIN RL_PRODUCTO_CATLOG RL
						On (P.PRODUCTO_ID=RL.PRODUCTO_ID AND P.CLIENTE_ID=RL.CLIENTE_ID),
						DOCUMENTO D
					Where RL.tipo_comprobante_id = D.tipo_comprobante_id
					AND D.DOCUMENTO_ID= @P_Documento_Id
					AND RL.PRODUCTO_ID= @P_Producto_Id 
					AND RL.CLIENTE_ID= @P_Cliente_Id 
	                
	                If @vCatLog is not null
						Begin
							Set @P_Cat_Log_Id_Final = @vCatLog
						End
				End

		Insert Into DET_DOCUMENTO ( 
							DOCUMENTO_ID
							, NRO_LINEA
							, CLIENTE_ID
							, PRODUCTO_ID
							, CANTIDAD
							, NRO_SERIE
							, NRO_SERIE_PADRE
							, EST_MERC_ID
							, CAT_LOG_ID
							, NRO_BULTO
							, DESCRIPCION
							, NRO_LOTE
							, FECHA_VENCIMIENTO
							, NRO_DESPACHO
							, NRO_PARTIDA
							, UNIDAD_ID
							, PESO
							, UNIDAD_PESO
							, VOLUMEN
							, UNIDAD_VOLUMEN
							, BUSC_INDIVIDUAL
							, TIE_IN
							, NRO_TIE_IN_PADRE
							, NRO_TIE_IN
							, ITEM_OK
							, CAT_LOG_ID_FINAL
							, MONEDA_ID
							, COSTO
							, PROP1
							, PROP2
							, PROP3
							, LARGO
							, ALTO
							, ANCHO
							, VOLUMEN_UNITARIO
							, PESO_UNITARIO
							, CANT_SOLICITADA
							, TRACE_BACK_ORDER
						   ) 
		Values (
							Upper(@P_Documento_Id) 
							, Cast(@P_Nro_Linea as varchar(10)) 
							, Upper(@P_Cliente_Id) 
							, Upper(@P_Producto_Id) 
							, Cast(@P_Cantidad as varchar(25)) 
							, Upper(@P_Nro_Serie) 
							, Upper(@P_Nro_Serie_Padre) 
							, Upper(@P_Est_Merc_Id) 
							, Upper(@P_Cat_Log_Id) 
							, Upper(@P_Nro_Bulto) 
							, ISNULL(Upper(@P_Descripcion),dbo.get_descripcion(@p_cliente_id,@p_Producto_id))
							, Upper(@P_Nro_Lote) 
							, Convert(Varchar, @P_Fecha_Vencimiento ,101) 
							, Upper(@P_Nro_Despacho) 
							, Upper(@P_Nro_Partida) 
							, isnull(Upper(@P_Unidad_Id),dbo.get_Unidad_Id(@p_Cliente_Id,@P_Producto_Id))
							, Cast(@P_Peso as varchar(25)) 
							, Upper(@P_Unidad_Peso) 
							, Cast(@P_Volumen as varchar(25)) 
							, Upper(@P_Unidad_Volumen) 
							, Upper(@P_Busc_Individual) 
							, Upper(@P_Tie_In) 
							, Upper(@P_Nro_Tie_In_Padre) 
							, Upper(@P_Nro_Tie_In) 
							, Upper(@P_Item_Ok) 
							, Upper(@P_Cat_Log_Id_Final) 
							, Upper(@P_Moneda_Id) 
							, Cast(@P_Costo as varchar(13)) 
							, Upper(@P_Prop1) 
							, Upper(@P_Prop2) 
							, Upper(@P_Prop3) 
							, Cast(@P_Largo as varchar(13)) 
							, Cast(@P_Alto as varchar(13)) 
							, Cast(@P_Ancho as varchar(13)) 
							, Upper(@P_Volumen_Unitario) 
							, Upper(@P_Peso_Unitario) 
							, Cast(@P_Cant_Solicitada as varchar(13)) 
							, Null
				)

		Update Documento 
		Set status = 'D10' 
		Where DOCUMENTO_ID = @P_DOCUMENTO_ID
	
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

ALTER   Procedure [dbo].[Det_Documento_Transaccion_Api#InsertRecord]
						@P_Doc_Trans_Id numeric(20,0)
						, @P_Nro_Linea_Trans numeric(10,0)
						, @P_Documento_Id  numeric(20,0)
						, @P_Nro_Linea_Doc numeric(10,0)
						, @P_Motivo_Id varchar(15)
						, @P_Est_Merc_Id varchar(15)
						, @P_Cliente_Id varchar(15)
						, @P_Cat_Log_Id varchar(50)
						, @P_Item_Ok varchar(1)
						, @P_Movimiento_Pendiente varchar(1)
As
Begin

	Insert Into DET_DOCUMENTO_TRANSACCION (
							DOC_TRANS_ID,
							NRO_LINEA_TRANS,
							DOCUMENTO_ID,
							NRO_LINEA_DOC,
							MOTIVO_ID,
							EST_MERC_ID,
							CLIENTE_ID,
							CAT_LOG_ID,
							ITEM_OK,
							MOVIMIENTO_PENDIENTE,
							DOC_TRANS_ID_REF,
							NRO_LINEA_TRANS_REF
							)
	Values (
							Upper(LTrim(RTrim(@P_Doc_Trans_Id))), 
							Upper(LTrim(RTrim(@P_Nro_Linea_Trans))),
							Upper(LTrim(RTrim(@P_Documento_Id))), 
							Upper(LTrim(RTrim(@P_Nro_Linea_Doc))), 
							Upper(LTrim(RTrim(@P_Motivo_Id))),  
							Upper(LTrim(RTrim(@P_Est_Merc_Id))), 
							Upper(LTrim(RTrim(@P_Cliente_Id))), 
							Upper(LTrim(RTrim(@P_Cat_Log_Id))), 
							Upper(LTrim(RTrim(@P_Item_Ok))),
							Upper(LTrim(RTrim(@P_Movimiento_Pendiente))),
							NULL,
							NULL
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

ALTER  PROCEDURE [dbo].[DET_EGR_AUTOCOMPLETA_SEXISTENCIA]
@DOCUMENTO_ID	AS NUMERIC(20,0)
AS
BEGIN
	Declare @NRO_LINEA 			numeric  (10, 0)
	Declare @CLIENTE_ID 		varchar  (15)
	Declare @PRODUCTO_ID 		varchar  (30)
	Declare @CANTIDAD 			numeric  (20, 5)
	Declare @NRO_SERIE 			varchar  (50)
	Declare @NRO_SERIE_PADRE 	varchar  (50)
	Declare @EST_MERC_ID 		varchar  (15)
	Declare @CAT_LOG_ID 		varchar  (50)
	Declare @NRO_BULTO 			varchar  (50)
	Declare @DESCRIPCION 		varchar  (200)
	Declare @NRO_LOTE 			varchar  (50)
	Declare @FECHA_VENCIMIENTO 	datetime   
	Declare @NRO_DESPACHO 		varchar  (50)
	Declare @NRO_PARTIDA 		varchar  (50)
	Declare @UNIDAD_ID 			varchar  (5)
	Declare @PESO 				numeric  (20, 5)
	Declare @UNIDAD_PESO 		varchar  (5)
	Declare @VOLUMEN 			numeric  (20, 5)
	Declare @UNIDAD_VOLUMEN 	varchar  (5)
	Declare @BUSC_INDIVIDUAL 	varchar  (1)
	Declare @TIE_IN 			varchar  (1)
	Declare @NRO_TIE_IN_PADRE 	varchar  (100)
	Declare @NRO_TIE_IN 		varchar  (100)
	Declare @ITEM_OK 			varchar  (1)
	Declare @CAT_LOG_ID_FINAL 	varchar  (50)
	Declare @MONEDA_ID 			varchar  (20)
	Declare @COSTO 				numeric  (10, 3)
	Declare @PROP1 				varchar  (100)
	Declare @PROP2 				varchar  (100)
	Declare @PROP3 				varchar  (100)
	Declare @LARGO 				numeric  (10, 3)
	Declare @ALTO 				numeric  (10, 3)
	Declare @ANCHO 				numeric  (10, 3)
	Declare @VOLUMEN_UNITARIO 	varchar  (1)
	Declare @PESO_UNITARIO 		varchar  (1)
	Declare @CANT_SOLICITADA 	numeric  (20, 5)
	Declare @TRACE_BACK_ORDER 	varchar  (1)
	Declare @xCursor			Cursor
	
	Set @xCursor= Cursor for
	SELECT 	 DOCUMENTO_ID
			,CLIENTE_ID
			,PRODUCTO_ID
			,CANTIDAD
			,NRO_SERIE
			,NRO_SERIE_PADRE
			,EST_MERC_ID
			,CAT_LOG_ID
			,NRO_BULTO
			,DESCRIPCION
			,NRO_LOTE
			,FECHA_VENCIMIENTO
			,NRO_DESPACHO
			,NRO_PARTIDA
			,UNIDAD_ID
			,PESO
			,UNIDAD_PESO
			,VOLUMEN
			,UNIDAD_VOLUMEN
			,BUSC_INDIVIDUAL
			,TIE_IN
			,NRO_TIE_IN_PADRE
			,NRO_TIE_IN
			,ITEM_OK
			,CAT_LOG_ID_FINAL
			,MONEDA_ID
			,COSTO
			,PROP1
			,PROP2
			,PROP3
			,LARGO
			,ALTO
			,ANCHO
			,VOLUMEN_UNITARIO
			,PESO_UNITARIO
			,CANT_SOLICITADA
			,TRACE_BACK_ORDER
	FROM 	DET_DOCUMENTO 
	WHERE	DOCUMENTO_ID=(@DOCUMENTO_ID)
			AND PRODUCTO_ID NOT IN(
			SELECT
					dd.producto_id as producto
			FROM 	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
					inner join posicion p on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
			WHERE
					rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
					and dd.producto_id in (	select 	producto_id 
											from 	det_documento 
											where 	documento_id=@DOCUMENTO_ID
										)
			UNION 
			SELECT
					dd.producto_id as producto
			FROM 	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
					inner join nave n on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
			WHERE
					rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR'
					and dd.producto_id in (	select 	producto_id 
											from 	det_documento 
											where 	documento_id=@DOCUMENTO_ID
										)
			)
	Open @xCursor

	Fetch Next from @xCursor into 	@DOCUMENTO_ID,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD,@NRO_SERIE,@NRO_SERIE_PADRE,
									@EST_MERC_ID,@CAT_LOG_ID,@NRO_BULTO,@DESCRIPCION,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_DESPACHO,
									@NRO_PARTIDA,@UNIDAD_ID,@PESO,@UNIDAD_PESO,@VOLUMEN,@UNIDAD_VOLUMEN,@BUSC_INDIVIDUAL,@TIE_IN,
									@NRO_TIE_IN_PADRE,@NRO_TIE_IN,@ITEM_OK,@CAT_LOG_ID_FINAL,@MONEDA_ID,@COSTO,@PROP1,@PROP2,@PROP3,
									@LARGO,@ALTO,@ANCHO,@VOLUMEN_UNITARIO,@PESO_UNITARIO,@CANT_SOLICITADA,@TRACE_BACK_ORDER
	While @@Fetch_Status=0
	Begin
		Select @Nro_Linea=Max(Isnull(Nro_Linea,0))+1 From Det_Documento_Aux Where Documento_id=@Documento_id
		
		Insert Into Det_Documento_Aux 
							     Values(@DOCUMENTO_ID,@Nro_Linea,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD,@NRO_SERIE,@NRO_SERIE_PADRE,
										@EST_MERC_ID,@CAT_LOG_ID,@NRO_BULTO,@DESCRIPCION,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_DESPACHO,
										@NRO_PARTIDA,@UNIDAD_ID,@PESO,@UNIDAD_PESO,@VOLUMEN,@UNIDAD_VOLUMEN,@BUSC_INDIVIDUAL,@TIE_IN,
										@NRO_TIE_IN_PADRE,@NRO_TIE_IN,@ITEM_OK,@CAT_LOG_ID_FINAL,@MONEDA_ID,@COSTO,@PROP1,@PROP2,@PROP3,
										@LARGO,@ALTO,@ANCHO,@VOLUMEN_UNITARIO,@PESO_UNITARIO,@CANT_SOLICITADA,@TRACE_BACK_ORDER
				)

		Fetch Next from @xCursor into 	@DOCUMENTO_ID,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD,@NRO_SERIE,@NRO_SERIE_PADRE,
										@EST_MERC_ID,@CAT_LOG_ID,@NRO_BULTO,@DESCRIPCION,@NRO_LOTE,@FECHA_VENCIMIENTO,@NRO_DESPACHO,
										@NRO_PARTIDA,@UNIDAD_ID,@PESO,@UNIDAD_PESO,@VOLUMEN,@UNIDAD_VOLUMEN,@BUSC_INDIVIDUAL,@TIE_IN,
										@NRO_TIE_IN_PADRE,@NRO_TIE_IN,@ITEM_OK,@CAT_LOG_ID_FINAL,@MONEDA_ID,@COSTO,@PROP1,@PROP2,@PROP3,
										@LARGO,@ALTO,@ANCHO,@VOLUMEN_UNITARIO,@PESO_UNITARIO,@CANT_SOLICITADA,@TRACE_BACK_ORDER

	End
	Close @xCursor
	Deallocate @xCursor
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

/*#14082008#*/ 
ALTER       PROCEDURE [dbo].[Det_Egr_IngresaSysInt]
@Documento_id Numeric(20,0)
As
Begin
	Declare @Doc_Ext 	as varchar(100)
	Declare @Control 	as Float
	Declare @StrInicial	as varchar(10)



	SELECT @StrInicial= Tipo_Comprobante_Id + '_'  From Documento Where Documento_id=@Documento_id

	SELECT @Doc_Ext=NRO_REMITO FROM DOCUMENTO WHERE DOCUMENTO_ID=@Documento_id
	
	SELECT @Control=count(*) FROM SYS_INT_DOCUMENTO WHERE DOC_EXT=@Doc_Ext
	
	if @Control>0
	begin
		Update sys_int_det_documento set Estado_gt='P', fecha_estado_gt=getdate(), documento_id=@Documento_Id where Doc_Ext=@Doc_Ext
		Update sys_int_documento set Estado_gt='P', fecha_estado_gt=getdate() where Doc_Ext=@Doc_Ext
	
		Return
	end

	--MANDO LA CABECERA.
	INSERT INTO SYS_INT_DOCUMENTO(
		CLIENTE_ID, TIPO_DOCUMENTO_ID, CPTE_PREFIJO, CPTE_NUMERO, FECHA_CPTE, FECHA_SOLICITUD_CPTE, AGENTE_ID,
		PESO_TOTAL, UNIDAD_PESO, VOLUMEN_TOTAL, UNIDAD_VOLUMEN, TOTAL_BULTOS, ORDEN_DE_COMPRA, OBSERVACIONES,
		NRO_REMITO, NRO_DESPACHO_IMPORTACION, DOC_EXT, CODIGO_VIAJE,INFO_ADICIONAL_1, INFO_ADICIONAL_2,
		INFO_ADICIONAL_3, TIPO_COMPROBANTE, ESTADO, FECHA_ESTADO, ESTADO_GT, FECHA_ESTADO_GT)
	SELECT 
			 CLIENTE_ID				
			,TIPO_COMPROBANTE_ID	
			,CPTE_PREFIJO
			,CPTE_NUMERO
			,FECHA_CPTE
			,FECHA_PEDIDA_ENT
			,SUCURSAL_DESTINO
			,PESO_TOTAL
			,UNIDAD_PESO
			,VOLUMEN_TOTAL
			,UNIDAD_VOLUMEN
			,TOTAL_BULTOS
			,ORDEN_DE_COMPRA
			,OBSERVACIONES
			,NRO_REMITO
			,NULL
			,@StrInicial + CAST(DOCUMENTO_ID AS VARCHAR)
			,NRO_DESPACHO_IMPORTACION
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,'P'
			,GETDATE()
	FROM 	DOCUMENTO
	WHERE	DOCUMENTO_ID=@DOCUMENTO_ID
	
	UPDATE DOCUMENTO SET NRO_REMITO=@STRINICIAL + CAST(@DOCUMENTO_ID AS VARCHAR) WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	--MANDO EL DETALLE
	INSERT INTO SYS_INT_DET_DOCUMENTO(
		DOC_EXT, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, CANTIDAD_SOLICITADA, CANTIDAD, EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,
		NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO, NRO_PARTIDA, UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO,
		VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, DOC_BACK_ORDER, ESTADO, FECHA_ESTADO, ESTADO_GT, 
		FECHA_ESTADO_GT, DOCUMENTO_ID, NAVE_ID, NAVE_COD)
	SELECT	 @StrInicial + CAST(DOCUMENTO_ID AS VARCHAR)
			,NRO_LINEA
			,CLIENTE_ID
			,PRODUCTO_ID
			,CANTIDAD
			,NULL
			,NULL
			,NULL
			,NULL
			,DESCRIPCION
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,UNIDAD_ID
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,'P'
			,GETDATE()
			,DOCUMENTO_ID
			,NULL
			,NULL
	FROM	DET_DOCUMENTO
	WHERE	DOCUMENTO_ID=@DOCUMENTO_ID


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
SET QUOTED_IDENTIFIER OFF
GO

ALTER     PROCEDURE [dbo].[DET_EGR_INSERT_CONSUMO_LOCATOR_EGR]
		@vDOC_ID			AS NUMERIC(20,0)	OUTPUT,
		@NRO_LINEA		AS NUMERIC(20,0)	OUTPUT,
		@vCLIENTE_ID 		AS VARCHAR(30)	OUTPUT,
		@vPRODUCTO_ID	AS VARCHAR(30)	OUTPUT,
		@vCANTIDAD		AS NUMERIC(20,5)	OUTPUT,
		@vRL_ID 			AS NUMERIC(20,0)	OUTPUT,
		@vSALDO			AS NUMERIC(20,5)	OUTPUT,
		@vTIPO				AS VARCHAR(20)	OUTPUT,
		@vPROCESADO		AS VARCHAR(1)		OUTPUT
AS
BEGIN
DECLARE @Qty_SALDO AS NUMERIC(20,5)

	IF (@vRL_ID IS NULL) OR (LTRIM(RTRIM(@vRL_ID))='') OR (@vRL_ID=0)
	BEGIN
		RAISERROR('El valor Rl no es valido',16,1)
		return
	END

	DELETE FROM CONSUMO_LOCATOR_EGR WHERE DOCUMENTO_ID = @vDOC_ID AND NRO_LINEA = @NRO_LINEA;
	DELETE FROM DET_DOCUMENTO_AUX WHERE DOCUMENTO_ID = @vDOC_ID AND NRO_LINEA = @NRO_LINEA;
	
	--Exec Dbo.Get_Qty_Stock @vCLIENTE_ID, @vPRODUCTO_ID, @Qty_SALDO Output
	
	SELECT @Qty_SALDO=CANTIDAD - @vCANTIDAD FROM RL_DET_DOC_TRANS_POSICION WHERE RL_ID=@vRL_ID

	INSERT INTO CONSUMO_LOCATOR_EGR (DOCUMENTO_ID,NRO_LINEA,CLIENTE_ID,PRODUCTO_ID, CANTIDAD, RL_ID, SALDO, TIPO, FECHA, PROCESADO)
	VALUES (@vDOC_ID, @NRO_LINEA, @vCLIENTE_ID, @vPRODUCTO_ID, @vCANTIDAD, @vRL_ID, @Qty_SALDO, @vTIPO, GETDATE(), @vPROCESADO)

	INSERT INTO DET_DOCUMENTO_AUX
	SELECT * FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@VDOC_ID AND NRO_LINEA=@NRO_LINEA
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

ALTER     PROCEDURE [dbo].[DEVO_REGISTRA_TEMP]
@VIAJE_ID 	AS VARCHAR(100) output,
@Pedido 	as varchar(100) output
AS 
Begin
	/*
	CREATE TABLE #FRONTERA_ING_EGR(
	DOCUMENTO_ID	NUMERIC(20,0),
	NRO_LINEA		NUMERIC(10,0))
	*/
	--Consigo que se cargue en la temporal los valores persistidos.
	INSERT INTO #FRONTERA_ING_EGR
	SELECT 	DISTINCT
			DD.DOCUMENTO_ID,
			DD.NRO_LINEA
	FROM	FRONTERA_ING_EGR FI (nolock)
			INNER JOIN vDET_DOCUMENTO DD (nolock)
			ON(FI.DOCUMENTO_ID=DD.DOCUMENTO_ID AND FI.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN vDOCUMENTO D (nolock)
			ON(FI.DOCUMENTO_ID=D.DOCUMENTO_ID)
	WHERE	D.NRO_DESPACHO_IMPORTACION=@VIAJE_ID
			AND D.TIPO_OPERACION_ID='EGR'
			AND ((@PEDIDO IS NULL) OR (D.NRO_REMITO LIKE '%' + @PEDIDO + '%'))
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
SET QUOTED_IDENTIFIER OFF
GO

ALTER     Procedure [dbo].[Documento_Api#InsertRecord]
 	@P_Documento_Id numeric(20,0) OUTPUT
 	,@P_Cliente_Id varchar(15)
 	,@P_Tipo_Comprobante_Id varchar(5) 
 	,@P_Tipo_Operacion_Id varchar(5)
	,@P_Det_Tipo_Operacion_Id varchar(5)
	,@P_Cpte_Prefijo varchar(6)
	,@P_Cpte_Numero varchar(20)
	,@P_Fecha_Cpte varchar(20)
	,@P_Fecha_Pedida_Ent varchar(20)
	,@P_Sucursal_Origen varchar(20)
	,@P_Sucursal_Destino varchar(20)
	,@P_Anulado varchar(1)
	,@P_Motivo_Anulacion varchar(15)
	,@P_Peso_Total numeric(20,5)
	,@P_Unidad_Peso varchar(5)
	,@P_Volumen_Total numeric(20,5)
	,@P_Unidad_Volumen varchar(5)
	,@P_Total_Bultos numeric(10,0)
	,@P_Valor_Declarado numeric(12,2)
	,@P_Orden_De_Compra varchar(20)
	,@P_Cant_Items numeric(10,0)
	,@P_Observaciones varchar(200)
	,@P_Status varchar(3)
	,@P_NroRemito varchar(30)
	,@P_Fecha_Alta_Gtw varchar(20)
	,@P_Fecha_Fin_Gtw varchar(20)
	,@P_Personal_Id varchar(20)
	,@P_Transporte_Id varchar(20)
	,@P_Nro_Despacho_Importacion varchar(30)
	,@P_Alto numeric(20,5)
	,@P_Ancho numeric(20,5)
	,@P_Largo numeric(20,5)
	,@P_Unidad_Medida varchar(5)
	,@P_Grupo_Picking varchar(50)
	,@P_Prioridad_Picking numeric(10,0)

As
Begin
	
	Declare @StrSql nvarchar(4000)
	Declare @V_Volumen_Total numeric(20,5)
	Declare @New_Status varchar(3)

	If (((@P_Alto * @P_Ancho * @P_Largo) / 1000000) is NULL)
		Begin
			Set @V_Volumen_Total = 0	
		End
	Else
		Begin
			Set @V_Volumen_Total = ((@P_Alto * @P_Ancho * @P_Largo) / 1000000)
		End

	If (dbo.ent_documento_api#Ya_Existe_Nro_Comprobante(@P_CLIENTE_ID, @P_TIPO_COMPROBANTE_ID, @P_CPTE_PREFIJO, @P_CPTE_NUMERO, Null, @P_SUCURSAL_ORIGEN)) = 1
		Begin        
            Raiserror ('Validacion de documentos',16,1)
			Return        
		End

	If (dbo.ent_documento_api#Ya_Existe_Orden_de_Compra(@P_CLIENTE_ID, @P_TIPO_COMPROBANTE_ID, @P_TIPO_OPERACION_ID, @P_ORDEN_DE_COMPRA, Null)) = 1 
		Begin        
            Raiserror ('Validacion de documentos',16,1)
			Return        
		End
             
	Set @NEW_STATUS = 'D05'

	Insert into Documento ( Cliente_Id
							, Tipo_Comprobante_Id
							, Tipo_Operacion_Id
							, Det_Tipo_Operacion_Id
							, Cpte_Prefijo
							, Cpte_Numero
							, Fecha_Cpte 
							, Fecha_Pedida_Ent
							, Sucursal_Origen
							, Sucursal_Destino
							, Anulado
							, Motivo_Anulacion 
							, Peso_Total
							, Unidad_Peso
							, Volumen_Total
							, Unidad_Volumen
							, Total_Bultos
							, Valor_Declarado 
							, Orden_De_Compra
							, Cant_Items
							, Observaciones
							, Status
							, Nro_Remito
							, Fecha_Alta_Gtw
							, Fecha_Fin_Gtw 
							, Personal_Id
							, Transporte_Id
							, Nro_Despacho_Importacion
							, Alto
							, Ancho
							, Largo
							, Unidad_Medida
							, Grupo_Picking
							, Prioridad_Picking
						   ) 
	Values ( 
			  Upper(@P_Cliente_Id)
			, Upper(@P_Tipo_Comprobante_Id)
			, Upper(@P_Tipo_Operacion_Id) 
			, Upper(@P_Det_Tipo_Operacion_Id) 
			, Upper(@P_Cpte_Prefijo) 
			, Upper(@P_Cpte_Numero)
			, getdate() -- cast(@P_Fecha_Cpte as datetime)
			, Cast(@P_Fecha_Pedida_Ent as datetime)
			, Upper(@P_Sucursal_Origen) 
			, Upper(@P_Sucursal_Destino) 
			, Upper(@P_Anulado) 
			, Upper(@P_Motivo_Anulacion) 
			, Cast(@P_Peso_Total as varchar(25)) 
			, Upper(@P_Unidad_Peso) + char(39) 
			, Cast(@P_Volumen_Total as varchar(25)) 
			, Upper(@P_Unidad_Volumen) 
			, Cast(@P_Total_Bultos as varchar(10)) 
			, Cast(@P_Valor_Declarado as varchar(14)) 
			, Upper(@P_Orden_De_Compra) 
			, Cast(@P_Cant_Items as varchar(10)) 
			, Upper(@P_Observaciones) 
			, Upper(@New_Status) 
			, Upper(@P_NroRemito) 
			, getdate() --Cast(@P_Fecha_Alta_Gtw as Datetime)
			, Cast(@P_Fecha_Fin_Gtw as Datetime)
			, Upper(@P_Personal_Id) 
			, Upper(@P_Transporte_Id) 
			, Upper(@P_Nro_Despacho_Importacion) 
			, Cast(@P_Alto as varchar(25)) 
			, Cast(@P_Ancho as varchar(25)) 
			, Cast(@P_Largo as varchar(25)) 
			, Upper(@P_Unidad_Medida) 
			, Upper(@P_Grupo_Picking) 
			, Cast(@P_Prioridad_Picking as varchar(10)) 
		   )

	Select @P_Documento_Id = Scope_Identity()

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

ALTER  Procedure [dbo].[Documento_Transaccion_Api#InsertRecord]
						@P_Completado as varchar(1)
						, @P_Observaciones as varchar(200)
						, @P_Transaccion_Id as varchar(15)
						, @P_Estacion_Actual as varchar(15)
						, @P_Status as varchar(3)
						, @P_Est_Mov_Actual as varchar(20)
						, @P_Orden_Id as numeric(20,0)
						, @P_It_Mover as varchar(1)
						, @P_Orden_Estacion as numeric(3,0)
						, @P_Tipo_Operacion_Id as varchar(5)
						, @P_Tr_Pos_Completa as varchar(1)
						, @P_Tr_Activo as varchar(1)
						, @P_Usuario_Id as varchar(20)
                        , @P_Terminal as varchar(20)
						, @P_Fecha_Alta_Gtw as datetime
                        , @P_Tr_Activo_Id as varchar(10)
						, @P_Session_Id as varchar(60)
                        , @P_Fecha_Cambio_Tr as datetime
						, @P_Fecha_Fin_Gtw as datetime
						, @P_Doc_Trans_Id as numeric(20,0) OUTPUT

As
Begin

	Declare @Usuario_Id varchar(30)
	Declare @Terminal varchar(30)

	Select @Usuario_Id = usuario_id, @Terminal = terminal From #temp_usuario_loggin
	
	Insert Into DOCUMENTO_TRANSACCION (
		COMPLETADO,
		OBSERVACIONES,
		TRANSACCION_ID,
		ESTACION_ACTUAL,
		STATUS,
		EST_MOV_ACTUAL,
		IT_MOVER,
		ORDEN_ESTACION,
		TIPO_OPERACION_ID,
		TR_POS_COMPLETA,
		TR_ACTIVO,
		USUARIO_ID,
		TERMINAL,
		FECHA_ALTA_GTW,
		TR_ACTIVO_ID,
		SESSION_ID,
		FECHA_CAMBIO_TR,
		FECHA_FIN_GTW
		)
	Values (
		Upper(LTrim(RTrim(@P_Completado)))
		,Upper(LTrim(RTrim(@P_Observaciones)))
		,Upper(LTrim(RTrim(@P_Transaccion_Id)))
		,Upper(LTrim(RTrim(@P_Estacion_Actual)))
		,Upper(LTrim(RTrim(@P_Status)))
		,Upper(LTrim(RTrim(@P_Est_Mov_Actual)))
		,Upper(LTrim(RTrim(@P_It_Mover)))
		,Upper(LTrim(RTrim(@P_Orden_Estacion)))
		,Upper(LTrim(RTrim(@P_Tipo_Operacion_Id)))
		,Upper(LTrim(RTrim(@P_Tr_Pos_Completa)))
		,Upper(LTrim(RTrim(@P_Tr_Activo)))
		,Upper(LTrim(RTrim(@Usuario_Id)))
		,Upper(LTrim(RTrim(@Terminal)))
		,GetDate()
		,Null
		,Null
		,Null
		,Null
	)
	
	SELECT @P_Doc_Trans_Id = SCOPE_IDENTITY()
 
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

ALTER PROCEDURE [dbo].[EGR_MATCH_COD]
	@PRODUCTO_ID 	AS VARCHAR(30),
	@CODE			AS VARCHAR(50),
	@VALIDO 		AS SMALLINT OUTPUT
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	DECLARE @DUN14 		VARCHAR(50)
	DECLARE @EAN13 		VARCHAR(50)
	DECLARE @USUARIO		VARCHAR(50)
	DECLARE @CLIENTE_ID	VARCHAR(15)
	DECLARE @CONTADOR	FLOAT

	SET @VALIDO='0'

	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	SELECT DISTINCT @CLIENTE_ID= CLIENTE_ID FROM PICKING WHERE PRODUCTO_ID=UPPER(LTRIM(RTRIM(@PRODUCTO_ID))) AND USUARIO=UPPER(LTRIM(RTRIM(@USUARIO))) AND FECHA_INICIO IS NOT NULL AND FECHA_FIN IS NULL AND CANT_CONFIRMADA IS NULL
	
	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID

	IF @CONTADOR=0
	BEGIN
		RAISERROR ('El producto tiene marcado validación al egreso, pero no se definieron códigos EAN13/DUN14. Por favor, verifique el maestro de productos',16,1)
		RETURN
	END

	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID
			AND CODIGO=@CODE


	IF @CONTADOR=0
	BEGIN
		RAISERROR('El codigo ingresado no se corresponde con los cargados en el maestro de productos.',16,1)
	END
	ELSE
	BEGIN
		SET @VALIDO='1'
	END
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

ALTER Procedure [dbo].[EliminacionUsuario]
@Usuario_id	varchar(20) Output
As
Begin
	
	delete from sys_lock_pallet where usuario_id=@Usuario_id
	delete from rl_sys_cliente_usuario where usuario_id=@Usuario_id
	delete from rl_viaje_usuario where usuario_id=@Usuario_id
	delete from sys_usu_permisos where usuario_id=@Usuario_id
	delete from rl_usuario_nave where usuario_id=@Usuario_id
	delete from sys_permisos_hh where usuario_id=@Usuario_id
	delete from sys_perfil_usuario where usuario_id=@Usuario_id
	delete from sys_usu_permisos where usuario_id=@Usuario_id
	delete from trace_documentos where usuario_id=@Usuario_id
	delete from sys_usuario where usuario_id=@Usuario_id
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

ALTER    PROCEDURE [dbo].[ENVIAR_RL_A_HISTORICO]
@P_DOC_TRANS_ID AS NUMERIC(20,0)
AS
BEGIN
	DECLARE @RL_HIST_ID 		AS NUMERIC(20,0)
	DECLARE @DOC_TRANS_ID 		AS NUMERIC(20,0)	
	DECLARE @NRO_LINEA_TR		AS NUMERIC(10,0)
	DECLARE @POS_ANTERIOR		AS NUMERIC(20,0)
	DECLARE @POS_ACTUAL			AS NUMERIC(20,0)
	DECLARE @CANTIDAD			AS NUMERIC(20,5)
	DECLARE @TIPO_MOV_ID		AS VARCHAR(5)
	DECLARE @ULTIMA_EST			AS VARCHAR(5)
	DECLARE @ULTIMA_SEC			AS NUMERIC(3,0)
	DECLARE @NAVE_ANT			AS NUMERIC(20,0)
	DECLARE @NAVE_ACT			AS NUMERIC(20,0)
	DECLARE @DOC_ID				AS NUMERIC(20,0)
	DECLARE @NRO_LINEA			AS NUMERIC(10,0)
	DECLARE @DISPONIBLE			AS VARCHAR(1)
	DECLARE @DOC_TRANS_ID_EGR	AS NUMERIC(20,0)
	DECLARE @NRO_LIN_TRANS_EGR	AS NUMERIC(10,0)
	DECLARE @DOC_TRANS_ID_TR	AS NUMERIC(20,0)
	DECLARE @NRO_LIN_TRAN_ID_TR	AS NUMERIC(10,0)
	DECLARE	@CLIENTE_ID			AS VARCHAR(15)
	DECLARE @CAT_LOG_ID			AS VARCHAR(50)	
	DECLARE @CAT_LOG_ID_FINAL 	AS VARCHAR(50)
	DECLARE	@EST_MERC_ID		AS VARCHAR(15)

	--CURSOR PARA LAS INSERCIONES.	
	DECLARE PCUR2 CURSOR FOR
		SELECT * FROM RL_DET_DOC_TRANS_POSICION
		WHERE DOC_TRANS_ID_TR = @P_DOC_TRANS_ID
	
	OPEN PCUR2

	SELECT * FROM RL_DET_DOC_TRANS_POSICION
	WHERE DOC_TRANS_ID_TR = @P_DOC_TRANS_ID

	FETCH NEXT FROM PCUR2 INTO 	  @RL_HIST_ID	, @DOC_TRANS_ID	, @NRO_LINEA_TR	, @POS_ANTERIOR	, @POS_ACTUAL
								, @CANTIDAD	, @TIPO_MOV_ID	, @ULTIMA_EST	, @ULTIMA_SEC	, @NAVE_ANT
								, @NAVE_ACT	, @DOC_ID	, @NRO_LINEA	, @DISPONIBLE	, @DOC_TRANS_ID_EGR
								, @NRO_LIN_TRANS_EGR	, @DOC_TRANS_ID_TR	, @NRO_LIN_TRAN_ID_TR	, @CLIENTE_ID	
								, @CAT_LOG_ID	, @CAT_LOG_ID_FINAL 	, @EST_MERC_ID

	WHILE @@FETCH_STATUS = 0
		BEGIN

			--SELECT @RL_HIST_ID=ISNULL(MAX(RL_ID), 0)+1 AS VALOR
			--FROM RL_DET_DOC_TR_POS_HIST
			
			INSERT INTO RL_DET_DOC_TR_POS_HIST VALUES(	  @DOC_TRANS_ID	, @NRO_LINEA_TR	, @POS_ANTERIOR	, @POS_ACTUAL
														, @CANTIDAD	, @TIPO_MOV_ID	, @ULTIMA_EST	, @ULTIMA_SEC	, @NAVE_ANT
														, @NAVE_ACT	, @DOC_ID	, @NRO_LINEA	, @DISPONIBLE	, @DOC_TRANS_ID_EGR
														, @NRO_LIN_TRANS_EGR	, @DOC_TRANS_ID_TR	, @NRO_LIN_TRAN_ID_TR	, @CLIENTE_ID	
														, @CAT_LOG_ID	, @CAT_LOG_ID_FINAL 	, @EST_MERC_ID )

	

			FETCH NEXT FROM PCUR2 INTO 	  @RL_HIST_ID	, @DOC_TRANS_ID	, @NRO_LINEA_TR	, @POS_ANTERIOR	, @POS_ACTUAL
										, @CANTIDAD	, @TIPO_MOV_ID	, @ULTIMA_EST	, @ULTIMA_SEC	, @NAVE_ANT
										, @NAVE_ACT	, @DOC_ID	, @NRO_LINEA	, @DISPONIBLE	, @DOC_TRANS_ID_EGR
										, @NRO_LIN_TRANS_EGR	, @DOC_TRANS_ID_TR	, @NRO_LIN_TRAN_ID_TR	, @CLIENTE_ID	
										, @CAT_LOG_ID	, @CAT_LOG_ID_FINAL 	, @EST_MERC_ID

		END

	CLOSE PCUR2
	DEALLOCATE PCUR2
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

ALTER PROCEDURE  [dbo].[Estacion_Picking_ActNroLinea] 
  @NewRl_Id		Numeric(20,0) Output,
  @Picking_Id		Numeric(20,0) Output
AS
Begin
	set xact_abort on
	-----------------------------------------------------------------------------
	--Declaracion de Variables.
	-----------------------------------------------------------------------------
	Declare @OldRl_Id			      as Numeric(20,0)
	Declare @QtyPicking			    as Float
	Declare @QtyRl				      as Float
	Declare @Documento_Id		    as Numeric(20,0)
	Declare @Nro_Linea			    as Numeric(10,0)
	Declare @PreEgrId			      as Numeric(20,0)
	Declare @Doc_Trans_IdEgr	  as Numeric(20,0)
	Declare @Nro_Linea_TransEgr	as Numeric(10,0)
	Declare @Documento_IdNew	  as Numeric(20,0)
	Declare @Nro_LineaNew		    as Numeric(10,0)
	Declare @Dif				        as Float
	Declare @MaxLinea			      as Numeric(10,0)
	Declare @Doc_Trans_Id		    as Numeric(20,0)
	Declare @MaxLineaDDT		    as Numeric(10,0)
	Declare @SplitRl			      as Numeric(20,0)
	Declare @Producto_IdC		    as Varchar(30)
	Declare @Cliente_IdC		    as Varchar(15)
	Declare @Cat_log_Id_Final	  as Varchar(50)
	-----------------------------------------------------------------------------
	Declare @NRO_SERIE			    as varchar(50)
	Declare @NRO_SERIE_PADRE	  as varchar(50)
	Declare @EST_MERC_ID		    as varchar(15)
	Declare @CAT_LOG_ID			    as varchar(15)
	Declare @NRO_BULTO			    as varchar(50)
	Declare @DESCRIPCION		    as varchar(200)
	Declare @NRO_LOTE			      as varchar(50)
	Declare @FECHA_VENCIMIENTO	as datetime
	Declare @NRO_DESPACHO		    as varchar(50)
	Declare @NRO_PARTIDA		    as varchar(50)
	Declare @UNIDAD_ID			    as varchar(5)
	Declare @PESO				        as numeric(20,5)
	Declare @UNIDAD_PESO		    as varchar(5)
	Declare @VOLUMEN			      as numeric(20,5)
	Declare @UNIDAD_VOLUMEN		  as varchar(5)
	Declare @BUSC_INDIVIDUAL	  as varchar(1)
	Declare @TIE_IN				      as varchar(1)
	Declare @NRO_TIE_IN			    as varchar(100)
	Declare @ITEM_OK			      as varchar(1)
	Declare @MONEDA_ID			    as varchar(20)
	Declare @COSTO				      as numeric(20,3)
	Declare @PROP1				      as varchar(100)
	Declare @PROP2				      as varchar(100)
	Declare @PROP3				      as varchar(100)
	Declare @LARGO				      as numeric(10,3)
	Declare @ALTO				        as numeric(10,3)
	Declare @ANCHO				      as numeric(10,3)
	Declare @VOLUMEN_UNITARIO	  as varchar(1)
	Declare @PESO_UNITARIO		  as varchar(1)
	Declare @CANT_SOLICITADA	  as numeric(20,5)	
	-----------------------------------------------------------------------------
	Declare @PALLET_HOMBRE		  as CHAR(1)
	Declare @Transf				      as char(1)

	--Obtengo las Cantidades.
	Select @QtyPicking=Cantidad from picking where picking_id=@Picking_Id
	Select @QtyRl= Cantidad From Rl_Det_Doc_Trans_Posicion Where Rl_Id=@NewRl_Id
	
	--Verifico que al momento de hacer el cambio no este tomada la tarea de picking
	If Dbo.Picking_inProcess(@Picking_Id)=1
	Begin
		Raiserror('La tarea de Picking ya fue asignada. No es posible realizar el cambio.',16,1);
		return
	End
	
	--Estos valores me van a servir mas adelante.
	Select	 @Documento_Id	=Documento_id
			    ,@Nro_Linea 	=Nro_linea
	From	  Picking
	Where	  Picking_Id		=@Picking_Id

	select	@PALLET_HOMBRE=flg_pallet_hombre
	from	  cliente_parametros c inner join documento d
			    on(c.cliente_id=d.cliente_id)
	where	  d.documento_id=@Documento_Id

	--Saco la nave de preegreso.
	Select	@PreEgrId=Nave_Id
	From	  Nave
	Where	  Pre_Egreso='1'

	--Obtengo el Nuevo Documento y numero de linea para Updetear.
	Select 	 Distinct
           @Documento_idNew	=dd.Documento_Id
          ,@Nro_lineaNew		=dd.Nro_Linea
	From	  Rl_Det_Doc_Trans_posicion Rl
			    Inner join Det_Documento_Transaccion ddt
			    On(Rl.Doc_Trans_id=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=ddt.Nro_Linea_Trans)
			    Inner Join Det_Documento dd
			    on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
	Where	  Rl.Rl_id=@NewRl_Id
	
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
			FROM 	DET_DOCUMENTO				
			Where	Documento_Id=@Documento_idNew
					And Nro_linea=@Nro_LineaNew

			--Actualizo Det_Documento
			Update Det_Documento
			Set     Nro_Serie			    =@NRO_SERIE				
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
              ,@PALLET_HOMBRE AS FLG_PALLET_HOMBRE --CAMBIAR
              ,0	  AS TRANSF_TERMINANDA	--CAMBIAR
              ,DD.NRO_LOTE    AS NRO_LOTE
              ,DD.NRO_PARTIDA AS NRO_PARTIDA
              ,DD.NRO_SERIE AS NRO_SERIE
			FROM	  DOCUMENTO D INNER JOIN DET_DOCUMENTO DD   ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
              INNER JOIN PRODUCTO P                     ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
              INNER JOIN DET_DOCUMENTO_TRANSACCION DDT  ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
              INNER JOIN RL_DET_DOC_TRANS_POSICION RL   ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
              LEFT JOIN NAVE N                          ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
              LEFT JOIN POSICION POS                    ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
              LEFT JOIN NAVE N2                         ON(POS.NAVE_ID=N2.NAVE_ID)
			WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
					    And dd.Nro_linea=@Nro_Linea

			Select 	@Cliente_IdC= Cliente_Id,
					    @Producto_idC= Producto_Id
			From	  Det_Documento 
			Where	  Documento_id=@Documento_id
					    And Nro_Linea=@Nro_Linea

			Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

		  Insert into Consumo_Locator_Egr (Documento_Id, Nro_Linea, Cliente_Id, Producto_Id, Cantidad, RL_ID,Saldo, Tipo, Fecha, Procesado)
		  Values(@Documento_Id, @Nro_Linea, @Cliente_IdC, @Producto_idC, @QtyPicking,@NewRl_Id,0,2,GETDATE(),'S')

	
	End--Fin Picking=Rl 1er. caso

	If (@QtyPicking < @QtyRL)
	Begin	
		Set @Dif= @QtyRL - @QtyPicking

		--Obtengo la Rl Anterior.
		Select 	@OldRl_Id=Rl.Rl_Id
		From	Rl_Det_Doc_Trans_posicion Rl
				Inner join Det_Documento_Transaccion ddt
				On(Rl.Doc_Trans_id_egr=ddt.Doc_Trans_Id And Rl.Nro_linea_Trans_egr=ddt.Nro_Linea_Trans)
				Inner Join Det_Documento dd
				on(ddt.Documento_Id=dd.Documento_Id And ddt.Nro_Linea_Doc=dd.Nro_Linea)
		Where	dd.Documento_id=@Documento_id and dd.Nro_linea=@Nro_Linea
			
		Select @Cat_log_Id_Final=cat_log_id_final from det_documento where Documento_id=@Documento_id and Nro_linea=@Nro_Linea

		--Obtengo El Documento de Transaccion y el Numero de Linea para Consumir la Nueva Rl.
		Select	 @Doc_Trans_IdEgr	=Doc_Trans_Id_Egr
				,@Nro_Linea_TransEgr=Nro_linea_Trans_Egr
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@OldRl_id
		
		--Spliteo la Rl.
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
		FROM	  DOCUMENTO D INNER JOIN DET_DOCUMENTO DD     ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
            INNER JOIN PRODUCTO P                       ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
            INNER JOIN DET_DOCUMENTO_TRANSACCION DDT    ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
            INNER JOIN RL_DET_DOC_TRANS_POSICION RL     ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
            LEFT JOIN NAVE N                            ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
            LEFT JOIN POSICION POS                      ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
            LEFT JOIN NAVE N2                           ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				    And dd.Nro_linea=@Nro_Linea

		Select 	@Cliente_IdC= Cliente_Id,
				    @Producto_idC= Producto_Id
		From	  Det_Documento 
		Where	  Documento_id=@Documento_id
				    And Nro_Linea=@Nro_Linea

		Delete from Consumo_Locator_Egr Where Documento_id=@Documento_id and Nro_linea=@Nro_linea

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
		FROM	  DOCUMENTO D INNER JOIN DET_DOCUMENTO DD     ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
            INNER JOIN PRODUCTO P                       ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
            INNER JOIN DET_DOCUMENTO_TRANSACCION DDT    ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
            INNER JOIN RL_DET_DOC_TRANS_POSICION RL     ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
            LEFT JOIN NAVE N                            ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
            LEFT JOIN POSICION POS                      ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
            LEFT JOIN NAVE N2                           ON(POS.NAVE_ID=N2.NAVE_ID)
		WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID
				    And dd.Nro_linea=@MaxLinea		

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

ALTER        PROCEDURE [dbo].[EtiquetaBulto]
AS

BEGIN
	select '63' as iddoc
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

ALTER    Procedure [dbo].[GrabarPos_Transf]
	@pDocTransID 		As numeric(20,0) 	output,
	@pNaveCod_o 		As varchar(15)		output,
	@pCalleCod_o 		As varchar(15)		output,
	@pColumnaCod_o 	As varchar(15)		output,
	@pNivelCod_o 		As varchar(15)		output,
	@pNaveID_d 		As varchar(15)		output,
	@pPosicionID_d 		As Numeric(20,0)	output
As
Begin
	--Declaracion de Cursor.	
	Declare @t_CurPos			as Cursor
	--Variables para el cursor.	
	Declare @nave_origen 		as int				
	Declare @posicion_origen 	as int				
	Declare @vRl_Id				as Numeric(20,0)	
	Declare @Nro_Linea_Tr		as Numeric(10,0)	
	Declare @Cliente_id			as varchar(15)	
	Declare @Usuario			as varchar(20)
	Declare @Trans				as Char(1)

	--Valido la nave destino.
	Select @Trans=Disp_Transf from nave where nave_id=@pNaveID_d
	If @Trans='0'
	Begin
		raiserror('La Nave Destino no esta disponible para realizar Transferencias.',16,1)
		Return
	End

	--Valido la posicion destino.
	Set @Trans=null
	Select @Trans=n.Disp_Transf from posicion p inner join nave n on(p.nave_id=n.nave_id) where posicion_id=@pPosicionID_d
	If @Trans='0'
	Begin
		raiserror('La posicion de la nave destino no esta disponible para realizar Transferencias.',16,1)
		Return
	End
	
	--valido la posicion origen
	Set @Trans=null
	if (@pNaveCod_o is not null) and (@pCalleCod_o is not null) and (@pColumnaCod_o is not null) and (@pNivelCod_o is not null)
	Begin
		select 	@Trans=n.disp_transf
		from	posicion p inner join nivel_nave nn 	on(p.nivel_id=nn.nivel_id)
				inner join columna_nave cn			on(nn.columna_id=cn.columna_id)
				inner join calle_nave	can				on(can.calle_id=cn.calle_id)
				inner join nave			n				on(can.nave_id=n.nave_id)
		where	n.nave_cod=@pNaveCod_o
				and can.calle_cod=@pCalleCod_o
				and cn.columna_cod=@pColumnaCod_o
				and nn.nivel_cod=@pNivelCod_o
		If @Trans is null
		Begin
			raiserror('No se encontro la posicion destino.',16,1)
			Return			
		End
		If @Trans='0'
		Begin
			raiserror('La posicion de la nave origen no esta disponible para realizar Transferencias.',16,1)
			Return	
		End		
	End
	If  (@pNaveCod_o is not null) and (@pCalleCod_o is null) and (@pColumnaCod_o is null) and (@pNivelCod_o is null)
	Begin
		Set @Trans=null
		Select @Trans= Disp_Transf from nave where nave_cod=@pNaveCod_o
		If @Trans is null
		Begin
			raiserror('No se encontro la nave destino.',16,1)
			Return			
		End
		If @Trans='0'
		Begin
			raiserror('La nave origen no esta disponible para realizar Transferencias.',16,1)
			Return	
		End		
	End
	Set @t_CurPos=Cursor for
		SELECT 	 RL.RL_ID
				,RL.CLIENTE_ID
				,NULL 			AS NAVE_ID
				,P.POSICION_ID	AS POSICION_ID
		FROM	RL_DET_DOC_TRANS_POSICION RL
				LEFT JOIN POSICION P 							ON(RL.POSICION_ACTUAL=P.POSICION_ID)
				LEFT JOIN NAVE N2								ON(P.NAVE_ID=N2.NAVE_ID)
				LEFT JOIN NIVEL_NAVE NN							ON(NN.NIVEL_ID=P.NIVEL_ID)
				LEFT JOIN COLUMNA_NAVE CN						ON(CN.COLUMNA_ID=P.COLUMNA_ID)
				LEFT JOIN CALLE_NAVE	CAN						ON(CAN.CALLE_ID=P.CALLE_ID)
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
				INNER JOIN DET_DOCUMENTO	DD					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN CATEGORIA_LOGICA CL				ON(RL.CLIENTE_ID=CL.CLIENTE_ID AND RL.CAT_LOG_ID=CL.CAT_LOG_ID AND CL.DISP_TRANSF='1')
				LEFT JOIN ESTADO_MERCADERIA_RL EM			ON(RL.CLIENTE_ID=EM.CLIENTE_ID AND RL.EST_MERC_ID=EM.EST_MERC_ID)
		WHERE	N2.NAVE_COD=@pNaveCod_o
				AND CAN.CALLE_COD=@pCalleCod_o
				AND CN.COLUMNA_COD=@pColumnaCod_o
				AND NN.NIVEL_COD=@pNivelCod_o
				AND RL.DISPONIBLE='1'
				AND ((EM.DISP_TRANSF IS NULL) OR (EM.DISP_TRANSF='1'))
		UNION
		SELECT 	 RL.RL_ID
				,RL.CLIENTE_ID
				,N.NAVE_ID	AS NAVE_ID
				,NULL AS POSICION_ID
		FROM	RL_DET_DOC_TRANS_POSICION RL
				LEFT JOIN NAVE N									ON(N.NAVE_ID=RL.NAVE_ACTUAL AND N.NAVE_TIENE_LAYOUT='0')
				INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
				INNER JOIN DET_DOCUMENTO	DD					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN CATEGORIA_LOGICA CL				ON(RL.CLIENTE_ID=CL.CLIENTE_ID AND RL.CAT_LOG_ID=CL.CAT_LOG_ID AND CL.DISP_TRANSF='1')
				LEFT JOIN ESTADO_MERCADERIA_RL EM			ON(RL.CLIENTE_ID=EM.CLIENTE_ID AND RL.EST_MERC_ID=EM.EST_MERC_ID AND EM.DISP_TRANSF='1')
		WHERE	N.NAVE_COD=@pNaveCod_o
				AND RL.DISPONIBLE='1'
				AND ((EM.DISP_TRANSF IS NULL) OR (EM.DISP_TRANSF='1'))
	
	Open @t_CurPos
	Fetch Next from @t_CurPos into @vRl_Id, @Cliente_id, @nave_origen, @posicion_origen
	While @@Fetch_Status=0
	Begin

		--Calculo la linea de det_documento_transaccion
		Select @Nro_Linea_Tr=Max(isnull(Nro_linea_trans,0))+1 from det_documento_transaccion where doc_trans_id=@pDocTransID
		if @Nro_Linea_Tr is null
		begin
			set @Nro_Linea_Tr=1
		end
		--Genero la linea en det_documento_transaccion
		Insert into Det_Documento_Transaccion (Doc_Trans_Id, Nro_Linea_Trans,Cliente_ID, Item_Ok, Movimiento_Pendiente)
		values(@pDocTransID,@Nro_Linea_Tr,@cliente_id,'0','0')

		Insert into 	Rl_Det_Doc_Trans_Posicion (
				  Doc_Trans_Id
				, Nro_Linea_Trans
				, Posicion_Anterior
				, Posicion_Actual
				, Cantidad
				, Tipo_movimiento_Id
				, Ultima_Secuencia
				, nave_anterior
				, nave_actual
				, documento_id
				, nro_linea
				, disponible
				, doc_trans_id_tr
				, nro_linea_trans_tr
				, cliente_id
				, cat_log_id
				, est_merc_id)

		Select 	 Doc_trans_id
				,nro_linea_trans
				,posicion_actual
				,@pPosicionID_d
				,cantidad
				,null
				,null
				,Nave_Actual
				,@pNaveID_d
				,Null
				,Null
				,0
				,@pDocTransID			
				,@Nro_Linea_Tr
				,Cliente_Id
				,Cat_Log_Id
				,Est_merc_Id
		From	Rl_Det_Doc_Trans_posicion
		Where	Rl_Id=@vRl_Id

		Delete from rl_det_doc_trans_posicion where rl_id=@vRl_id

		
		Exec auditoria_hist_insert_tr		@doc		= @pDocTransID,
										@nro_linea	= @Nro_Linea_Tr,
										@nave_o	= @nave_origen,
										@nave_d	= @pNaveID_d,
										@posicion_o	= @posicion_origen,
										@posicion_d	= @pPosicionID_d

		Fetch Next from @t_CurPos into @vRl_Id, @Cliente_id, @nave_origen, @posicion_origen

	End
	Close @t_CurPos
	Deallocate @t_CurPos

	Select @Usuario= Usuario_Id from #temp_usuario_loggin
	--Set @Usuario='USER'
	If @posicion_origen is not null
	Begin
		UPDATE posicion SET 	 pos_lockeada='1',LCK_TIPO_OPERACION='TR'	,LCK_USUARIO_ID=@Usuario,LCK_DOC_TRANS_ID=@pDocTransID,LCK_OBS='LOCKEO POR TRANSFERENCIA-ORIGEN'
		WHERE posicion_id=@posicion_origen
	End
	If @pPosicionID_d is not null
	Begin
		UPDATE posicion SET 	 pos_lockeada='1',LCK_TIPO_OPERACION='TR'	,LCK_USUARIO_ID=@Usuario	,LCK_DOC_TRANS_ID=@pDocTransID,LCK_OBS='LOCKEO POR TRANSFERENCIA-DESTINO'
		WHERE posicion_id=@pPosicionID_d
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

ALTER PROCEDURE [dbo].[IMP_ETIQ_VERIF_PALLET] 
@PALLET	AS VARCHAR(100) OUTPUT,
@VERIF AS CHAR(1) OUTPUT,
@DOCUMENT_ID AS VARCHAR(100) OUTPUT


AS
BEGIN
	DECLARE @CANT INT

	select @CANT = COUNT(*) from DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC) 
	INNER JOIN RL_DET_DOC_TRANS_POSICION RL ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_DOC = RL.NRO_LINEA_TRANS)
	WHERE DD.PROP1 = @PALLET
	
	IF @CANT = 0
		BEGIN
			SET @VERIF = '0'
		END
	ELSE
		BEGIN
			SET @VERIF = '1'
			SELECT DOCUMENTO_ID FROM DET_DOCUMENTO WHERE PROP1 = @PALLET GROUP BY DOCUMENTO_ID

		END

	
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

ALTER     PROCEDURE [dbo].[IMPRESION_AUDITORIA_CAT_LOGICA] 
	@P_CLIENTE AS VARCHAR (50) OUTPUT, 
	@P_PRODUCTO_ID As VARCHAR (50) OUTPUT, 
	@P_FechaDesde As VARCHAR (50) OUTPUT, 
	@P_FechaHasta As VARCHAR (50) OUTPUT, 
	@P_USUARIO As VARCHAR (50) OUTPUT, 
	@P_OLD As VARCHAR (50) OUTPUT, 
	@P_NEW As VARCHAR (50) OUTPUT,
	@P_PALLET as Varchar (100) OUTPUT
AS
BEGIN

	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)

	Set @StrWhere = ''
	
	SELECT 	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal 
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT AUDITORIA_ID AS ID' + char(13)
	Set @StrSql = @StrSql + ' ,CAST(DD.PRODUCTO_ID AS VARCHAR) AS PRODUCTO_ID' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(PRO.DESCRIPCION AS VARCHAR) AS PRODUCTO_COD' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CLI.CLIENTE_ID AS VARCHAR) AS CLIENTE_ID' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(CLI.RAZON_SOCIAL AS VARCHAR) AS CLIENTE_COD' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.OLD AS VARCHAR) AS OLDID' + CHAR(13)
	Set @StrSql = @StrSql + ' ,CAST(CL.DESCRIPCION AS VARCHAR) AS OLDDESC '  + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.NEW AS VARCHAR) AS NEWID' + char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CL2.DESCRIPCION AS VARCHAR) AS NEWDESC '+ Char(13)
	Set @StrSql = @StrSql + ' ,SA.QTY_NEW AS QTY_NEW ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.USUARIO_ID ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.TERMINAL ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.FECHA ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(P.POSICION_COD,N.NAVE_COD) AS POS ' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)) AS FECHA_VENCIMIENTO ' + Char(13)
	--Set @StrSql = @StrSql + ' ,' + Char(39) + '(Nro.Pallet:' + char(39) + ' + ISNULL(DD.PROP1, ' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Nro.Lote:' + char(39) + ' + ISNULL(DD.NRO_LOTE, ' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + '+ char(39) + 'Nro.Partida:' + char(39) + ' + ISNULL(DD.NRO_PARTIDA,' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Nro.Bulto:' + char(39) + ' + ISNULL(DD.NRO_BULTO,' + char(39) + '-' + char(39)+ ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Nro.Despacho:' + char(39) + ' + ISNULL(DD.NRO_DESPACHO,' + char(39) + '-' + char(39) + ') + ' + char(39) + ', ' + char(39) + ' + ' + char(39) + 'Fecha Vto.:' + char(39) + ' + CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4))' + ' + ' + char(39) + ' )' + char(39) + ' AS DETALLES ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.PROP1,' + char(39) + ' - ' + char(39) + ') AS PALLET ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_LOTE, ' + char(39) + ' - ' + char(39) + ') AS LOTE ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_PARTIDA,' + char(39) + ' - ' + char(39) + ') AS PARTIDA ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_BULTO, ' + char(39) + ' - ' + char(39) + ') AS BULTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_DESPACHO, ' + char(39) + ' - ' + char(39) + ') AS DESPACHO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)), ' + char(39) + ' - ' + char(39) + ') AS FECHA_VENCIMIENTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,SA.PREFIJO ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + ' FROM 	SYS_AUDITORIA_CAT_MERC SA ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN vDET_DOCUMENTO  DD ' + Char(13)
	Set @StrSql = @StrSql + ' ON(DD.DOCUMENTO_ID=SA.DOCUMENTO_ID AND DD.NRO_LINEA=SA.NRO_LINEA) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CATEGORIA_LOGICA CL ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.OLD=CL.CAT_LOG_ID AND SA.CLIENTE_ID=CL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CATEGORIA_LOGICA CL2 ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NEW=CL2.CAT_LOG_ID AND SA.CLIENTE_ID=CL2.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN POSICION P ' + Char(13)
	Set @StrSql = @StrSql + ' ON(P.POSICION_ID=SA.POSICION_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN NAVE N ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NAVE_ID=N.NAVE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN PRODUCTO PRO ON(PRO.PRODUCTO_ID = DD.PRODUCTO_ID AND PRO.CLIENTE_ID=DD.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CLIENTE CLI ON(CLI.CLIENTE_ID = SA.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' WHERE SA.PREFIJO = ' + CHAR(39) + 'CATEGORIA LOGICA' + CHAR(39) + Char(13)

	If @P_CLIENTE Is not null and  @P_CLIENTE <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.CLIENTE_ID =' + Char(39) + @P_CLIENTE + Char(39) + Char(13)
		End

	If @P_PRODUCTO_ID Is not null and @P_PRODUCTO_ID <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PRODUCTO_ID =' + Char(39) + @P_PRODUCTO_ID + Char(39) + Char(13)
		End

	If @P_USUARIO Is not null and @P_USUARIO <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.USUARIO_ID =' + Char(39) + @P_USUARIO + Char(39) + Char(13)
		End

	If @P_OLD Is not null and @P_OLD <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.OLD =' + Char(39) + @P_OLD + Char(39) + Char(13)
		End

	If @P_NEW Is not null and @P_NEW <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.NEW =' + Char(39) + @P_NEW + Char(39) + Char(13)
		End

	If @P_PALLET Is not null and @P_PALLET <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PROP1 =' + Char(39) + @P_PALLET + Char(39) + Char(13)
		End

	if @P_FechaDesde is not null and @P_FechaHasta is not null and @P_FechaDesde <> '' and @P_FechaHasta <> ''
		Begin

			Set @StrWhere = @StrWhere + 'and cast(sa.fecha as datetime) between cast(' + char(39) + @P_FechaDesde + char(39)+  ' as datetime) and cast(' + char(39) + @P_FechaHasta + char(39) + ' as datetime)'		
		End 

	Set @strsql =  @strsql + isnull(@StrWhere, '')

	EXECUTE SP_EXECUTESQL @StrSql 

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

ALTER     PROCEDURE [dbo].[IMPRESION_AUDITORIA_EST_MERC]
	@P_CLIENTE AS VARCHAR (50) OUTPUT, 
	@P_PRODUCTO_ID As VARCHAR (50) OUTPUT, 
	@P_FechaDesde As VARCHAR (50) OUTPUT, 
	@P_FechaHasta As VARCHAR (50) OUTPUT, 
	@P_USUARIO As VARCHAR (50) OUTPUT, 
	@P_OLD As VARCHAR (50) OUTPUT, 
	@P_NEW As VARCHAR (50) OUTPUT,
	@P_PALLET as Varchar (100) OUTPUT
AS
BEGIN

	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @cWhere		as INT
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)

	Set @cWhere = 0
	Set @StrWhere = ''
	
	SELECT 	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal 
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT AUDITORIA_ID AS ID' + char(13)
	Set @StrSql = @StrSql + ' ,CAST(DD.PRODUCTO_ID AS VARCHAR) AS PRODUCTO_ID' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(PRO.DESCRIPCION AS VARCHAR) AS PRODUCTO_COD' + Char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CLI.CLIENTE_ID AS VARCHAR) AS CLIENTE_ID' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(CLI.RAZON_SOCIAL AS VARCHAR) AS CLIENTE_COD' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.OLD AS VARCHAR) AS OLDID' + CHAR(13)
	Set @StrSql = @StrSql + ' ,CAST(CL.DESCRIPCION AS VARCHAR) AS OLDDESC '  + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(SA.NEW AS VARCHAR) AS NEWID' + char(13) 
	Set @StrSql = @StrSql + ' ,CAST(CL2.DESCRIPCION AS VARCHAR) AS NEWDESC '+ Char(13)
	Set @StrSql = @StrSql + ' ,SA.QTY_NEW AS QTY_NEW ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.USUARIO_ID ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.TERMINAL ' + Char(13)
	Set @StrSql = @StrSql + ' ,SA.FECHA ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(P.POSICION_COD,N.NAVE_COD) AS POS ' + Char(13)
	Set @StrSql = @StrSql + ' ,CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)) AS FECHA_VENCIMIENTO ' + Char(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.PROP1,' + char(39) + ' - ' + char(39) + ') AS PALLET ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_LOTE, ' + char(39) + ' - ' + char(39) + ') AS LOTE ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_PARTIDA,' + char(39) + ' - ' + char(39) + ') AS PARTIDA ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_BULTO, ' + char(39) + ' - ' + char(39) + ') AS BULTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(DD.NRO_DESPACHO, ' + char(39) + ' - ' + char(39) + ') AS DESPACHO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,ISNULL(CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)), ' + char(39) + ' - ' + char(39) + ') AS FECHA_VENCIMIENTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' ,SA.PREFIJO ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' , ' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + ' FROM 	SYS_AUDITORIA_CAT_MERC SA ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN vDET_DOCUMENTO  DD ' + Char(13)
	Set @StrSql = @StrSql + ' ON(DD.DOCUMENTO_ID=SA.DOCUMENTO_ID AND DD.NRO_LINEA=SA.NRO_LINEA) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN ESTADO_MERCADERIA_RL CL ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.OLD=CL.EST_MERC_ID AND SA.CLIENTE_ID=CL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN ESTADO_MERCADERIA_RL CL2 ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NEW=CL2.EST_MERC_ID AND SA.CLIENTE_ID=CL2.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN POSICION P ' + Char(13)
	Set @StrSql = @StrSql + ' ON(P.POSICION_ID=SA.POSICION_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' LEFT JOIN NAVE N ' + Char(13)
	Set @StrSql = @StrSql + ' ON(SA.NAVE_ID=N.NAVE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN PRODUCTO PRO ON(PRO.PRODUCTO_ID = DD.PRODUCTO_ID AND PRO.CLIENTE_ID=DD.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' INNER JOIN CLIENTE CLI ON(CLI.CLIENTE_ID = SA.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + ' WHERE SA.PREFIJO = ' + CHAR(39) + 'ESTADO MERCADERIA' + CHAR(39) + Char(13)
	If @P_CLIENTE Is not null and  @P_CLIENTE <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.CLIENTE_ID =' + Char(39) + @P_CLIENTE + Char(39) + Char(13)
		End

	If @P_PRODUCTO_ID Is not null and @P_PRODUCTO_ID <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PRODUCTO_ID =' + Char(39) + @P_PRODUCTO_ID + Char(39) + Char(13)
		End

	If @P_USUARIO Is not null and @P_USUARIO <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.USUARIO_ID =' + Char(39) + @P_USUARIO + Char(39) + Char(13)
		End

	If @P_OLD Is not null and @P_OLD <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.OLD =' + Char(39) + @P_OLD + Char(39) + Char(13)
		End

	If @P_NEW Is not null and @P_NEW <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and SA.NEW =' + Char(39) + @P_NEW + Char(39) + Char(13)
		End

	If @P_PALLET Is not null and @P_PALLET <> ''
		Begin		
			Set @StrWhere = @StrWhere + 'and DD.PROP1 =' + Char(39) + @P_PALLET + Char(39) + Char(13)
		End

	if @P_FechaDesde is not null and @P_FechaHasta is not null and @P_FechaDesde <> '' and @P_FechaHasta <> ''
		Begin

			Set @StrWhere = @StrWhere + 'and cast(sa.fecha as datetime) between cast(' + char(39) + @P_FechaDesde + char(39)+  ' as datetime) and cast(' + char(39) + @P_FechaHasta + char(39) + ' as datetime)'		
		End 

	Set @strsql =  @strsql + isnull(@StrWhere, '')
	EXECUTE SP_EXECUTESQL @StrSql 

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
SET QUOTED_IDENTIFIER OFF
GO

ALTER       PROCEDURE [dbo].[IMPRESION_HISTORICO_PRODUCTO_RL]
	@CLIENTE AS VARCHAR (50) OUTPUT, 
	@PRODUCTO As VARCHAR (50) OUTPUT, 
	@FECHA_AUD_DESDE As VARCHAR (50) OUTPUT,
	@FECHA_AUD_HASTA As VARCHAR (50) OUTPUT
AS
BEGIN

	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)
	DECLARE @CWhere		AS NUMERIC(1,0)

	Set @StrWhere = ''
	Set @CWhere = 0
	
	SELECT 	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal 
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT	DD.CLIENTE_ID AS CLIENTE_ID' + CHAR(13)
	Set @StrSql = @StrSql + '	,C.RAZON_SOCIAL AS CLIENTE_COD' + CHAR(13)
	Set @StrSql = @StrSql + '	,DD.PRODUCTO_ID AS PRODUCTO_ID' + Char(13)
	Set @StrSql = @StrSql + '	,P.DESCRIPCION AS PRODUCTO_COD' + CHAR(13)  
	Set @StrSql = @StrSql + '	,CAST(DAY(HPRL.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(HPRL.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(HPRL.FECHA) AS VARCHAR(4)) AS FECHA_AUD ' + Char(13)
	Set @StrSql = @StrSql + '	,HPRL.CANTIDAD' + CHAR(13)
	Set @StrSql = @StrSql + '	,ISNULL(POS.POSICION_COD,N.NAVE_COD) AS UBICACION' + CHAR(13)
	Set @StrSql = @StrSql + '	,CAST(DAY(D.FECHA_ALTA_GTW) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(D.FECHA_ALTA_GTW) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(D.FECHA_ALTA_GTW) AS VARCHAR(4)) AS FECHA_ING ' + Char(13)
	Set @StrSql = @StrSql + '	,CAST(HPRL.CAT_LOG_ID AS VARCHAR) AS CAT_LOG_ID ' + char(13) 
	Set @StrSql = @StrSql + '	,CAST(CL.DESCRIPCION AS VARCHAR) AS CAT_LOG_ID_FINAL'  + Char(13)
	Set @StrSql = @StrSql + '	,CAST(HPRL.EST_MERC_ID AS VARCHAR) AS EST_MERC_ID' + char(13)
	Set @StrSql = @StrSql + '	,CAST(EMRL.DESCRIPCION AS VARCHAR) AS EST_MERC_COD '+ Char(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_SERIE, ' + char(39) + ' - ' + char(39) + ') AS SERIE ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_BULTO, ' + char(39) + ' - ' + char(39) + ') AS BULTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_LOTE, ' + char(39) + ' - ' + char(39) + ') AS LOTE ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(CAST(DAY(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(DD.FECHA_VENCIMIENTO) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(DD.FECHA_VENCIMIENTO) AS VARCHAR(4)), ' + char(39) + ' - ' + char(39) + ') AS FECHA_VENCIMIENTO ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_DESPACHO, ' + char(39) + ' - ' + char(39) + ') AS DESPACHO ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.NRO_PARTIDA,' + char(39) + ' - ' + char(39) + ') AS PARTIDA ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(DD.PROP1,' + char(39) + ' - ' + char(39) + ') AS PALLET ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + 'FROM	HISTORICO_PRODUCTO_RL HPRL' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON (HPRL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND HPRL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN DET_DOCUMENTO DD ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN DOCUMENTO D ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN PRODUCTO P ON (P.PRODUCTO_ID=DD.PRODUCTO_ID AND P.CLIENTE_ID =DD.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN CLIENTE C ON (C.CLIENTE_ID = DD.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN CATEGORIA_LOGICA CL ON(HPRL.CAT_LOG_ID = CL.CAT_LOG_ID AND DD.CLIENTE_ID=CL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + '	LEFT JOIN ESTADO_MERCADERIA_RL EMRL ON(HPRL.EST_MERC_ID = EMRL.EST_MERC_ID AND DD.CLIENTE_ID=EMRL.CLIENTE_ID) ' + Char(13)
	Set @StrSql = @StrSql + '	LEFT JOIN POSICION POS ON(POS.POSICION_ID = HPRL.POSICION_ACTUAL)' + CHAR(13) 	Set @StrSql = @StrSql + '	LEFT JOIN NAVE N ON(HPRL.NAVE_ACTUAL=N.NAVE_ID) ' + CHAR(13)

	If @CLIENTE Is not null and  @CLIENTE <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where DD.CLIENTE_ID =' + Char(39) + @CLIENTE + Char(39) + Char(13)
				Set @CWhere = 1
			End

	If @PRODUCTO Is not null and @PRODUCTO <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where DD.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and DD.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
			End

	if @FECHA_AUD_DESDE is not null and @FECHA_AUD_HASTA is not null and @FECHA_AUD_DESDE <> '' and @FECHA_AUD_HASTA <> ''
		If @CWhere = 0
			Begin
				Set @StrWhere = @StrWhere + 'Where cast(HPRL.FECHA as datetime) between cast(' + char(39) + @FECHA_AUD_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_AUD_HASTA + char(39) + ' as datetime)'		
				Set @CWhere = 1
			End 
		Else
			Begin	
				Set @StrWhere = @StrWhere + 'and cast(HPRL.FECHA as datetime) between cast(' + char(39) + @FECHA_AUD_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_AUD_HASTA + char(39) + ' as datetime)'		
			End 

	Set @StrSql = @StrSql + @StrWhere 

	EXECUTE SP_EXECUTESQL @StrSql 

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

ALTER   PROCEDURE [dbo].[IMPRESION_HISTORICO_SALDO_PRODUCTO]
	@CLIENTE AS VARCHAR (50) OUTPUT, 
	@PRODUCTO As VARCHAR (50) OUTPUT, 
	@FECHA_DESDE As VARCHAR (50) OUTPUT,
	@FECHA_HASTA As VARCHAR (50) OUTPUT,
	@CAT_LOG_ID As VARCHAR (50) OUTPUT, 
	@EST_MERC_ID As VARCHAR (50) OUTPUT
AS
BEGIN
	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrWhere 	AS NVARCHAR(4000) 
	DECLARE @USUARIO	AS VARCHAR(15)
	DECLARE @TERMINAL	AS VARCHAR(50)
	DECLARE @CWhere		AS NUMERIC(1,0)

	Set @StrWhere = ''
	Set @CWhere = 0
	
	SELECT	@USUARIO = Su.nombre, @TERMINAL= tul.Terminal
	FROM	#TEMP_USUARIO_LOGGIN TUL 
		INNER JOIN SYS_USUARIO SU 
		ON (TUL.USUARIO_ID = SU.USUARIO_ID)

	Set @StrSql = 'SELECT	HSP.CLIENTE_ID' + CHAR(13)
	Set @StrSql = @StrSql + '	,CLI.RAZON_SOCIAL' + CHAR(13)
	Set @StrSql = @StrSql + '	,HSP.PRODUCTO_ID' + CHAR(13)
	Set @StrSql = @StrSql + '	,P.DESCRIPCION AS PDESC' + CHAR(13)
	Set @StrSql = @StrSql + '	,HSP.CANTIDAD' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(HSP.CAT_LOG_ID, ' + char(39) + ' - ' + char(39) + ') AS CAT_LOG_ID ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(CL.DESCRIPCION, ' + char(39) + ' - ' + char(39) + ') AS CDESC ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(HSP.EST_MERC_ID, ' + char(39) + ' - ' + char(39) + ') AS EST_MERC_ID ' + CHAR(13)
	Set @StrSql = @StrSql + ' 	,ISNULL(EM.DESCRIPCION, ' + char(39) + ' - ' + char(39) + ') AS EDESC ' + CHAR(13)
	Set @StrSql = @StrSql + '	,CAST(DAY(HSP.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(MONTH(HSP.FECHA) AS VARCHAR(2)) +' + char(39) + '/' + char(39) + '+ CAST(YEAR(HSP.FECHA) AS VARCHAR(4)) AS FECHA ' + Char(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @USUARIO + char(39) + ' AS USOINTERNOUsuario ' + Char(13)
	Set @StrSql = @StrSql + ' 	,' + char(39) + @TERMINAL + char(39) + ' AS USOINTERNOTerminal ' + Char(13)
	Set @StrSql = @StrSql + 'FROM	HISTORICO_SALDO_PRODUCTO HSP' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN CLIENTE CLI' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.CLIENTE_ID = CLI.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	INNER JOIN PRODUCTO P' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.PRODUCTO_ID = P.PRODUCTO_ID AND HSP.CLIENTE_ID = P.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	LEFT JOIN CATEGORIA_LOGICA CL' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.CAT_LOG_ID = CL.CAT_LOG_ID AND HSP.CLIENTE_ID = CL.CLIENTE_ID)' + CHAR(13)
	Set @StrSql = @StrSql + '	LEFT JOIN ESTADO_MERCADERIA_RL EM' + CHAR(13)
	Set @StrSql = @StrSql + '		ON (HSP.EST_MERC_ID = EM.EST_MERC_ID AND HSP.CLIENTE_ID = EM.CLIENTE_ID)' + CHAR(13)


	If @CLIENTE Is not null and  @CLIENTE <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.CLIENTE_ID =' + Char(39) + @CLIENTE + Char(39) + Char(13)
				Set @CWhere = 1
			End

	If @PRODUCTO Is not null and @PRODUCTO <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and HSP.PRODUCTO_ID =' + Char(39) + @PRODUCTO + Char(39) + Char(13)
			End

	If @FECHA_DESDE is not null and @FECHA_HASTA is not null and @FECHA_DESDE <> '' and @FECHA_HASTA <> ''
		If @CWhere = 0
			Begin
				Set @StrWhere = @StrWhere + 'Where CAST(HSP.FECHA as datetime) between cast(' + char(39) + @FECHA_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_HASTA + char(39) + ' as datetime)'		
				Set @CWhere = 1
			End 
		Else
			Begin	
				Set @StrWhere = @StrWhere + 'and CAST(HSP.FECHA as datetime) between cast(' + char(39) + @FECHA_DESDE + char(39)+  ' as datetime) and cast(' + char(39) + @FECHA_HASTA + char(39) + ' as datetime)'		
			End 

	If @CAT_LOG_ID Is not null and @CAT_LOG_ID <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.CAT_LOG_ID =' + Char(39) + @CAT_LOG_ID + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and HSP.CAT_LOG_ID =' + Char(39) + @CAT_LOG_ID + Char(39) + Char(13)
			End


	If @EST_MERC_ID Is not null and @EST_MERC_ID <> ''
		If @CWhere = 0
			Begin		
				Set @StrWhere = @StrWhere + 'Where HSP.EST_MERC_ID =' + Char(39) + @EST_MERC_ID + Char(39) + Char(13)
				Set @CWhere = 1
			End
		Else
			Begin		
				Set @StrWhere = @StrWhere + 'and HSP.EST_MERC_ID =' + Char(39) + @EST_MERC_ID + Char(39) + Char(13)
			EnD

	Set @StrSql = @StrSql + @StrWhere 

	EXECUTE SP_EXECUTESQL @StrSql 

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

ALTER PROCEDURE [dbo].[ING_MATCH_CODE]
@DOCUMENTO_ID 	NUMERIC(20,0),
@NRO_LINEA		NUMERIC(10,0),
@CODE				VARCHAR(50),
@CONTROL			CHAR(1) OUT
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	DECLARE @CONTADOR FLOAT

	SET @CONTROL='0'


	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS PC (NOLOCK) INNER JOIN DET_DOCUMENTO DD (NOLOCK)
			ON(PC.CLIENTE_ID=DD.CLIENTE_ID AND PC.PRODUCTO_ID=DD.PRODUCTO_ID)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA

	IF @CONTADOR=0
	BEGIN
		RAISERROR ('El producto tiene marcado validación al ingreso, pero no se definieron códigos EAN13/DUN14. Por favor, verifique el maestro de productos',16,1)
		RETURN
	END

	SELECT 	@CONTADOR=COUNT(*)
	FROM	RL_PRODUCTO_CODIGOS PC (NOLOCK) INNER JOIN DET_DOCUMENTO DD (NOLOCK)
			ON(PC.CLIENTE_ID=DD.CLIENTE_ID AND PC.PRODUCTO_ID=DD.PRODUCTO_ID)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA
			AND PC.CODIGO=@CODE		

	IF @CONTADOR>0 
	BEGIN
		SET @CONTROL='1'
	END
	ELSE
	BEGIN
		RAISERROR('El codigo ingresado no se corresponde con los cargados en el Maestro de productos.',16,1)
		return
	END
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

ALTER PROCEDURE [dbo].[INGRESA_OC]  
@CLIENTE_ID    VARCHAR(15),  
@OC          VARCHAR(100),  
@Remito       varchar(30),  
@DOCUMENTO_ID   NUMERIC(20,0) OUTPUT,  
@USUARIO_IMP   VARCHAR(20)  
  
AS  
BEGIN  
 SET XACT_ABORT ON  
 SET NOCOUNT ON  
  
 DECLARE @DOC_ID				NUMERIC(20,0)  
 DECLARE @DOC_TRANS_ID			NUMERIC(20,0)  
 DECLARE @DOC_EXT				VARCHAR(100)  
 DECLARE @SUCURSAL_ORIGEN		VARCHAR(20)  
 DECLARE @CAT_LOG_ID			VARCHAR(50)  
 DECLARE @DESCRIPCION			VARCHAR(30)  
 DECLARE @UNIDAD_ID				VARCHAR(15)  
 DECLARE @NRO_PARTIDA			VARCHAR(100)  
 DECLARE @LOTE_AT				VARCHAR(50)  
 DECLARE @Preing				VARCHAR(45)  
 DECLARE @CatLogId				Varchar(50)  
 DECLARE @LineBO				Float  
 DECLARE @qtyBO					Float  
 DECLARE @ToleranciaMax			Float  
 DECLARE @QtyIngresada			Float  
 DECLARE @tmax					Float  
 DECLARE @MAXP					VARCHAR(50)  
 DECLARE @NROLINEA				INTEGER  
 DECLARE @cantidad				numeric(20,5)  
 DECLARE @fecha					datetime   
 DECLARE @PRODUCTO_ID			VARCHAR(30)  
 DECLARE @PALLET_AUTOMATICO		VARCHAR(1)  
 DECLARE @LOTE					VARCHAR(1)  
 DECLARE @NRO_PALLET			VARCHAR(100)  
 -- Catalina Castillo.25/01/2012.Se agrega variable para saber si tiene registros de contenedoras, el producto   
 DECLARE @NRO_REG_CONTENEDORAS	INTEGER  
 DECLARE @NROBULTO				INTEGER  
 DECLARE @NRO_LINEA_CONT		INTEGER  
 DECLARE @CPTE_PREFIJO			VARCHAR(10)  
 DECLARE @CPTE_NUMERO			VARCHAR(20)  
 -- LRojas TrackerID 3851 29/03/2012: Control, si el producto genera Back Order se crea un nuevo ingreso, de lo contrario no  
 DECLARE @GENERA_BO				VARCHAR(1)  
 DECLARE @NRO_LOTE				VARCHAR(100)  
 DECLARE @INGLOTEPROVEEDOR		VARCHAR(1)  
 DECLARE @VLOTE_DOC				VARCHAR(100)
 DECLARE @VPARTIDA_DOC			VARCHAR(100)
 -----------------------------------------------------------------------------------------------------------------  
 --obtengo los valores de las secuencias.  
 -----------------------------------------------------------------------------------------------------------------   
 --obtengo la secuencia para el numero de partida.  
 -- exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output  
 SET @NROBULTO = 0  
 SET @NRO_LINEA_CONT = 0  
  SELECT  TOP 1  
    @DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID, @cpte_prefijo=sd.CPTE_PREFIJO , @cpte_numero=sd.CPTE_NUMERO  
  FROM  SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
  WHERE  ORDEN_DE_COMPRA=@OC  
    AND SD.CLIENTE_ID=@CLIENTE_ID  
    and SDD.fecha_estado_gt is null  
    and SDD.estado_gt is null  
       
 -----------------------------------------------------------------------------------------------------------------  
 --Comienzo con la carga de las tablas.  
 -----------------------------------------------------------------------------------------------------------------  
 Begin transaction   
 --Creo Documento  
 Insert into Documento ( Cliente_id , Tipo_comprobante_id , tipo_operacion_id , det_tipo_operacion_id , sucursal_origen  , fecha_cpte , fecha_pedida_ent , Status , anulado , nro_remito ,orden_de_compra, nro_despacho_importacion ,GRUPO_PICKING  , fecha_alta_gtw, CPTE_PREFIJO , CPTE_NUMERO)  
     Values( @Cliente_Id , 'DO'     , 'ING'    , 'MAN'     ,@SUCURSAL_ORIGEN  , GETDATE()  , GETDATE()   ,'D05'  ,'0'  , @Remito  ,@oc   ,@DOC_EXT     ,null   , getdate(),@cpte_prefijo, @cpte_numero)    
 --Obtengo el Documento Id recien creado.   
 Set @Doc_ID= Scope_identity()  
   
 declare Ingreso_Cursor CURSOR FOR  
 select doc_ext,producto_id, cantidad, fecha, CASE WHEN nro_partida = '' THEN NULL ELSE nro_partida END, CASE WHEN nro_lote = '' THEN NULL ELSE nro_lote END from ingreso_oc WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PROCESADO = 0)order
 by CANT_CONTENEDORAS   
  
 set @Nrolinea=0  
 open Ingreso_Cursor  
 fetch next from Ingreso_Cursor INTO @doc_ext,@producto_id, @cantidad, @fecha, @nro_partida, @nro_lote  
   
 WHILE @@FETCH_STATUS = 0  
 BEGIN   
  
  IF @NRO_LOTE = ''  
   SET @NRO_LOTE = NULL  
    
  IF @NRO_PARTIDA = ''  
   SET @NRO_PARTIDA = NULL  
  
  --exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output  
  SET @PALLET_AUTOMATICO=NULL  
  set @lote=null  
  set @Nrolinea= @Nrolinea + 1  
    
    select @SUCURSAL_ORIGEN=agente_id from sys_int_documento where doc_ext = @DOC_EXT and cliente_id = @CLIENTE_ID  
  /*SELECT  TOP 1  
    @DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID  
  FROM  SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
  WHERE  ORDEN_DE_COMPRA=@OC  
    AND PRODUCTO_ID=@PRODUCTO_ID  
    AND SD.CLIENTE_ID=@CLIENTE_ID  
        AND ISNULL(SDD.NRO_LOTE,'') = @nro_lote  
        AND ISNULL(SDD.NRO_PARTIDA,'')=@nro_partida  
    and SDD.fecha_estado_gt is null  
    and SDD.estado_gt is null  
          
    PRINT 'DOC_EXT EN BSUQUEDA = ' + ISNULL(@DOC_EXT,'') + ', PRODUCTO_ID = ' + @PRODUCTO_ID  
          
    IF ISNULL(@DOC_EXT,'')=''  
    BEGIN  
    SELECT  TOP 1  
      @DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID  
    FROM  SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
    WHERE  ORDEN_DE_COMPRA=@OC  
      AND PRODUCTO_ID=@PRODUCTO_ID  
      AND SD.CLIENTE_ID=@CLIENTE_ID  
      and SDD.fecha_estado_gt is null  
      and SDD.estado_gt is null  
    END*/  
      
    PRINT 'DOC_EXT EN BSUQUEDA = ' + ISNULL(@DOC_EXT,'') + ', PRODUCTO_ID = ' + @PRODUCTO_ID  
      
  if @doc_ext is null  
  begin  
   raiserror('El producto %s no se encuentra en la orden de compra %s',16,1,@producto_id, @oc)  
   return  
  end  
  SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0) from producto where cliente_id=@cliente_id and producto_id=@producto_id  
  
  -----------------------------------------------------------------------------------------------------------------  
  --tengo que controlar el maximo en cuanto a tolerancias.  
  -----------------------------------------------------------------------------------------------------------------  
  --Cambio esta linea x la de abajo ya que el control lo tengo que hacer por OC y producto_id y no por @doc_ext  
  Select  @qtyBO=sum(cantidad_solicitada)  
  from sys_int_det_documento  
  where doc_ext=@doc_ext  
    and fecha_estado_gt is null  
    and estado_gt is null  
    
  
  set @tmax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)  
    
  if @cantidad > @tmax  
  begin  
   Set @maxp=ROUND(@tmax,0)  
   raiserror('1- La cantidad recepcionada supera a la tolerancia maxima permitida.  Maximo permitido: %s ',16,1, @maxp)  
   return  
  end  
  -----------------------------------------------------------------------------------------------------------------  
  --Obtengo las categorias logicas antes de la transaccion para acortar el lockeo.  
  -----------------------------------------------------------------------------------------------------------------  
  SELECT  @CAT_LOG_ID=PC.CAT_LOG_ID  
  FROM  RL_PRODUCTO_CATLOG PC   
  WHERE  PC.CLIENTE_ID=@CLIENTE_ID  
    AND PC.PRODUCTO_ID=@PRODUCTO_ID  
    AND PC.TIPO_COMPROBANTE_ID='DO'  
  
  If @CAT_LOG_ID Is null begin  
   --entra porque no tiene categorias particulares y busca la default.  
   select  @CAT_LOG_ID=p.ing_cat_log_id,  
     @PALLET_AUTOMATICO=PALLET_AUTOMATICO,  
     @lote=lote_automatico,  
          @INGLOTEPROVEEDOR=isnull(ingloteproveedor,'0')  
   From  producto p   
   where   p.cliente_id=@CLIENTE_ID  
     and p.producto_id=@PRODUCTO_ID  
  end   
  IF @PALLET_AUTOMATICO = '1'  
   BEGIN  
    --obtengo la secuencia para el numero de partida.  
      exec get_value_for_sequence  'NROPALLET_SEQ', @nro_pallet Output  
   END  
     
  if @lote='1' AND @INGLOTEPROVEEDOR='0'  
   begin    
    --obtengo la secuencia para el numero de Lote.  
    exec get_value_for_sequence 'NROLOTE_SEQ', @NRO_LOTE Output     
   end  
  select @descripcion=descripcion, @unidad_id=unidad_id from producto where cliente_id=@cliente_id and producto_id=@producto_id  
  
  -- Esto se usa para los clientes que no usan pallet caso contrario comentarlo  
  --set @nro_pallet = '99999'   
    
  --Catalina Castillo.25/01/2012.Se verifica que existan registros en la tabal configuracion_contenedoras  
   SELECT @NRO_REG_CONTENEDORAS=COUNT(*) from CONFIGURACION_CONTENEDORAS   
   WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
	AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
	AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

   SET @NRO_LINEA_CONT = @NroLinea  
   IF @NRO_REG_CONTENEDORAS>0  
    BEGIN  
     DECLARE Contenedoras_Cursor CURSOR FOR  
     SELECT Nro_Contenedora, Cantidad FROM CONFIGURACION_CONTENEDORAS   
      WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id)
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

       
       
     OPEN Contenedoras_Cursor  
     FETCH NEXT FROM Contenedoras_Cursor INTO @NROBULTO, @cantidad  
       
     WHILE @@FETCH_STATUS = 0  
     BEGIN   
  
     -- INSERTANDO EL DETALLE  
      INSERT INTO det_documento (documento_id, nro_linea , cliente_id , producto_id , cantidad , cat_log_id , cat_log_id_final , tie_in , fecha_vencimiento , nro_partida , unidad_id  , descripcion , busc_individual , item_ok , cant_solicitada , prop1 , prop2   , nro_bulto ,nro_lote)  
           VALUES(@doc_id, @Nrolinea , @cliente_id , @producto_id , @cantidad , null   , @cat_log_id  , '0'  , null   , @NRO_PARTIDA , @unidad_id , @descripcion , '1'    , '1'  ,@cantidad   , @nro_pallet ,@DOC_EXT , @NROBULTO  , @NRO_LOTE)  
  
     SET @Nrolinea=@Nrolinea+1  
     FETCH NEXT FROM Contenedoras_Cursor INTO @NROBULTO, @cantidad  
     END   
     --COMMIT TRANSACTION  
     CLOSE Contenedoras_Cursor  
     DEALLOCATE Contenedoras_Cursor  
      SET @NroLinea = @NRO_LINEA_CONT   
    END  
  ELSE  
   BEGIN  
  
  -- INSERTANDO EL DETALLE  
  insert into det_documento (documento_id, nro_linea , cliente_id , producto_id , cantidad , cat_log_id , cat_log_id_final , tie_in , fecha_vencimiento , nro_partida , unidad_id  , descripcion , busc_individual , item_ok , cant_solicitada , prop1 , prop2 
  , nro_bulto ,nro_lote)  
        values(@doc_id, @Nrolinea , @cliente_id , @producto_id , @cantidad , null   , @cat_log_id  , '0'  , null   , @nro_partida , @unidad_id , @descripcion , '1'    , '1'  ,@qtyBO   , @nro_pallet ,@DOC_EXT , null  , @NRO_LOTE)  
   END  
  --Documento a Ingreso.  
  select  @Preing=nave_id  
  from nave  
  where pre_ingreso='1'  
    
  SELECT  @catlogid=cat_log_id  
  FROM  categoria_stock cs  
    INNER JOIN categoria_logica cl  
    ON cl.categ_stock_id = cs.categ_stock_id  
  WHERE  cs.categ_stock_id = 'TRAN_ING'  
    And cliente_id =@cliente_id  
  
  UPDATE det_documento  
  Set cat_log_id =@catlogid  
  WHERE documento_id = @Doc_ID  
  
  Update documento set status='D20' where documento_id=@doc_id  
  
  
  --Catalina Castillo.25/01/2012.Se verifica que existan registros en la tabal configuracion_contenedoras  
   SELECT @NRO_REG_CONTENEDORAS= COUNT(*) from CONFIGURACION_CONTENEDORAS   
   WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
	AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
	AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

  
   IF @NRO_REG_CONTENEDORAS>0  
    BEGIN  
     DECLARE Contenedoras_RL_Cursor CURSOR FOR  
     SELECT Cantidad FROM CONFIGURACION_CONTENEDORAS   
      WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

       
     OPEN Contenedoras_RL_Cursor  
     FETCH NEXT FROM Contenedoras_RL_Cursor INTO @cantidad  
       
     WHILE @@FETCH_STATUS = 0  
     BEGIN   
  
      Insert Into RL_DET_DOC_TRANS_POSICION (  
      DOC_TRANS_ID,    NRO_LINEA_TRANS,  
      POSICION_ANTERIOR,   POSICION_ACTUAL,  
      CANTIDAD,     TIPO_MOVIMIENTO_ID,  
      ULTIMA_ESTACION,   ULTIMA_SECUENCIA,  
      NAVE_ANTERIOR,    NAVE_ACTUAL,  
      DOCUMENTO_ID,    NRO_LINEA,  
      DISPONIBLE,     DOC_TRANS_ID_EGR,  
      NRO_LINEA_TRANS_EGR,  DOC_TRANS_ID_TR,  
      NRO_LINEA_TRANS_TR,   CLIENTE_ID,  
      CAT_LOG_ID,     CAT_LOG_ID_FINAL,  
      EST_MERC_ID)  
      Values (NULL, NULL, NULL, NULL, @cantidad, NULL, NULL, NULL, NULL, @PREING, @doc_id, @Nrolinea, null, null, null, null, null, @cliente_id, @catlogid,@CAT_LOG_ID,null)  
       
     SET @Nrolinea=@Nrolinea+1  
     FETCH NEXT FROM Contenedoras_RL_Cursor INTO @cantidad  
     END   
     --COMMIT TRANSACTION  
     CLOSE Contenedoras_RL_Cursor  
     DEALLOCATE Contenedoras_RL_Cursor  
    --Sumo el total de la cantidad para setear y que no genere un backorder  
     SELECT @cantidad = SUM(CANTIDAD) FROM CONFIGURACION_CONTENEDORAS  
      WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

    --Elimino los registros que cumplan los filtros de la tabla CONFIGURACION_CONTENEDORAS  
     DELETE FROM CONFIGURACION_CONTENEDORAS WHERE (CLIENTE_ID = @CLIENTE_ID) AND (ORDEN_COMPRA = @oc) AND (PRODUCTO_ID = @producto_id) 
		AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)
		AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)

     SET @Nrolinea=@Nrolinea-1  
    END  
  ELSE  
   BEGIN    
  Insert Into RL_DET_DOC_TRANS_POSICION (  
     DOC_TRANS_ID,    NRO_LINEA_TRANS,  
     POSICION_ANTERIOR,   POSICION_ACTUAL,  
     CANTIDAD,     TIPO_MOVIMIENTO_ID,  
     ULTIMA_ESTACION,   ULTIMA_SECUENCIA,  
     NAVE_ANTERIOR,    NAVE_ACTUAL,  
     DOCUMENTO_ID,    NRO_LINEA,  
     DISPONIBLE,     DOC_TRANS_ID_EGR,  
     NRO_LINEA_TRANS_EGR,  DOC_TRANS_ID_TR,  
     NRO_LINEA_TRANS_TR,   CLIENTE_ID,  
     CAT_LOG_ID,     CAT_LOG_ID_FINAL,  
     EST_MERC_ID)  
  Values (NULL, NULL, NULL, NULL, @cantidad, NULL, NULL, NULL, NULL, @PREING, @doc_id, @Nrolinea, null, null, null, null, null, @cliente_id, @catlogid,@CAT_LOG_ID,null)  
  END  
  ------------------------------------------------------------------------------------------------------------------------------------  
  --Generacion del Back Order.  
  -----------------------------------------------------------------------------------------------------------------  
  select @lineBO=max(isnull(nro_linea,1))+1 from sys_int_det_documento WHERE   DOC_EXT=@doc_ext  
      
    PRINT 'DOC_EXT= ' + @DOC_EXT + ', NRO_LINEA = ' + CAST(@LINEBO AS VARCHAR)  
      
  Select  @qtyBO=sum(cantidad_solicitada)  
  from sys_int_det_documento  
  where doc_ext=@doc_ext  
    and fecha_estado_gt is null  
    and estado_gt is null  
  
  PRINT 'DOC_EXT= ' + @DOC_EXT + ', QTY_BO = ' + CAST(@qtyBO AS VARCHAR)  

  SELECT	@VLOTE_DOC=NRO_LOTE, @VPARTIDA_DOC=NRO_PARTIDA
  FROM		SYS_INT_DET_DOCUMENTO
  WHERE		DOC_EXT=@doc_ext
           
  UPDATE	SYS_INT_DET_DOCUMENTO 
  SET		ESTADO_GT='P', 
			DOC_BACK_ORDER=@doc_ext,
			FECHA_ESTADO_GT=getdate(), 
			DOCUMENTO_ID=@Doc_ID 
			--NRO_PARTIDA	=CASE(ISNULL(NRO_PARTIDA,'#'))  WHEN '#' THEN NULL ELSE @NRO_PARTIDA END,
			--NRO_LOTE	=CASE(ISNULL(NRO_LOTE,'#'))		WHEN '#' THEN NULL ELSE @NRO_LOTE END			
  WHERE		DOC_EXT=@doc_ext and documento_id is null  
  
  set @qtyBO=@qtyBO - @cantidad  
          
  SELECT @GENERA_BO =   
     CASE P.BACK_ORDER   
   WHEN '1' THEN 'S'   
   WHEN '0' THEN 'N'  
     END  
  FROM PRODUCTO P INNER JOIN SYS_INT_DET_DOCUMENTO SIDD ON (P.PRODUCTO_ID = SIDD.PRODUCTO_ID)  
  WHERE SIDD.DOC_EXT = @doc_ext AND SIDD.DOCUMENTO_ID = @Doc_ID AND P.CLIENTE_ID=@CLIENTE_ID  
       
  -- LRojas TrackerID 3851 29/03/2012: Se debe tener en cuenta la parametrización del producto.  
  IF (@qtyBO > 0) AND (@GENERA_BO = 'S') --Si esta variable es mayor a 0, genero el backorder.  
  begin  
  insert into sys_int_det_documento   
   select TOP 1   
     DOC_EXT, @lineBO ,CLIENTE_ID, PRODUCTO_ID, @qtyBO ,Cantidad , EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO, NRO_PARTIDA, UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, NULL, NULL, NULL,  NULL,NULL,NULL,NULL,NULL   
   from  sys_int_det_documento   
   WHERE  DOC_EXT=@Doc_Ext   
  end  
  ------------------------------------------------------------------------------------------------------------------------------------  
  --Guardo en la tabla de auditoria  
  -----------------------------------------------------------------------------------------------------------------  
  exec dbo.AUDITORIA_HIST_INSERT_ING @doc_id  
  --insert into IMPRESION_RODC VALUES(@Doc_id, 1, @Tipo_eti,'0')  
  --COMMIT TRANSACTION  
  Set @DOCUMENTO_ID=@doc_id  
  
  update ingreso_oc  
  set procesado = 1  
  WHERE     (CLIENTE_ID = @CLIENTE_ID) AND (PRODUCTO_ID = @producto_id) AND (ORDEN_COMPRA = @oc)   
   AND (((@NRO_LOTE = '' OR @NRO_LOTE IS NULL) AND (NRO_LOTE = '' OR NRO_LOTE IS NULL)) OR @NRO_LOTE = NRO_LOTE)  
   AND (((@NRO_PARTIDA = '' OR @NRO_PARTIDA IS NULL) AND (NRO_PARTIDA = '' OR NRO_PARTIDA IS NULL)) OR @NRO_PARTIDA = NRO_PARTIDA)  
  
    
    SET @DOC_EXT = NULL  
  fetch next from Ingreso_Cursor INTO @doc_ext,@producto_id, @cantidad, @fecha, @nro_partida, @nro_lote  
 END   
 --COMMIT TRANSACTION  
 CLOSE Ingreso_Cursor  
 DEALLOCATE Ingreso_Cursor  
   
 -- LRojas 02/03/2012 TrackerID 3806: Inserto Usuario para Demonio de Impresion  
 INSERT INTO IMPRESION_RODC VALUES(@Doc_ID,0,'D',0, @USUARIO_IMP)  
 -----------------------------------------------------------------------------------------------------------------  
 --ASIGNO TRATAMIENTO...  
 -----------------------------------------------------------------------------------------------------------------  
 exec asigna_tratamiento#asigna_tratamiento_ing @doc_id   
 exec dbo.AUDITORIA_HIST_INSERT_ING @doc_id  
 if @@error<>0  
 begin  
  rollback transaction  
  raiserror('No se pudo completar la transaccion',16,1)  
 end  
 else  
 begin  
  commit transaction  
 end   
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

ALTER          PROCEDURE [dbo].[INGRESA_ODC]
@CLIENTE_ID		VARCHAR(15),
@PRODUCTO_ID		VARCHAR(30),
@ODC				VARCHAR(100),
@QTY				FLOAT,
@LOTEPROV			VARCHAR(50),
@FECHA_VTO		VARCHAR(20),
@PALLET			VARCHAR(50),
@TIPO_ETI			CHAR(1),
@DOCUMENTO_ID	BIGINT OUTPUT,
-- LRojas 02/03/2012 TrackerID 3806: Usuario para Demonio de Impresion
@USUARIO_IMP	VARCHAR(20)

AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @DOC_ID				NUMERIC(20,0)
	DECLARE @DOC_TRANS_ID		NUMERIC(20,0)
	DECLARE @DOC_EXT				VARCHAR(100)
	DECLARE @SUCURSAL_ORIGEN	VARCHAR(20)
	DECLARE @CAT_LOG_ID			VARCHAR(50)
	DECLARE @DESCRIPCION			VARCHAR(30)
	DECLARE @UNIDAD_ID			VARCHAR(15)
	DECLARE @NRO_PARTIDA			NUMERIC(38)
	DECLARE @LOTE_AT				VARCHAR(50)
	DECLARE @Preing				VARCHAR(45)
	DECLARE @CatLogId				Varchar(50)
	DECLARE @LineBO				Float
	DECLARE @qtyBO				Float
	DECLARE @ToleranciaMax		Float
	DECLARE @QtyIngresada			Float
	DECLARE @tmax					Float
	DECLARE @MAXP					VARCHAR(50)
	-- LRojas TrackerID 3851 29/03/2012: Control, si el producto genera Back Order se crea un nuevo ingreso, de lo contrario no
	DECLARE @GENERA_BO          VARCHAR(1)
	/*
	CREATE TABLE #temp_usuario_loggin (
		usuario_id            			VARCHAR(20)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		terminal              			VARCHAR(100)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		fecha_loggin       			DATETIME,
		session_id            			VARCHAR(60)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		rol_id                			VARCHAR(5)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		emplazamiento_default 	VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		deposito_default      		VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
	)

	EXEC FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN 'SGOMEZ'
	*/
	SELECT 	TOP 1
			@DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID
	FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
	WHERE 	ORDEN_DE_COMPRA=@ODC
			AND PRODUCTO_ID=@PRODUCTO_ID
			AND SD.CLIENTE_ID=@CLIENTE_ID
			and SDD.fecha_estado_gt is null
			and SDD.estado_gt is null

	if @doc_ext is null
	begin
		raiserror('El producto %s no se encuentra en la orden de compra %s',16,1,@producto_id, @odc)
		return
	end
	SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0) from producto where cliente_id=@cliente_id and producto_id=@producto_id

	-----------------------------------------------------------------------------------------------------------------
	--tengo que controlar el maximo en cuanto a tolerancias.
	-----------------------------------------------------------------------------------------------------------------
	Select 	@qtyBO=sum(cantidad_solicitada)
	from	sys_int_det_documento
	where	doc_ext=@doc_ext
			and fecha_estado_gt is null
			and estado_gt is null

	set @tmax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)
	
	if @qty> @tmax
	begin
		Set @maxp=ROUND(@tmax,0)
		raiserror('1- La cantidad recepcionada supera a la tolerancia maxima permitida.  Maximo permitido: %s ',16,1, @maxp)
		return
	end
	-----------------------------------------------------------------------------------------------------------------
	--Obtengo las categorias logicas antes de la transaccion para acortar el lockeo.
	-----------------------------------------------------------------------------------------------------------------
	SELECT 	@CAT_LOG_ID=PC.CAT_LOG_ID
	FROM 	RL_PRODUCTO_CATLOG PC 
	WHERE 	PC.CLIENTE_ID=@CLIENTE_ID
			AND PC.PRODUCTO_ID=@PRODUCTO_ID
			AND PC.TIPO_COMPROBANTE_ID='DO'

	If @CAT_LOG_ID Is null begin
		--entra porque no tiene categorias particulares y busca la default.
		select 	@CAT_LOG_ID=p.ing_cat_log_id
		From 	producto p 
		where  	p.cliente_id=@CLIENTE_ID
				and p.producto_id=@PRODUCTO_ID
	end 

	-----------------------------------------------------------------------------------------------------------------
	--obtengo los valores de las secuencias.
	-----------------------------------------------------------------------------------------------------------------	
	--obtengo la secuencia para el numero de partida.
	exec get_value_for_sequence  'NRO_PARTIDA', @nro_partida Output
	--obtengo la secuencia para el numero de Lote.
	exec get_value_for_sequence 'NROLOTE_SEQ', @Lote_At Output
	-----------------------------------------------------------------------------------------------------------------
	--Saco la descripcion y la unidad del producto
	-----------------------------------------------------------------------------------------------------------------
	select @descripcion=descripcion, @unidad_id=unidad_id from producto where cliente_id=@cliente_id and producto_id=@producto_id
	-----------------------------------------------------------------------------------------------------------------


	-----------------------------------------------------------------------------------------------------------------
	--Comienzo con la carga de las tablas.
	-----------------------------------------------------------------------------------------------------------------
	Begin transaction	
	--Creo Documento
	Insert into Documento (	Cliente_id	, Tipo_comprobante_id	, tipo_operacion_id	, det_tipo_operacion_id	, sucursal_origen		, fecha_cpte	, fecha_pedida_ent	, Status	, anulado	, nro_remito	, nro_despacho_importacion	,GRUPO_PICKING		, fecha_alta_gtw)
					Values(	@Cliente_Id	, 'DO'					, 'ING'				, 'MAN'					,@SUCURSAL_ORIGEN		, GETDATE()		, GETDATE()			,'D05'		,'0'		, NULL			,@DOC_EXT					,null			, getdate())		
	--Obtengo el Documento Id recien creado.	
	Set @Doc_ID= Scope_identity()
	
	--Creo el detalle de det_documento
	insert into det_documento (documento_id, nro_linea	, cliente_id	, producto_id	, cantidad	, cat_log_id	, cat_log_id_final	, tie_in	, fecha_vencimiento	, nro_partida	, unidad_id	, descripcion		, busc_individual	, item_ok	, cant_solicitada	, prop1	, prop2		, nro_bulto	,nro_lote)
					       values(@doc_id		, 1			, @cliente_id	, @producto_id	, @qty		, null		, @cat_log_id		, '0'		, @fecha_vto		, @nro_partida	, @unidad_id	, @descripcion	, '1'				, '1'			,@qtyBO			, @pallet, UPPER(@loteProv)	, NULL		, @lote_at)

	------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	--Documento a Ingreso.
	select 	@Preing=nave_id
	from	nave
	where	pre_ingreso='1'
	
	SELECT 	@catlogid=cat_log_id
	FROM 	categoria_stock cs
			INNER JOIN categoria_logica cl
			ON cl.categ_stock_id = cs.categ_stock_id
	WHERE 	cs.categ_stock_id = 'TRAN_ING'
			And cliente_id =@cliente_id

	UPDATE det_documento
	Set cat_log_id =@catlogid
	WHERE documento_id = @Doc_ID

	Update documento set status='D20' where documento_id=@doc_id
	
	Insert Into RL_DET_DOC_TRANS_POSICION (
				DOC_TRANS_ID,				NRO_LINEA_TRANS,
				POSICION_ANTERIOR,		POSICION_ACTUAL,
				CANTIDAD,					TIPO_MOVIMIENTO_ID,
				ULTIMA_ESTACION,			ULTIMA_SECUENCIA,
				NAVE_ANTERIOR,				NAVE_ACTUAL,
				DOCUMENTO_ID,				NRO_LINEA,
				DISPONIBLE,					DOC_TRANS_ID_EGR,
				NRO_LINEA_TRANS_EGR,		DOC_TRANS_ID_TR,
				NRO_LINEA_TRANS_TR,		CLIENTE_ID,
				CAT_LOG_ID,				CAT_LOG_ID_FINAL,
				EST_MERC_ID)
	Values (NULL, NULL, NULL, NULL, @qty, NULL, NULL, NULL, NULL, @PREING, @doc_id, 1, null, null, null, null, null, @cliente_id, @catlogid,@CAT_LOG_ID,null)

	-----------------------------------------------------------------------------------------------------------------
	--ASIGNO TRATAMIENTO...
	-----------------------------------------------------------------------------------------------------------------
	exec asigna_tratamiento#asigna_tratamiento_ing @doc_id

	------------------------------------------------------------------------------------------------------------------------------------
	--Generacion del Back Order.
	-----------------------------------------------------------------------------------------------------------------
	select @lineBO=max(isnull(nro_linea,1))+1 from sys_int_det_documento WHERE 	 DOC_EXT=@doc_ext

	Select 	@qtyBO=sum(cantidad_solicitada)
	from	sys_int_det_documento
	where	doc_ext=@doc_ext
			and fecha_estado_gt is null
			and estado_gt is null

	UPDATE SYS_INT_DOCUMENTO SET ESTADO_GT='P'	,FECHA_ESTADO_GT=getdate() WHERE DOC_EXT=@doc_ext
	
	UPDATE SYS_INT_DET_DOCUMENTO SET ESTADO_GT='P', DOC_BACK_ORDER=@doc_ext,FECHA_ESTADO_GT=getdate(), DOCUMENTO_ID=@Doc_ID
	WHERE 	DOC_EXT=@doc_ext	and documento_id is null

	set @qtyBO=@qtyBO - @qty

	SELECT @GENERA_BO = 
	   CASE P.BACK_ORDER 
		WHEN '1' THEN 'S' 
		WHEN '0' THEN 'N'
	   END
	FROM PRODUCTO P INNER JOIN SYS_INT_DET_DOCUMENTO SIDD ON (P.PRODUCTO_ID = SIDD.PRODUCTO_ID)
	WHERE SIDD.DOC_EXT = @doc_ext	AND SIDD.DOCUMENTO_ID = @Doc_ID AND P.CLIENTE_ID=@cliente_id

	-- LRojas TrackerID 3851 29/03/2012: Se debe tener en cuenta la parametrización del producto.
	IF (@qtyBO > 0) AND (@GENERA_BO = 'S') --Si esta variable es mayor a 0, genero el backorder.
	begin
	insert into sys_int_det_documento 
		select TOP 1 
				DOC_EXT, @lineBO ,CLIENTE_ID, PRODUCTO_ID, @qtyBO ,Cantidad , EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, DESCRIPCION, NRO_LOTE, NRO_PALLET, FECHA_VENCIMIENTO, NRO_DESPACHO, NRO_PARTIDA, UNIDAD_ID, UNIDAD_CONTENEDORA_ID, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN, PROP1, PROP2, PROP3, LARGO, ALTO, ANCHO, NULL, NULL, NULL,  NULL,NULL,NULL,NULL,NULL 
		from 	sys_int_det_documento 
		WHERE 	DOC_EXT=@Doc_Ext
	end
	------------------------------------------------------------------------------------------------------------------------------------
	--Guardo en la tabla de auditoria
	-----------------------------------------------------------------------------------------------------------------
	exec dbo.AUDITORIA_HIST_INSERT_ING @doc_id
	-- LRojas 02/03/2012 TrackerID 3806: Inserto Usuario para Demonio de Impresion
	insert into IMPRESION_RODC VALUES(@Doc_id, 1, @Tipo_eti,'0', @USUARIO_IMP)
	COMMIT TRANSACTION
	Set @DOCUMENTO_ID=@doc_id
END-- FIN PROCEDIMIENTO.
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
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREACION: 		29-06-2007
VERSION:		1.3
AUTOR:			SEBASTIAN GOMEZ.
DESCRIPCION:	PROCEDIMIENTO ALMACENADO. DADO UN DOCUMENTO_ID ALIMENTA MEDIANTE UN QUERY A LA TABLA DE PICKING.
				ADICIONALMENTE EVALUA SI LA OPERACION ES UNA OPERACION DE EGRESO CASO CONTRARIO SE TERMINA 
				LA EJECUCION DEL PROCEDIMIENTO
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
ALTER                         PROCEDURE [dbo].[INGRESA_PICKING]
	@DOCUMENTO_ID NUMERIC(20,0) OUTPUT
AS
BEGIN
	--DECLARACIONES.
	DECLARE @TIPO_OPERACION VARCHAR(5)
	DECLARE @CANT			AS INT


	DECLARE @TCUR				CURSOR
	DECLARE @VIAJEID			VARCHAR(100)
	DECLARE @PRODUCTO_ID		VARCHAR(30)
	DECLARE @POSICION_COD	VARCHAR(50)
	DECLARE @PALLET			VARCHAR(100)
	DECLARE @RUTA				VARCHAR(100)
	DECLARE @ID				NUMERIC(20,0)		

	--START
	SELECT 	@TIPO_OPERACION = TIPO_OPERACION_ID
	FROM	DOCUMENTO
	WHERE 	DOCUMENTO_ID=@DOCUMENTO_ID

	IF @TIPO_OPERACION <> 'EGR'
		BEGIN
			--SI LA OPERACION NO ES UN EGRESO ENTONCES...
			RAISERROR ('EL NRO. DE DOCUMENTO INGRESADO NO CORRESPONDE A UNA OPERACION DE EGRESO.', 16, 1)
		END
	ELSE
		BEGIN
			SELECT 	@CANT=COUNT(VIAJE_ID) 
			FROM 	PICKING P INNER JOIN DOCUMENTO DD
					ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			WHERE 	DD.DOCUMENTO_ID=@DOCUMENTO_ID

			IF @CANT>0 
			BEGIN
				RAISERROR('El picking ya fue ingresado.',16,1)
				RETURN
			END			

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
					,ISNULL(LTRIM(RTRIM(D.GRUPO_PICKING)),ISNULL(LTRIM(RTRIM(D.SUCURSAL_DESTINO)),ISNULL(D.NRO_REMITO,LTRIM(RTRIM(D.DOCUMENTO_ID)))))AS RUTA
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
					,C.FLG_PALLET_HOMBRE
					,'0'  AS TRANSF_TERMINADA
					,DD.NRO_LOTE AS NRO_LOTE
					,DD.NRO_PARTIDA AS NRO_PARTIDA
					,DD.NRO_SERIE AS NRO_SERIE
			FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
					ON (D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
					INNER JOIN PRODUCTO P
					ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(RL.DOC_TRANS_ID_EGR=DDT.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ANTERIOR=N.NAVE_ID)
					LEFT JOIN POSICION POS
					ON(RL.POSICION_ANTERIOR=POS.POSICION_ID)
					LEFT JOIN NAVE N2
					ON(POS.NAVE_ID=N2.NAVE_ID)
					INNER JOIN CLIENTE_PARAMETROS C
					ON(D.CLIENTE_ID = C.CLIENTE_ID)
			WHERE 	D.DOCUMENTO_ID=@DOCUMENTO_ID


------CONTROLO QUE SERIES FUERON OBLIGATORIAS Y CUALES NO.
	
	UPDATE DET_DOCUMENTO
	SET NRO_SERIE = NULL
	WHERE DOCUMENTO_ID = @DOCUMENTO_ID
			AND NOT EXISTS (SELECT 1 FROM SYS_INT_DET_DOCUMENTO SS
							INNER JOIN SYS_INT_DOCUMENTO S ON (SS.CLIENTE_ID = S.CLIENTE_ID AND SS.DOC_EXT = S.DOC_EXT)
							WHERE S.DOC_EXT = (SELECT NRO_REMITO FROM DOCUMENTO WHERE DOCUMENTO_ID = @DOCUMENTO_ID)
									AND PROP3=DET_DOCUMENTO.NRO_SERIE)

------

		END --FIN ELSE
END --FIN PROCEDURE
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER   procedure [dbo].[IngresaMandatorioInicial]
@cliente_id as varchar(15),
@OPERACION AS VARCHAR(5)
as

declare @articulo as varchar(30)

declare pcur cursor for
select distinct producto_id 
from producto 
where cliente_id=@cliente_id and producto_id not in(	select producto_id
							from mandatorio_producto
							where cliente_id=@cliente_id
						    );


open pcur
fetch next from pcur into @articulo
while @@fetch_status = 0
begin
	insert into mandatorio_producto	values(
			 UPPER(LTRIM(RTRIM(@cliente_id)))
			,UPPER(LTRIM(RTRIM(@articulo)))
			,UPPER(LTRIM(RTRIM(@OPERACION)))
			,'CANTIDAD')
	fetch next from PCUR into @articulo
end
CLOSE PCUR
DEALLOCATE PCUR
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

ALTER Procedure [dbo].[Ingreso_CrossDock]
@Documento_id	Numeric(20,0) Output,
@Nro_Linea		Numeric(10,0) Output	
As
Begin
	Set xAct_Abort on 
	Declare @pCur 	Cursor
	Declare @RL_Id	Numeric(20,0)

	Set @pCur=Cursor For
		Select 	Rl.Rl_Id
		from	Det_Documento DD (NoLock) inner join Det_Documento_Transaccion DDT (NoLock)
				on(dd.Documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
				Inner Join Rl_Det_Doc_Trans_Posicion Rl (NoLock)
				on(ddt.doc_trans_id=rl.doc_trans_id And ddt.nro_linea_trans=rl.nro_linea_trans)
		Where	dd.Documento_id=@Documento_id
				and dd.nro_linea=@Nro_linea

	Open @pCur
	Fetch Next from @pCur into @RL_Id
	While @@Fetch_Status=0
	Begin
		Update Rl_Det_Doc_Trans_posicion Set Disponible='1',  Cat_Log_ID=Cat_Log_Id_Final Where Rl_Id=@Rl_ID
		Fetch Next from @pCur into @RL_Id
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

ALTER PROCEDURE [dbo].[INGRESO_OC_ACTUALIZA]
	@CLIENTE_ID			varchar(15),
	@PRODUCTO_ID		varchar(30),
	@ORDEN_COMPRA		varchar(100),
	@CANTIDAD			numeric(20,5),
    @CANT_CONTENEDORAS	numeric(20,5)=NULL,
    @LOTEPROVEEDOR		VARCHAR(100),
	@PARTIDA			VARCHAR(100),
  @DOC_EXT      VARCHAR(100)
AS
UPDATE    INGRESO_OC
SET       CANTIDAD = @CANTIDAD, CANT_CONTENEDORAS = @CANT_CONTENEDORAS, NRO_LOTE = @LOTEPROVEEDOR, NRO_PARTIDA = @PARTIDA
WHERE     (CLIENTE_ID = @CLIENTE_ID) AND (PRODUCTO_ID = @PRODUCTO_ID) AND (ORDEN_COMPRA = @ORDEN_COMPRA) AND DOC_EXT = @DOC_EXT
	/* SET NOCOUNT ON */ 
	/*RETURN*/
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

ALTER PROCEDURE [dbo].[INGRESO_OC_ALTA]
	@CLIENTE_ID			varchar(15),
	@PRODUCTO_ID		varchar(30),
	@ORDEN_COMPRA		varchar(100),
	@CANTIDAD			numeric(20,5),
	@CANT_CONTENEDORAS	numeric(20,5)=NULL,
	@FECHA				datetime,
	@procesado			char(1),
	@LOTEPROVEEDOR		VARCHAR(100),
	@PARTIDA			VARCHAR(100),
  @DOC_EXT      VARCHAR(100)
AS
begin
	DECLARE @USUARIO	VARCHAR(50)
	DECLARE @TERMINAL VARCHAR(100)

SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	SET @TERMINAL=HOST_NAME()
	
INSERT INTO INGRESO_OC(CLIENTE_ID, PRODUCTO_ID, ORDEN_COMPRA, CANTIDAD, CANT_CONTENEDORAS, USUARIO, TERMINAL, FECHA, PROCESADO, NRO_LOTE, NRO_PARTIDA, DOC_EXT)                    
VALUES                (@CLIENTE_ID,@PRODUCTO_ID,@ORDEN_COMPRA,@CANTIDAD,@CANT_CONTENEDORAS,@USUARIO,@TERMINAL,@FECHA,@PROCESADO, @LOTEPROVEEDOR, @PARTIDA, @DOC_EXT)
end
	/* SET NOCOUNT ON */ 
	/*RETURN*/
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

ALTER PROCEDURE [dbo].[INGRESO_OC_BORRAR]
	@ING_ID	NUMERIC(20,0)
AS
BEGIN
	DELETE	FROM INGRESO_OC
	WHERE	ING_ID=@ING_ID;
END;
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

ALTER PROCEDURE [dbo].[INGRESO_OC_EXISTE_PROD]
	@CLIENTE_ID		varchar(15),
	@PRODUCTO_ID	varchar(30),
	@ORDEN_COMPRA	varchar(100),
	@LOTE_PROVEEDOR	varchar(100),
	@PARTIDA		varchar(100)
AS
	SELECT	producto_id
	FROM	INGRESO_OC
	WHERE   CLIENTE_ID = @CLIENTE_ID
			AND PRODUCTO_ID = @PRODUCTO_ID
			AND ORDEN_COMPRA = @ORDEN_COMPRA
			AND ISNULL(PROCESADO,'0') = '0'
			AND NRO_LOTE=@LOTE_PROVEEDOR
			AND NRO_PARTIDA = @PARTIDA
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

ALTER    procedure [dbo].[IngVerificaIntermedia]
@Doc_trans_id numeric(20,0) output,
@Out int output
As
Begin

	Declare @vRlId  as Numeric(20,0)
	Declare @Q1		as int
	Declare @Q2		as int
	Declare @Return as int

	Declare Cur_VerIntIng cursor For
		Select 	Rl_id
		from	Rl_Det_Doc_trans_posicion rl inner join Det_documento_transaccion ddt
				on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans =ddt.nro_linea_trans)
		Where	ddt.doc_trans_id=@Doc_trans_id


	Open Cur_VerIntIng
		
	Fetch Next from Cur_VerIntIng Into @vRlId
	While @@Fetch_Status=0
		Begin
		
			SELECT 	@Q1=COUNT(RL_ID)
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	RL.RL_ID=@vRlId
					AND N.INTERMEDIA='1'
		
		
		
		
			SELECT 	@Q2=COUNT(RL_ID)
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	RL.RL_ID=@vRlId
					AND P.INTERMEDIA='1'
		
			
		
			If @Q1=1 Or @Q2=1
				Begin
					set @Return=1
					Break
				End
			Else
				Begin
					set @Return=0
				End
	
			Fetch Next from Cur_VerIntIng Into @vRlId
					
		End --Fin While
	set @Out=@Return

	Close Cur_VerIntIng
	deallocate Cur_VerIntIng
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

ALTER PROCEDURE [dbo].[INSERT_CONTENEDORAS]
@PRODUCTO_ID	VARCHAR(30),
@CANTIDAD		FLOAT
AS
BEGIN
	DECLARE @UNIDAD_ID	VARCHAR(5)
	DECLARE @CLIENTE_ID	VARCHAR(15)
	SET @CLIENTE_ID='LEADER PRICE'

	SELECT @UNIDAD_ID=UNIDAD_ID FROM PRODUCTO WHERE CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
	
	INSERT INTO RL_PRODUCTO_UNIDAD_CONTENEDORA (CLIENTE_ID, PRODUCTO_ID, UNIDAD_ID,CANTIDAD,FLG_PICKING, INGRESO)
	VALUES(@CLIENTE_ID, @PRODUCTO_ID, @UNIDAD_ID, @CANTIDAD,'1','1')

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

ALTER PROCEDURE [dbo].[INSERT_IMPRESION_CAMBIO_CAT_LOG]
@PALLET				VARCHAR(100),
@CONTENEDORA		VARCHAR(100),
@Tipo_eti			VARCHAR(1),
-- LRojas 02/03/2012 TrackerID 3806: Usuario para Demonio de Impresion
@USUARIO_IMP	VARCHAR(20)
AS
BEGIN

	DECLARE	@DOCUMENTO_ID	NUMERIC(20,0)
	DECLARE @NRO_LINEA		NUMERIC(10,0)

	Declare PCURSOR_ETIQUETAS Cursor For
			SELECT	DOCUMENTO_ID,NRO_LINEA
			FROM	DET_DOCUMENTO
			WHERE	PROP1 = LTRIM(RTRIM(UPPER(@PALLET))) 
					AND ((@CONTENEDORA IS NULL) OR (NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))	
		
		Open PCURSOR_ETIQUETAS
		Fetch Next From PCURSOR_ETIQUETAS Into	@DOCUMENTO_ID,@NRO_LINEA
	
	While @@Fetch_Status = 0
		Begin
	
			-- LRojas 02/03/2012 TrackerID 3806: Inserto Usuario para Demonio de Impresion
			insert into IMPRESION_RODC VALUES(@DOCUMENTO_ID, @NRO_LINEA, @Tipo_eti,'0', @USUARIO_IMP)

			Fetch Next From PCURSOR_ETIQUETAS Into	@DOCUMENTO_ID,@NRO_LINEA
		end

	CLOSE PCURSOR_ETIQUETAS
	DEALLOCATE PCURSOR_ETIQUETAS
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

ALTER procedure [dbo].[Insert_Picking_Wave]
@PWave_id			as Numeric(20,0) Output,
@PDocTransId		as Numeric(20,0) Output
As

Begin
	Declare @Secuencia 	as Numeric(20,0)
	Declare @Usuario_id	as Varchar(20)

	If @PWave_id is null
	Begin
		Exec Dbo.Get_Value_For_Sequence 'PICKING_WAVE', @Secuencia Output
		Set @PWave_id=@Secuencia
	End;

	Select @Usuario_id=Usuario_Id	From #Temp_Usuario_Loggin
		
	Insert into Sys_Picking_Wave (Wave_Id, Doc_Trans_id, Fecha, Usuario_Id) 
	Values(
			 @PWave_id
			,@PDocTransId
			,GetDate()
			,@Usuario_Id
			);
End;
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

ALTER PROCEDURE [dbo].[INSERT_RL_VH]
@POSICION	VARCHAR(45),
@VEHICULO 	VARCHAR(50)
AS
BEGIN
	DECLARE @POSICION_ID	AS BIGINT
	
	SELECT 	@POSICION_ID=POSICION_ID
	FROM	POSICION
	WHERE 	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION)))

	IF @VEHICULO='NO'
	BEGIN
		RETURN
	END 
	IF @POSICION_ID IS NOT NULL
	BEGIN
		INSERT INTO RL_VEHICULO_POSICION (VEHICULO_ID, POSICION_ID)
		VALUES(@VEHICULO,@POSICION_ID )
	END
	
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

ALTER PROCEDURE [dbo].[INSERT_RLPPP]
@CLIENTE_ID		VARCHAR(15),
@PRODUCTO_ID	VARCHAR(30),
@NAVE_COD		VARCHAR(15),
@POSICION_COD	VARCHAR(45)
As
Begin

	Declare @Nave_ID 		as Numeric(20,0)
	Declare @Posicion_Id	as Numeric(20,0)
	Declare @Control		as float(1)
	Declare @Msg			as varchar(4000)
	Declare @error_var		as int

	--Obtengo la Posicion Id en caso de que no sea null
	If @POSICION_COD is not null
	Begin
		Set @Posicion_Id=Dbo.Get_Posicion_id(@Posicion_Cod)
	End
	Else
	Begin
		Set @Posicion_Id=Null
	End	

	--Obtengo la Posicion Id en caso de que no sea null
	If @NAVE_COD is not null
	Begin
		Select 	@Nave_ID=Nave_Id
		From 	Nave
		Where	nave_cod=ltrim(rtrim(upper(@NAVE_COD)))
	End
	Else
	Begin
		Set @Nave_ID=Null
	End	

	If (@Producto_id Is null) Or (ltrim(rtrim(upper(@Producto_Id)))='')
	Begin
		Set @Msg='El campo producto no puede estar vacio.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	--Controlo el producto
	Select 	@Control=Count(*)
	from 	Producto
	Where	Cliente_id=ltrim(rtrim(Upper(@Cliente_id)))
			and Producto_id=ltrim(rtrim(Upper(@Producto_id)))

	If @Control=0
	Begin
		Set @Msg='Producto inexistente, por favor verifique estos valores.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	-- Controlo que no se ingrese basura a la tabla, al menos uno deberia tener valores.
	If (@Nave_id is null) and (@posicion_id is null)
	Begin
		Set @Msg='La nave o la posicion no existen, por favor verifique estos valores.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	--Inserto en la tabla
	INSERT INTO RL_PRODUCTO_POSICION_PERMITIDA (CLIENTE_ID, PRODUCTO_ID, NAVE_ID, POSICION_ID) 
	VALUES(@Cliente_id, @Producto_Id, @Nave_Id, @Posicion_id)


	--Controlo la condicion de error.

	SELECT @error_var = @@ERROR
	If @error_var<> 0 
	Begin
		Set @Msg='Ocurrio un error inesperado al insertar en la tabla Rl_Producto_Posicion_Permitida. - COD. ERROR: ' + CAST(@error_var AS VARCHAR(10))
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg );
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

ALTER  PROCEDURE [dbo].[InsertPosicion]
	@NaveCod 	as varchar(15),
	@CalleCod 	as varchar(15),
	@ColCod 	as varchar(15),
	@NivelCod	as varchar(15),
	@Prof		as varchar(15),
	@Peso		as Numeric(20,5),
	@Largo		as Numeric(20,5),
	@Alto		as Numeric(20,5),
	@Ancho 		as Numeric(20,5),
	@Picking	as varchar(1),
	@OrdenPick 	as Numeric(6),
	@OrdenIng 	as Numeric(6),
	@Inter		as varchar(1)
As
Begin
	--Comienzo con el proceso.
	Declare @vControl 	as smallint
	Declare @vMax 		as Numeric(20,0)
	Declare @Calculo	as Float
	
	Select	@vControl=count(*)
	from	Posicion
	Where	Posicion_Cod=@CalleCod + '-' + @ColCod + '-' + @NivelCod
	IF @vControl>0
	begin
		return
	End
	Set @vControl=null
	-------------------------------------------------------------------------------------------------------------------------
	--Existe la calle?
	-------------------------------------------------------------------------------------------------------------------------
	Select 	@vControl=Count(*)
	from	Nave N Inner Join Calle_Nave CN
			On(N.Nave_Id=cN.Nave_ID)
	where	N.Nave_Cod=@NaveCod
			and Cn.Calle_Cod=@CalleCod

	If @vControl=0 
	Begin
		--Tengo q crear la calle
		Select @vMax=Max(Calle_ID) +1 from Calle_Nave
		If @vMax is null
		begin
			Set @vMax=1
		end
		Select 	@Calculo= pos_y
		from	Calle_Nave 
		where  	Calle_ID=(Select Max(Calle_ID) from calle_nave)
				and nave_id=DBO.GetNave(@NaveCod)
	
		If @Calculo is null
		begin
			set @Calculo=465
		end
		else
		begin
			Set  @Calculo=@Calculo +585
		end
		Insert into Calle_Nave Values(
			@vMax
			,DBO.GetNave(@NaveCod)
			,LTRIM(UPPER(@CalleCod))
			,'CALLE '  + LTRIM(RTRIM(UPPER(@CalleCod)))
			,'H'
			,330
			,@Calculo
			,'0'
			)
	End
	-------------------------------------------------------------------------------------------------------------------------
	--Existe la Columna?
	-------------------------------------------------------------------------------------------------------------------------
	Set @vControl=null
	Set @vMax=null

	Select 	@vControl=Count(*)
	From	Columna_Nave Cn Inner Join Calle_Nave CNav
			On(Cn.Calle_ID=Cnav.Calle_ID)
			Inner Join Nave n
			ON(Cn.Nave_ID=N.Nave_ID)
	Where	Cn.Columna_Cod= @ColCod
			And CNav.Calle_Cod=@CalleCod
			And N.Nave_Cod=@NaveCod
	
	If @vControl=0
	Begin
		--Tengo q crear la columna.
		Select	@vMax=Max(Columna_ID) +1 from Columna_Nave
		If @vMax is null
		begin
			Set @vMax=1
		end
		Insert into Columna_Nave Values(
			 @vMax
			,DBO.GetNave(@NaveCod)
			,Dbo.GetCalle(@NaveCod, @CalleCod)
			,@ColCod
			,'COLUMNA ' + LTRIM(RTRIM(UPPER(@ColCod)))
			,'0'
		)

	End	
	-------------------------------------------------------------------------------------------------------------------------
	--Creo el Nivel Nave.
	-------------------------------------------------------------------------------------------------------------------------
	Set @vControl=null
	Set @vMax=null
	Set @Picking = 1

	If @OrdenPick = 0
	begin
		Set @OrdenPick=null
		Set @Picking=0
	end

	If @OrdenIng = 0
	begin
		Set @OrdenIng=null
	end

	Select	@vMax=Max(Nivel_Id) +1 from Nivel_Nave
	If @vMax is null
	begin
		Set @vMax=1
	end

	Insert into Nivel_Nave Values(
		 @vMax
		,DBO.GetNave(@NaveCod)
		,Dbo.GetCalle(@NaveCod, @CalleCod)
		,dbo.GetColumna(@NaveCod, @CalleCod,@ColCod)
		,LTRIM(RTRIM(UPPER(@NivelCod)))
		,'NIVEL ' + LTRIM(RTRIM(UPPER(@NivelCod)))
		,1
		,0
		,NULL
	)	
	--select @vMax as Maximo
	-------------------------------------------------------------------------------------------------------------------------
	--Creo la posicion.
	-------------------------------------------------------------------------------------------------------------------------
	--No tengo que tocar el valor de vmax
	Insert into posicion
	values(
		 @vMax
		,Dbo.GetNave(@NaveCod)
		,Dbo.GetCalle(@NaveCod, @CalleCod)
		,Dbo.GetColumna(@NaveCod, @CalleCod,@ColCod)
		,@vMax
		,LTRIM(RTRIM(UPPER(@NaveCod))) + '-' + LTRIM(RTRIM(UPPER(@CalleCod))) + '-'  + LTRIM(RTRIM(UPPER(@ColCod))) + '-' + LTRIM(RTRIM(UPPER(@NivelCod)))
		,null
		,@peso
		,Null
		,1
		,0
		,null
		,0
		,0
		,0
		,0
		,0
		,null
		,@largo
		,@alto
		,@ancho
		,'1'
		,'0'
		,null
		,null
		,@OrdenPick
		,0
		,@OrdenIng
		,@Inter
		,'0'
		,NULL
		,NULL
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

ALTER PROCEDURE [dbo].[Jb_Close_Documents_Egr]
As
Begin
	Set xAct_abort On
	Declare @Doc_trans	numeric(20,0)
	Declare @CloseDoc	Cursor

	Set @CloseDoc=Cursor for
		select	ddt.doc_trans_id
		from	picking p inner join det_documento dd
				on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
				inner join documento d 
				on(d.documento_id=dd.documento_id)
				inner join det_documento_transaccion ddt
				on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
		where	d.status='D30'
				and p.facturado='1'
				and p.fin_picking='2'
		group by
				d.documento_id, d.status, ddt.doc_trans_id
	Open @CloseDoc
	Fetch Next From @CloseDoc into @Doc_Trans
	While @@Fetch_Status=0
	Begin
		exec Egr_aceptar_job @Doc_Trans
		Fetch Next From @CloseDoc into @Doc_Trans
	End
	Close @CloseDoc
	Deallocate @CloseDoc
End	--End Job.
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

ALTER procedure [dbo].[Job_Ft_Rl_Doc_Trans_Posicion]
As
Begin
	DECLARE @FECHA	DATETIME
	DECLARE @AUDITA	CHAR(1)

	SELECT @AUDITA=AUDITABLE FROM PARAMETROS_AUDITORIA WHERE TIPO_AUDITORIA_ID=13

	IF @AUDITA='1'
	BEGIN

		SET @FECHA=GETDATE()
		
		INSERT INTO DBO.RL_DET_DOC_TRANS_POSICION_HISTORICO 
		SELECT 	 RL_ID
				,DOC_TRANS_ID
				,NRO_LINEA_TRANS
				,POSICION_ANTERIOR
				,POSICION_ACTUAL
				,CANTIDAD
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
				,@FECHA
		FROM 	RL_DET_DOC_TRANS_POSICION
	END
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

ALTER  Procedure [dbo].[Job_Libera_Tareas]
As
Begin
	Declare @Dif		as Int
	Declare @Cur	Cursor
	Declare @Doc	as Numeric(20,0)
	Declare @Line	as Numeric(10,0)
	Declare @Fecha	as DateTime
	
	Set @Cur= Cursor For
		Select	Documento_Id, Nro_Linea, Fecha_Lock
		From	Sys_Lock_Pallet
		Where	Lock='1'

	Open @Cur

	Fetch Next From @Cur Into @Doc, @Line, @Fecha
	While @@Fetch_Status=0
	Begin
		Select @Dif=DateDiff(mi, @Fecha, Getdate())	
		if @Dif >= 15
		Begin
			Update Sys_Lock_Pallet Set Lock='0' Where Documento_Id=@Doc and Nro_Linea=@Line
		End
		Fetch Next From @Cur Into @Doc, @Line, @Fecha
	End	
	Close @Cur
	Deallocate @Cur
End -- Fin Procedure.
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

ALTER   PROCEDURE [dbo].[JOB_PROD_AGRUPADO_HISTORICO]
AS
BEGIN

	INSERT INTO DBO.PRODUCTO_AGRUPADO_HISTORICO
	SELECT 	RL.CLIENTE_ID, DD.PRODUCTO_ID, SUM(RL.CANTIDAD), GETDATE()
	FROM	RL_DET_DOC_TRANS_POSICION RL
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DOCUMENTO_TRANSACCION DT
			ON(DT.DOC_TRANS_ID=DDT.DOC_TRANS_ID)
			INNER JOIN DET_DOCUMENTO DD 
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN DOCUMENTO D
			ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			LEFT JOIN NAVE N ON(RL.NAVE_ACTUAL=N.NAVE_ID)
	WHERE	D.STATUS='D40'
	GROUP BY
			RL.CLIENTE_ID, DD.PRODUCTO_ID


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

ALTER     Procedure [dbo].[Libera_Lockeo_Pallet]
@Pallet		Varchar(100) Output
As
Begin
	Declare @Documento_Id		as Numeric(20,0)
	Declare @Nro_Linea			as Numeric(10,0)

	Select 	@Documento_id=Documento_id--, @Nro_Linea=Nro_Linea
	From	Det_Documento
	Where	Prop1=Ltrim(Rtrim(Upper(@Pallet)))

	Update Sys_Lock_Pallet	Set	Lock='0' Where	Documento_Id=@Documento_Id And  Pallet=@Pallet --Nro_Linea=@Nro_Linea And
	
	If @@RowCount=0
	Begin
		Raiserror('No se actualizo ningun registro. Libera_Lockeo_Pallet.',16,1)
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

ALTER PROCEDURE [dbo].[LIBERAR_POSLOCKEADA]
	@POSICION_ID	NUMERIC(20,0) OUTPUT
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	
	UPDATE POSICION SET POS_LOCKEADA='0' WHERE POSICION_ID=@POSICION_ID
	DELETE FROM lockeo_posicion WHERE POSICION_ID=@POSICION_ID

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

ALTER              Procedure [dbo].[Locator_Api#Get_Productos_Locator_Rl]
  @p_cliente_id 				as varchar(15)
, @p_producto_id 				as varchar(30)
, @p_cantidad 					As numeric(20,5)
, @p_TieIN 						As varchar(1)
, @p_nro_serie 					As varchar(50)
, @p_nro_lote 					As varchar(50)
, @p_fecha_vencimiento 			As varchar(20)
, @p_Nro_Despacho 				As varchar(50)
, @p_Nro_Bulto 					As varchar(50)
, @p_nro_partida 				As varchar(50)
, @p_CatLog_ID 					As varchar(50)
, @P_PESO 						As numeric(20,5)
, @P_VOLUMEN 					As numeric(20,5)
, @p_Nave 						As numeric(20,0)
, @P_CALLE 						As numeric(20,0)
, @P_COLUMNA 					As numeric(20,0)
, @p_Nivel 						As numeric(20,0)
, @p_TipoOperacion_ID 			As varchar(5)
, @p_UseNullOnEmpty 			As Char(1)
, @p_EstMercID 					As varchar(15)
, @p_Llamada 					As varchar(50)
, @P_PROP1 						As varchar(100)
, @p_Prop2 						As varchar(100)
, @p_Prop3 						As varchar(100)
, @P_UNIDAD_ID 					As varchar(5)
, @P_UNIDAD_PESO 				As varchar(5)
, @P_UNIDAD_VOLUMEN 			As varchar(5)
, @P_DOCUMENTO_ID 				As numeric(20,0)
, @P_DOC_TRANS_ID 				As numeric(20,0)
, @p_Moneda_Id 					As varchar(20)
, @p_Costo 						As numeric(20,5)
, @p_Ilimitado 					As Char(1)
, @pLocator_Automatico			As Char(1)
, @P_NAVE_ID 					As Numeric(20,0)
, @P_POSICION_ID 				As numeric(20,0)
--, @c_egr						Cursor varying Output
As
Begin
	-------------------------------------------------
	--			Cursores
	-------------------------------------------------
	Declare @pAux 				Cursor
	Declare @InCur 				Cursor
	Declare @pcur 				Cursor
	Declare @cpCur 				Cursor
	Declare @TedCur 			Cursor
	-------------------------------------------------
	--			Generales
	-------------------------------------------------
	Declare @xSQL 				As varchar(8000)
	Declare @strsql 			As varchar(4000)
	Declare @StrSql1 			As varchar(4000)
	Declare @StrSql2 			As varchar(4000)
	Declare @StrSql3 			As varchar(4000)
	Declare @StrSql4 			As varchar(4000)
	Declare @StrSql5 			As varchar(4000)
	Declare @NtrSql2 			As varchar(4000)
	Declare @NtrSql3 			As varchar(4000)
	Declare @NtrSql4 			As varchar(4000)
	Declare @NtrSql5 			As varchar(4000)
	Declare @StrSql6 			As varchar(4000)
	Declare @StrSqlOrderBy 		As varchar(4000)
	Declare @varStrIn 			As varchar(4000)
	Declare @varSumCantidad 	As Float
	Declare @vDepositoID 		As varchar(4000)
	Declare @PCLIENTE_ID 		As varchar(15)
	Declare @PCANTIDAD 			As Float
	Declare @VCANTIDAD 			As Float
	Declare @PNRO_SERIE 		As varchar(50)
	Declare @PNRO_PARTIDA 		As varchar(50)
	Declare @PNRO_LOTE 			As varchar(50)
	Declare @PNRO_DESPACHO 		As varchar(50)
	Declare @PPRODUCTO_ID 		As varchar(30)
	Declare @PPESO 				As Float
	Declare @PVOLUMEN 			As Float
	Declare @PNRO_BULTO 		As varchar(50)
	Declare @P_FECHA_VTO_DDE 	As Datetime
	Declare @P_FECHA_VTO_HTA 	As Datetime
	Declare @P_TIE_IN 			As Char(1)
	Declare @pCat_Log_ID 		As varchar(50)
	Declare @PPROP1 			As varchar(100)
	Declare @PPROP2 			As varchar(100)
	Declare @PPROP3 			As varchar(100)
	Declare @nSQL				As nvarchar(4000)
	Declare @ParmDefinition 	As nvarchar(500)
	-------------------------------------------------
	-- 		Cursor @TedCur
	-------------------------------------------------
	Declare @clienteidT         VARCHAR(15)
	Declare @productoidT        VARCHAR(30)
	Declare @cantidadT          NUMERIC(20,5)
	Declare @nro_serieT         VARCHAR(50)
	Declare @nro_loteT          VARCHAR(50)
	Declare @fecha_vencimientoT DATETIME
	Declare @nro_despachoT      VARCHAR(50)
	Declare @nro_bultoT         VARCHAR(50)
	Declare @nro_partidaT       VARCHAR(50)
	Declare @pesoT              NUMERIC(20,5)
	Declare @volumenT           NUMERIC(20,5)
	Declare @tie_inT            CHAR(1)
	Declare @cantidad_dispT     NUMERIC(20,5)
	Declare @codeT              CHAR(1)
	Declare @descriptionT       VARCHAR(100)
	Declare @cat_log_idT        VARCHAR(50)
	Declare @prop1T             VARCHAR(100)
	Declare @prop2T             VARCHAR(100)
	Declare @prop3T             VARCHAR(100)
	Declare @unidad_idT         VARCHAR(5)
	Declare @unidad_pesoT       VARCHAR(5)
	Declare @unidad_volumenT    VARCHAR(5)
	Declare @est_merc_idT       VARCHAR(15)
	Declare @moneda_idT         VARCHAR(20)
	Declare @costoT             NUMERIC(10,3)
	Declare @ordenT             NUMERIC(20,0)
	-------------------------------------------------	
	--		Cursor @Incur
	-------------------------------------------------
	Declare @rl_idL             NUMERIC(20,0)
	Declare @clienteidL         VARCHAR(15)
	Declare @productoidL        VARCHAR(30)
	Declare @cantidadL          NUMERIC(20,5)
	Declare @nro_serieL         VARCHAR(50)
	Declare @nro_loteL          VARCHAR(50)
	Declare @fecha_vencimientoL DATETIME
	Declare @nro_despachoL      VARCHAR(50)
	Declare @nro_bultoL         VARCHAR(50)
	Declare @nro_partidaL       VARCHAR(50)
	Declare @pesoL              NUMERIC(20,5)
	Declare @volumenL           NUMERIC(20,5)
	Declare @cat_log_idL        VARCHAR(50)
	Declare @prop1L             VARCHAR(100)
	Declare @prop2L             VARCHAR(100)
	Declare @prop3L             VARCHAR(100)
	Declare @fecha_cpteL        DATETIME
	Declare @fecha_alta_gtwL    DATETIME
	Declare @unidad_idL         VARCHAR(5)
	Declare @unidad_pesoL       VARCHAR(5)
	Declare @unidad_volumenL    VARCHAR(5)
	Declare @est_merc_idL       VARCHAR(15)
	Declare @moneda_idL         VARCHAR(20)
	Declare @costoL             NUMERIC(10,3)
	------------------------------------------------
	--			Sys_Criterios_Locator
	------------------------------------------------
	Declare @Criterio_id		as varchar(30)
	Declare @Order_id			as varchar(5)
	Declare @Forma_id			as Varchar(30)
	-------------------------------------------------



	Select 	@vDepositoId= Deposito_Default from #Temp_Usuario_loggin;

    Set @P_TIE_IN 			= @p_TieIN
    Set @PNRO_SERIE 		= @p_nro_serie
    Set @PNRO_PARTIDA 		= @p_nro_partida
    Set @PPESO 				= @P_PESO
    Set @PVOLUMEN 			= @P_VOLUMEN
    Set @PNRO_LOTE 			= @p_nro_lote
    Set @PNRO_DESPACHO 		= @p_Nro_Despacho
    Set @PNRO_BULTO 		= @p_Nro_Bulto
    Set @P_FECHA_VTO_DDE 	= @p_fecha_vencimiento
    Set @pCat_Log_ID 		= @p_CatLog_ID
	Set @PPROP1 			= @P_PROP1
	Set @PPROP2 			= @p_Prop2
	Set @PPROP3 			= @p_Prop3



	Truncate table #temp_rl_existencia_doc

	Set @pAux=Cursor For
		select 	criterio_id,order_id,forma_id
		from 	sys_criterio_locator
		where 	cliente_id =ltrim(rtrim(upper(@p_cliente_id))) 
				and producto_id =ltrim(rtrim(upper(@p_producto_id)))
				and criterio_id <> 'ORDEN_PICKING'
		order by posicion_id
	Open @pAux
	Set  @StrSqlOrderBy='ORDER BY '

	Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	While @@Fetch_Status=0
	Begin
		if @Forma_id='TO_NUMBER'
		Begin
			Set	@StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(NUMERIC(20, 5), CASE WHEN ISNUMERIC(' + @Criterio_id + ') = 1 THEN ' + @CRITERIO_ID + ' ELSE NULL END) ' + @ORDER_ID + ', '
		End
		Else
		Begin
			if @Forma_id='TO_CHAR'
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + ' ' + @Criterio_id + ' ' + @Order_id + ', '
			End
			Else
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(DATETIME, ' + ' (' + @CRITERIO_ID + ')) ' + @ORDER_ID + ', '
			End	
		End				
		Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	End --fin While @pAux

	Close @pAux
	Deallocate @pAux

	If @StrSqlOrderBy <> 'ORDER BY '
	Begin
		Set @StrSqlOrderBy = Substring(@StrSqlOrderBy, 1, Len(@StrSqlOrderBy) - 1)
	End
    Else
	Begin
		Set @StrSqlOrderBy = ''
	End

	Exec Locator_Api#Verifica_Existencia_Ubic_Mov 	@P_CLIENTE_ID		, @P_PRODUCTO_ID, 
													@P_CANTIDAD			, @p_TieIN,
													@p_nro_serie		, @p_nro_lote,
													@p_fecha_vencimiento, @p_Nro_Despacho, 
													@p_Nro_Bulto		, @p_nro_partida, 
													@p_CatLog_ID		, @P_PESO, 
													@P_VOLUMEN			, @p_Nave, 
													@P_CALLE			, @P_COLUMNA, 
													@p_Nivel			, @P_PROP1, 
													@p_Prop2			, @p_Prop3, 
													@P_UNIDAD_ID		, @P_UNIDAD_PESO, 
													@P_UNIDAD_VOLUMEN	, @p_EstMercID, 
													@p_Moneda_Id		, @p_Costo, 
													@P_DOCUMENTO_ID		, @P_DOC_TRANS_ID, 
													@p_TipoOperacion_ID	, @p_Ilimitado, 
													@P_NAVE_ID			, @P_POSICION_ID--, 
													--@cpCur

	Set @strsql = ' SELECT * FROM #TEMP_EXISTENCIA_LOCATOR_RL '

	Set @varStrIn = ' and RL.RL_id in('
	

	Set @TedCur= Cursor For
		Select * from #temp_existencia_doc order by orden asc

	Open @TedCur
	
	Fetch Next from @TedCur Into     	  @clienteidT		, @productoidT
										, @cantidadT		, @nro_serieT
										, @nro_loteT		, @fecha_vencimientoT
										, @nro_despachoT	, @nro_bultoT
										, @nro_partidaT		, @pesoT
										, @volumenT			, @tie_inT
										, @cantidad_dispT	, @codeT
										, @descriptionT		, @cat_log_idT
										, @prop1T			, @prop2T
										, @prop3T			, @unidad_idT
										, @unidad_pesoT		, @unidad_volumenT
										, @est_merc_idT		, @moneda_idT
										, @costoT			, @ordenT
	While @@Fetch_Status=0
	Begin
		

		Set @ParmDefinition=N'@inCur Cursor Output'
		Set @nSQL=N' Set @inCur= Cursor For ' + @StrSQl + 	@StrSqlOrderBy + '; Open @Incur'
		Exec sp_executesql @nSQL,@ParmDefinition,@inCur=@inCur Output

		Fetch Next From @inCur Into  @rl_idL				,@clienteidL
									,@productoidL			,@cantidadL
									,@nro_serieL			,@nro_loteL
									,@fecha_vencimientoL	,@nro_despachoL
									,@nro_bultoL			,@nro_partidaL
									,@pesoL					,@volumenL
									,@cat_log_idL			,@prop1L
									,@prop2L				,@prop3L
									,@fecha_cpteL			,@fecha_alta_gtwL
									,@unidad_idL			,@unidad_pesoL
									,@unidad_volumenL		,@est_merc_idL
									,@moneda_idL			,@costoL
		While @@Fetch_Status=0
		Begin
			If 	@ProductoidT=@ProductoidL And @Nro_serieT=@Nro_serieL And @Nro_loteT=@Nro_loteL And
				@Fecha_vencimientoT=@Fecha_vencimientoL And @Nro_despachoT=@Nro_despachoL And 
				@Nro_bultoT= @Nro_bultoL And @Nro_partidaT=@Nro_partidaL And @Cat_log_idT=@Cat_log_idL And
                @Prop1T=@Prop1L And @Prop2T=@Prop2L And @Prop3T=@Prop3L  And @Unidad_idT=@Unidad_idL And
                @Est_merc_idT=@Est_merc_idL
			Begin
                If @pLocator_Automatico = '0'
				Begin
                    Set @varStrIn = @varStrIn + Cast(@rl_idL as varchar(20)) + ', '
				End

                If @pLocator_Automatico ='1'
				Begin
                    If @varSumCantidad < @P_CANTIDAD
					Begin
						Set @varStrIn = @varStrIn + Cast(@rl_idL as varchar(20)) + ', '
                        Set @varSumCantidad = @varSumCantidad + @cantidadL
                    End
                End 
            End 

			Fetch Next From @inCur Into  @rl_idL				,@clienteidL
										,@productoidL			,@cantidadL
										,@nro_serieL			,@nro_loteL
										,@fecha_vencimientoL	,@nro_despachoL
										,@nro_bultoL			,@nro_partidaL
										,@pesoL					,@volumenL
										,@cat_log_idL			,@prop1L
										,@prop2L				,@prop3L
										,@fecha_cpteL			,@fecha_alta_gtwL
										,@unidad_idL			,@unidad_pesoL
										,@unidad_volumenL		,@est_merc_idL
										,@moneda_idL			,@costoL
		End
		Close @InCur
		Deallocate @Incur

		Fetch Next from @TedCur Into     	  @clienteidT		, @productoidT
											, @cantidadT		, @nro_serieT
											, @nro_loteT		, @fecha_vencimientoT
											, @nro_despachoT	, @nro_bultoT
											, @nro_partidaT		, @pesoT
											, @volumenT			, @tie_inT
											, @cantidad_dispT	, @codeT
											, @descriptionT		, @cat_log_idT
											, @prop1T			, @prop2T
											, @prop3T			, @unidad_idT
											, @unidad_pesoT		, @unidad_volumenT
											, @est_merc_idT		, @moneda_idT
											, @costoT			, @ordenT
	End	
	
    Set @varStrIn = Upper(Substring(@varStrIn, 1, Len(@varStrIn) - 2))
    Set @varStrIn = @varStrIn + ')'

    If @varStrIn = ' AND RL.RL_ID I)'
	Begin
        Set @varStrIn = ''
    End

	Set @StrSql1 = ' select X.* from (' + Char(13)
	Set @StrSql2 = ' SELECT DD.DOCUMENTO_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.CLIENTEID AS CLIENTEID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PRODUCTOID AS PRODUCTOID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,IsNull(SUM(TEL.CANTIDAD),0) AS CANTIDAD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.UNIDAD_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_SERIE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_LOTE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.FECHA_VENCIMIENTO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_DESPACHO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_BULTO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_PARTIDA ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PESO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.VOLUMEN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,PROD.KIT ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,DD.TIE_IN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,DD.NRO_TIE_IN_PADRE AS TIE_IN_PADRE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,DD.NRO_TIE_IN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,IsNull(N.NAVE_COD,N2.NAVE_COD) AS STORAGE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,IsNull(RL.NAVE_ACTUAL,P.NAVE_ID) AS NAVEID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,CALN.CALLE_COD AS CALLECOD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,CALN.CALLE_ID AS CALLEID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,COLN.COLUMNA_COD AS COLUMNACOD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,COLN.COLUMNA_ID AS COLUMNAID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,NN.NIVEL_COD AS NIVELCOD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,NN.NIVEL_ID AS NIVELID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.CAT_LOG_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.EST_MERC_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,RL.POSICION_ACTUAL AS POSICIONID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PROP1 ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PROP2 ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PROP3 ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.FECHA_CPTE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.FECHA_ALTA_GTW ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.RL_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.UNIDAD_PESO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.UNIDAD_VOLUMEN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.MONEDA_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.COSTO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END AS ORDEN_PICKING ' + Char(13)
	Set @StrSql3 = ' FROM RL_DET_DOC_TRANS_POSICION RL ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN DET_DOCUMENTO_TRANSACCION  DDT ON DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN DET_DOCUMENTO               DD ON DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN CLIENTE                      C ON C.CLIENTE_ID = DD.CLIENTE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN PRODUCTO                  PROD ON DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN CATEGORIA_LOGICA            CL ON DD.CLIENTE_ID = CL.CLIENTE_ID AND RL.CAT_LOG_ID = CL.CAT_LOG_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN NAVE                          N ON RL.NAVE_ACTUAL = N.NAVE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN POSICION                      P ON RL.POSICION_ACTUAL = P.POSICION_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN NAVE                         N2 ON N2.NAVE_ID = P.NAVE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN CALLE_NAVE                 CALN ON CALN.CALLE_ID = P.CALLE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN COLUMNA_NAVE               COLN ON COLN.COLUMNA_ID = P.COLUMNA_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN NIVEL_NAVE                   NN ON NN.NIVEL_ID = P.NIVEL_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN #TEMP_EXISTENCIA_LOCATOR_RL TEL ON TEL.CLIENTEID = DD.CLIENTE_ID AND TEL.PRODUCTOID = DD.PRODUCTO_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' AND rl.rl_id = tel.rl_id ' + Char(13)
	Set @StrSql4 = ' WHERE RL.RL_ID = TEL.RL_ID ' + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND CL.DISP_EGRESO = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND RL.DISPONIBLE = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(N.DISP_EGRESO, IsNull(N2.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ')) = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(P.POS_LOCKEADA, ' + Char(39) + '0' + Char(39) + ') = ' + Char(39) + '0' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(n.deposito_id, n2.deposito_Id) = ' + Char(39) + @vDepositoID + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND ' + Char(13)
	Set @StrSql4 = @StrSql4 + ' (SELECT CASE WHEN (Count(posicion_id)) > 0 THEN 1 ELSE 0 END' + Char(13)
	Set @StrSql4 = @StrSql4 + '  FROM   rl_posicion_prohibida_cliente' + Char(13)
	Set @StrSql4 = @StrSql4 + '  WHERE  Posicion_ID = IsNull(P.NAVE_ID, 0)' + Char(13)
	Set @StrSql4 = @StrSql4 + '        AND cliente_id = DD.CLIENTE_ID' + Char(13)
	Set @StrSql4 = @StrSql4 + ' ) = 0' + Char(13)
	           Set @StrSql5 = ' GROUP BY ' + Char(13)
	Set @StrSql5 = @StrSql5 + '  dd.documento_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.clienteid ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.productoid ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.nro_serie ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_lote ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Fecha_vencimiento ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_Despacho ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_Bulto ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_Partida ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Peso ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Volumen ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,prod.kit ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,dd.tie_in ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,dd.nro_tie_in_padre ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,dd.nro_tie_in ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,n.nave_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,n2.nave_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,rl.nave_actual ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,p.nave_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,caln.calle_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,caln.calle_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,coln.columna_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,coln.columna_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,nn.nivel_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,nn.nivel_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.cat_log_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,rl.posicion_actual ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.est_merc_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.prop1 ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.prop2 ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.prop3 ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.fecha_cpte ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.fecha_alta_gtw ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.rl_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.unidad_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.unidad_peso ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.unidad_volumen ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.moneda_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.costo ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END ' + Char(13)
	Set @NtrSql2 = ' UNION ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' SELECT DD.DOCUMENTO_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.CLIENTEID AS CLIENTEID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PRODUCTOID AS PRODUCTOID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,IsNull(SUM(TEL.CANTIDAD),0) AS CANTIDAD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.UNIDAD_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_SERIE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_LOTE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.FECHA_VENCIMIENTO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_DESPACHO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_BULTO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_PARTIDA ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PESO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.VOLUMEN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,PROD.KIT ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,DD.TIE_IN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,DD.NRO_TIE_IN_PADRE AS TIE_IN_PADRE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,DD.NRO_TIE_IN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,IsNull(N.NAVE_COD,N2.NAVE_COD) AS STORAGE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,IsNull(RL.NAVE_ANTERIOR,P.NAVE_ID) AS NAVEID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,CALN.CALLE_COD AS CALLECOD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,CALN.CALLE_ID AS CALLEID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,COLN.COLUMNA_COD AS COLUMNACOD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,COLN.COLUMNA_ID AS COLUMNAID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,NN.NIVEL_COD AS NIVELCOD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,NN.NIVEL_ID AS NIVELID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.CAT_LOG_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.EST_MERC_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,RL.POSICION_ANTERIOR AS POSICIONID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PROP1 ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PROP2 ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PROP3 ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.FECHA_CPTE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.FECHA_ALTA_GTW ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.RL_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.UNIDAD_PESO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.UNIDAD_VOLUMEN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.MONEDA_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.COSTO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END AS ORDEN_PICKING ' + Char(13)
	Set @NtrSql3 = ' FROM RL_DET_DOC_TRANS_POSICION RL ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN DET_DOCUMENTO_TRANSACCION  DDT ON DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN DET_DOCUMENTO               DD ON DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN CLIENTE                      C ON C.CLIENTE_ID = DD.CLIENTE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN PRODUCTO                  PROD ON DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN CATEGORIA_LOGICA            CL ON DD.CLIENTE_ID = CL.CLIENTE_ID AND RL.CAT_LOG_ID_FINAL = CL.CAT_LOG_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN NAVE                          N ON RL.NAVE_ANTERIOR = N.NAVE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN POSICION                      P ON RL.POSICION_ANTERIOR = P.POSICION_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN NAVE                         N2 ON N2.NAVE_ID = P.NAVE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN CALLE_NAVE                 CALN ON CALN.CALLE_ID = P.CALLE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN COLUMNA_NAVE               COLN ON COLN.COLUMNA_ID = P.COLUMNA_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN NIVEL_NAVE                   NN ON NN.NIVEL_ID = P.NIVEL_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN #TEMP_EXISTENCIA_LOCATOR_RL TEL ON TEL.CLIENTEID = DD.CLIENTE_ID AND TEL.PRODUCTOID = DD.PRODUCTO_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' AND rl.rl_id = tel.rl_id ' + Char(13)
	Set @NtrSql4 = ' WHERE RL.RL_ID = TEL.RL_ID ' + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND CL.DISP_EGRESO = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND RL.DISPONIBLE = ' + Char(39) + '0' + Char(39) + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND IsNull(N.DISP_EGRESO, IsNull(N2.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ')) = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND IsNull(P.POS_LOCKEADA, ' + Char(39) + '1' + Char(39) + ') = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(n.deposito_id, n2.deposito_Id) = ' + Char(39) + @vDepositoID + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND ' + Char(13)
	Set @StrSql4 = @StrSql4 + ' (SELECT CASE WHEN (Count(posicion_id)) > 0 THEN 1 ELSE 0 END' + Char(13)
	Set @StrSql4 = @StrSql4 + '  FROM   rl_posicion_prohibida_cliente' + Char(13)
	Set @StrSql4 = @StrSql4 + '  WHERE  Posicion_ID = IsNull(P.NAVE_ID, 0)' + Char(13)
	Set @StrSql4 = @StrSql4 + '         AND cliente_id = DD.CLIENTE_ID' + Char(13)
	Set @StrSql4 = @StrSql4 + '  ) = 0' + Char(13)
	Set @NtrSql5 = ' GROUP BY ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + '  dd.documento_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.clienteid ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.productoid ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.nro_serie ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_lote ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Fecha_vencimiento ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_Despacho ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_Bulto ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_Partida ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Peso ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Volumen ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,prod.kit ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,dd.tie_in ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,dd.nro_tie_in_padre ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,dd.nro_tie_in ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,n.nave_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,n2.nave_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,rl.nave_ANTERIOR ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,p.nave_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,caln.calle_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,caln.calle_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,coln.columna_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,coln.columna_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,nn.nivel_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,nn.nivel_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.cat_log_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,rl.posicion_ANTERIOR ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.est_merc_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.prop1 ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.prop2 ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.prop3 ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.fecha_cpte ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.fecha_alta_gtw ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.rl_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.unidad_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.unidad_peso ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.unidad_volumen ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.moneda_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.costo ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END  ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ') x '

	Set @pAux=Cursor For
	select 	criterio_id,order_id,forma_id
	from 	sys_criterio_locator
	where 	cliente_id =ltrim(rtrim(upper(@p_cliente_id))) 
			and producto_id =ltrim(rtrim(upper(@p_producto_id)))
	order by posicion_id

	Open @pAux
	Set  @StrSqlOrderBy='ORDER BY '

	Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	While @@Fetch_Status=0
	Begin
		if @Forma_id='TO_NUMBER'
		Begin
			Set	@StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(NUMERIC(20, 5), CASE WHEN ISNUMERIC(' + @Criterio_id + ') = 1 THEN ' + @CRITERIO_ID + ' ELSE NULL END) ' + @ORDER_ID + ', '
		End
		Else
		Begin
			if @Forma_id='TO_CHAR'
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + ' ' + @Criterio_id + ' ' + @Order_id + ', '
			End
			Else
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(DATETIME, ' + ' (' + @CRITERIO_ID + ')) ' + @ORDER_ID + ', '
			End	
		End				
		Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	End --fin While @pAux

	Close @pAux
	Deallocate @pAux

	If @StrSqlOrderBy <> 'ORDER BY '
	Begin
		Set @StrSqlOrderBy = Substring(@StrSqlOrderBy, 1, Len(@StrSqlOrderBy) - 1)
	End
    Else
	Begin
		Set @StrSqlOrderBy = ''
	End

	Set @xSQL=' Insert into #Tmp_Q2 '
	Set @xSQL= @xSQL + @StrSql1
	Set @xSQL= @xSQL + @StrSql2
	Set @xSQL= @xSQL + @StrSql3
	Set @xSQL= @xSQL + @StrSql4
	Set @xSQL= @xSQL + @varStrIn
	Set @xSQL= @xSQL + @StrSql5
	Set @xSQL= @xSQL + @NtrSql2
	Set @xSQL= @xSQL + @NtrSql3
	Set @xSQL= @xSQL + @NtrSql4
	Set @xSQL= @xSQL + @varStrIn
	Set @xSQL= @xSQL + @NtrSql5
	Set @xSQL= @xSQL + @StrSqlOrderBy

	Execute (@xSQL)

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

ALTER PROCEDURE [dbo].[LOCATOR_ING_X_ALTURA]
  @DOCUMENTO_ID		AS NUMERIC(20),
  @NRO_LINEA		AS NUMERIC(20),
  @NROPALLET 		AS VARCHAR(100),
  @CANT			    AS NUMERIC(5,0) -- CANTDAD DE CAJAS APILADAS EN EL PALLET
AS
  DECLARE @CLIENTEID 		AS VARCHAR(15)
  DECLARE @PRODUCTOID		AS VARCHAR(30)
  DECLARE @VCANT 		    AS NUMERIC(20)
  ----VARIABLES PARA HACER INSERT
  DECLARE @POSICION_ID 		AS NUMERIC(20,0)
  DECLARE @POSICION_COD 	AS VARCHAR(45)
  DECLARE @NAVE_ID			AS NUMERIC(20,0)
  DECLARE @ORDEN_LOCATOR 	AS NUMERIC(6)
  DECLARE @CASO				AS INT
  DECLARE @ALTURA			AS NUMERIC(10,3)
  DECLARE @ALTO_PALLET		AS NUMERIC(6,3) --FIJO ES LA ALTURA DEL PALLET APROX 20 CM

BEGIN

	IF @DOCUMENTO_ID IS NULL
	BEGIN
			RAISERROR ('EL PARAMETRO @DOCUMENTO_ID NO PUEDE SER NULO. SQLSERVER', 16, 1)
	END
	IF @NRO_LINEA IS NULL
	BEGIN
			RAISERROR ('EL PARAMETRO @NRO_LINEA NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END
	IF @NROPALLET IS NULL
	BEGIN
			RAISERROR ('EL PARAMETRO @NROPALLET NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END

	IF @CANT IS NULL
	BEGIN
			RAISERROR ('EL PARAMETRO @CANT NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END

	DELETE FROM SYS_LOCATOR_ING WHERE POSICION_ID IS NULL AND NAVE_ID IS NULL
	DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA;

	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ

	SELECT 	@CLIENTEID=DD.CLIENTE_ID,@PRODUCTOID=DD.PRODUCTO_ID
	FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
			ON (DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN NAVE N
			ON(RL.NAVE_ACTUAL=N.NAVE_ID)
	WHERE	DD.PROP1=UPPER(LTRIM(RTRIM(@NROPALLET)))
			AND D.STATUS IN('D30','D35')
			AND (N.PRE_INGRESO='1' OR N.INTERMEDIA='1')

	BEGIN
		SELECT 	@VCANT=COUNT(*)
		FROM 	  RL_PRODUCTO_POSICION_PERMITIDA
		WHERE 	CLIENTE_ID=@CLIENTEID AND PRODUCTO_ID=@PRODUCTOID
		  
		SELECT @ALTO_PALLET = ISNULL(CONVERT(NUMERIC(6,3),VALOR), 0.20) FROM SYS_PARAMETRO_PROCESO WHERE PROCESO_ID = 'WMOV' AND SUBPROCESO_ID = 'UBIC_PALLET' AND PARAMETRO_ID = 'ALT_BASE_PALLET'
		SELECT @ALTURA      = ((ALTO * @CANT)+ @ALTO_PALLET) FROM PRODUCTO WHERE PRODUCTO_ID = @PRODUCTOID
				
	
		IF @VCANT > 0
		BEGIN
			SELECT  TOP 1
					 @POSICION_ID   =X.POSICION_ID
					,@POSICION_COD  =X.POSICION_COD
					,@NAVE_ID       =X.NAVE_ID
					,@ORDEN_LOCATOR =X.ORDENLOCATOR
					,@CASO          =X1
			FROM(   SELECT   P.POSICION_ID					AS POSICION_ID
					        ,P.POSICION_COD					AS POSICION_COD
					        ,NULL							AS NAVE_ID
					        ,ISNULL(P.ORDEN_LOCATOR,99999)	AS ORDENLOCATOR
					        ,1								AS X1
							,ISNULL(X.CONT_UBIC,0)			AS CONT_UBIC
				    FROM 	POSICION P INNER JOIN
						    RL_PRODUCTO_POSICION_PERMITIDA RLPP
							ON(P.POSICION_ID=RLPP.POSICION_ID)
							LEFT JOIN(  SELECT	COUNT(P.COLUMNA_ID)CONT_UBIC, P.COLUMNA_ID
										FROM	POSICION P INNER JOIN RL_DET_DOC_TRANS_POSICION RL
												ON(P.POSICION_ID=RL.POSICION_ACTUAL) INNER JOIN
												RL_PRODUCTO_POSICION_PERMITIDA RLPP2
												ON(RL.POSICION_ACTUAL=RLPP2.POSICION_ID)
										WHERE   RL.POSICION_ACTUAL IS NOT NULL
												AND RLPP2.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID)))
												AND RLPP2.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
										GROUP BY
												P.COLUMNA_ID
							)X ON(X.COLUMNA_ID=P.COLUMNA_ID)                        
					WHERE	P.POS_VACIA='1' AND P.POS_LOCKEADA='0'
							AND P.POSICION_ID NOT IN(SELECT 	ISNULL(POSICION_ID,0)FROM SYS_LOCATOR_ING)
							AND RLPP.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID))) 
							AND RLPP.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
							AND ((@ALTURA IS NULL) OR (@ALTURA <= P.ALTO))
					UNION ALL
					SELECT 	 NULL							AS POSICION_ID
							,N.NAVE_COD						AS POSICION_COD
							,N.NAVE_ID						AS NAVE_ID
							,ISNULL(N.ORDEN_LOCATOR,99999)	AS ORDENLOCATOR
							,0								AS X1
							,0								AS CONT_UBIC
					FROM 	NAVE N INNER JOIN
							RL_PRODUCTO_POSICION_PERMITIDA RLPP
							ON(N.NAVE_ID=RLPP.NAVE_ID)
					WHERE	N.DISP_INGRESO='1' AND N.PRE_INGRESO='0' 
							AND PRE_EGRESO='0'
							AND RLPP.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID)))
							AND RLPP.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
			)AS X
			ORDER BY 
					ISNULL(X.CONT_UBIC,0) DESC,X.ORDENLOCATOR ASC
		END		
		ELSE
		BEGIN
			SELECT   TOP 1
					 @POSICION_ID=X.POSICION_ID
					,@POSICION_COD=X.POSICION_COD
					,@NAVE_ID=X.NAVE_ID
					,@ORDEN_LOCATOR=X.ORDENLOCATOR
					,@CASO=X1
			FROM(   SELECT 	 P.POSICION_ID  AS POSICION_ID
							,POSICION_COD AS POSICION_COD
							,NULL AS NAVE_ID
							,ISNULL(ORDEN_LOCATOR,99999) AS ORDENLOCATOR
							,1 AS X1
							,X.CONT_UBIC
					FROM 	POSICION P 
							LEFT JOIN RL_DET_DOC_TRANS_POSICION TP 
							ON (P.POSICION_ID = TP.POSICION_ACTUAL)
							LEFT JOIN(  SELECT	COUNT(P.COLUMNA_ID)CONT_UBIC, P.COLUMNA_ID
										FROM	POSICION P INNER JOIN RL_DET_DOC_TRANS_POSICION RL
												ON(P.POSICION_ID=RL.POSICION_ACTUAL) 
												INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
												ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
												INNER JOIN DET_DOCUMENTO DD
												ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
										WHERE   RL.POSICION_ACTUAL IS NOT NULL
												AND DD.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTOID)))
												AND DD.CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTEID)))
										GROUP BY
												P.COLUMNA_ID
							)X ON(X.COLUMNA_ID=P.COLUMNA_ID)                          
					WHERE	P.POS_VACIA='1' AND P.POS_LOCKEADA='0'
							AND P.POSICION_ID NOT IN(	SELECT POSICION_ID FROM SYS_LOCATOR_ING)
							AND TP.POSICION_ACTUAL IS NULL
					UNION ALL
					SELECT 	 NULL AS POSICION_ID
							,NAVE_COD AS POSICION_COD
							,NAVE_ID  AS NAVE_ID
							,ISNULL(ORDEN_LOCATOR,99999) AS ORDENLOCATOR
							,0 AS X1
							,NULL CONT_UBIC
					FROM 	NAVE N
					WHERE	N.DISP_INGRESO='1' 
							AND N.PRE_INGRESO='0' 
							AND PRE_EGRESO='0'
							AND NAVE_TIENE_LAYOUT='0'
				)AS X
			ORDER BY 
                 ISNULL(X.CONT_UBIC,0) DESC,X.ORDENLOCATOR
		END
		
		BEGIN TRANSACTION
		IF @CASO=1
		BEGIN
			DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
			IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
			BEGIN
				RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PALLET.',16,1)
			END

			INSERT INTO  SYS_LOCATOR_ING (DOCUMENTO_ID, NRO_LINEA, NRO_PALLET, POSICION_ID)
			VALUES (@DOCUMENTO_ID, @NRO_LINEA, @NROPALLET, @POSICION_ID )
		END
		ELSE
		BEGIN
			DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

			IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
			BEGIN
				RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PALLET.',16,1)
			END

			INSERT INTO  SYS_LOCATOR_ING (DOCUMENTO_ID, NRO_LINEA, NRO_PALLET, NAVE_ID)
			VALUES (@DOCUMENTO_ID, @NRO_LINEA, @NROPALLET, @NAVE_ID )
		END
		COMMIT TRANSACTION
		SELECT 	@POSICION_ID	AS POSICION_ID, 
				@POSICION_COD	AS POSICION_COD,
				@NAVE_ID		AS NAVE_ID, 
				@ORDEN_LOCATOR	AS ORDEN_LOCATOR		
	END
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

ALTER PROCEDURE [dbo].[Locator_Transferencia]
	@UbicacionOrigen	varchar(50)
--	@NroPallet			varchar(100)
As
Begin
	Declare @CantProducto 	as Float
	Declare @Cliente_ID 		as varchar(15)
	Declare @Producto_id	as varchar(30)
	Declare @vCant			as float		--con esto puedo conocer si tiene posiciones permitidas o no.
	declare @posicion_id 	as numeric(20,0)
	declare @posicion_cod 	as varchar(45)
	declare @nave_id		as numeric(20,0)
	declare @orden_locator 	as numeric(6)
	declare @caso			as int

	select 	distinct
			@CantProducto=count(dd.producto_id), @cliente_id=dd.cliente_id, @producto_id=dd.producto_id
	from	det_documento dd inner join det_documento_transaccion ddt 	on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
			inner join rl_det_doc_trans_posicion rl 						on(ddt.doc_trans_id=rl.doc_trans_id and ddt.nro_linea_trans=rl.nro_linea_trans)
			left join posicion p											on(rl.posicion_actual=p.posicion_id)
			left join nave n												on(rl.nave_actual=n.nave_id)
			left join estado_mercaderia_rl em							on(rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
			inner join categoria_logica cl									on(rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id)
	where	((p.posicion_cod=@UbicacionOrigen) or(n.nave_cod=@UbicacionOrigen))
			and cl.disp_transf='1'
			and ((em.disp_transf is null) or (em.disp_transf='1'))
			and rl.disponible='1'
	group by dd.producto_id, dd.cliente_id
--dd.prop1=@NroPallet			and 
	
	select 	@vcant=count(*)
	from 	rl_producto_posicion_permitida
	where 	cliente_id=@cliente_id and producto_id=@producto_id
			and cliente_id is not null and producto_id is not null

	if @vcant > 0
		begin
			select top 1
					 @posicion_id=x.posicion_id
					,@posicion_cod=x.posicion_cod
					,@nave_id=x.nave_id
					,@orden_locator=x.ordenlocator
					,@caso=x1
			from(	select 	 Top 5
							 p.posicion_id  as posicion_id
							,p.posicion_cod as posicion_cod
							,null as nave_id
							,isnull(p.orden_locator,999999) as ordenlocator
							,1 as x1
					from 	posicion p inner join
							rl_producto_posicion_permitida rlpp
							on(p.posicion_id=rlpp.posicion_id)
					where	p.pos_lockeada='0'
							--p.pos_vacia='1' 
							--and rlpp.posicion_id not in(	select 	isnull(posicion_id,0)	from 	sys_locator_ing)
							--and rlpp.posicion_id not in(select posicion_actual from rl_det_doc_trans_posicion where posicion_actual is not null)
							--and rlpp.producto_id=ltrim(rtrim(upper(@producto_id))) 
							and rlpp.cliente_id=ltrim(rtrim(upper(@cliente_id)))
							--and p.posicion_cod<>@UbicacionOrigen
					union all
					select 	top 5
							 null as 	posicion_id
							,n.nave_cod as posicion_cod
							,n.nave_id  as nave_id
							,isnull(n.orden_locator,999999) as ordenlocator
							,0 as x1
					from 	nave n inner join
							rl_producto_posicion_permitida rlpp
							on(n.nave_id=rlpp.nave_id)
					where	n.disp_transf='1' and n.pre_ingreso='0' 
							and pre_egreso='0'
							--and rlpp.producto_id=ltrim(rtrim(upper(@producto_id)))
							and rlpp.cliente_id=ltrim(rtrim(upper(@cliente_id)))
							and nave_cod<>@UbicacionOrigen
			
			)as x
			order by x.ordenlocator asc
			/*
				DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
				IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
				BEGIN
					RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PALLET.',16,1)
				END
				INSERT INTO  SYS_LOCATOR_ING (DOCUMENTO_ID, NRO_LINEA, NRO_PALLET, POSICION_ID)
				VALUES (@DOCUMENTO_ID, @NRO_LINEA, @NROPALLET, @POSICION_ID )
			*/
			If @posicion_cod is not null
			begin
				select 	@posicion_id as posicion_id, @posicion_cod as posicion_cod,@nave_id as nave_id, @orden_locator as orden_locator
			end
			else
			begin
				raiserror('No quedan Ubicaciones disponibles para el pallet',16,1)
				return
			end
		end		
	else
		begin
				select top 1
						 @posicion_id=x.posicion_id
						,@posicion_cod=x.posicion_cod
						,@nave_id=x.nave_id
						,@orden_locator=x.ordenlocator
						,@caso=x1
				from(	select 	 posicion_id  as posicion_id
								,posicion_cod as posicion_cod
								,null as nave_id
								,isnull(orden_locator,999999) as ordenlocator
								,1 as x1
						from 	posicion p
						where	'1'='1' and p.pos_lockeada='0'
								--and p.posicion_cod not in (@UbicacionOrigen)
						union all
						select 	 null as posicion_id
								,nave_cod as posicion_cod
								,nave_id  as nave_id
								,isnull(orden_locator,999999) as ordenlocator
								,0 as x1
						from 	nave n
						where	n.disp_transf='1' and n.pre_ingreso='0' 
								and pre_egreso='0'
								and nave_tiene_layout='0'
								and nave_cod<>@UbicacionOrigen
				)as x
				order by x.ordenlocator
			select 	@posicion_id as posicion_id, @posicion_cod as posicion_cod,@nave_id as nave_id, @orden_locator as orden_locator
	end
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

ALTER        Procedure [dbo].[LocatorEgreso]
@pDocumento_id 		as Numeric(20,0) Output,
@pViaje_id			as varchar(100) Output
As
Begin

declare @Fecha_Vto				as datetime
declare @OrdenPicking			as numeric(10,0)
declare @Tipo_Posicion			as varchar(10)
declare @Codigo_Posicion		as varchar(100)
declare @Cliente_id				as varchar(15)
declare @Producto_id			as varchar(30)
declare @Cantidad				as numeric(20,5)
declare @Aux					as varchar(50)
declare @NewProducto			as varchar(30)
declare @OldProducto			as varchar(30)
declare @vQtyResto				as numeric(20,5)
declare @vRl_id					as numeric(20)
declare @QtySol					as numeric(20,5)
declare @vNroLinea				as numeric(20)
declare @NRO_BULTO				as varchar(50)
declare @NRO_LOTE				as varchar(50)
declare @EST_MERC_ID			as varchar(15)
declare @NRO_DESPACHO			as varchar(50)
declare @NRO_PARTIDA			as varchar(50)
declare @UNIDAD_ID				as varchar(5)
declare @PROP1					as varchar(100)
declare @PROP2					as varchar(100)
declare @PROP3					as varchar(100)
declare @DESC					as varchar(200)
declare @CAT_LOG_ID				as varchar(50)
declare @id						as numeric(20,0)
declare @Documento_id 			as Numeric(20,0)
declare @Saldo					as numeric(20,5)
declare @TipoSaldo				as varchar(20)
declare @Doc_Trans 				as numeric(20)
declare @QtyDetDocumento		as numeric(20)
declare @vUsuario_id			as varchar(50)
declare @vTerminal				as varchar(50)
declare @RsExist				as Cursor
declare @RsActuRL				as Cursor
declare @Crit1					as varchar(30)
declare @Crit2					as varchar(30)
declare @Crit3					as varchar(30)
declare @fecha_alta_gtw			as datetime
declare @nro_serie				as varchar(50)
declare @NewLoteProveedor			as varchar(100)
declare @OldLoteProveedor			as varchar(100)
declare @NewNroPartida			as varchar(100)
declare @OldNroPartida			as varchar(100)
declare @NewNroSerie			as varchar(50)
declare @OldNroSerie			as varchar(50)
declare @RSDOCEGR				as cursor
declare @DOCIDPIVOT				as numeric(20,0)
declare @NROLINEAPIVOT			as numeric(20,0)

SET NOCOUNT ON;
SET @vNroLinea = 0
--Obtengo los criterios de ordenamiento.
Select	@Crit1=CRITERIO_1, @Crit2=CRITERIO_2, @Crit3=CRITERIO_3
From	RL_CLIENTE_LOCATOR
Where	Cliente_id=(select Cliente_id from documento where documento_id=@pDocumento_id)

if (@Crit1 is null) and (@Crit2 is null) and (@Crit3 is null)
begin
	--Si todos son nulos entonces x default salgo con orden de picking.
	Set @Crit1='ORDEN_PICKING'
end

select @Cliente_id = cliente_id from documento where documento_id = @pDocumento_id

SET @RSDOCEGR = CURSOR FOR
SELECT DOCUMENTO_ID, NRO_LINEA FROM DET_DOCUMENTO WHERE DOCUMENTO_ID = @pDocumento_id

OPEN @RSDOCEGR
FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @QtySol=0
	set @QtySol=dbo.GetQtySol(@pDocumento_id,@NROLINEAPIVOT,@Cliente_id)
	set @vQtyResto=@QtySol

	Set @RsExist = Cursor For
		Select	X.*
		from	(
			SELECT	 dd.fecha_vencimiento
					,isnull(p.orden_picking,999) as ORDEN_PICKING
					,'POS' as ubicacion
					,p.posicion_cod as posicion
					,dd.cliente_id
					,dd.producto_id as producto
					,rl.cantidad
					,rl.rl_id
					,dd.NRO_BULTO
					,dd.NRO_LOTE
					,RL.EST_MERC_ID
					,dd.NRO_DESPACHO
					,dd.NRO_PARTIDA
					,dd.UNIDAD_ID
					,dd.PROP1
					,dd.PROP2
					,dd.PROP3
					,dd.DESCRIPCION
					,RL.CAT_LOG_ID
					,d.fecha_alta_gtw
					,dd.nro_serie
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join posicion p on (rl.posicion_actual=p.posicion_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
					inner join documento d on(dd.documento_id=d.documento_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and p.pos_lockeada='0' and p.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
					--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id)
					and exists (select 1 from det_documento ddegr
								where	ddegr.documento_id = @pDocumento_id AND ddegr.nro_linea = @NROLINEAPIVOT
										and ddegr.producto_id = dd.producto_id
										and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
					and d.cliente_id = @cliente_id
			UNION
			SELECT	 dd.fecha_vencimiento
					,isnull(n.orden_locator,999) as ORDEN_PICKING
					,'NAV' as ubicacion
					,n.nave_cod as posicion
					,dd.cliente_id
					,dd.producto_id as producto
					,rl.cantidad
					,rl.rl_id
					,dd.NRO_BULTO
					,dd.NRO_LOTE
					,RL.EST_MERC_ID
					,dd.NRO_DESPACHO
					,dd.NRO_PARTIDA
					,dd.UNIDAD_ID
					,dd.PROP1
					,dd.PROP2
					,dd.PROP3
					,dd.DESCRIPCION
					,RL.CAT_LOG_ID
					,d.fecha_alta_gtw
					,dd.nro_serie
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join nave n on (rl.nave_actual=n.nave_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
					inner join documento d on(dd.documento_id=d.documento_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR'
					and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id)
					and exists (select 1 from det_documento ddegr
								where	ddegr.documento_id = @pDocumento_id AND ddegr.nro_linea = @NROLINEAPIVOT
										and ddegr.producto_id = dd.producto_id
										and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
					and d.cliente_id = @cliente_id
			)X		
			order by--order by producto,dd.fecha_vencimiento asc,orden  
					(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
					(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
					(CASE WHEN @Crit1 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
					(CASE WHEN @Crit1 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
					(CASE WHEN @Crit1 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
					(CASE WHEN @Crit1 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
					(CASE WHEN @Crit1 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
					(CASE WHEN @Crit1 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
					(CASE WHEN @Crit1 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
					(CASE WHEN @Crit1 = 'PROP1'				THEN x.PROP1 END),
					(CASE WHEN @Crit1 = 'PROP2'				THEN x.PROP2 END),
					(CASE WHEN @Crit1 = 'PROP3'				THEN x.PROP3 END),
					(CASE WHEN @Crit1 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
					(CASE WHEN @Crit1 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
					 --2
					(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
					(CASE WHEN @Crit2 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
					(CASE WHEN @Crit2 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
					(CASE WHEN @Crit2 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
					(CASE WHEN @Crit2 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
					(CASE WHEN @Crit2 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
					(CASE WHEN @Crit2 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
					(CASE WHEN @Crit2 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
					(CASE WHEN @Crit2 = 'PROP1'				THEN x.PROP1 END),
					(CASE WHEN @Crit2 = 'PROP2'				THEN x.PROP2 END),
					(CASE WHEN @Crit2 = 'PROP3'				THEN x.PROP3 END),
					(CASE WHEN @Crit2 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
					(CASE WHEN @Crit2 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
					--3
					(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
					(CASE WHEN @Crit3 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
					(CASE WHEN @Crit3 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
					(CASE WHEN @Crit3 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
					(CASE WHEN @Crit3 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
					(CASE WHEN @Crit3 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
					(CASE WHEN @Crit3 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
					(CASE WHEN @Crit3 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
					(CASE WHEN @Crit3 = 'PROP1'				THEN x.PROP1 END),
					(CASE WHEN @Crit3 = 'PROP2'				THEN x.PROP2 END),
					(CASE WHEN @Crit3 = 'PROP3'				THEN x.PROP3 END),
					(CASE WHEN @Crit3 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
					(CASE WHEN @Crit3 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END)
			
	Open @RsExist
	Fetch Next From @RsExist into	@Fecha_Vto,
									@OrdenPicking,
									@Tipo_Posicion,
									@Codigo_Posicion,
									@Cliente_id,
									@Producto_id,
									@Cantidad,
									@vRl_id,
									@NRO_BULTO,
									@NRO_LOTE,				
									@EST_MERC_ID,			
									@NRO_DESPACHO,		
									@NRO_PARTIDA,			
									@UNIDAD_ID,			
									@PROP1,					
									@PROP2,					
									@PROP3,
									@DESC,
									@CAT_LOG_ID,
									@fecha_alta_gtw,
									@nro_serie


	While @@Fetch_Status=0 AND @vQtyResto>0
	Begin	

		if (@vQtyResto>0) begin   
				if (@vQtyResto>=@Cantidad) begin
					set @vNroLinea=@vNroLinea+1
					set @vQtyResto=@vQtyResto-@Cantidad
					insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
								values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
					--Insert con todas las propiedades en det_documento
					insert into det_documento_aux 
							(	documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
								cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
								unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
					values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
							,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
							,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)		
				end
				else begin
					set @vNroLinea=@vNroLinea+1
					insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
								values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')
					--Insert con todas las propiedades en det_documento
					insert into det_documento_aux (
								documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
								cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
								unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
								values 
								(@pDocumento_id,@vNroLinea
								,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
								,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
								,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
					set @vQtyResto=0
				end --if
		end --if
		Fetch Next From @RsExist into	@Fecha_Vto,
										@OrdenPicking,
										@Tipo_Posicion,
										@Codigo_Posicion,
										@Cliente_id,
										@Producto_id,
										@Cantidad,
										@vRl_id,
										@NRO_BULTO,
										@NRO_LOTE,				
										@EST_MERC_ID,			
										@NRO_DESPACHO,		
										@NRO_PARTIDA,			
										@UNIDAD_ID,			
										@PROP1,					
										@PROP2,					
										@PROP3,
										@DESC,
										@CAT_LOG_ID,
										@fecha_alta_gtw,
										@nro_serie
	End	--End While @RsExist.

	CLOSE @RsExist
	DEALLOCATE @RsExist
	
	
	FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT
END
CLOSE @RSDOCEGR
DEALLOCATE @RSDOCEGR


--GUARDO SERIES INICIALES
--SELECT DISTINCT NRO_SERIE INTO #TMPSERIES FROM DET_DOCUMENTO WHERE DOCUMENTO_ID = @pDocumento_id

--Borro det_documento y lo vuelvo a insertar con las nuevas propiedades
delete det_documento where documento_id=@pDocumento_id
insert into det_documento select * from det_documento_aux where documento_id=@pDocumento_id


update documento set status='D20' where documento_id=@pDocumento_id
Exec Asigna_Tratamiento#Asigna_Tratamiento_EGR @pDocumento_id
select distinct @Doc_Trans=doc_trans_id from det_documento_transaccion where documento_id=@pDocumento_id
--Hago la reserva en RL
Set @RsActuRL = Cursor For select [id],documento_id,Nro_Linea,Cliente_id,Producto_id,Cantidad,rl_id,saldo,tipo from consumo_locator_egr where procesado='N' and Documento_id=@pDocumento_id
Open @RsActuRL
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

While @@Fetch_Status=0
Begin
	if (@Saldo=0) begin
		update rl_det_doc_trans_posicion set doc_trans_id_egr=@Doc_Trans, nro_linea_trans_egr=@vNroLinea,disponible='0'
														,cat_log_id='TRAN_EGR',nave_anterior=nave_actual,posicion_anterior=posicion_actual
														,nave_actual='2',posicion_actual=null where rl_id=@vRl_id
		update consumo_locator_egr set procesado='S' where [id]=@id
	end --if	

	if (@Saldo>0) begin
		insert into rl_det_doc_trans_posicion (doc_trans_id,nro_linea_trans,posicion_anterior,posicion_actual,cantidad,tipo_movimiento_id,
															ultima_estacion,ultima_secuencia,nave_anterior,nave_actual,documento_id,nro_linea,
															disponible,doc_trans_id_egr,nro_linea_trans_egr,doc_trans_id_tr,nro_linea_trans_tr,
															cliente_id,cat_log_id,cat_log_id_final,est_merc_id)
					  select doc_trans_id,nro_linea_trans,posicion_anterior,posicion_actual,@Saldo,tipo_movimiento_id,
								ultima_estacion,ultima_secuencia,nave_anterior,nave_actual,documento_id,nro_linea,
								disponible,doc_trans_id_egr,nro_linea_trans_egr,doc_trans_id_tr,nro_linea_trans_tr,
								cliente_id,cat_log_id,cat_log_id_final,est_merc_id
					  from rl_det_doc_trans_posicion 
					  where rl_id=@vRl_id 	
		update rl_det_doc_trans_posicion set cantidad=@Cantidad,doc_trans_id_egr=@Doc_Trans, nro_linea_trans_egr=@vNroLinea,disponible='0'
														,cat_log_id='TRAN_EGR',nave_anterior=nave_actual,posicion_anterior=posicion_actual
														,nave_actual='2',posicion_actual=null where rl_id=@vRl_id
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

--Si no hay existencia de ningun producto del documento lo borro para que no quede solo cabecera
select @QtyDetDocumento=count(documento_id) from det_documento where documento_id=@pDocumento_id
if (@QtyDetDocumento=0) begin
	delete documento where documento_id=@pDocumento_id 
end else begin
	select @vUsuario_id=usuario_id, @vTerminal=Terminal from #temp_usuario_loggin
	insert into docxviajesprocesados values (@pViaje_id,@pDocumento_id,'P',getdate(),@vUsuario_id,@vTerminal)
end --if


Set NoCount Off;
End -- Fin Procedure.
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

ALTER        Procedure [dbo].[LocatorEgreso_RemanenteDoc]
@pDocumento_id 	as Numeric(20,0) Output,
@pCliente_id	as varchar(15) Output,
@pViaje_id		as varchar(100) Output,
@vNroLinea		as Numeric(20,0) Output,
@pProducto_id   as varchar(100) Output,
@pCantRem		as Numeric(20,0) Output,
@Crit1			as varchar(30) Output,
@Crit2			as varchar(30) Output,
@Crit3			as varchar(30) Output			

As
Begin

declare @Fecha_Vto				as datetime
declare @OrdenPicking			as numeric(10,0)
declare @Tipo_Posicion			as varchar(10)
declare @Codigo_Posicion		as varchar(100)
declare @Cliente_id				as varchar(15)
declare @Producto_id			as varchar(30)
declare @Cantidad				as numeric(20,5)
declare @Aux					as varchar(50)
declare @NewProducto			as varchar(30)
declare @OldProducto			as varchar(30)
declare @vQtyResto				as numeric(20,5)
declare @vRl_id					as numeric(20)
declare @QtySol					as numeric(20,5)
declare @NRO_BULTO				as varchar(50)
declare @NRO_LOTE				as varchar(50)
declare @EST_MERC_ID			as varchar(15)
declare @NRO_DESPACHO			as varchar(50)
declare @NRO_PARTIDA			as varchar(50)
declare @UNIDAD_ID				as varchar(5)
declare @PROP1					as varchar(100)
declare @PROP2					as varchar(100)
declare @PROP3					as varchar(100)
declare @DESC					as varchar(200)
declare @CAT_LOG_ID				as varchar(50)
declare @Fecha_Alta_GTW			as datetime
declare @RsRem			        as Cursor
declare @auxErr					as varchar(4000)
declare @nro_serie				as varchar(50)


SET NOCOUNT ON;

		set @vQtyResto = @pCantRem  
		set @QtySol = @pCantRem  
		--set @vNroLinea=0
		Set @RsRem = Cursor For
			Select	X.*
			From	(
				SELECT	 dd.fecha_vencimiento
						,isnull(p.orden_picking,99999) as ORDEN_PICKING
						,'POS' as ubicacion
						,p.posicion_cod as posicion
						,dd.cliente_id
						,dd.producto_id as producto
						,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
						,rl.rl_id
						,dd.NRO_BULTO
						,dd.NRO_LOTE
						,RL.EST_MERC_ID
						,dd.NRO_DESPACHO
						,dd.NRO_PARTIDA
						,dd.UNIDAD_ID
						,dd.PROP1
						,dd.PROP2
						,dd.PROP3
						,dd.DESCRIPCION
						,RL.CAT_LOG_ID
						,D.FECHA_ALTA_GTW
						,dd.nro_serie
				FROM	rl_det_doc_trans_posicion rl
						inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
						inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
						inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
						inner join posicion p on (rl.posicion_actual=p.posicion_id)
						left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
						inner join documento d on(dd.documento_id=d.documento_id)
						left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
				WHERE	rl.doc_trans_id_egr is null
						and rl.nro_linea_trans_egr is null
						and rl.disponible='1'
						and isnull(em.disp_egreso,'1')='1'
						and isnull(em.picking,'1')='1'
						and p.pos_lockeada='0' and p.picking='1'
						and cl.disp_egreso='1' and cl.picking='1'
						and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
						and dd.producto_id =@pProducto_id
						--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
						and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
				UNION
				SELECT	 dd.fecha_vencimiento
						,isnull(n.orden_locator,99999) as ORDEN_PICKING
						,'NAV' as ubicacion
						,n.nave_cod as posicion
						,dd.cliente_id
						,dd.producto_id as producto
						,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
						,rl.rl_id
						,dd.NRO_BULTO
						,dd.NRO_LOTE
						,RL.EST_MERC_ID
						,dd.NRO_DESPACHO
						,dd.NRO_PARTIDA
						,dd.UNIDAD_ID
						,dd.PROP1
						,dd.PROP2
						,dd.PROP3
						,dd.DESCRIPCION
						,RL.CAT_LOG_ID
						,D.FECHA_ALTA_GTW
						,dd.nro_serie
				FROM	rl_det_doc_trans_posicion rl
						inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
						inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
						inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
						inner join nave n on (rl.nave_actual=n.nave_id)
						left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
						inner join documento d on(dd.documento_id=d.documento_id)
						left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
				WHERE	rl.doc_trans_id_egr is null
						and rl.nro_linea_trans_egr is null
						and rl.disponible='1'
						and isnull(em.disp_egreso,'1')='1'
						and isnull(em.picking,'1')='1'
						and rl.cat_log_id<>'TRAN_EGR'
						and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
						and cl.disp_egreso='1' and cl.picking='1'
						and dd.producto_id =@pProducto_id
						--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
						and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
						)X
				order by--order by producto,dd.fecha_vencimiento asc,orden  
						(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
						(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
						(CASE WHEN @Crit1 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
						(CASE WHEN @Crit1 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
						(CASE WHEN @Crit1 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
						(CASE WHEN @Crit1 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
						(CASE WHEN @Crit1 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
						(CASE WHEN @Crit1 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
						(CASE WHEN @Crit1 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
						(CASE WHEN @Crit1 = 'PROP1'				THEN x.PROP1 END),
						(CASE WHEN @Crit1 = 'PROP2'				THEN x.PROP2 END),
						(CASE WHEN @Crit1 = 'PROP3'				THEN x.PROP3 END),
						(CASE WHEN @Crit1 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
						(CASE WHEN @Crit1 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
						 --2
						(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
						(CASE WHEN @Crit2 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
						(CASE WHEN @Crit2 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
						(CASE WHEN @Crit2 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
						(CASE WHEN @Crit2 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
						(CASE WHEN @Crit2 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
						(CASE WHEN @Crit2 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
						(CASE WHEN @Crit2 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
						(CASE WHEN @Crit2 = 'PROP1'				THEN x.PROP1 END),
						(CASE WHEN @Crit2 = 'PROP2'				THEN x.PROP2 END),
						(CASE WHEN @Crit2 = 'PROP3'				THEN x.PROP3 END),
						(CASE WHEN @Crit2 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
						(CASE WHEN @Crit2 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),
						--3
						(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
						(CASE WHEN @Crit3 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),
						(CASE WHEN @Crit3 = 'NRO_BULTO'			THEN x.NRO_BULTO END),
						(CASE WHEN @Crit3 = 'NRO_LOTE'			THEN x.NRO_LOTE END),
						(CASE WHEN @Crit3 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),
						(CASE WHEN @Crit3 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),
						(CASE WHEN @Crit3 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),
						(CASE WHEN @Crit3 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),
						(CASE WHEN @Crit3 = 'PROP1'				THEN x.PROP1 END),
						(CASE WHEN @Crit3 = 'PROP2'				THEN x.PROP2 END),
						(CASE WHEN @Crit3 = 'PROP3'				THEN x.PROP3 END),
						(CASE WHEN @Crit3 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),
						(CASE WHEN @Crit3 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END)
								
	Open @RsRem
	Fetch Next From @RsRem into
											@Fecha_Vto,
											@OrdenPicking,
											@Tipo_Posicion,
											@Codigo_Posicion,
											@Cliente_id,
											@Producto_id,
											@Cantidad,
											@vRl_id,
											@NRO_BULTO,
											@NRO_LOTE,				
											@EST_MERC_ID,			
											@NRO_DESPACHO,		
											@NRO_PARTIDA,			
											@UNIDAD_ID,			
											@PROP1,					
											@PROP2,					
											@PROP3,
											@DESC,
											@CAT_LOG_ID,
											@Fecha_Alta_GTW,
											@nro_serie
	While ((@@Fetch_Status=0) AND (@vQtyResto>0))
	begin --While Picking = 1
	-- Aca se replica la logica de Pickin=1
			if (@vQtyResto>=@Cantidad) 
				begin -- (@vQtyResto>=@Cantidad) 
				set @vNroLinea=@vNroLinea+1
				set @vQtyResto=@vQtyResto-@Cantidad
				insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
							values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
				--Insert con todas las propiedades en det_documento
				insert into det_documento_aux (
							documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
							cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
							unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
							values 
							(@pDocumento_id,@vNroLinea
							,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
							,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
							,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
				insert into #tmp_consumo_locator_egr values (@vRl_id, @Cantidad)	
			end
			else begin
				set @vNroLinea=@vNroLinea+1
				insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
							values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')
				--Insert con todas las propiedades en det_documento
				insert into det_documento_aux (
							documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
							cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
							unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
							values 
							(@pDocumento_id,@vNroLinea
							,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
							,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
							,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_Serie)	
				insert into #tmp_consumo_locator_egr values (@vRl_id, @vQtyResto)	
				set @vQtyResto=0
			end --if (@vQtyResto>=@Cantidad) 
			Fetch Next From @RsRem into	@Fecha_Vto,
												@OrdenPicking,
												@Tipo_Posicion,
												@Codigo_Posicion,
												@Cliente_id,
												@Producto_id,
												@Cantidad,
												@vRl_id,
												@NRO_BULTO,
												@NRO_LOTE,				
												@EST_MERC_ID,			
												@NRO_DESPACHO,		
												@NRO_PARTIDA,			
												@UNIDAD_ID,			
												@PROP1,					
												@PROP2,					
												@PROP3,
												@DESC,
												@CAT_LOG_ID,
												@Fecha_Alta_GTW,
												@nro_serie
		end -- End While Picking = 1
CLOSE @RsRem
DEALLOCATE @RsRem

--if @vQtyResto > 0 begin
--	set @auxErr = 'No se pudo asignar del producto ' + @pProducto_id + ', la cantidad total solicitada, para completar falta la cantidad de ' + convert(varchar,convert(int,@vQtyResto)) + ' unidades. '
--	RAISERROR (@auxErr,16,1)
--end --if


Set NoCount Off;
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

ALTER PROCEDURE [dbo].[LOCK_POSITION]
@POSICION_ID	NUMERIC(20,0),
@MOTIVO_ID	VARCHAR(5),
@USUARIO		VARCHAR(20),
@OBS			VARCHAR(100)
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	BEGIN TRANSACTION

	UPDATE POSICION SET POS_LOCKEADA='1' WHERE POSICION_ID=@POSICION_ID;

	INSERT INTO LOCKEO_POSICION (POSICION_ID, MOTIVO_ID, F_LCK, USR_LCK, TRM_LCK, OBS_LCK)
	VALUES(@POSICION_ID, @MOTIVO_ID, GETDATE(), @USUARIO, HOST_NAME(), @OBS);

	DELETE FROM SYS_LOCATOR_ING WHERE POSICION_ID=@POSICION_ID

	COMMIT
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

ALTER  PROCEDURE [dbo].[LOCK_RECEPCION]
	@DOC_EXT 	VARCHAR(100) 	OUTPUT,
	@CLIENTE	VARCHAR(15)		OUTPUT,
	@LOCK		CHAR(1)			OUTPUT
AS
BEGIN
	DECLARE @USUARIO 	AS VARCHAR(15)
	DECLARE @TERMINAL 	AS VARCHAR(100)
	DECLARE @EXISTE		AS FLOAT
	DECLARE @USR		AS VARCHAR(15)
	DECLARE @TLOCK		AS VARCHAR(100)
	DECLARE @NAME		AS VARCHAR(50)
	DECLARE @PROCESADO	AS FLOAT

	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	--SELECT @USUARIO='SGG' 
	SET @TERMINAL=HOST_NAME()
	If @LOCK='1'
	BEGIN
		SELECT 	@EXISTE=COUNT(*)	
		FROM 	SYS_LOCK_RECEPCION
		WHERE	CLIENTE_ID=@CLIENTE
				AND DOC_EXT=@DOC_EXT
		IF @EXISTE > 0
		BEGIN
			SELECT 	@USR=L.USUARIO_ID,@NAME=U.NOMBRE,@TLOCK=TERMINAL
			FROM	SYS_LOCK_RECEPCION L (NOLOCK) INNER JOIN SYS_USUARIO U (NOLOCK)
					ON(L.USUARIO_ID=U.USUARIO_ID)
			WHERE	L.DOC_EXT=@DOC_EXT AND L.CLIENTE_ID=@CLIENTE AND L.LOCK='1'
					AND L.USUARIO_ID<>@USUARIO

			SET @PROCESADO=DBO.RECEPCION_PROCESADA(@CLIENTE, @DOC_EXT)
			IF (@PROCESADO=1)
			BEGIN
				RAISERROR('El Documento %s ya fue procesado. Presione Actualizar Datos.',16,1,@DOC_EXT)
				RETURN
			END
			IF (@USR IS NOT NULL)
			BEGIN
				RAISERROR('El Documento %s esta siendo procesado por %s en la terminal %s',16,1,@DOC_EXT, @NAME, @TLOCK)
				RETURN
			END
			ELSE
			BEGIN
				UPDATE SYS_LOCK_RECEPCION SET USUARIO_ID=@USUARIO, TERMINAL=@TERMINAL, LOCK='1' WHERE CLIENTE_ID=@CLIENTE AND DOC_EXT=@DOC_EXT
				RETURN
			END 
		END
		INSERT INTO SYS_LOCK_RECEPCION (CLIENTE_ID, DOC_EXT, USUARIO_ID, TERMINAL, LOCK, FECHA_LOCK)
							     VALUES(@CLIENTE, @DOC_EXT, @USUARIO, @TERMINAL, 1, GETDATE())
	END
	
	IF @LOCK='0' 
	BEGIN
		UPDATE SYS_LOCK_RECEPCION SET LOCK='0' WHERE DOC_EXT=@DOC_EXT AND CLIENTE_ID=@CLIENTE --LIBERO EL LOCKEO
	END

	IF @LOCK='2'
	BEGIN
		DELETE FROM SYS_LOCK_RECEPCION WHERE CLIENTE_ID=@CLIENTE AND DOC_EXT=@DOC_EXT
	END

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
SET QUOTED_IDENTIFIER OFF
GO

ALTER   procedure [dbo].[loggin_Usuarios]
@usuario nvarchar(50)
as

declare @terminal as varchar(100)
declare @fecha_loggin as DATETIME
declare @session_id as varchar(60)
declare @rol_id as varchar(5)
declare @emplazamiento_default as varchar(15)
declare @deposito_default as varchar(15)

set @terminal =''
set @fecha_loggin=''
set @session_id =''
set @rol_id =''
set @emplazamiento_default =''
set @deposito_default =''

	CREATE TABLE #temp_usuario_loggin ( 
		usuario_id                        VARCHAR(20)  not null,
		terminal                            VARCHAR(100) not null,
		fecha_loggin                    DATETIME     not null,
		session_id                        VARCHAR(60)  not null,
		rol_id                                VARCHAR(5)   not null,
		emplazamiento_default    VARCHAR(15)  NULL,
		deposito_default              VARCHAR(15)  NULL
	); 

	SELECT @session_id= USER_NAME(),@terminal= HOST_NAME(),@fecha_loggin= GETDATE()

	SELECT @emplazamiento_default=emplazamiento_default,@deposito_default=deposito_default
	FROM   sys_perfil_usuario
	WHERE  usuario_id = @usuario

	SELECT @rol_id=rol_id 
	FROM   sys_usuario
	WHERE  usuario_id = @usuario

	insert INTO #temp_usuario_loggin(usuario_id,terminal,fecha_loggin,session_id,rol_id, emplazamiento_default,deposito_default)
	values
	(ltrim(rtrim(@usuario)), @terminal, @fecha_loggin, @session_id, @rol_id,@emplazamiento_default, @deposito_default);
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

ALTER  Procedure [dbo].[Migra_Interfaces]
As
Begin
	Set xact_abort On
	Set Nocount On
	/*
	--=================================================================================================
	--Paso 1, muevo la Sys_int...
	--=================================================================================================

	select * into #cabeceraSI from Sys_int_Documento s  where DBO.VerificaDocExt(s.Cliente_Id, s.Doc_Ext)=1 and  s.fecha_estado_gt < DATEADD(DD,-7,GETDATE())

	Select * into #DetalleSI from Sys_Int_Det_Documento s Where exists (Select 1 from #cabeceraSI c where s.Cliente_Id=c.Cliente_ID and s.Doc_Ext=c.Doc_Ext)

	Begin Transaction

	--Guardo las cabeceras.
	Insert into Sys_Int_Documento_Historico
	Select * from #CabeceraSI

	--Guardo los Detalles
	Insert into Sys_Int_Det_Documento_Historico
	Select * from #DetalleSI

	--Borro Detalles
	Delete from Sys_Int_Det_Documento 
	Where Exists (Select 1 From #DetalleSI d  where Sys_Int_Det_Documento.Cliente_id=d.Cliente_Id and Sys_Int_Det_Documento.Doc_Ext=d.Doc_Ext)
	
	--Borro Cabeceras
	Delete from Sys_Int_Documento 
	Where Exists (Select 1 From #DetalleSI d  where Sys_Int_Documento.Cliente_id=d.Cliente_Id and Sys_Int_Documento.Doc_Ext=d.Doc_Ext)
	
	Drop Table #DetalleSI
	Drop Table #CabeceraSI

	Commit Transaction
	*/
	--=================================================================================================
	--Paso 2, muevo la Sys_Dev...
	--=================================================================================================
	select 	* into #cabeceraSD 
	from 	Sys_dev_Documento 
	Where 	flg_movimiento='1' and Fecha_Estado_Gt< DateAdd(DD,-7,Getdate()) 
			and dbo.VerificaMovDocExt(cliente_id, Doc_Ext)='1'
			and dbo.VerificaPenDocExt(cliente_id, Doc_Ext)='1'


	select * into #DetalleSD 	from Sys_Dev_Det_Documento s 	where exists (Select 1 from #cabeceraSD c where s.Cliente_Id=c.Cliente_ID and s.Doc_Ext=c.Doc_Ext) and  s.flg_movimiento='1' 

	begin transaction
	
	--Guardo Cabeceras
	Insert Into Sys_Dev_Documento_Historico
	Select * from #cabeceraSD

	--Guardo Detalles
	Insert Into Sys_Dev_Det_Documento_Historico						
	Select * from #detalleSD

	Delete from Sys_dev_Det_Documento 
	Where Exists (Select 1 From #DetalleSD d  where Sys_dev_Det_Documento.Cliente_id=d.Cliente_Id and Sys_dev_Det_Documento.Doc_Ext=d.Doc_Ext)
	
	--Borro Cabeceras
	Delete from Sys_Dev_Documento 
	Where Exists (Select 1 From #DetalleSD d  where Sys_Dev_Documento.Cliente_id=d.Cliente_Id and Sys_Dev_Documento.Doc_Ext=d.Doc_Ext)
	

	Commit Transaction

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

ALTER procedure [dbo].[Mob_AbrirPalletCerrado]
@Pallet numeric(20,0)
as
begin
	update picking set pallet_cerrado='0' where pallet_final=@pallet

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

ALTER PROCEDURE [dbo].[MOB_AFP]
@CLIENTE	VARCHAR(20),
@PRODUCTO	VARCHAR(50),
@VIAJE_ID	VARCHAR(30),
@TIPO		CHAR(1),
@PF			BIGINT,
@USUARIO	VARCHAR(30),
--------OPCIONALES--------
@QTYXCAMA	BIGINT=0,
@QTYCAMA	INT=0,
@QTYSUELTO	INT=0,
@TOTAL		BIGINT=0
AS
BEGIN
	--GENERALES
	DECLARE @PICKING_ID		NUMERIC(20,0)

	--PARA @TIPO='0'
	DECLARE @NEWPICK		NUMERIC(20,0)

	--PARA @TIPO='1'
	DECLARE @CURSOR			CURSOR
	DECLARE @SUMCONTROL		FLOAT
	DECLARE @VSUMCONTROL	VARCHAR(10)
	DECLARE @REMANENTE		FLOAT
	DECLARE @CANT_CONF		FLOAT
	DECLARE @QTY_CONT		FLOAT
	DECLARE @PC				CHAR(1)
	DECLARE @QTY			FLOAT
	SET XACT_ABORT ON
	
	IF @TIPO='0'
	BEGIN
		--Selecciono la primer linea que encuentre disponible
		Select	TOP 1
				@PICKING_ID=PICKING_ID
		From	Picking P (Nolock) 
		Where	p.Cliente_id=@Cliente and P.Producto_id=@Producto
				and p.viaje_id=@viaje_id
				and p.Cant_Confirmada>ISNULL(p.Qty_Controlado,0)
				and p.facturado<>'1' and fecha_inicio is not null and fecha_fin is not null
				and p.cant_confirmada>0
				and ((p.pallet_final is null) or (p.pallet_final=@PF))
				and ISNULL(p.pallet_cerrado,'0')<>'1'
		
		if @picking_ID is null
		begin
			-- Si es nulo me fijo si quedaron pendientes con pallet cerrado
			Select	TOP 1
					@PICKING_ID=PICKING_ID
			From	Picking P (Nolock) 
			Where	p.Cliente_id=@Cliente and P.Producto_id=@Producto
					and p.viaje_id=@viaje_id
					and p.Cant_Confirmada>ISNULL(p.Qty_Controlado,0)
					and p.facturado<>'1' and fecha_inicio is not null and fecha_fin is not null
					and cant_confirmada > 0
					and pallet_final<>@PF
					--and pallet_cerrado='1'

			if @Picking_id is not null
			Begin
				BEGIN TRANSACTION
				--Split de Picking, ya que tengo al menos un producto pendiente en pallet cerrado.
				INSERT INTO PICKING(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, 
									RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
									USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
									TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
									USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, 
									PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS,BULTOS_NO_CONTROLADOS,NRO_LOTE,NRO_PARTIDA,NRO_SERIE)
				SELECT 	DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, 1, NAVE_COD, POSICION_COD, 
						RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, 1, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
						USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
						TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
						USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, 1, @PF, 
						PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, ISNULL(PICKING_ID_REF, PICKING_ID), NULL,NULL,NRO_LOTE,NRO_PARTIDA,NRO_SERIE
				FROM 	PICKING
				WHERE	PICKING_ID=@Picking_id
				-- recupero el id insertado
				SELECT @NEWPICK=SCOPE_IDENTITY()
				--actualizo id para no mandar de mas (qty's).
				UPDATE PICKING SET CANTIDAD=CANTIDAD-1, CANT_CONFIRMADA=CANT_CONFIRMADA-1 WHERE PICKING_ID=@Picking_id

				COMMIT TRANSACTION
				SET @Picking_id=@NEWPICK
				UPDATE PICKING SET	PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
									TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
				WHERE  PICKING_ID=@PICKING_ID		
				RETURN
			End
			IF @PICKING_ID IS NULL
			BEGIN
				RaisError('No se encontro registros para Confirmar.',16,1)
				Return
			END
		end
		UPDATE PICKING SET	QTY_CONTROLADO=ISNULL(QTY_CONTROLADO,0)+1, PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
							TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
		WHERE  PICKING_ID=@PICKING_ID		
	END --FIN TIPO='0'

	IF @TIPO='1'
	BEGIN
		--CONTROLO LAS CANTIDADES A INGRESAR.
		SELECT	@SUMCONTROL=SUM(CANT_CONFIRMADA) - SUM(ISNULL(QTY_CONTROLADO,0))
		FROM	PICKING P
		WHERE	P.CLIENTE_ID			=@CLIENTE
				AND P.VIAJE_ID			=@VIAJE_ID
				AND P.PRODUCTO_ID		=@PRODUCTO
				AND P.CANT_CONFIRMADA	>ISNULL(P.QTY_CONTROLADO,0)
				AND P.FECHA_INICIO		IS NOT NULL
				AND P.FECHA_FIN			IS NOT NULL
				AND P.FACTURADO			='0'

		IF @SUMCONTROL<@TOTAL
		BEGIN
			SET @VSUMCONTROL=CAST(@SUMCONTROL AS VARCHAR)
			RAISERROR('La cantidad ingresada es mayor a la disponible para controlar. Disponible a controlar %s',16,1,@VSUMCONTROL)
			RETURN
		END
		ELSE
		BEGIN
			SET @CURSOR=CURSOR FOR
				SELECT	PICKING_ID
				FROM	PICKING P 
				WHERE	P.CLIENTE_ID			=@CLIENTE
						AND P.VIAJE_ID			=@VIAJE_ID
						AND P.PRODUCTO_ID		=@PRODUCTO
						AND P.CANT_CONFIRMADA	>ISNULL(P.QTY_CONTROLADO,0)
						AND P.FECHA_INICIO		IS NOT NULL
						AND P.FECHA_FIN			IS NOT NULL
						AND P.FACTURADO			='0'

			SET @REMANENTE=@TOTAL
			OPEN @CURSOR
			FETCH NEXT FROM @CURSOR INTO @PICKING_ID
			WHILE @@FETCH_STATUS=0
			BEGIN
				SELECT	@CANT_CONF=ISNULL(CANT_CONFIRMADA,0), @QTY_CONT=ISNULL(QTY_CONTROLADO,0),@PC=ISNULL(PALLET_CERRADO,0)
				FROM	PICKING 
				WHERE	PICKING_ID=@PICKING_ID
				
				IF(@CANT_CONF>=@QTY_CONT) AND (@PC='1')
				BEGIN
					INSERT INTO PICKING(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, 
										RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
										USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
										TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
										USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, 
										PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS,BULTOS_NO_CONTROLADOS,NRO_LOTE,NRO_PARTIDA,NRO_SERIE)

					SELECT 	DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANT_CONFIRMADA-ISNULL(QTY_CONTROLADO,0)
							, NAVE_COD, POSICION_COD, 
							RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA-ISNULL(QTY_CONTROLADO,0), PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
							USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
							TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
							USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, CANT_CONFIRMADA-ISNULL(QTY_CONTROLADO,0), 999, 
							PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, ISNULL(PICKING_ID_REF, PICKING_ID), NULL,NULL,NRO_LOTE,NRO_PARTIDA,NRO_SERIE
					FROM 	PICKING
					WHERE	PICKING_ID=@PICKING_ID

					-- recupero el id insertado
					SELECT @NEWPICK=SCOPE_IDENTITY()
					--actualizo id para no mandar de mas (qty's).
					UPDATE PICKING SET CANTIDAD=QTY_CONTROLADO, CANT_CONFIRMADA=QTY_CONTROLADO WHERE PICKING_ID=@Picking_id

					SET @Picking_id=@NEWPICK

					UPDATE PICKING SET	PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
										TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
					WHERE  PICKING_ID=@PICKING_ID	
				END
				IF(@CANT_CONF>=@QTY_CONT) AND (@PC='0')
				BEGIN
					IF @CANT_CONF>@REMANENTE
					BEGIN
						SET @QTY=@REMANENTE
						SET @REMANENTE=0
					END
					IF @CANT_CONF<=@REMANENTE
					BEGIN
						SET @QTY=@CANT_CONF -@QTY_CONT
						SET @REMANENTE=@REMANENTE-(@CANT_CONF-@QTY_CONT)
					END
					UPDATE PICKING SET	QTY_CONTROLADO=ISNULL(QTY_CONTROLADO,0) + @QTY,PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
										TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
					WHERE  PICKING_ID=@PICKING_ID					

				END
				IF @REMANENTE=0
				BEGIN
					BREAK
				END
				FETCH NEXT FROM @CURSOR INTO @PICKING_ID	
			END
			CLOSE @CURSOR
			DEALLOCATE @CURSOR
		END
	END	--FIN @TIPO='1'
END--FIN PROCEDURE.
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

ALTER      PROCEDURE [dbo].[Mob_Busca_Usuario]
@usuario_id as nvarchar(20),
@password_handheld as nvarchar(50)
as

select  NOMBRE from SYS_USUARIO where RTRIM(LTRIM(upper(USUARIO_ID))) = upper(@usuario_id) and RTRIM(LTRIM(upper(password_handheld))) = upper(@password_handheld)
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

ALTER   PROCEDURE [dbo].[Mob_ConsultaStock]
@Codigo as nvarchar(100),
@TipoOperacion as integer,
@Cliente as varchar(15)
as


IF @TipoOperacion=1
BEGIN
SELECT X.ProductoID 
     ,cast(sum(X.cantidad)as int) AS Cantidad 
     ,X.EST_MERC_ID
     ,X.CategLogID
     ,X.Nro_Lote
     ,X.prop1 AS Property_1
     ,CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103) as Fecha_Vencimiento
     ,PR.DESCRIPCION AS PRODUCTO
FROM CLIENTE C, PRODUCTO PR 
     ,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID 
             ,cast(sum(rl.cantidad)as int) AS Cantidad 
             ,dd.unidad_id ,dd.moneda_id ,dd.costo 
             ,dd.nro_serie AS Nro_Serie 
             ,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
             ,dd.Nro_Partida 
             ,dd.Nro_Despacho, dd.Nro_Bulto 
             ,dd.Prop1, dd.Prop2, dd.Prop3 
             ,dd.Peso ,dd.Unidad_Peso 
             ,dd.Volumen ,dd.Unidad_Volumen 
             ,prod.kit AS Kit 
             ,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
             ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
             ,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
             ,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
             ,ISNULL(caln.calle_cod,Null) AS CalleCod 
             ,ISNULL(caln.calle_id,Null) AS CalleID 
             ,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
             ,ISNULL(coln.columna_id,Null) AS ColumnaID
             ,ISNULL(nn.nivel_cod,Null) AS NivelCod 
             ,ISNULL(nn.nivel_id,Null) AS NivelID 
             ,rl.cat_log_id as CategLogID 
     FROM 
         rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
         ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
         left join nave n  ON rl.nave_actual=n.nave_id 
         left join posicion p  ON rl.posicion_actual=p.posicion_id 
         left join nave n2 ON p.nave_id=n2.nave_id 
         left join calle_nave caln ON  p.calle_id=caln.calle_id 
         left join columna_nave coln ON p.columna_id=coln.columna_id 
         left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
         inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
         inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
         inner join cliente c ON dd.cliente_id=c.cliente_id 
         inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
         inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
     WHERE 1<>0  
   AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
   AND dd.prop1 = UPPER(LTRIM(RTRIM(@Codigo)))
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote, dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3 
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,p.nave_id,n.nave_cod 
     ,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
     ,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
     ,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
UNION ALL  
     SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID  
           ,cast(sum(rl.cantidad)as int) AS Cantidad  
           ,dd.unidad_id ,dd.moneda_id ,dd.costo  
           ,dd.nro_serie AS Nro_Serie  
           ,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23), dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
           ,dd.Nro_Partida  
           ,dd.Nro_Despacho, dd.Nro_Bulto  
           ,dd.Prop1, dd.Prop2, dd.Prop3  
           ,cast(dd.Peso as float) AS Peso ,dd.Unidad_Peso  
           ,cast(dd.Volumen as float) AS Volumen,dd.Unidad_Volumen  
           ,prod.kit AS Kit  
           ,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
           ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
           ,n.nave_cod AS Storage  
           ,rl.nave_actual as NaveID  
           ,null AS CalleCod  
           ,null AS CalleID  
           ,null AS ColumnaCod  
           ,null AS ColumnaID  
           ,null AS NivelCod  
           ,null AS NivelID 
           ,rl.cat_log_id as CategLogID  
     FROM  
           rl_det_doc_trans_posicion rl inner join det_documento dd  
           ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
           left join nave n  ON rl.nave_actual=n.nave_id  
           inner join cliente c  ON dd.cliente_id=c.cliente_id  
           inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
           inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
     WHERE 1<>0  
 AND dd.Cliente_ID = @Cliente
 AND dd.prop1 = @Codigo
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote ,dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,n.nave_cod 
     ,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
     ) x 
WHERE C.CLIENTE_ID = X.CLIENTEID 
     AND PR.CLIENTE_ID = X.CLIENTEID 
     AND PR.PRODUCTO_ID = X.PRODUCTOID 
     group by X.ClienteID, X.ProductoID 
     ,X.Storage 
     ,X.NaveID 
     ,X.CalleCod 
     ,X.CalleID 
     ,X.ColumnaCod 
     ,X.ColumnaID 
     ,X.NivelCod 
     ,X.NivelID 
     ,X.EST_MERC_ID 
     ,X.CategLogID 
     ,X.Nro_Serie 
     ,X.Nro_Bulto 
     ,X.Nro_Lote 
     ,X.Nro_Despacho 
     ,X.Nro_Partida 
     ,X.prop1 
     ,X.prop2 
     ,X.prop3 
     ,X.Fecha_Vencimiento 
     ,X.Peso 
     ,X.Unidad_Peso 
     ,X.Volumen 
     ,X.Unidad_Volumen 
     ,X.Kit 
     ,X.TIE_IN  ,X.TIE_IN_PADRE 
     ,X.NRO_TIE_IN 
     ,C.RAZON_SOCIAL 
     ,PR.DESCRIPCION 
     ,X.unidad_id 
     ,X.moneda_id 
     ,x.costo 
END
ELSE
	BEGIN
	IF  @TipoOperacion=2
		BEGIN
		--CONSULTA UBICACION
		SELECT DD.CLIENTE_ID, DD.PRODUCTO_ID, cast(SUM(DD.CANTIDAD)as int) AS CANTIDAD_TOTAL, CONVERT(VARCHAR(23), DD.FECHA_VENCIMIENTO, 103) as FECHA_VENCIMIENTO, DD.NRO_LOTE
		FROM RL_DET_DOC_TRANS_POSICION RL INNER JOIN
	        POSICION P ON RL.POSICION_ACTUAL = P.POSICION_ID INNER JOIN
	        DET_DOCUMENTO_TRANSACCION DDT ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND 
	        RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS INNER JOIN
	        DOCUMENTO_TRANSACCION DT ON DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID INNER JOIN
	        DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA
		WHERE     --(RL.POSICION_ACTUAL = @Codigo) SGG
			  P.POSICION_COD=UPPER(LTRIM(RTRIM(@Codigo)))
		GROUP BY DD.CLIENTE_ID, DD.PRODUCTO_ID, DD.FECHA_VENCIMIENTO, DD.NRO_LOTE
		END
	ELSE
BEGIN
		--CONSULTA PRODUCTO
SELECT X.ProductoID 
     ,cast(sum(X.cantidad)as int) AS Cantidad 
     ,X.EST_MERC_ID
     ,X.CategLogID
     ,X.Nro_Lote
     ,X.prop1 AS Property_1
     ,CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103) as Fecha_Vencimiento
     ,PR.DESCRIPCION AS PRODUCTO
FROM CLIENTE C, PRODUCTO PR 
     ,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID 
             ,cast(sum(rl.cantidad)as int) AS Cantidad 
             ,dd.unidad_id ,dd.moneda_id ,dd.costo 
             ,dd.nro_serie AS Nro_Serie 
             ,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
             ,dd.Nro_Partida 
             ,dd.Nro_Despacho, dd.Nro_Bulto 
             ,dd.Prop1, dd.Prop2, dd.Prop3 
             ,cast(dd.Peso as float) as Peso,dd.Unidad_Peso 
             ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen 
             ,prod.kit AS Kit 
             ,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
             ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
             ,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
             ,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
             ,ISNULL(caln.calle_cod,Null) AS CalleCod 
             ,ISNULL(caln.calle_id,Null) AS CalleID 
             ,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
             ,ISNULL(coln.columna_id,Null) AS ColumnaID
             ,ISNULL(nn.nivel_cod,Null) AS NivelCod 
             ,ISNULL(nn.nivel_id,Null) AS NivelID 
             ,rl.cat_log_id as CategLogID 
     FROM 
         rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
         ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
         left join nave n  ON rl.nave_actual=n.nave_id 
         left join posicion p  ON rl.posicion_actual=p.posicion_id 
         left join nave n2 ON p.nave_id=n2.nave_id 
         left join calle_nave caln ON  p.calle_id=caln.calle_id 
         left join columna_nave coln ON p.columna_id=coln.columna_id 
         left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
         inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
         inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
         inner join cliente c ON dd.cliente_id=c.cliente_id 
         inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
         inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
     WHERE 1<>0  
   AND dd.Cliente_ID = @Cliente
   AND dd.Producto_ID = @Codigo
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote, dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3 
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,p.nave_id,n.nave_cod 
     ,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
     ,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
     ,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
UNION ALL  
     SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID  
           ,cast(sum(rl.cantidad)as int) AS Cantidad  
           ,dd.unidad_id ,dd.moneda_id ,dd.costo  
           ,dd.nro_serie AS Nro_Serie  
           ,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23),dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
           ,dd.Nro_Partida  
           ,dd.Nro_Despacho, dd.Nro_Bulto  
           ,dd.Prop1, dd.Prop2, dd.Prop3  
           ,cast(dd.Peso as float) as Peso ,dd.Unidad_Peso  
           ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen  
           ,prod.kit AS Kit  
           ,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
           ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
           ,n.nave_cod AS Storage  
           ,rl.nave_actual as NaveID  
           ,null AS CalleCod  
           ,null AS CalleID  
           ,null AS ColumnaCod  
           ,null AS ColumnaID  
           ,null AS NivelCod  
           ,null AS NivelID 
           ,rl.cat_log_id as CategLogID  
     FROM  
           rl_det_doc_trans_posicion rl inner join det_documento dd  
           ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
           left join nave n  ON rl.nave_actual=n.nave_id  
           inner join cliente c  ON dd.cliente_id=c.cliente_id  
           inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
           inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
     WHERE 1<>0    
 AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
 AND dd.Producto_ID = UPPER(LTRIM(RTRIM(@Codigo)))
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote ,dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,n.nave_cod 
     ,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
     ) x 
WHERE C.CLIENTE_ID = X.CLIENTEID 
     AND PR.CLIENTE_ID = X.CLIENTEID 
     AND PR.PRODUCTO_ID = X.PRODUCTOID 
     group by X.ClienteID, X.ProductoID 
     ,X.Storage 
     ,X.NaveID 
     ,X.CalleCod 
     ,X.CalleID 
     ,X.ColumnaCod 
     ,X.ColumnaID 
     ,X.NivelCod 
     ,X.NivelID 
     ,X.EST_MERC_ID 
     ,X.CategLogID 
     ,X.Nro_Serie 
     ,X.Nro_Bulto 
     ,X.Nro_Lote 
     ,X.Nro_Despacho 
     ,X.Nro_Partida 
     ,X.prop1 
     ,X.prop2 
     ,X.prop3 
     ,X.Fecha_Vencimiento 
     ,X.Peso 
     ,X.Unidad_Peso 
     ,X.Volumen 
     ,X.Unidad_Volumen 
     ,X.Kit 
     ,X.TIE_IN  ,X.TIE_IN_PADRE 
     ,X.NRO_TIE_IN 
     ,C.RAZON_SOCIAL 
     ,PR.DESCRIPCION 
     ,X.unidad_id 
     ,X.moneda_id 
     ,x.costo 

	
END

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