
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 03:57 p.m.
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

--	begin transaction

-- egr_aceptar 879
/*
Commit
*/
-- rollback


ALTER    procedure [dbo].[CorrerTodo]
@DocTransId	as numeric(20,0)
As
Begin
	
	Declare @Status	as varchar(3)	
	Declare @Fi		as datetime

	Set @Fi=getdate()

	Select	@Status=Status
	from	documento_transaccion
	where	doc_trans_id=@DocTransId


	while @Status <>'T40'
	Begin

		Exec Egr_Aceptar @DocTransId

		Select	@Status=Status
		from	documento_transaccion
		where	doc_trans_id=@DocTransId
	End

	select datediff(ms,@fi,getdate())

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

ALTER PROCEDURE [dbo].[CREATE_CHILD]
	@VIAJE_ID	AS VARCHAR(100)output
AS
BEGIN
	DECLARE @TIPO_OPERACION VARCHAR(5)
	DECLARE @CANT			AS INT
	DECLARE @TCUR			CURSOR
	DECLARE @VIAJEID		VARCHAR(100)
	DECLARE @PRODUCTO_ID	VARCHAR(30)
	DECLARE @POSICION_COD	VARCHAR(50)
	DECLARE @PALLET			VARCHAR(100)
	DECLARE @RUTA			VARCHAR(100)
	DECLARE @ID				NUMERIC(20,0)	

	SET @TCUR= CURSOR FOR
		SELECT 	SP.VIAJE_ID, SP.PRODUCTO_ID, SP.POSICION_COD, PROP1, RUTA, 
				DBO.GETPICKINGID(SP.VIAJE_ID, SP.PRODUCTO_ID, SP.POSICION_COD, PROP1, RUTA)
		FROM 	PICKING SP
				INNER JOIN PRIORIDAD_VIAJE SPV
				ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
				INNER JOIN PRODUCTO PROD
				ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
				LEFT JOIN POSICION POS ON(SP.POSICION_COD=POS.POSICION_COD)
		WHERE 	SPV.PRIORIDAD = ( SELECT 	MIN(PRIORIDAD) FROM	PRIORIDAD_VIAJE	WHERE	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))								
				AND SP.VIAJE_ID=@VIAJE_ID
				AND	SP.FECHA_INICIO IS NULL
				AND	SP.FECHA_FIN IS NULL			
				AND	SP.USUARIO IS NULL
				AND	SP.CANT_CONFIRMADA IS NULL 
				AND SP.FIN_PICKING <>'2'

		GROUP BY 	
				SP.VIAJE_ID, SP.PROP1, SP.RUTA, SP.DOCUMENTO_ID ,SP.NRO_LINEA	,SP.PRODUCTO_ID,SPV.PRIORIDAD,SP.TIPO_CAJA, POS.ORDEN_PICKING, SP.POSICION_COD
		ORDER BY	SPV.PRIORIDAD ASC,CAST(SP.TIPO_CAJA AS NUMERIC(10,1)) DESC, POS.ORDEN_PICKING, SP.POSICION_COD ASC
	OPEN @TCUR
	FETCH NEXT FROM @TCUR INTO  @VIAJEID,	@PRODUCTO_ID, @POSICION_COD, @PALLET, @RUTA, @ID
	WHILE @@FETCH_STATUS=0
	BEGIN
		EXEC DBO.ACTUALIZA_RELACION_PICKING @VIAJEID, @PRODUCTO_ID, @POSICION_COD, @PALLET, @RUTA, @ID
		FETCH NEXT FROM @TCUR INTO  @VIAJEID,	@PRODUCTO_ID, @POSICION_COD, @PALLET, @RUTA, @ID
	END
	CLOSE @TCUR
	DEALLOCATE @TCUR

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