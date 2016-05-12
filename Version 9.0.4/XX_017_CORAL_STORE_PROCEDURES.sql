
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 05:00 p.m.
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

ALTER  Procedure [dbo].[Trd_GetProductosByProd]
	 @Producto_id as varchar(30)
	,@Pallet as varchar(100)
	,@U_Orig as varchar(45)

As
Begin
	SELECT X.*
	FROM
	(
		SELECT		 
					 P.POSICION_COD AS POSICION_COD
					,DD.PROP1 AS PALLET
					,SUM(CAST(RL.CANTIDAD AS INT)) AS QTY
					,DD.NRO_LOTE
					,ISNULL(P.ORDEN_LOCATOR,N.ORDEN_LOCATOR) AS ORDEN_LOCATOR
		FROM		RL_DET_DOC_TRANS_POSICION RL 
						INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
							ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
						INNER JOIN DET_DOCUMENTO DD
							ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
						LEFT JOIN NAVE N
							ON(RL.NAVE_ACTUAL=N.NAVE_ID)
						LEFT JOIN POSICION P
							ON(RL.POSICION_ACTUAL=P.POSICION_ID)
						INNER JOIN PRODUCTO PROD
							ON(DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID)
						INNER JOIN NAVE N1
							ON(P.NAVE_ID = N1.NAVE_ID)
		WHERE		DD.PRODUCTO_ID = @Producto_id
					AND P.POSICION_COD IS NOT NULL
					AND N1.PRE_INGRESO <> '1'
					AND N1.PRE_EGRESO <> '1'
					AND N1.DISP_TRANSF = '1'
					AND POSICION_COD<>@U_Orig
		GROUP BY	DD.PRODUCTO_ID,PROD.DESCRIPCION,P.POSICION_COD,DD.PROP1,DD.NRO_LOTE,ISNULL(P.ORDEN_LOCATOR,N.ORDEN_LOCATOR)
		
		UNION ALL
		
		SELECT		
					 N.NAVE_COD AS POSICION_COD
					,DD.PROP1 AS PALLET
					,SUM(CAST(RL.CANTIDAD AS INT)) AS QTY
					,DD.NRO_LOTE
					,ISNULL(P.ORDEN_LOCATOR,N.ORDEN_LOCATOR) AS ORDEN_LOCATOR
		FROM		RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
					INNER JOIN PRODUCTO PROD
					ON(DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID)
		WHERE		DD.PRODUCTO_ID=@Producto_id
					AND N.PRE_INGRESO <>'1'
					AND N.PRE_EGRESO <>'1'
					AND N.DISP_TRANSF = '1'
					AND NAVE_COD<>@U_Orig
		GROUP BY	DD.PRODUCTO_ID,PROD.DESCRIPCION,N.NAVE_COD,DD.PROP1,DD.NRO_LOTE,ISNULL(P.ORDEN_LOCATOR,N.ORDEN_LOCATOR)

	)AS X
--	WHERE NOT (X.POSICION_COD =@U_Orig AND X.PALLET =@Pallet)
	WHERE NOT (X.PALLET =@Pallet)
	ORDER BY ISNULL(X.ORDEN_LOCATOR, 999), X.NRO_LOTE, X.PALLET

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

ALTER PROCEDURE [dbo].[UBICA_CROSS_DOCK] 
@DOCUMENTO_ID	NUMERIC(20,0) OUTPUT
AS
BEGIN
	SET XACT_ABORT ON
	DECLARE @NAVE_ID			NUMERIC(20,0)		
	DECLARE @DOC_TRANS_ID 	NUMERIC(20,0)
	DECLARE @NRO_LINEA		NUMERIC(10,0)
	DECLARE @PCUR				CURSOR
	BEGIN TRANSACTION

	SELECT 	@NAVE_ID=C.NAVE_ID_CROSSDOCK, @DOC_TRANS_ID=DT.DOC_TRANS_ID
	FROM	DOCUMENTO D INNER JOIN CLIENTE_PARAMETROS C				ON(D.CLIENTE_ID=C.CLIENTE_ID)
			INNER JOIN DET_DOCUMENTO DD 					ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN DOCUMENTO_TRANSACCION DT			ON(DDT.DOC_TRANS_ID=DT.DOC_TRANS_ID)
	WHERE	D.DOCUMENTO_ID=@DOCUMENTO_ID	

	IF (@NAVE_ID IS NOT NULL) AND (@DOC_TRANS_ID IS NOT NULL)
	BEGIN
		UPDATE RL_DET_DOC_TRANS_POSICION SET NAVE_ANTERIOR=NAVE_ACTUAL, NAVE_ACTUAL=@NAVE_ID WHERE DOC_TRANS_ID=@DOC_TRANS_ID
	END

	SET @PCUR = CURSOR FOR
		SELECT NRO_LINEA FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	OPEN @PCUR
	FETCH NEXT FROM @PCUR INTO @NRO_LINEA
	WHILE @@FETCH_STATUS=0
	BEGIN
		EXEC AUDITORIA_HIST_INSERT_UBIC @DOCUMENTO_ID, @NRO_LINEA, NULL, 1
		FETCH NEXT FROM @PCUR INTO @NRO_LINEA
	END 
	COMMIT TRANSACTION
	CLOSE @PCUR
	DEALLOCATE @PCUR
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

ALTER   PROCEDURE [dbo].[UPDATEESTACIONACTUAL]
	@DOC_TRANS_ID AS NUMERIC(20,0)
AS
BEGIN
	UPDATE DOCUMENTO_TRANSACCION
       SET ESTACION_ACTUAL = NULL
           ,STATUS = 'T40'
           ,FECHA_FIN_GTW = GETDATE()
     WHERE DOC_TRANS_ID = @DOC_TRANS_ID


	UPDATE 	RL_DET_DOC_TRANS_POSICION
       		SET DOC_TRANS_ID_TR = NULL,
       		NRO_LINEA_TRANS_TR = NULL
 	WHERE 	DOC_TRANS_ID_TR =@DOC_TRANS_ID
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

ALTER PROCEDURE [dbo].[usp_GetErrorInfo]
AS
    SELECT 
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_SEVERITY() AS ErrorSeverity,
        ERROR_STATE() as ErrorState,
        ERROR_PROCEDURE() as ErrorProcedure,
        ERROR_LINE() as ErrorLine,
        ERROR_MESSAGE() as ErrorMessage;
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

ALTER PROCEDURE [dbo].[usp_RethrowError] AS
    -- Return if there is no error information to retrieve.
    IF ERROR_NUMBER() IS NULL
        RETURN;

    DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);

    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE(),
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');

    -- Build the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
	    N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE()
	

    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        1,               
        @ErrorNumber,    -- parameter: original error number.
        --convert(varchar, @ErrorSeverity),  -- parameter: original error severity.
		@ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );
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

ALTER PROCEDURE [dbo].[VAL_COD_APF]
@PICKING_ID NUMERIC(20,0) OUTPUT
AS
BEGIN
	SELECT	S.SUCURSAL_ID,P.PRODUCTO_ID,HIJO
	FROM	PICKING P INNER JOIN DET_DOCUMENTO DD ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN DOCUMENTO D ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_DESTINO=S.SUCURSAL_ID)
	WHERE	P.PICKING_ID=@PICKING_ID
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

ALTER PROCEDURE [dbo].[VAL_COD_BULTO_APF]
@PICKING_ID NUMERIC(20,0) OUTPUT,
@BULTO		VARCHAR(20) OUTPUT,
@INGRESADO	CHAR(1) OUTPUT
AS
BEGIN
	DECLARE @VAR	VARCHAR(8000)
	DECLARE @BNC	VARCHAR(8000)
	DECLARE @COUNT	SMALLINT
	--SET  @VAR='|1|2|3|4|7|'
	IF (@BULTO='')OR(@BULTO IS NULL)
	BEGIN
		RAISERROR('Etiqueta no valida',16,1)
		return
	END
	SELECT @BNC=bultos_no_controlados FROM PICKING WHERE PICKING_ID=@PICKING_ID
	SELECT @VAR=bultos_controlados FROM PICKING WHERE PICKING_ID=@PICKING_ID

	select	@COUNT=COUNT(*) 
	from	fnSplit(@BNC, '|') 
	WHERE	ITEM=@BULTO
	If @Count=0
	Begin
		set @Count=null
		select	@COUNT=COUNT(*) 
		from	fnSplit(@VAR, '|') 
		WHERE	ITEM=@BULTO

		IF @COUNT=0
		BEGIN
			Raiserror('El bulto ingresado no existe.',16,1)
			Return
		END
		ELSE
		BEGIN
			SET @INGRESADO='1'
			RETURN
		END

		Raiserror('El bulto ingresado no existe.',16,1)
		Return
	End

	set @Count=null

	select	@COUNT=COUNT(*) 
	from	fnSplit(@VAR, '|') 
	WHERE	ITEM=@BULTO

	IF @COUNT=0
	BEGIN
		SET @INGRESADO='0'
	END
	ELSE
	BEGIN
		SET @INGRESADO='1'
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

ALTER PROCEDURE [dbo].[VAL_PALLET_FINAL]
@AGENTE 			AS VARCHAR(20) OUTPUT,
@PRODUCTO_ID		AS VARCHAR(39) OUTPUT,
@VALIDO			AS CHAR(1)	OUTPUT
AS
BEGIN
	DECLARE @CONTROL AS INT

	
	SELECT 	@CONTROL=COUNT(*)
	FROM	DOCUMENTO D (NOLOCK) INNER JOIN DET_DOCUMENTO DD (NOLOCK)
			ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
	WHERE	D.SUCURSAL_DESTINO=@AGENTE
			AND DD.PRODUCTO_ID=@PRODUCTO_ID

	IF @CONTROL=0
	BEGIN
		SET @VALIDO='0'
		RAISERROR('El producto ingresado, no esta destinado al agente %s',16,1,@AGENTE)
		RETURN
	END
	ELSE
	BEGIN
		SET @VALIDO='1'
	END
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
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[VALIDA_CODIGO_RODC]
@CODIGO		AS VARCHAR(50),
@ODC			AS VARCHAR(50),
@CLIENTE_ID	AS VARCHAR(15),
@STATUS		AS CHAR(1) 			OUTPUT,
@PROD_ID		AS VARCHAR(30) 	OUTPUT,
@UNIDAD_ID	AS VARCHAR(5)		OUTPUT,
@REMANENTE	AS FLOAT			OUTPUT
AS
BEGIN
	DECLARE @PRODUCTO_ID	VARCHAR(30)
	DECLARE @CONT			SMALLINT
	DECLARE @DOC_EXT		VARCHAR(100)
	

	--Inicializo los de salida
	set @STATUS=null
	set @prod_id=null
	set @unidad_id=null

	SELECT 	@CONT=COUNT(*)
	FROM	PRODUCTO
	WHERE 	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@CODIGO

	IF @CONT=1 --PRODUCTO_ID
	BEGIN
		SET @PRODUCTO_ID=@CODIGO
	
		SELECT 	@PROD_ID=PRODUCTO_ID, @UNIDAD_ID=UNIDAD_ID FROM PRODUCTO WHERE CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID=@PRODUCTO_ID

		SET @CONT=NULL

		SELECT 	@CONT=COUNT(*)
		FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		WHERE 	ORDEN_DE_COMPRA=@ODC
				AND SD.CLIENTE_ID=@CLIENTE_ID
				AND SDD.PRODUCTO_ID=@PRODUCTO_ID
				AND SDD.ESTADO_GT IS NULL
				AND SDD.FECHA_ESTADO_GT IS NULL

		SELECT 	TOP 1
				@DOC_EXT=SD.DOC_EXT
		FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		WHERE 	ORDEN_DE_COMPRA=@ODC
				AND PRODUCTO_ID=@PRODUCTO_ID
				AND SD.CLIENTE_ID=@CLIENTE_ID

		Select 	@REMANENTE=sum(cantidad_solicitada)
		from	sys_int_det_documento
		where	doc_ext=@doc_ext
				and fecha_estado_gt is null
				and estado_gt is null


		IF @CONT>0 --EXISTE EN LA ODC
		BEGIN
			set @status='1'
			Return
		END
		ELSE
		BEGIN
			RAISERROR('El producto ingresado no existe en la orden de compra o el mismo ya fue recibido en su totalidad',16,1)
			return
		END
	END	--

	--SEGUN DEFINICION DE FO. ESTE CODIGO ES UNICO PARA CADA PRODUCTO.
	SELECT 	@PRODUCTO_ID=PRODUCTO_ID
	FROM	RL_PRODUCTO_CODIGOS
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND CODIGO=@CODIGO

	IF @PRODUCTO_ID IS NULL
	BEGIN
		RAISERROR('El codigo ingresado no corresponde a un producto o a un DUN14/EAN13',16,1)
		return	
	END
	ELSE
	BEGIN
		SET @CONT=NULL

		SELECT 	@PROD_ID=PRODUCTO_ID, @UNIDAD_ID=UNIDAD_ID FROM PRODUCTO WHERE CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID=@PRODUCTO_ID

		SELECT 	@CONT=COUNT(*)
		FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		WHERE 	ORDEN_DE_COMPRA=@ODC
				AND SD.CLIENTE_ID=@CLIENTE_ID
				AND SDD.PRODUCTO_ID=@PRODUCTO_ID
				AND SDD.ESTADO_GT IS NULL
				AND SDD.FECHA_ESTADO_GT IS NULL

		SELECT 	TOP 1
				@DOC_EXT=SD.DOC_EXT
		FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		WHERE 	ORDEN_DE_COMPRA=@ODC
				AND PRODUCTO_ID=@PRODUCTO_ID
				AND SD.CLIENTE_ID=@CLIENTE_ID
				and SDD.fecha_estado_gt is null
				and SDD.estado_gt is null
				

		Select 	@REMANENTE=sum(cantidad_solicitada)
		from	sys_int_det_documento
		where	doc_ext=@doc_ext
				and fecha_estado_gt is null
				and estado_gt is null


		IF @CONT>0 --EXISTE EN LA ODC
		BEGIN
			set @status='1'
		END
		ELSE
		BEGIN
			RAISERROR('El producto ingresado no existe en la orden de compra o el mismo ya fue recibido en su totalidad',16,1)
			return
		END

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

ALTER PROCEDURE [dbo].[VALIDAR_VEHICULO]
@VEHICULO_ID 	AS VARCHAR(50),
@VALIDO		AS SMALLINT OUTPUT
AS
BEGIN
	DECLARE @CONTROL	SMALLINT 
	
	SELECT 	@CONTROL=COUNT(*) 
	FROM 	VEHICULO_PICKING
	WHERE	VEHICULO_ID=LTRIM(RTRIM(UPPER(@VEHICULO_ID)))
	
	IF @CONTROL=0 BEGIN
		SET @VALIDO=0
	END
	ELSE BEGIN
		SET @VALIDO=1
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

ALTER procedure [dbo].[VerExistencia]
@Cliente	varchar(20),
@Producto	varchar(30)
As
Begin
	SELECT 	 dd.fecha_vencimiento
			,isnull(p.orden_picking,999) as orden
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
	FROM	rl_det_doc_trans_posicion rl
			inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
			inner join posicion p on (rl.posicion_actual=p.posicion_id)
			left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
	WHERE	rl.doc_trans_id_egr is null
			and rl.nro_linea_trans_egr is null
			and rl.disponible='1'
			and isnull(em.disp_egreso,'1')='1'
			and isnull(em.picking,'1')='1'
			and p.pos_lockeada='0' and p.picking='1'
			and cl.disp_egreso='1' and cl.picking='1'
			and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
			and dd.cliente_id=@Cliente
			and dd.producto_id=@Producto
	UNION
	SELECT	 dd.fecha_vencimiento
			,isnull(n.orden_locator,999) as orden
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
	FROM	rl_det_doc_trans_posicion rl
			inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
			inner join nave n on (rl.nave_actual=n.nave_id)
			left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
	WHERE	rl.doc_trans_id_egr is null
			and rl.nro_linea_trans_egr is null
			and rl.disponible='1'
			and isnull(em.disp_egreso,'1')='1'
			and isnull(em.picking,'1')='1'
			and rl.cat_log_id<>'TRAN_EGR'
			and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
			and cl.disp_egreso='1' and cl.picking='1'
			and dd.cliente_id=@Cliente
			and dd.producto_id=@Producto
	order by 
			producto,dd.fecha_vencimiento asc,orden 

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

ALTER Procedure [dbo].[Verif_Exist_Pos]
	@Posicion_Id 	numeric(20,0) output,
	@Out			int output
As
Begin
	Declare @Cant as numeric(10,0)

	Set @Out = 0	
	
	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Det_Conteo
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Det_Inventario
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Historico_Posicion
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Historico_Posicion_Ocupadas
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Historico_Producto
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Rl_Det_Doc_Trans_Posicion
			Where	Posicion_Anterior = @Posicion_Id
					Or Posicion_Actual = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
	End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From Rl_Doc_Tr_Egreso
			Where	Posicion_Origen = @Posicion_Id
					Or Posicion_Destino = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From 	Rl_Posicion_Prohibida_Cliente
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Rl_Producto_Navepos_Picking
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Rl_Producto_Picking
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Rl_Producto_Posicion_Permitida
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From	Sys_Locator_Ing
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
		End

	If @Out = 0
		Begin
			Select	@Cant = Count(*)
			From 	Unidad_Contenedora
			Where	Posicion_Id = @Posicion_Id

			If @Cant > 0
				Begin
					Set @Out=1
				End
	End

	--Con este Strored Proc. verifico que ho exista la posion en estas tablas, 
	--Para deletearlo desde "Generacion del Layout"
	select @Out

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

ALTER   Procedure [dbo].[Verifica_Lock_Pallet]
@Pallet		as Varchar(100),
@LockOut	as Char(1)	Output
As
Begin
	Declare 	@Documento_Id		as Numeric(20,0)
	Declare 	@Nro_Linea			as Numeric(10,0)
	Declare 	@Lock				as Char(1)
	Declare  @Usuario			as Varchar(20)
	Declare  @Terminal			as Varchar(100)
	Declare  @Count				as Int
	Declare  @Usuario_id			as Varchar(20)

	Select 	@Documento_id=Documento_id, @Nro_Linea=Nro_linea
	From	Det_Documento
	Where	Prop1=Ltrim(Rtrim(Upper(@Pallet)))

	Select 	@Lock=Lock
	From	Sys_Lock_Pallet
	Where	Documento_id=@Documento_Id
			and Nro_Linea= @Nro_Linea

	If @Lock='1'
	Begin
		
		Select	@Usuario=Usuario_id, @Terminal=Terminal
		From	Sys_Lock_pallet
		Where	Documento_id=@Documento_id and Nro_Linea=@Nro_Linea
		
		Select 	@Usuario_id=Usuario_id From #Temp_Usuario_loggin
		
		If @Usuario<>@Usuario_id
		Begin
			Set @LockOut='1'
			Raiserror('El Pallet esta siendo procesado por %s en la Terminal %s .',16,1,@Usuario,@Terminal)				
			Return
		End
		Else
		Begin
			Set @LockOut='0'
			Return
		End
	End
	Else
	Begin
		Select	@Count=Count(*)
		From	Sys_Lock_Pallet
		Where	Documento_id=@Documento_id
				and Nro_Linea=@Nro_Linea

		Set @LockOut='0'

		If @Count=1
		Begin
			Select 	@Usuario=Usuario_id From #Temp_Usuario_loggin
			Update 	Sys_lock_Pallet Set Lock='1', Fecha_Lock=Getdate(),Usuario_id=@Usuario, Terminal=Host_Name()
			Where 	Documento_id=@Documento_id and Pallet=@pallet
	
		End
		Else
		Begin
			Select 	@Usuario=Usuario_id From #Temp_Usuario_loggin
			Insert Into Sys_Lock_Pallet 
				Select 	Documento_id,
						Nro_Linea,
						@pallet,
						@usuario,
						Host_name(),
						'1',
						Getdate()
				from	Det_Documento
				Where	Documento_id=@Documento_id
						and Prop1=Ltrim(Rtrim(Upper(@Pallet)))


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

ALTER    PROCEDURE [dbo].[VERIFICA_LOCKEO_POS]
@POSICION_O	as varchar(45),
@OUT	char(1) output
AS
BEGIN
	DECLARE @VPOSLOCK AS CHAR(1)
	DECLARE @EXISTPOS AS INT


	SELECT 	@EXISTPOS=COUNT(POSICION_ID)
	FROM	POSICION
	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
	
	IF @EXISTPOS=0
		BEGIN
			SELECT 	@EXISTPOS=COUNT(NAVE_ID)	
			FROM 	NAVE
			WHERE 	NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
					AND NAVE_TIENE_LAYOUT='1'
			
			IF @EXISTPOS=0
				BEGIN
					SELECT 	@EXISTPOS=COUNT(NAVE_ID)	
					FROM 	NAVE
					WHERE 	NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
							AND NAVE_TIENE_LAYOUT='0'					

					IF @EXISTPOS=0
						BEGIN
							SET @OUT='1'
							RAISERROR('La posicion o nave es inexistente.',16,1)
							RETURN
						END
				END
			ELSE
				BEGIN
					SET @OUT='1'
					RAISERROR('Debe ingresar la posicion y no la nave.',16,1)
					RETURN
				END
		END
	SELECT 	 @VPOSLOCK=ISNULL(POS_LOCKEADA,0)
	FROM   	 POSICION
	WHERE 	 POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
	
	IF @@ROWCOUNT =0
		BEGIN
			SET @OUT='0'
		END
	ELSE
		BEGIN
			IF @VPOSLOCK='0' 
				BEGIN
					SET @OUT='0'
				END		
			ELSE
				BEGIN
					SET @OUT='1'
					RAISERROR('La posicion esta lockeada, no es posible transferir',16,1)
				END
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

ALTER   PROCEDURE [dbo].[VERIFICA_NAVECALLE]
@NAVECALLE		AS VARCHAR(45),
@CONTROL		AS INTEGER output
AS
BEGIN
	DECLARE @MYPOS	INTEGER
	DECLARE @NAVE		VARCHAR(45)
	DECLARE @CALLE		VARCHAR(45)
	DECLARE @VERIFICA	INTEGER

	SET 	@MYPOS=CharIndex('-',@NAVECALLE,1)
	IF @MYPOS>0
	BEGIN
		SET @NAVE=SUBSTRING(@NAVECALLE, 1, @MYPOS-1)
		SET @CALLE=SUBSTRING(@NAVECALLE, @MYPOS +1, LEN(@NAVECALLE))

		SELECT 	@VERIFICA=COUNT(*)
		FROM	NAVE N (NOLOCK) INNER JOIN CALLE_NAVE CN (NOLOCK)
				ON(N.NAVE_ID=CN.NAVE_ID)
		WHERE	CN.CALLE_COD=@CALLE
				AND N.NAVE_COD=@NAVE
	END
	ELSE
	BEGIN
		SELECT 	@VERIFICA=COUNT(*)
		FROM	NAVE N (NOLOCK) INNER JOIN CALLE_NAVE CN (NOLOCK)
				ON(N.NAVE_ID=CN.NAVE_ID)
		WHERE	CN.CALLE_COD=@NAVECALLE
	END

	IF @VERIFICA>0
	BEGIN
		SET @CONTROL=1
	END
	ELSE
	BEGIN
		SELECT 	@VERIFICA=COUNT(*)
		FROM	NAVE N 
		WHERE	N.NAVE_COD=@NAVECALLE
		IF  @VERIFICA>0 
		BEGIN
			SET @CONTROL=1
		END
		ELSE
		BEGIN
			SET @CONTROL=0
		END		
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

ALTER     PROCEDURE [dbo].[VERIFICA_PALLET_POS_TR]
@POSICION_O AS VARCHAR(45),
@PALLET		AS VARCHAR(100)
AS
BEGIN
	DECLARE @CONTROL 	AS INT
	DECLARE @EXISTE 	AS INT

	--SE AGREGAN ESTAS LINEAS PARA CONTROLAR LA EXISTENCIA DEL PALLET.
	SET 	@EXISTE = (SELECT COUNT(PROP1) AS EXISTE
	FROM 	DET_DOCUMENTO
	WHERE 	PROP1 = UPPER(LTRIM(RTRIM(@PALLET))))

	IF @EXISTE =0
		BEGIN
			RAISERROR('El pallet ingresado no existe.',16,1)
			RETURN
		END


	SELECT 	@CONTROL=COUNT(RL.RL_ID)
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
			AND RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID
									FROM 	NAVE
									WHERE	NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
								)
			OR RL.POSICION_ACTUAL=	(	SELECT 	TOP 1 POSICION_ID
										FROM 	POSICION
										WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))
									)
			AND RL.CANTIDAD >0

	IF @CONTROL=0
		BEGIN
			RAISERROR('1-El pallet no esta en la posicion especificada.',16,1)
			RETURN
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

ALTER  PROCEDURE [dbo].[Verifica_Picking]
@Pallet			varchar(100),
@Movimiento		char(1) Output
As
Begin
	Declare @Control 	as Int

	SELECT 	@Control=Count(*)
	FROM 	PICKING 
	WHERE 	PROP1=@pallet
			AND CANT_CONFIRMADA	IS NULL

	If @Control>0 
	Begin
		set @Movimiento=1
		return
	End 
	Else
	Begin
		set @Movimiento=0
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

ALTER Procedure [dbo].[Verifica_Rem_CrossDock]
@Documento_id		Numeric(20,0) Output,
@Remanente		BigInt Output
As
Begin

	SELECT	@Remanente=count(dd.prop1)
	FROM 	rl_det_doc_trans_posicion rl
	        	inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
	        	inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			left join nave n on(rl.nave_actual=n.nave_id)
			left join posicion p on(rl.posicion_actual=p.posicion_id)
	WHERE	dd.documento_id=@Documento_id
			and n.pre_ingreso='1'

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

ALTER  Procedure [dbo].[Verifica_Vale_Envase]
@Viaje_id		as Varchar(30)Output,
@Secuencia	as numeric(20,0)Output
As
Begin
	----------------------------------------------------------------------
	--- 			Declaracion de Variables.				 ----
	----------------------------------------------------------------------
	Declare @Control			as Int
	Declare	@Documento		as Numeric(20,0)
	Declare @Seq				as Numeric(20,0)
	----------------------------------------------------------------------

	Select	@Control=Count(*)
	From	Rl_Env_Documento_Viaje
	Where	viaje_id=Ltrim(Rtrim(Upper(@Viaje_id)))

	If @Control=1
	Begin
		Select 	@Secuencia=Nro_Vale
		From	Rl_Env_Documento_Viaje
		Where	viaje_id=Ltrim(Rtrim(Upper(@Viaje_id)))
	End
	Else
	Begin
		
		Select 	@Documento=D.Documento_id
		From 	Documento D Inner Join Det_Documento DD 
				On(D.Documento_Id=DD.Documento_Id)
				Inner Join Producto P
				On(DD.Cliente_Id=P.Cliente_Id And DD.Producto_Id=P.Producto_Id)
		Where	D.Nro_Despacho_Importacion=Ltrim(Rtrim(Upper(@Viaje_id)))
				And P.Envase='1'
		Group By
				DD.Producto_Id, P.Descripcion, D.Nro_Despacho_Importacion,D.Documento_id

		If @Documento is not Null
		Begin
			Exec Get_Value_for_Sequence 'VALE_ENVASE'	, @Seq Output
		
			Insert into Rl_Env_Documento_Viaje Values(@Viaje_Id,@Documento,@Seq);
		
			Set @Secuencia=@Seq
		End
		Else
		Begin
			RaisError('El viaje no tiene Envases Cargados',16,1)
			Set @Secuencia=Null
			return
		End	
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

ALTER           Procedure [dbo].[Verifica_Vencimiento_Producto]
As
Begin
	Set language Español
	CREATE TABLE #temp_usuario_loggin (
		usuario_id            VARCHAR(20)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		terminal              VARCHAR(100)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		fecha_loggin          DATETIME,
		session_id            VARCHAR(60)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		rol_id                VARCHAR(5)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		emplazamiento_default VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		deposito_default      VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
	)

	CREATE TABLE #temp_saldos_catlog (
		cliente_id     VARCHAR(15)    COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		producto_id    VARCHAR(30)    COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		cat_log_id     VARCHAR(50)    COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		cantidad       NUMERIC(20,5) NOT NULL,
		categ_stock_id VARCHAR(15)    COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		est_merc_id    VARCHAR(15)    COLLATE SQL_Latin1_General_CP1_CI_AS NULL
	)

	Exec Funciones_loggin_Api#Registra_Usuario_Loggin 'USER'
	---------------------------------------------
	--			Cursores.
	---------------------------------------------
	Declare @CProductos		Cursor
	---------------------------------------------
	--			Cursor @CProductos.
	---------------------------------------------
	Declare @Cliente_Id 	As varchar(15)
	Declare @Producto_Id 	As varchar(30)
	Declare @Fec_Vto 		As datetime
	Declare @Nro_Lote 		As varchar(50)
	Declare @Nro_Partida 	As varchar(50)
	Declare @Nro_Despacho 	As varchar(50)
	Declare @Nro_Bulto 		As varchar(50)
	Declare @nro_serie 		As varchar(50)
	Declare @Nave 			As numeric(20,0)
	Declare @Posicion 		As numeric(20,0)
	Declare @PValor 		As numeric(20,5)
	Declare @pCatLogOld 	As varchar(50)
	Declare @pCatLogNew 	As varchar(50)
	Declare @Est_Merc_Id 	As varchar(15)
	Declare @PROP1 			As varchar(100)
	Declare @Prop2 			As varchar(100)
	Declare @Prop3 			As varchar(100)
	Declare @Peso 			As numeric(20,5)
	Declare @Volumen 		As numeric(20,5)
	Declare @Unidad_Id 		As varchar(5)
	Declare @Unidad_Peso 	As varchar(5)
	Declare @Unidad_Volumen As varchar(5)
	Declare @Moneda_Id 		As varchar(20)
	Declare @Costo 			As numeric(20,5)	
	---------------------------------------------	
	Declare @seq			As Numeric(38,0)
	Declare @DocExt			As Varchar(100)
	Declare @Nave_Cod		As Varchar(45)
	Declare @Rl_Id			As Numeric(20,0)

	Set @CProductos=Cursor For
		Select 
				DD.Cliente_id, DD.Producto_Id, DD.Fecha_Vencimiento, DD.Nro_Lote, DD.Nro_Partida, DD.Nro_Despacho, DD.Nro_Bulto,
				DD.Nro_Serie, RL.Nave_Actual, RL.Posicion_Actual, Rl.Cantidad, Rl.Cat_Log_Id, 'PV' as NewCatLogId, Rl.Est_Merc_Id, DD.Prop1,
				DD.Prop2, DD.Prop3, DD.Peso, DD.Volumen, DD.Unidad_ID, DD.Unidad_Peso, DD.Unidad_Volumen, DD.Moneda_id, DD.Costo,Rl.Rl_Id
		from 	Det_Documento DD 
				Inner Join Det_Documento_Transaccion DDT
				On(DD.Documento_id=DDT.Documento_id and DD.Nro_Linea=DDT.Nro_Linea_Doc)
				Inner Join Rl_Det_Doc_Trans_Posicion Rl
				On(Rl.Doc_Trans_id=DDT.Doc_Trans_Id and Rl.Nro_Linea_Trans=DDT.Nro_Linea_Trans)
				Left Join Nave N 
				on(rl.nave_actual=N.Nave_Id)
		Where	(DD.Fecha_Vencimiento <=GetDate())
				And rl.Cat_Log_Id<>'PV' and dd.Fecha_Vencimiento is not null
				And Rl.Cat_Log_Id<>'SCRAP';

	Open @CProductos;




	Fetch Next From @CProductos into   	  @Cliente_Id		, @Producto_Id			, @Fec_Vto
										, @Nro_Lote			, @Nro_Partida			, @Nro_Despacho
										, @Nro_Bulto		, @nro_serie			, @Nave
										, @Posicion			, @PValor				, @pCatLogOld
										, @pCatLogNew		, @Est_Merc_Id			, @PROP1
										, @Prop2			, @Prop3				, @Peso
										, @Volumen			, @Unidad_Id			, @Unidad_Peso
										, @Unidad_Volumen	, @Moneda_Id			, @Costo
										, @Rl_Id;			
	While @@Fetch_Status=0
	Begin
		Exec Funciones_Inventario_Api#Realizar_Ajuste_Cat_log
											  @Cliente_Id		, @Producto_Id			, @Fec_Vto
											, @Nro_Lote			, @Nro_Partida			, @Nro_Despacho
											, @Nro_Bulto		, @nro_serie			, @Nave
											, @Posicion			, @PValor				, @pCatLogOld
											, @pCatLogNew		, @Est_Merc_Id			, @PROP1
											, @Prop2			, @Prop3				, @Peso
											, @Volumen			, @Unidad_Id			, @Unidad_Peso
											, @Unidad_Volumen	, @Moneda_Id			, @Costo
											, @Rl_Id;													
		----------------------------------------------------------------------------------------------------
		--DEVOLUCION A JDE.
		----------------------------------------------------------------------------------------------------
		if (@Nave is not null) and (@posicion is null)
		Begin
			Select @Nave_cod=Nave_cod from Nave Where Nave_Id=@Nave;
		End
		Else
		Begin
			Select 	Distinct 
					@Nave_Cod=n.Nave_Cod
			from	Posicion P inner join Nave N On (P.nave_id=N.Nave_Id)
			Where	P.Posicion_Id=@Posicion;
			
		End

		Exec Dbo.GET_VALUE_FOR_SEQUENCE 'AJUSTE_CAT_LOG', @Seq Output;

		Exec Ajuste_Categoria_Logica 	@Cliente_id, 'ST01', @Seq, @Producto_id,@PValor, @Est_Merc_Id, 'PV',
										Null, @Nro_Lote, @PROP1, @Fec_Vto, @Nro_Despacho, @Nro_Partida, 
										@Unidad_Id, Null, @Nave_Cod, @pCatLogOld;
		----------------------------------------------------------------------------------------------------
		Fetch Next From @CProductos into   	  @Cliente_Id		, @Producto_Id			, @Fec_Vto
											, @Nro_Lote			, @Nro_Partida			, @Nro_Despacho
											, @Nro_Bulto		, @nro_serie			, @Nave
											, @Posicion			, @PValor				, @pCatLogOld
											, @pCatLogNew		, @Est_Merc_Id			, @PROP1
											, @Prop2			, @Prop3				, @Peso
											, @Volumen			, @Unidad_Id			, @Unidad_Peso
											, @Unidad_Volumen	, @Moneda_Id			, @Costo
											, @Rl_Id;			
	End;
	Close @CProductos;
	Deallocate @CProductos;
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

ALTER PROCEDURE [dbo].[VERIFICA_VERSION_HH]
	@VER_HH 	VARCHAR(10),
	@VERIFICA	CHAR(1)	OUTPUT
AS
BEGIN
	DECLARE @VERSION_INT VARCHAR(10)


	SELECT 	@VERSION_INT = VER_ACTUAL_HH
	FROM	SYS_VERSION_HH
	
	IF LTRIM(RTRIM(UPPER(@VER_HH)))<> LTRIM(RTRIM(UPPER(@VERSION_INT)))
		BEGIN
			SET @VERIFICA='0'
			RAISERROR('LA VERSION NO ES CORRECTA. CONSULTE AL DPTO DE SISTEMAS.',16,1)
			RETURN 
		END
	ELSE
		BEGIN
			SET @VERIFICA='1'			
			RETURN
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

ALTER  PROCEDURE [dbo].[VERIFY_SYS_LOCATOR_ING]
@DOCUMENTO_ID 	AS NUMERIC(20,0),
@NRO_LINEA		AS NUMERIC(20,0),
@NRO_PALLET		AS VARCHAR(100)
AS

BEGIN
	BEGIN
		IF @DOCUMENTO_ID IS NULL
			RAISERROR ('EL PARAMETRO @DOCUMENTO_ID NO PUEDE SER NULO. SQLSERVER', 16, 1)
	END
	BEGIN
		IF @NRO_LINEA IS NULL
			RAISERROR ('EL PARAMETRO @NRO_LINEA NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END
	BEGIN
		IF @NRO_PALLET IS NULL
			RAISERROR ('EL PARAMETRO @NROPALLET NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END
	BEGIN
		SELECT 	SLI.POSICION_ID, P.POSICION_COD, P.ORDEN_LOCATOR
		FROM 	SYS_LOCATOR_ING SLI
				INNER JOIN POSICION P
				ON(SLI.POSICION_ID=P.POSICION_ID)
		WHERE 	SLI.DOCUMENTO_ID=@DOCUMENTO_ID 
				AND NRO_LINEA=@NRO_LINEA 
				AND NRO_PALLET=UPPER(LTRIM(RTRIM(@NRO_PALLET)))
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

ALTER   PROCEDURE [dbo].[VerLock]
AS

BEGIN

	select distinct 
		convert (smallint, req_spid) As spid,
		object_name(rsc_objid) As ObjId,
		dbo.Get_data_Session_Login(req_spid,'1') as usuario, 
		dbo.Get_data_Session_Login(req_spid,'2') as nombre_usuario,
		dbo.Get_data_Session_Login(req_spid,'3') as terminal,
		dbo.Get_data_Session_Login(req_spid,'4') as fecha_login,
		dbo.Sys_Obj_Locking(req_spid,'1') as status, 
		dbo.Sys_Obj_Locking(req_spid,'2') as hostname,
		dbo.Sys_Obj_Locking(req_spid,'3') as program_name,
		dbo.Sys_Obj_Locking(req_spid,'4') as cmd,
		dbo.Sys_Obj_Locking(req_spid,'5') as loginname,
		dbo.Sys_Obj_Locking(req_spid,'6') as fecha_lock,
		dbo.Sys_Obj_Locking(req_spid,'7') as dbname,
		getdate() as fecha_registro
	from 	master.dbo.syslockinfo,
		master.dbo.spt_values v,
		master.dbo.spt_values x,
		master.dbo.spt_values u

	where   master.dbo.syslockinfo.rsc_type = v.number
			and v.type = 'LR'
			and master.dbo.syslockinfo.req_status = x.number
			and x.type = 'LS'
			and master.dbo.syslockinfo.req_mode + 1 = u.number
			and u.type = 'L'
			and object_name(rsc_objid) is not null
			and substring (u.name, 1, 8)='X'
			and upper(dbo.Sys_Obj_Locking(req_spid,'7'))='AGUAS_DESA'

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