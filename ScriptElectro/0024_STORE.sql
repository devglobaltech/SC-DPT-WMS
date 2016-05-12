USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 05:34 p.m.
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

ALTER    Procedure [dbo].[Sys_Dev_Transferencia] 
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
				Values (@Cliente_id,'T01',Getdate(),@Doc_Trans_id,null,null,'P',Getdate(), Null)

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
		
			Select @Nro_Linea=IsNull(Max(Nro_Linea),0)+1 From Sys_Dev_Det_Documento where Doc_Ext=Cast(@Doc_Trans_id as varchar(20))

			--El Primero (-)
			Insert into Sys_Dev_Det_Documento (	Doc_Ext,Nro_Linea,Cliente_Id,Producto_Id,Cantidad_Solicitada,Cantidad,Est_Merc_Id,Cat_Log_Id,Nro_Bulto,
											Descripcion,Nro_Lote,Nro_Pallet,Fecha_Vencimiento,Nro_Despacho,Unidad_id,Estado_GT,Fecha_Estado_Gt,
											Documento_Id,Nave_Id,Nave_Cod, Flg_Movimiento)
										  (
											Select 	Distinct
													 @Doc_Trans_Id,@Nro_Linea,dd.Cliente_id,dd.Producto_id,(Rl.Cantidad-(Rl.Cantidad*2)),(Rl.Cantidad-(Rl.Cantidad*2))
													,dd.Est_Merc_id,Rl.Cat_Log_id,dd.Nro_Bulto,Prod.Descripcion,dd.Nro_Lote,dd.Prop1
													,dd.Fecha_Vencimiento,dd.Nro_Despacho,dd.Unidad_id,'P',Getdate()
													,dd.Documento_id,@NavIdAnt,@NavAnt, Null
											from	Rl_Det_Doc_Trans_Posicion Rl Inner Join Det_Documento_Transaccion Ddt
													On(Rl.Doc_Trans_Id=Ddt.Doc_Trans_Id And Rl.Nro_linea_Trans=Ddt.Nro_Linea_Trans)
													Inner join Det_Documento Dd
													On(Ddt.Documento_id=Dd.Documento_id And Ddt.Nro_Linea_Doc=Dd.Nro_Linea)
													Inner Join Producto Prod
													On(Dd.Cliente_id=Prod.Cliente_Id And Dd.Producto_id=Prod.Producto_id)
											Where	Rl.Rl_Id=@vRl and Rl.Doc_Trans_Id_Tr=@Doc_Trans_id
											)


			Select @Nro_Linea=IsNull(Max(Nro_Linea),0)+1 From Sys_Dev_Det_Documento where Doc_Ext=Cast(@Doc_Trans_id as varchar(20))


			--El Segundo(+)
			Insert into Sys_Dev_Det_Documento (	Doc_Ext,Nro_Linea,Cliente_Id,Producto_Id,Cantidad_Solicitada,Cantidad,Est_Merc_Id,Cat_Log_Id,Nro_Bulto,
											Descripcion,Nro_Lote,Nro_Pallet,Fecha_Vencimiento,Nro_Despacho,Unidad_id,Estado_GT,Fecha_Estado_Gt,
											Documento_Id,Nave_Id,Nave_Cod, Flg_Movimiento)
										  (
											Select 	Distinct
													 @Doc_Trans_Id,@Nro_Linea,dd.Cliente_id,dd.Producto_id,(Rl.Cantidad),(Rl.Cantidad)
													,dd.Est_Merc_id,Rl.Cat_Log_id,dd.Nro_Bulto,Prod.Descripcion,dd.Nro_Lote,dd.Prop1
													,dd.Fecha_Vencimiento,dd.Nro_Despacho,dd.Unidad_id,'P',Getdate(),dd.Documento_id
													,@NavIdAct,@NavAct, Null
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

ALTER PROCEDURE [dbo].[REPALETIZAR]
	@POSICION_O		AS VARCHAR(45),
	@POSICION_D		AS VARCHAR(45),
	@PALLET_O		AS VARCHAR(100),
	@PALLET_D		AS VARCHAR(100),
	@USUARIO 		AS VARCHAR(30),
	@CONTENEDORA	AS VARCHAR(100)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	DECLARE @CLIENTE_ID		AS VARCHAR(5)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(1,0)
	DECLARE @LIMITE			AS NUMERIC(20,0)
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)
	
	EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
	IF @OUT='1'
		BEGIN
			RETURN
		END
	--Actualizo el pallet en det_documento
	UPDATE det_documento SET PROP1 = LTRIM(RTRIM(UPPER(@PALLET_D))) 
	WHERE  PROP1=LTRIM(RTRIM(UPPER(@PALLET_O)))  AND NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA))) 


	--OBTENGO LA RL DEL PALLET EN UN CURSOR POR SI HAY MAS DE UNA LINEA EN RL.--
	DECLARE CUR_RL_TR CURSOR FOR
		SELECT 	RL.RL_ID,DD.CLIENTE_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET_D))) 
				AND NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA))) 
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		--Aca verifico que el pallet este en la posicion indicada.
		SELECT 	@CONTROL=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET_D))) 
				AND NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA))) 
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		IF @CONTROL=0
			BEGIN
				RAISERROR('1-El pallet: %s o la contenedora: %s no esta en la posicion especificada.',16,1,@PALLET_O,@CONTENEDORA)
				DEALLOCATE CUR_RL_TR
				RETURN
			END
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		--Controlo que la categoria logica no sea TRAN_ING, TRAN_EGR
		Exec Mob_Transf_VerificaCatLog @Pallet_D, @Posicion_O, @TRANSFIERE OUTPUT
		If @Transfiere=0
		Begin
			Return
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		-- Voy por las posiciones de Picking
		Exec Verifica_Picking @PALLET_D, @PICKING OUTPUT
		If @Picking=1
		Begin
			--Aca tengo que verificar que la posicion de destino sea de picking.
			Select @Picking=Dbo.IsPosPicking(@POSICION_D)
			If @Picking=0
			Begin
				RAISERROR('1- La ubicacion destino no es una ubicacion de Picking.',16,1)
				DEALLOCATE CUR_RL_TR
				RETURN
			End
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		SELECT 	@LIMITE=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET_D))) 
				AND NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		SELECT @EXISTE = COUNT(*)
		FROM  TRANSACCION T
			  INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
			  INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
			  AND RTC.CLIENTE_ID IN (SELECT CLIENTE_ID FROM CLIENTE
									 WHERE (SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
										   FROM   RL_SYS_CLIENTE_USUARIO
										   WHERE  CLIENTE_ID = RTC.CLIENTE_ID
										   AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)WHERE T.TIPO_OPERACION_ID='TR' AND RTE.ORDEN=1	
		IF @EXISTE = 0
		BEGIN
			RAISERROR('El usuario %s no posee clientes asignados',16,1,@USUARIO)
			return
		END

		--GENERO EL DOCUMENTO DE TRANSACCION .--
		EXEC CREAR_DOC_TRANSFERENCIA @USUARIO=@USUARIO
	
		--OBTENGO EL DOC_TRANS_ID INSERTADO.--
		SET @VDOCNEW=@@IDENTITY
	
		UPDATE DOCUMENTO_TRANSACCION SET TR_POS_COMPLETA= '0' WHERE DOC_TRANS_ID=@VDOCNEW
	
		--ABRO EL CURSOR PARA SU POSTERIOR USO	
		OPEN CUR_RL_TR
	
		SET @CONT_LINEA= 0
		SET @LIM_CONT=0

		FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID
		WHILE (@@FETCH_STATUS=0)
		BEGIN
				SET @CONT_LINEA= @CONT_LINEA + 1 
				INSERT INTO DET_DOCUMENTO_TRANSACCION (
				        DOC_TRANS_ID,     NRO_LINEA_TRANS,
				        DOCUMENTO_ID,     NRO_LINEA_DOC,
				        MOTIVO_ID,        EST_MERC_ID,
				        CLIENTE_ID,       CAT_LOG_ID,
				        ITEM_OK,          MOVIMIENTO_PENDIENTE,
				        DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF)
				VALUES (
				        @VDOCNEW
				        ,@CONT_LINEA --NRO DE LINEA DE DET_DOCUMENTO_TRANSACCION
				        ,NULL   ,NULL   ,NULL     ,NULL
				        ,@CLIENTE_ID
				        ,NULL ,'0' ,'0' ,NULL     ,NULL)
	
		
				SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@POSICION_D) AS INT)
				SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@POSICION_D) AS INT)
	
				INSERT INTO RL_DET_DOC_TRANS_POSICION
				           (DOC_TRANS_ID,
				            NRO_LINEA_TRANS,
				            POSICION_ANTERIOR,
				            POSICION_ACTUAL,
				            CANTIDAD,
				            TIPO_MOVIMIENTO_ID,
				            ULTIMA_SECUENCIA,
				            NAVE_ANTERIOR,
				            NAVE_ACTUAL,
				            DOCUMENTO_ID,
				            NRO_LINEA,
				            DISPONIBLE,
				            DOC_TRANS_ID_TR,
				            NRO_LINEA_TRANS_TR,
				            CLIENTE_ID,
				            CAT_LOG_ID,
				            EST_MERC_ID)
				            (SELECT   DOC_TRANS_ID 
									, NRO_LINEA_TRANS
									, POSICION_ACTUAL
									, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
									, CANTIDAD
									, NULL
									, NULL
									, NAVE_ACTUAL
									, @NEWNAVE
									, NULL
									, NULL
									, 0
									, @vDocNew
				                    , 1 
									, CLIENTE_ID
									, CAT_LOG_ID
									, EST_MERC_ID
				             FROM RL_DET_DOC_TRANS_POSICION
				             WHERE RL_ID = @vRLID
				             ) 
			-- GENERAR AUDITORIA DEL REPALETIZADO
			EXEC AUDITORIA_HIST_INSERT_REPALETIZADO @vRLID, @PALLET_O, @PALLET_D

			EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE

			DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			
			----------------------------------------------------------------------------
			--	ESTO ES SEGURAMENTE UN BUG DE SQL SERVER. REVISAR PARA CORREGIR
			--	ESTE ALGORITMO.
			SET @LIM_CONT=@LIM_CONT +1
			IF (@LIMITE=@LIM_CONT)
				BEGIN
					BREAK
				END
			----------------------------------------------------------------------------
			FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID
			
		END

		-- CARGO LA TABLA PARA IMPRESION DE ETIQUETAS
		-- LRojas 02/03/2012 TrackerID 3806: Inserto Usuario para Demonio de Impresion
		INSERT INTO IMPRESION_RODC (DOCUMENTO_ID,NRO_LINEA,TIPO_ETI,IMPRESO, USUARIO_IMP)
		(		SELECT	
						DOCUMENTO_ID,NRO_LINEA,0 AS TIPO_ETI,0 AS IMPRESO, @USUARIO
				FROM
						DET_DOCUMENTO
				WHERE	
						PROP1=LTRIM(RTRIM(UPPER(@PALLET_D))) 
						AND NRO_BULTO=LTRIM(RTRIM(UPPER(@CONTENEDORA))))			

		-- DEVOLUCION
		EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW
	
		--FINALIZA LA TRANSFERENCIA	
		EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
	
		-- BORRO POR SI QUEDA ALGUNA RL EN 0
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE CANTIDAD =0 AND DOC_TRANS_ID=@VDOCNEW
	
		--ACTUALIZO LAS POSICIONES DE PICKING
		EXEC ACTUALIZA_POS_PICKING_TR @POSICION_O,@POSICION_D,@PALLET_D

		UPDATE POSICION SET POS_VACIA='0' WHERE POSICION_ID IN (SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
			
		UPDATE POSICION SET POS_VACIA='1' WHERE POSICION_ID  NOT IN
		(SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
	
		CLOSE CUR_RL_TR
		DEALLOCATE CUR_RL_TR
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

ALTER procedure [dbo].[Restriccion]
@Posicion	varchar(45),
@prod		varchar(30)
as
Begin
	declare @producto_id as varchar(30)
	declare @posicion_id as numeric(30,0)
	--creo temporal
	select * into #d from rl_producto_posicion_permitida where 1=0
	
	set @producto_id=null
	select @producto_id=producto_id from producto where cliente_id='VITALCAN' AND PRODUCTO_ID=@PROD
	
	if @producto_id is null
	begin
		select @producto_id=producto_id from producto where cliente_id='VITALCAN' AND PRODUCTO_ID='0'+@PROD
	end

	select @posicion_id=posicion_id from posicion where posicion_cod=@Posicion

	if (@producto_id is not null) And(@posicion_id is not null)
	begin
		insert into rl_producto_posicion_permitida values('VITALCAN',@PRODUCTO_ID, NULL, @POSICION_ID)
	end
	else
	begin
		if @prod='Cualq. Cód.'
		begin
			insert into rl_producto_posicion_permitida
			select	cliente_id, producto_id, null, @posicion_id
			from	producto 
			where	cliente_id='VITALCAN'
		end
		else
		begin
			insert into #d values('VITALCAN',@PRODUCTO_ID, NULL, @POSICION_ID)
		end
	end

	select * from #d
End--fin procedure.
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

ALTER    PROCEDURE [dbo].[RPT_DEVOLUCIONES]
	 @VIAJE			VARCHAR(100) OUTPUT
	,@PEDIDO		VARCHAR(100) OUTPUT
	,@AGENTE		VARCHAR(100) OUTPUT
	,@F_DESDE		VARCHAR(10)	 OUTPUT
	,@F_HASTA		VARCHAR(10)	 OUTPUT
AS
BEGIN
	DECLARE @USR_RPT	VARCHAR(15)
	DECLARE @TERMINAL	VARCHAR(15)

	SET @TERMINAL=HOST_NAME()
	SELECT @USR_RPT=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	SELECT 	
			 CONVERT(VARCHAR,D.FECHA_CPTE,103)					[FECHA_DEVOLUCION]
			,D.NRO_DESPACHO_IMPORTACION							[COD_VIAJE]
			,DD.PRODUCTO_ID	
			,DD.DESCRIPCION
			,DD.CANTIDAD
			,DD.UNIDAD_ID
			,DDA.MOTIVO_ID		
			,M.DESCRIPCION										[DESC_MOTIVO]
			,DDA.OBSERVACION
			,DBO.GET_DATA_I08(DD.DOCUMENTO_ID,DD.NRO_LINEA,'1')	+ ' - ' +
			 DBO.GET_AGENTE_DESC(DD.DOCUMENTO_ID, DD.NRO_LINEA,'1') [COD_AGENTE]
			,DBO.GET_DATA_I08(DD.DOCUMENTO_ID,DD.NRO_LINEA,'2')	[PEDIDO]
			,DD.CAT_LOG_ID_FINAL								[CAT_LOG_ID]
			,DD.EST_MERC_ID
			,ISNULL(P.POSICION_COD,N.NAVE_COD)					[UBICACION]
			,CASE 	WHEN DD.PROP1 IS NULL THEN '' 
					ELSE 'Nro. Pallet: ' + CAST(DD.PROP1 AS VARCHAR) + ', '  
			 END +
			 CASE 	WHEN DD.NRO_LOTE IS NULL THEN ''  
					ELSE 'Nro. Lote: ' + CAST(DD.NRO_LOTE AS VARCHAR)+ ', '  
			 END +
			 CASE 	WHEN DD.NRO_PARTIDA IS NULL THEN ''
					ELSE 'Nro. Partida: ' + CAST(DD.NRO_PARTIDA AS VARCHAR)+ ', '  
			 END +
			 CASE	WHEN DD.NRO_BULTO IS NULL THEN ''
					ELSE 'Nro. Bulto: ' + CAST(DD.NRO_BULTO AS VARCHAR)+ ', '  
			 END +
			 CASE 	WHEN DD.FECHA_VENCIMIENTO IS NULL THEN ''
					ELSE 'Fecha Vto.: ' + CONVERT(VARCHAR,DD.FECHA_VENCIMIENTO,103)
			 END AS [PROPIEDADES]
			,DD.PROP1
			,DD.NRO_LOTE
			,DD.NRO_PARTIDA
			,DD.NRO_BULTO
			,CONVERT(VARCHAR,DD.FECHA_VENCIMIENTO,103)			[FECHA_VENCIMIENTO]
			,'Terminal: ' 	+ @TERMINAL							[TERMINAL_RPT]
			,'Usuario: '	+ @USR_RPT							[USUARIO_RPT]
			,CONVERT(VARCHAR,GETDATE(),103)						[F_IMPRESION]
			,DBO.GET_AGENTE_DESC(DD.DOCUMENTO_ID, DD.NRO_LINEA,'1') [DESCRIPCION_AGENTE]
	FROM 	VDOCUMENTO D (NOLOCK) INNER JOIN VDET_DOCUMENTO DD 		(NOLOCK) ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN AUX_DET_DOCUMENTO DDA 						(NOLOCK) ON(DD.DOCUMENTO_ID=DDA.DOCUMENTO_ID AND DD.NRO_LINEA=DDA.NRO_LINEA)
			LEFT JOIN MOTIVO M 										(NOLOCK) ON(DDA.MOTIVO_ID=M.MOTIVO_ID)
			INNER JOIN VDET_DOCUMENTO_TRANSACCION DDT 				(NOLOCK) ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			LEFT JOIN RL_DET_DOC_TRANS_POSICION RL					(NOLOCK) ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
			LEFT JOIN POSICION P									(NOLOCK) ON(P.POSICION_ID=RL.POSICION_ACTUAL)
			LEFT JOIN NAVE	N										(NOLOCK) ON(N.NAVE_ID=RL.NAVE_ACTUAL)
	WHERE 	
			((@VIAJE IS NULL) OR (D.NRO_DESPACHO_IMPORTACION LIKE  '%'+@VIAJE+'%'))
			AND ((@F_DESDE IS NULL) OR (D.FECHA_CPTE BETWEEN @F_DESDE AND DATEADD(DD,1,@F_HASTA)))
			AND ((@PEDIDO IS NULL) OR (DBO.GET_DATA_I08(DD.DOCUMENTO_ID,DD.NRO_LINEA,'2') LIKE '%'+ @PEDIDO +'%'))
			AND ((@AGENTE IS NULL) OR (DBO.GET_DATA_I08(DD.DOCUMENTO_ID,DD.NRO_LINEA,'1')=@AGENTE))
	ORDER BY	
			D.FECHA_CPTE,D.NRO_DESPACHO_IMPORTACION,10
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

ALTER               PROCEDURE [dbo].[RPT_MOVIMIENTOS]
	@CLIENTE_ID		VARCHAR(15)	OUTPUT,
	@PRODUCTO_ID	VARCHAR(30)		OUTPUT,
	@NRO_PALLET		VARCHAR(100)	OUTPUT,
	@NRO_PARTIDA	VARCHAR(50)		OUTPUT,
	@FECHA_VTO		VARCHAR(8)		OUTPUT,	--ANSI
	@F_DESDE		VARCHAR(8)			OUTPUT,	--ANSI
	@F_HASTA		VARCHAR(8)			OUTPUT,	--ANSI
	@NRO_LOTE		VARCHAR(50)		OUTPUT,
	@PROP2			VARCHAR(100)		OUTPUT,
	@PROP3			VARCHAR(100)		OUTPUT,
	@USUARIO		VARCHAR(20) 		OUTPUT,
	@COD_PEDIDO	VARCHAR(30)		OUTPUT,
	@COD_VIAJE	VARCHAR(100)		OUTPUT
AS
BEGIN
	/*
	----------------------------------------------------------------------
	CREATE TABLE #TEMP_CRITERIOS_RPT(
		TIPO_AUDITORIA_ID NUMERIC(20,0)
	)
	INSERT INTO #TEMP_CRITERIOS_RPT
	SELECT 	TIPO_AUDITORIA_ID
	FROM	PARAMETROS_AUDITORIA
	*/
	----------------------------------------------------------------------
	--					DECLARACION DE VARIABLES.
	----------------------------------------------------------------------
	DECLARE @SALDO_INICIAL	FLOAT
	DECLARE @TERMINAL_RTP	VARCHAR(100)
	DECLARE @USUARIO_RTP	VARCHAR(20)
	DECLARE @P_FECHA_I		VARCHAR(8)
	DECLARE @CONT			FLOAT
	
	DECLARE @PICK			NUMERIC(20,0)
	DECLARE @SERIE			VARCHAR(100)
	DECLARE @PICK_ANT			NUMERIC(20,0)
	DECLARE @SERIE_ACUM			VARCHAR(3000)
	----------------------------------------------------------------------
	SET 	@TERMINAL_RTP	=HOST_NAME()
	SELECT	@USUARIO_RTP	=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	--SET	@USUARIO_RTP	='USER'
	
	SELECT 	@CONT=COUNT(*)
	FROM	#TEMP_CRITERIOS_RPT
	WHERE	TIPO_AUDITORIA_ID IN(4,15,16)

	IF @CONT=3
	BEGIN
		SET @P_FECHA_I=@F_DESDE
	END
	IF (@NRO_PALLET IS NOT NULL) OR(@NRO_PARTIDA IS NOT NULL)OR (@FECHA_VTO IS NOT NULL)
	    OR(@NRO_LOTE IS NOT NULL) OR (@PROP2 IS NOT NULL) OR(@PROP3 IS NOT NULL)OR(@USUARIO IS NOT NULL)
	BEGIN
	       SET @P_FECHA_I=NULL
	END
	
	SELECT 	 DBO.GET_SALDO_INICIAL(AH.CLIENTE_ID,AH.PRODUCTO_ID,@P_FECHA_I,@F_HASTA)	AS [SD_INICIAL]
			,AH.CLIENTE_ID + ' - ' +C.RAZON_SOCIAL										AS [CLIENTE_RZ]
			,AH.PRODUCTO_ID																AS [PRODUCTO_ID]
			,PA.DESCRIPCION																AS [DESC_OP]
			,'Usuario: ' + @USUARIO_RTP													AS [USOINTERNOUSUARIO]
			,'Terminal: ' + @TERMINAL_RTP												AS [USOINTERNOTERMINAL]
			,AH.FECHA_AUDITORIA															AS [FECHA_AUDITORIA]
			,CONVERT(VARCHAR,AH.FECHA_AUDITORIA,103) 									AS [FECHA]
			,TC.DESCRIPCION	+ ' - ' + D.NRO_DESPACHO_IMPORTACION						AS [DESC_TIPO_COMPROBANTE]
			,AH.USUARIO_ID +' - '+ SU.NOMBRE											AS [USUARIO_OPERACION]
			,AH.TERMINAL																AS [TERMINAL_OPERACION]
			,CAST(CASE WHEN P.POSICION_COD IS NULL THEN ISNULL(N2.NAVE_COD,ISNULL(N.NAVE_COD,'PREING')) ELSE P.POSICION_COD END AS VARCHAR) AS [UBICACION]
			,AH.CANTIDAD																AS [CANTIDAD]
			,AH.DOC_EXT																	AS [DOC_EXT]
			,AH.CAT_LOG_ID																AS [CAT_LOG_ID]
			,DD.PRODUCTO_ID + ' - ' + DD.DESCRIPCION									AS [PROD_DESC]
			,AH.CLIENTE_ID																AS [CLIENTE_ID]
			,CASE 
			  	WHEN AH.NRO_SERIE IS NULL THEN '' 
			  	Else 'Nro.Serie: ' + CAST(AH.NRO_SERIE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_BULTO IS NULL THEN '' 
			    Else 'Nro.Bulto: ' + CAST(AH.NRO_BULTO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_LOTE IS NULL THEN '' 
			    Else 'Nro.Lote: ' + CAST(AH.NRO_LOTE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_DESPACHO IS NULL THEN '' 
			    Else 'Nro.Despacho: ' + CAST(AH.NRO_DESPACHO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_PARTIDA IS NULL THEN '' 
			    Else 'Nro.Partida: ' + CAST(AH.NRO_PARTIDA AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.PROP1 IS NULL THEN '' 
			    Else 'Nro.Pallet: ' + CAST(AH.PROP1 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.PROP2 IS NULL THEN '' 
			    Else 'Lote Prov.: ' + CAST(AH.PROP2 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.PROP3 IS NULL THEN '' 
			    Else 'Property 3: ' + CAST(AH.PROP3 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.FECHA_VENCIMIENTO IS NULL THEN '' 
			    Else 'Fecha Vencimiento: ' + CONVERT(VARCHAR,AH.FECHA_VENCIMIENTO,103) + ', '
			 END +
			 CASE
				WHEN D.SUCURSAL_ORIGEN IS NULL THEN ''
				ELSE 'Proveedor: ' +CAST(D.SUCURSAL_ORIGEN AS VARCHAR) + ' - ' +CAST(S.NOMBRE AS VARCHAR)+', '
			 END +
			 CASE
				WHEN AH.EST_MERC_ID IS NULL THEN ''
				ELSE 'Est.Merc. : ' + CAST(AH.EST_MERC_ID AS VARCHAR)
			 END +
			 CASE
				WHEN D.NRO_REMITO IS NULL THEN ''
				ELSE 'Nro. Remito : ' + CAST(D.NRO_REMITO AS VARCHAR)
			 END AS [PROPIEDADES]
			,AH.NRO_SERIE
			,AH.NRO_BULTO
			,AH.NRO_LOTE
			,AH.NRO_DESPACHO
			,AH.NRO_PARTIDA				
			,AH.PROP1						[NRO_PALLET]
			,AH.PROP2						[LOTE_PROVEEDOR]
			,AH.FECHA_VENCIMIENTO			[FECHA_VENCIMIENTO]
			,D.SUCURSAL_ORIGEN				[ORIGEN_DESTINO]
			,S.NOMBRE		
			,AH.EST_MERC_ID				
			,AH.AUDITORIA_ID				[AUDITORIA_ID]
	FROM	AUDITORIA_HISTORICOS AH (NOLOCK)
			INNER JOIN vDOCUMENTO D	(NOLOCK)						ON(AH.DOCUMENTO_ID=D.DOCUMENTO_ID)
			LEFT JOIN TIPO_COMPROBANTE TC (NOLOCK)					ON(D.TIPO_COMPROBANTE_ID=TC.TIPO_COMPROBANTE_ID)
			LEFT JOIN POSICION P (NOLOCK)							ON(AH.POSICION_ID_FINAL=P.POSICION_ID)
			LEFT JOIN NAVE	N 	(NOLOCK)							ON(AH.NAVE_ID_FINAL=N.NAVE_ID)
			LEFT JOIN NAVE  N2	(NOLOCK)							ON(P.POSICION_ID=N2.NAVE_ID)
			INNER JOIN PARAMETROS_AUDITORIA PA (NOLOCK) 			ON(AH.TIPO_AUDITORIA_ID=PA.TIPO_AUDITORIA_ID)
			INNER JOIN #TEMP_CRITERIOS_RPT TMPC (NOLOCK)			ON(AH.TIPO_AUDITORIA_ID=TMPC.TIPO_AUDITORIA_ID)
			INNER JOIN CLIENTE C (NOLOCK)							ON(AH.CLIENTE_ID=C.CLIENTE_ID)
			INNER JOIN vDET_DOCUMENTO DD(NOLOCK)					ON(AH.DOCUMENTO_ID=DD.DOCUMENTO_ID AND AH.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN SYS_USUARIO SU (NOLOCK)						ON(AH.USUARIO_ID=SU.USUARIO_ID)
			LEFT JOIN SUCURSAL S(NOLOCK)							ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_ORIGEN=S.SUCURSAL_ID)
	WHERE	((@NRO_PALLET IS NULL)OR(AH.PROP1=@NRO_PALLET))
			AND((@F_DESDE IS NULL)OR(AH.FECHA_AUDITORIA BETWEEN @F_DESDE AND DATEADD(DD,1,@F_HASTA)))
			AND((@CLIENTE_ID IS NULL) OR (AH.CLIENTE_ID=@CLIENTE_ID))
			AND((@PRODUCTO_ID IS NULL) OR(AH.PRODUCTO_ID=@PRODUCTO_ID))
			AND((@NRO_PARTIDA IS NULL) OR (AH.NRO_PARTIDA=@NRO_PARTIDA))
			AND((@FECHA_VTO IS NULL) OR(AH.FECHA_VENCIMIENTO=@FECHA_VTO))
			AND((@USUARIO IS NULL) OR (AH.USUARIO_ID=@USUARIO))
			AND((@NRO_LOTE IS NULL) OR (AH.NRO_LOTE=@NRO_LOTE))
			AND((@PROP2 IS NULL) OR (AH.PROP2=@PROP2))
			AND((@PROP3 IS NULL) OR (AH.PROP3=@PROP3))
	UNION	
	--Egresos
	SELECT 
			 DBO.GET_SALDO_INICIAL(P.CLIENTE_ID,P.PRODUCTO_ID,@P_FECHA_I,@F_HASTA) AS [SD_INICIAL]
			,P.CLIENTE_ID + ' - ' +C.RAZON_SOCIAL			AS [CLIENTE_RZ]
			,P.PRODUCTO_ID									AS [PRODUCTO_ID]
			,'Egresos'										AS [DESC_OP]
			,'Usuario: ' + @USUARIO_RTP						AS [USOINTERNOUSUARIO]
			,'Terminal:' + @TERMINAL_RTP					AS [USOINTERNOTERMINAL]
			,P.FECHA_CONTROL_FAC							AS [FECHA_AUDITORIA]
			,CONVERT(VARCHAR,P.FECHA_CONTROL_FAC,103) 		AS [FECHA]
			,CASE 
			  	WHEN D.NRO_REMITO IS NULL THEN TC.DESCRIPCION	+ ' - ' + CAST(DD.DOCUMENTO_ID AS VARCHAR)
			  	Else TC.DESCRIPCION	+ ' - ' + D.NRO_REMITO
			 END AS [DESC_TIPO_COMPROBANTE]
			--,TC.DESCRIPCION	+ ' - ' + D.NRO_REMITO			AS [DESC_TIPO_COMPROBANTE]
			,P.USUARIO_CONTROL_FAC + ' - ' +SU.NOMBRE AS [USUARIO_OPERACION]
			,NULL											AS [TERMINAL_OPERACION]
			,P.POSICION_COD									AS [UBICACION]
			,ISNULL((P.CANT_CONFIRMADA *(-1)),0)			AS [CANTIDAD]
			,D.NRO_REMITO									AS [DOC_EXT]
			,DD.CAT_LOG_ID_FINAL							AS [CAT_LOG_ID]
			,P.PRODUCTO_ID + ' - ' + P.DESCRIPCION		AS [PROD_DESC]
			,P.CLIENTE_ID								AS [CLIENTE_ID]
			,CASE 
			  	WHEN DD.NRO_SERIE IS NULL THEN '' 
			  	Else 'Nro.Serie: ' + CAST(DD.NRO_SERIE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_BULTO IS NULL THEN '' 
			    Else 'Nro.Bulto: ' + CAST(DD.NRO_BULTO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_LOTE IS NULL THEN '' 
			    Else 'Nro.Lote: ' + CAST(DD.NRO_LOTE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_DESPACHO IS NULL THEN '' 
			    Else 'Nro.Despacho: ' + CAST(DD.NRO_DESPACHO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_PARTIDA IS NULL THEN '' 
			    Else 'Nro.Partida: ' + CAST(DD.NRO_PARTIDA AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.PROP1 IS NULL THEN '' 
			    Else 'Nro.Pallet: ' + CAST(DD.PROP1 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.PROP2 IS NULL THEN '' 
			    Else 'Lote Prov.: ' + CAST(DD.PROP2 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.PROP3 IS NULL THEN '' 
			    Else 'PROPERTY 3: ' + CAST(DD.PROP3 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.FECHA_VENCIMIENTO IS NULL THEN '' 
			    Else 'Fecha Vencimiento: ' + CONVERT(VARCHAR,DD.FECHA_VENCIMIENTO,103) + ' '
			 END +
			 CASE
				WHEN D.SUCURSAL_DESTINO IS NULL THEN ''
				ELSE 'Destino: ' + CAST(D.SUCURSAL_DESTINO AS VARCHAR) + ' - ' + CAST(S.NOMBRE AS VARCHAR)+', '
			 END +
			 CASE
				WHEN DD.EST_MERC_ID IS NULL THEN ''
				ELSE 'Est.Merc. : ' + CAST(DD.EST_MERC_ID AS VARCHAR) + ', '
			 END +
			 CASE
				WHEN D.NRO_REMITO IS NULL THEN ''
				ELSE 'Nro. Remito : ' + CAST(D.NRO_REMITO AS VARCHAR)
			 END +
			 CASE 
				WHEN D.TIPO_COMPROBANTE_ID IN('E02','E01','E03','E04','AJE')
				THEN ISNULL(DBO.GET_USUARIO_PEDIDO(P.CLIENTE_ID,D.NRO_DESPACHO_IMPORTACION),'')
			 END +
			 CASE 
				WHEN NOT SP.PICKING_ID IS NULL THEN ',  Núm. Series : ' + CAST(SP.SERIES AS VARCHAR)
				ELSE ''
			END
			 AS [PROPIEDADES]
			,DD.NRO_SERIE
			,DD.NRO_BULTO
			,DD.NRO_LOTE
			,DD.NRO_DESPACHO
			,DD.NRO_PARTIDA				
			,DD.PROP1						[NRO_PALLET]
			,DD.PROP2						[LOTE_PROVEEDOR]
			,DD.FECHA_VENCIMIENTO			[FECHA_VENCIMIENTO]
			,D.SUCURSAL_DESTINO			[ORIGEN_DESTINO]
			,S.NOMBRE	
			,DD.EST_MERC_ID					
			,P.PICKING_ID [AUDITORIA_ID]
	FROM	vPICKING P (NOLOCK) INNER JOIN vDET_DOCUMENTO DD	(NOLOCK) ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN vDOCUMENTO D 				(NOLOCK) ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN TIPO_COMPROBANTE TC 			(NOLOCK) ON(D.TIPO_COMPROBANTE_ID=TC.TIPO_COMPROBANTE_ID)
			INNER JOIN CLIENTE C					(NOLOCK) ON(C.CLIENTE_ID=P.CLIENTE_ID)
			LEFT JOIN SYS_USUARIO SU				(NOLOCK) ON(P.USUARIO_CONTROL_FAC=SU.USUARIO_ID)
			LEFT JOIN SUCURSAL	S					(NOLOCK) ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_DESTINO)
		    LEFT JOIN tmpSeriesPicking SP         (NOLOCK) ON(SP.PICKING_ID = P.PICKING_ID)
	WHERE	((@NRO_PALLET IS NULL)OR(P.PROP1=@NRO_PALLET))
			AND((@F_DESDE IS NULL)OR(d.FECHA_ALTA_GTW BETWEEN @F_DESDE AND DATEADD(DD,1,@F_HASTA)))
			AND ((P.FACTURADO='1') OR((P.FACTURADO='0') AND (TC.TIPO_COMPROBANTE_ID = 'AJE')))
			AND 16 In (Select Tipo_Auditoria_Id from #TEMP_CRITERIOS_RPT)
			AND((@CLIENTE_ID IS NULL) OR (P.CLIENTE_ID=@CLIENTE_ID))
			AND((@PRODUCTO_ID IS NULL) OR (P.PRODUCTO_ID=@PRODUCTO_ID))
			AND((@NRO_PARTIDA IS NULL) OR (DD.NRO_PARTIDA=@NRO_PARTIDA))
			AND((@FECHA_VTO IS NULL) OR (DD.FECHA_VENCIMIENTO=@FECHA_VTO))
			AND((@USUARIO IS NULL) OR (P.USUARIO_CONTROL_FAC=@USUARIO))
			AND((@NRO_LOTE IS NULL) OR (DD.NRO_LOTE=@NRO_LOTE))
			AND((@PROP2 IS NULL) OR (DD.PROP2=@PROP2))
			AND((@PROP3 IS NULL) OR (DD.PROP3=@PROP3))
			AND((@COD_PEDIDO IS NULL) OR(D.NRO_REMITO LIKE  '%'+ @COD_PEDIDO +  '%'))
			AND((@COD_VIAJE IS NULL) OR(D.NRO_DESPACHO_IMPORTACION LIKE '%' + @COD_VIAJE + '%'))
	ORDER BY 3,7,30
	--DROP TABLE #tmpSeriesPicking
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

ALTER                     PROCEDURE [dbo].[SALTO_PICKING]
	@USUARIO 			AS VARCHAR(30),
	@VIAJEID 			AS VARCHAR(100),
	@PRODUCTO_ID		AS VARCHAR(50),
	@POSICION_COD		AS VARCHAR(45),
	@PALLET				AS VARCHAR(100),
	@RUTA				AS VARCHAR(50)
AS
BEGIN

	DECLARE @MAX AS INT 
	DECLARE @SALTO AS INT
	DECLARE @XSQL AS VARCHAR(100)

	SELECT 	@SALTO=COUNT(PICKING_ID)
	FROM	PICKING
	WHERE	FECHA_INICIO IS NULL AND
			FECHA_FIN IS NULL AND
			CANT_CONFIRMADA IS NULL AND
			USUARIO=LTRIM(RTRIM(UPPER(@USUARIO))) AND
			LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID))) 
			AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))


	IF @SALTO=0
		BEGIN
			RAISERROR('Este es el ultimo item de la ruta. No es posible realizar el salto.',16,1)
			RETURN
		END

	SELECT 	@MAX=MAX(SALTO_PICKING) +1
	FROM 	PICKING
	WHERE	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))

	UPDATE 	PICKING SET FECHA_INICIO=NULL, PALLET_PICKING=NULL, SALTO_PICKING=@MAX
	WHERE 	USUARIO=LTRIM(RTRIM(UPPER(@USUARIO)))
			AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
			AND PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
			AND POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD)))
			AND PROP1=LTRIM(RTRIM(UPPER(@PALLET)))
			AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))

			AND PICKING_ID NOT IN (	SELECT 	PICKING_ID 
									FROM 	PICKING P2
									WHERE	USUARIO=LTRIM(RTRIM(UPPER(@USUARIO)))
											AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
											AND PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
											AND POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD)))
											AND PROP1=LTRIM(RTRIM(UPPER(@PALLET)))
											AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
											AND FECHA_INICIO IS NOT NULL AND FECHA_FIN IS NOT NULL AND PALLET_PICKING IS NOT NULL
											AND (PICKING.PICKING_ID=P2.PICKING_ID)
			)

	IF @@ERROR <>0
		BEGIN
			PRINT 'OCURRIO UN ERROR AL SALTAR EL PICKING'
			RETURN (99)
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

ALTER PROCEDURE [dbo].[SET_PRINTER]
AS
BEGIN
	DECLARE @HOST	VARCHAR(100)
	SET @HOST=HOST_NAME()

	INSERT INTO SYS_PRINTER_DEFAULT_ETIQUETA VALUES('','\\DEPOLEADER2\ZDESIGNER S4M ZPL 203DPI')
	
	SELECT * FROM SYS_PRINTER_DEFAULT_ETIQUETA WHERE TERMINAL_ID=@HOST
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

ALTER             procedure [dbo].[SetValuesEtiProv]
@ETI		as varchar(20) Output
As	
Begin
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @MARGH        		NUMERIC(6,2)
	DECLARE @MARGV        		NUMERIC(6,2)
	DECLARE @DISTANCIA    		NUMERIC(6,2)
	DECLARE @QTYXLINEA    		NUMERIC(2,0)
	DECLARE @ALTO         			NUMERIC(6,2)
	DECLARE @ANCHO        		NUMERIC(6,2)
	DECLARE @IMPRESORA    		VARCHAR(50)
	DECLARE @ETIQUETA_ID  		NUMERIC(20, 0)
	DECLARE @PRINTER			VARCHAR(200)

	DECLARE @TCUR				CURSOR
	DECLARE @VIAJEID			VARCHAR(100)
	DECLARE @PRODUCTO_ID		VARCHAR(30)
	DECLARE @POSICION_COD	VARCHAR(50)
	DECLARE @PALLET			VARCHAR(100)
	DECLARE @RUTA				VARCHAR(100)
	DECLARE @ID				NUMERIC(20,0)	

	TRUNCATE TABLE #TEMP_ETIQUETA

	SELECT @PRINTER=PRINTER_ID FROM SYS_PRINTER_DEFAULT_ETIQUETA WHERE TERMINAL_ID =HOST_NAME()
	
	SELECT 	 @MARGH		=MARGH
			,@MARGV		=MARGV
			,@DISTANCIA	=DISTENTREETIQUETAS
			,@QTYXLINEA	=QTYXLINEA
			,@ALTO			=ALTO
			,@ANCHO		=ANCHO
			,@IMPRESORA 	=IMPRESORA
			,@ETIQUETA_ID 	=ETIQUETA_ID
	FROM 	ETIQUETA_PRODUCTO 
	WHERE 	PRODUCTO_ID=@ETI
	
	BEGIN TRANSACTION
	INSERT INTO #TEMP_ETIQUETA
	SELECT 	--DISTINCT 
			'1'
			,1
			,1
			,1
			,1
			,1
			,1
			,@MARGH
			,@MARGV
			,@DISTANCIA
			,@QTYXLINEA
			,@ALTO
			,@ANCHO
			,ISNULL(@PRINTER,@IMPRESORA)
			,@ETIQUETA_ID

		
	COMMIT TRANSACTION

	SELECT * FROM #TEMP_ETIQUETA
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

ALTER Procedure [dbo].[SinPosicion]
As
Begin
declare @Rl_id 		as Numeric(20,0)
declare @Doc 			as Numeric(20,0)
declare @Linea 		as Numeric(20,0)
declare @Qty 			as Numeric(20,5)
declare @nav_act		as Numeric(20,0)
declare @nav_ant		as Numeric(20,0)
declare @pos_act		as Numeric(20,0)
declare @pos_ant		as Numeric(20,0)

declare @RsActuRL	as Cursor
SET NOCOUNT ON;


Set @RsActuRL = Cursor For
	SELECT
	rl.rl_id,rl.doc_trans_id,rl.nro_linea_trans,rl.cantidad
	FROM rl_det_doc_trans_posicion rl
                        inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
                        inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
	WHERE
	nave_anterior is null
	and nave_actual is null
	and posicion_actual is null
	and posicion_anterior is null
	order by rl.doc_trans_id,rl.nro_linea_trans,rl.cantidad

Open @RsActuRL
Fetch Next From @RsActuRL into @Rl_id,@Doc,@Linea,@Qty
While @@Fetch_Status=0
Begin	
	SELECT
	top 1 @pos_ant=rl.posicion_anterior,@pos_act=rl.posicion_actual,@nav_ant=rl.nave_anterior,@nav_act=rl.nave_actual
	FROM rl_det_doc_trans_posicion rl
	where doc_trans_id=@Doc and nro_linea_trans=@Linea
	and (posicion_actual is not null or nave_actual is not null)
	and doc_trans_id_egr is null
	and nave_actual<>1
	
	update rl_det_doc_trans_posicion set posicion_anterior=@pos_ant, posicion_actual=@pos_act, nave_anterior=@nav_ant, nave_actual=@nav_act
	where rl_id=@Rl_id
	
	insert into audi_fab values (@Rl_id,@Doc,@Linea,@Qty)

	Fetch Next From @RsActuRL into @Rl_id,@Doc,@Linea,@Qty
End	--End While @RsExist.

CLOSE @RsActuRL
DEALLOCATE @RsActuRL

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

ALTER PROCEDURE [dbo].[SOBRANTES_PICKING]
	@POSICION_O		AS VARCHAR(45),
	@POSICION_D		AS VARCHAR(45),
	@CONTENEDORA	AS VARCHAR(100),
	@PALLET			AS VARCHAR(100),
	@USUARIO 		AS VARCHAR(30)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	--DECLARE @PALLET			AS VARCHAR(100)
	DECLARE @CLIENTE_ID		AS VARCHAR(5)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(1,0)
	DECLARE @LIMITE			AS NUMERIC(20,0)
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)

	EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
	IF @OUT='1'
		BEGIN
			RETURN
		END
	
	IF @PALLET IS NULL
	BEGIN
		SET @CONTENEDORA=NULL
		SET @PALLET = (SELECT	distinct PROP1	FROM	DET_DOCUMENTO WHERE	NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA))))
	END
	
	--OBTENGO LA RL DEL PALLET EN UN CURSOR POR SI HAY MAS DE UNA LINEA EN RL.--
	DECLARE CUR_RL_TR CURSOR FOR
		SELECT 	RL.RL_ID,DD.CLIENTE_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND ((@CONTENEDORA IS NULL) OR (NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID 	FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		--Aca verifico que el pallet este en la posicion indicada.
		SELECT 	@CONTROL=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND ((@CONTENEDORA IS NULL) OR (NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))))
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		IF @CONTROL=0
			BEGIN
				RAISERROR('1-El pallet: %s o la contenedora: %s no esta en la posicion especificada.',16,1,@PALLET,@CONTENEDORA)
				DEALLOCATE CUR_RL_TR
				RETURN
			END
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		--Controlo que la categoria logica no sea TRAN_ING, TRAN_EGR
		Exec Mob_Transf_VerificaCatLog @Pallet, @Posicion_O, @TRANSFIERE OUTPUT
		If @Transfiere=0
		Begin
			Return
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		-- Voy por las posiciones de Picking
		Exec Verifica_Picking @PALLET, @PICKING OUTPUT
		If @Picking=1
		Begin
			--Aca tengo que verificar que la posicion de destino sea de picking.
			Select @Picking=Dbo.IsPosPicking(@POSICION_D)
			If @Picking=0
			Begin
				RAISERROR('1- La ubicacion destino no es una ubicacion de Picking.',16,1)
				DEALLOCATE CUR_RL_TR
				RETURN
			End
		End
		--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		SELECT 	@LIMITE=COUNT(RL.RL_ID)
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
				AND NRO_BULTO = LTRIM(RTRIM(UPPER(@CONTENEDORA)))
				AND (RL.NAVE_ACTUAL=	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@POSICION_O))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_O)))))
				AND RL.CANTIDAD >0

		SELECT @EXISTE = COUNT(*)
		FROM  TRANSACCION T
			  INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
			  INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
			  AND RTC.CLIENTE_ID IN (SELECT CLIENTE_ID FROM CLIENTE
									 WHERE (SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
										   FROM   RL_SYS_CLIENTE_USUARIO
										   WHERE  CLIENTE_ID = RTC.CLIENTE_ID
										   AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)WHERE T.TIPO_OPERACION_ID='TR' AND RTE.ORDEN=1	
		IF @EXISTE = 0
		BEGIN
			RAISERROR('El usuario %s no posee clientes asignados',16,1,@USUARIO)
			return
		END

		--GENERO EL DOCUMENTO DE TRANSACCION .--
		EXEC CREAR_DOC_TRANSFERENCIA @USUARIO=@USUARIO
	
		--OBTENGO EL DOC_TRANS_ID INSERTADO.--
		SET @VDOCNEW=@@IDENTITY
	
		UPDATE DOCUMENTO_TRANSACCION SET TR_POS_COMPLETA= '0' WHERE DOC_TRANS_ID=@VDOCNEW
	
		--ABRO EL CURSOR PARA SU POSTERIOR USO	
		OPEN CUR_RL_TR
	
		SET @CONT_LINEA= 0
		SET @LIM_CONT=0

		FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID
		WHILE (@@FETCH_STATUS=0)
		BEGIN
				SET @CONT_LINEA= @CONT_LINEA + 1 
				INSERT INTO DET_DOCUMENTO_TRANSACCION (
				        DOC_TRANS_ID,     NRO_LINEA_TRANS,
				        DOCUMENTO_ID,     NRO_LINEA_DOC,
				        MOTIVO_ID,        EST_MERC_ID,
				        CLIENTE_ID,       CAT_LOG_ID,
				        ITEM_OK,          MOVIMIENTO_PENDIENTE,
				        DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF)
				VALUES (
				        @VDOCNEW
				        ,@CONT_LINEA --NRO DE LINEA DE DET_DOCUMENTO_TRANSACCION
				        ,NULL   ,NULL   ,NULL     ,NULL
				        ,@CLIENTE_ID
				        ,NULL ,'0' ,'0' ,NULL     ,NULL)
	
		
				SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@POSICION_D) AS INT)
				SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@POSICION_D) AS INT)
	
				INSERT INTO RL_DET_DOC_TRANS_POSICION
				           (DOC_TRANS_ID,
				            NRO_LINEA_TRANS,
				            POSICION_ANTERIOR,
				            POSICION_ACTUAL,
				            CANTIDAD,
				            TIPO_MOVIMIENTO_ID,
				            ULTIMA_SECUENCIA,
				            NAVE_ANTERIOR,
				            NAVE_ACTUAL,
				            DOCUMENTO_ID,
				            NRO_LINEA,
				            DISPONIBLE,
				            DOC_TRANS_ID_TR,
				            NRO_LINEA_TRANS_TR,
				            CLIENTE_ID,
				            CAT_LOG_ID,
				            EST_MERC_ID)
				            (SELECT   DOC_TRANS_ID 
									, NRO_LINEA_TRANS
									, POSICION_ACTUAL
									, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
									, CANTIDAD
									, NULL
									, NULL
									, NAVE_ACTUAL
									, @NEWNAVE
									, NULL
									, NULL
									, 0
									, @vDocNew
				                    , 1 
									, CLIENTE_ID
									, CAT_LOG_ID
									, EST_MERC_ID
				             FROM RL_DET_DOC_TRANS_POSICION
				             WHERE RL_ID = @vRLID
				             ) 

			EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE

			DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			
			----------------------------------------------------------------------------
			--	ESTO ES SEGURAMENTE UN BUG DE SQL SERVER. REVISAR PARA CORREGIR
			--	ESTE ALGORITMO.
			SET @LIM_CONT=@LIM_CONT +1
			IF (@LIMITE=@LIM_CONT)
				BEGIN
					BREAK
				END
			----------------------------------------------------------------------------
			FETCH NEXT FROM CUR_RL_TR INTO @VRLID,@CLIENTE_ID
			
		END		

		-- DEVOLUCION
		EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW
	
		--FINALIZA LA TRANSFERENCIA	
		EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
	
		-- BORRO POR SI QUEDA ALGUNA RL EN 0
		DELETE FROM RL_DET_DOC_TRANS_POSICION WHERE CANTIDAD =0 AND DOC_TRANS_ID=@VDOCNEW
	
		--ACTUALIZO LAS POSICIONES DE PICKING
		EXEC ACTUALIZA_POS_PICKING_TR @POSICION_O,@POSICION_D,@PALLET

		UPDATE POSICION SET POS_VACIA='0' WHERE POSICION_ID IN (SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
			
		UPDATE POSICION SET POS_VACIA='1' WHERE POSICION_ID  NOT IN
		(SELECT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)
	
		CLOSE CUR_RL_TR
		DEALLOCATE CUR_RL_TR
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

ALTER PROCEDURE [dbo].[SOLICITA_MAND]
	@CLIENTE_ID	VARCHAR(15),
	@PRODUCTO_ID	VARCHAR(30),
	@LOTE_PROV	CHAR(1) OUTPUT,
	@FVTO			CHAR(1) OUTPUT
AS
BEGIN
	DECLARE @CONT	SMALLINT

	SELECT	@CONT=ISNULL(ingLoteProveedor,0)
	FROM	PRODUCTO 
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID;
			
	IF @CONT=1
	BEGIN
		SET @LOTE_PROV='1'
	END
	ELSE
	BEGIN
		SET @LOTE_PROV='0'
	END

	SET @CONT=NULL

	SELECT 	@CONT=COUNT(*)
	FROM 	MANDATORIO_PRODUCTO 
	WHERE 	CLIENTE_ID=@CLIENTE_ID
			AND PRODUCTO_ID=@PRODUCTO_ID
			AND TIPO_OPERACION ='ING'
			AND CAMPO='FECHA_VENCIMIENTO'
	IF @CONT=1
	BEGIN
		SET @FVTO='1'
	END
	ELSE
	BEGIN
		SET @FVTO='0'
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

ALTER procedure [dbo].[SolicitaCodigos]
@Documento_id  	Numeric(20,0),
@Nro_Linea 			Numeric(10,0),
@pOut				int Out
as
Begin
	Declare @Retorno Int
	
	Select 	@pOut= isnull(val_cod_ing,0)
	from	Det_Documento dd(Nolock) inner join  producto p(nolock)
			on(dd.cliente_id=p.cliente_id and dd.producto_id=p.producto_id)
	where	dd.documento_id=@Documento_id
			and dd.nro_linea=@nro_linea



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

ALTER   PROCEDURE [dbo].[SP_HISTORICO_PRODUCTO_RL]
AS
BEGIN
	DECLARE @SEQ NUMERIC(38)
	DECLARE @SECUENCIA VARCHAR(30)

	SET @SECUENCIA = 'HIST_PROD_RL'
	EXEC DBO.GET_VALUE_FOR_SEQUENCE @SECUENCIA, @SEQ OUTPUT

	INSERT INTO HISTORICO_PRODUCTO_RL 
	SELECT	RL_ID
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
		,GETDATE()
		,@SEQ
	FROM RL_DET_DOC_TRANS_POSICION

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

ALTER  PROCEDURE [dbo].[SP_HISTORICO_SALDO_PRODUCTO]
AS
BEGIN
	INSERT INTO HISTORICO_SALDO_PRODUCTO
	SELECT	P.CLIENTE_ID
		,P.PRODUCTO_ID
		,SUM(DD.CANTIDAD)
		,RDDTP.CAT_LOG_ID
		,RDDTP.EST_MERC_ID
		,GETDATE()
	FROM 	PRODUCTO P 
	INNER JOIN DET_DOCUMENTO DD 
		ON (P.CLIENTE_ID = DD.CLIENTE_ID AND P.PRODUCTO_ID = DD.PRODUCTO_ID)
	INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
		ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
	INNER JOIN RL_DET_DOC_TRANS_POSICION RDDTP
		ON (RDDTP.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RDDTP.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
	GROUP BY P.CLIENTE_ID
		,P.PRODUCTO_ID
		,RDDTP.CAT_LOG_ID
		,RDDTP.EST_MERC_ID
	ORDER BY 2
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

ALTER   procedure [dbo].[SQLDin]
@DocId as numeric(20,0)
as
Begin
	Declare @xSQL as nvarchar(4000)
	
	Set @xSQL='Select nro_linea from Det_Documento where documento_id=' + cast(@Docid as varchar(20))

	Execute sp_executesql  @xSQl
	
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

ALTER   Procedure [dbo].[St_Egr_FinPicking]
@Doc_Trans_id    Numeric(20,0) Output
As
Begin

   	Declare @FinPicking   	Char(1)
	Declare @Total			Float
	Declare @Finalizados 	Float

	Select 	@Total=Count(P.Picking_Id)
	From    	Picking P,
			(
			Select    Documento_id, Nro_Linea_doc,Doc_trans_id
			From      Det_documento_transaccion
			Where   Doc_trans_id=@Doc_Trans_id
			)X
	Where    P.Documento_id=x.Documento_id
			and	p.Nro_linea=x.Nro_linea_Doc

	Select 	@Finalizados=Count(P.Picking_Id)
	From    	Picking P,
			(
			Select    Documento_id, Nro_Linea_doc,Doc_trans_id
			From      Det_documento_transaccion
			Where   Doc_trans_id=@Doc_Trans_id
			)X
	Where    P.Documento_id=x.Documento_id
			and	p.Nro_linea=x.Nro_linea_Doc
			and p.fecha_inicio is not null
			and p.Fecha_Fin is not null
			and p.Usuario is not null
			and p.Pallet_Picking is not null
			and p.Cant_Confirmada is not null

	If @Total<>@Finalizados
	Begin
		Raiserror('No es posible finalizar el documento, aun tiene tareas de picking pendientes.',16,1)
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

ALTER    PROCEDURE [dbo].[St_Egr_GetTareasPickingTomadas]
@DOCTRANSID 		numeric(20,0) output,
@tipo			int	     output	
AS
BEGIN
--Tareas en Curso
if @tipo=1 begin	 
	select 
	'0' as [check]
	,p.usuario
	,su.nombre
	,p.producto_id
	,p.descripcion
	,p.cantidad as qty_pick
	,p.posicion_cod
	,p.fecha_inicio
	,p.pallet_picking
	,p.tipo_caja
	,p.ruta
	,p.prop1 as pallet_pick
	,salto_picking
	,picking_id
	from picking p
		inner join sys_usuario su on (p.usuario=su.usuario_id)
		inner join det_documento_transaccion ddt on (p.documento_id = ddt.documento_id and ddt.nro_linea_doc = p.nro_linea)
	where 	
		ddt.doc_trans_id = @DOCTRANSID		
		and p.usuario is not null
		and p.fecha_fin is null
end --if

--Tareas en Pendientes
if @tipo=2 begin	 
	select
	'1' as [CHECK] 
	,p.producto_id
	,p.descripcion
	,p.cantidad as qty_pick
	,p.cantidad as qty_pickeada
	,p.posicion_cod
	,p.tipo_caja
	,p.ruta
	,p.prop1 as pallet_pick
	,p.salto_picking
	,p.picking_id
	from picking p
		inner join det_documento_transaccion DDT on (ddt.documento_id = p.documento_id and ddt.nro_linea_doc = p.nro_linea)
	where 
		ddt.doc_trans_id = @DOCTRANSID
		and usuario is null
		and fecha_inicio is null
		and fecha_fin is null

end --if

--Tareas Finalizadas
if @tipo=3 begin	 
	select 
	'0' as [CHECK] 
	,p.usuario
	,su.nombre	
	,p.producto_id
	,p.descripcion
	,p.posicion_cod	
	,p.cantidad
	,p.cant_confirmada
	,p.cant_confirmada-p.cantidad as dif
	,p.fecha_inicio
	,p.fecha_fin
	,p.pallet_picking
	,p.tipo_caja
	,p.ruta
	,p.prop1 as pallet_pick
	,p.salto_picking
	,picking_id
	from picking p
		inner join sys_usuario su on (p.usuario=su.usuario_id)
		inner join det_documento_transaccion DDT on (ddt.documento_id = p.documento_id and ddt.nro_linea_doc = p.nro_linea)
	where 
		ddt.doc_trans_id = @DOCTRANSID
		and p.usuario is not null
		and p.fecha_inicio is not null
		and p.fecha_fin is not null
end --if

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

ALTER PROCEDURE [dbo].[St_Egr_RevertirPicking]
@PICKING_ID 		numeric(20,0) output
AS
BEGIN

	UPDATE	Picking
	SET	cant_confirmada = null,
		usuario = null,
		fecha_inicio = null,
		fecha_fin = null,
		pallet_picking = null
	WHERE 	picking_id = @PICKING_ID

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

ALTER  PROCEDURE [dbo].[St_Egr_UPDATE_CANT_PICKEADA]
	@PICKINGID 		numeric(20,0) output,
	@CANT			numeric(20,5) output
AS
BEGIN

	UPDATE	PICKING 
	SET 	CANTIDAD = @CANT
	WHERE	PICKING_ID = @PICKINGID

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

ALTER PROCEDURE [dbo].[St_Egr_UPDATE_PICK_CANTCONF]
@DOCTRANSID 		varchar(100) output
AS
BEGIN

	UPDATE PICKING SET CANT_CONFIRMADA=CANTIDAD
	WHERE	DOCUMENTO_ID=(	SELECT 	DOCUMENTO_ID
				FROM 	DET_DOCUMENTO_TRANSACCION
				WHERE	PICKING.DOCUMENTO_ID=DET_DOCUMENTO_TRANSACCION.DOCUMENTO_ID AND
						PICKING.NRO_LINEA=DET_DOCUMENTO_TRANSACCION.NRO_LINEA_DOC
						AND DET_DOCUMENTO_TRANSACCION.DOC_TRANS_ID=@DOCTRANSID)

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

ALTER       PROCEDURE [dbo].[st_egr_UpdateQtyPickeada]
@pPicking_id     varchar(100) output,
@pCantidad	 Numeric(10,5) output
AS

BEGIN
	Declare @xUSER 		as varchar (20)
	Declare @NEW_PALLET 	as NUMERIC(38) 
	Declare @ViajeId	as varchar(100)
	Declare @Cantidad	as Float
	Declare @Dif		as Float

	Select @xUSER = usuario_id from #temp_usuario_loggin 

	exec dbo.get_value_for_sequence 'PALLET_PICKING', @NEW_PALLET output

	UPDATE	picking 
	SET 	cant_confirmada = @pCantidad,
		usuario = @xUSER,
		fecha_inicio = getdate(),
		fecha_fin = getdate(),
		pallet_picking = @NEW_PALLET		
	WHERE 	picking_id = @pPicking_id

	Select	Distinct
		@ViajeId=Viaje_id
	From	Picking
	Where	Picking_Id=@pPicking_id


	SELECT 	@CANTIDAD=COUNT(PICKING_ID)
	FROM	PICKING
	WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(UPPER(RTRIM(@VIAJEID)))
	
	
	SELECT 	@DIF=COUNT(PICKING_ID)
	FROM 	PICKING 
	WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(UPPER(RTRIM(@VIAJEID)))
		AND FECHA_INICIO IS NOT NULL
		AND FECHA_FIN IS NOT NULL
		AND PALLET_PICKING IS NOT NULL
		AND USUARIO IS NOT NULL
		AND CANT_CONFIRMADA IS NOT NULL
	
	
	IF @CANTIDAD=@DIF
	BEGIN
		UPDATE PICKING SET FIN_PICKING='2' WHERE LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
	END
	ELSE
	BEGIN
		UPDATE PICKING SET FIN_PICKING='1' WHERE LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
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

ALTER PROCEDURE [dbo].[SWITCH_ETIQUETA]
	@DOCUMENTO_ID 	AS NUMERIC(20,0) OUTPUT,
	@TIPO_ETI			AS CHAR(1) OUTPUT
AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
	DECLARE @PRODUCTO_ID 	AS VARCHAR(30)
	DECLARE @CLIENTE_ID		AS VARCHAR(20)
	DECLARE @FLG_BULTO		AS CHAR(1)
	DECLARE @QTY_BULTO		AS FLOAT
	DECLARE @FLG_PALLET		AS CHAR(1)
	DECLARE @QTY_PALLET		AS FLOAT
	DECLARE @QTY_DOC			AS FLOAT

	SET @TIPO_ETI=NULL

	SELECT @CLIENTE_ID=CLIENTE_ID, @PRODUCTO_ID=PRODUCTO_ID FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	SELECT @QTY_DOC=SUM(CANTIDAD) FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID

	SELECT 	 @FLG_BULTO=FLG_BULTO
			,@FLG_PALLET=FLG_VOLUMEN_ETI
			,@QTY_BULTO=QTY_BULTO
			,@QTY_PALLET=QTY_VOLUMEN_ETI
	FROM 	PRODUCTO WHERE CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID=@PRODUCTO_ID

	IF @FLG_BULTO='1'
	BEGIN
		IF @QTY_DOC <= @QTY_BULTO
		BEGIN
			SET @TIPO_ETI='0'
			RETURN
		END
		ELSE
		BEGIN
			SET @TIPO_ETI='1'
			RETURN
		END
	END
	IF @FLG_PALLET='1'
	BEGIN
		IF @QTY_DOC <= @QTY_PALLET
		BEGIN
			SET @TIPO_ETI='1'
			RETURN
		END
		ELSE
		BEGIN
			SET @TIPO_ETI='0'
		END
	END	

	IF @TIPO_ETI IS NULL
	BEGIN
		SET @TIPO_ETI='1'
	END
END-- FIN PROCEDURE.
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

ALTER   PROCEDURE [dbo].[SYS_AUDITORIA_CAT_MERC_INSERT_RECORD]
		@vPREFIJO	AS VARCHAR(20)		OUTPUT,
		@vRL_ID 	AS NUMERIC(20,0)	OUTPUT,
		@vCLIENTE_ID 	AS VARCHAR(15)		OUTPUT,
		@vNAVE_ID	AS NUMERIC(20,0)	OUTPUT,
		@vPOSICION_ID	AS NUMERIC(20,0)	OUTPUT,
		@vOLD		AS VARCHAR(50)		OUTPUT,
		@vNEW     	AS VARCHAR(50)		OUTPUT,
		@vQTY_OLD	AS NUMERIC(25,5)	OUTPUT,
		@vQTY_NEW	AS NUMERIC(25,5)	OUTPUT
AS
BEGIN

	INSERT INTO SYS_AUDITORIA_CAT_MERC (PREFIJO,CLIENTE_ID,RL_ID,DOCUMENTO_ID,NRO_LINEA,NAVE_ID,POSICION_ID, OLD,NEW,QTY_OLD,QTY_NEW,FECHA,USUARIO_ID,TERMINAL)
	(SELECT	@vPREFIJO
		,@vCLIENTE_ID
		,@vRL_ID
		,DDT.DOCUMENTO_ID
		,DDT.NRO_LINEA_DOC
		,@vNAVE_ID
		,@vPOSICION_ID
		,@vOLD
		,@vNEW
		,@vQTY_OLD
		,@vQTY_NEW
		,GETDATE()
		,SU.USUARIO_ID
		,TUL.TERMINAL
	FROM	RL_DET_DOC_TRANS_POSICION RDDTP
		INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
			ON (RDDTP.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RDDTP.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS ),
		#TEMP_USUARIO_LOGGIN TUL 
			INNER JOIN SYS_USUARIO SU ON (TUL.USUARIO_ID = SU.USUARIO_ID)
	WHERE	RDDTP.RL_ID = @vRL_ID)

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

ALTER    PROCEDURE [dbo].[SYS_AUDITORIA_PROP_INSERT_RECORD]
		@vPREFIJO	AS VARCHAR(20)	OUTPUT,
		@vRL_ID 	AS NUMERIC(20,0)	OUTPUT,
		@vCLIENTE_ID 	AS VARCHAR(15)	OUTPUT,
		@vNAVE_ID	AS NUMERIC(20,0)	OUTPUT,
		@vPOSICION_ID	AS NUMERIC(20,0)	OUTPUT,
		@vOLD		AS VARCHAR(50)	OUTPUT,
		@vNEW     	AS VARCHAR(50)	OUTPUT,
		@vQTY_OLD	AS NUMERIC(25,5)	OUTPUT,
		@vQTY_NEW	AS NUMERIC(25,5)	OUTPUT,
		@DOC		AS NUMERIC(20,0)	OUTPUT,
		@NRO_LINEA	AS NUMERIC(10,0)	OUTPUT
AS
BEGIN

		INSERT INTO SYS_AUDITORIA_CAT_MERC (PREFIJO,CLIENTE_ID,RL_ID,DOCUMENTO_ID,NRO_LINEA,NAVE_ID,POSICION_ID, OLD,NEW,QTY_OLD,QTY_NEW,FECHA,USUARIO_ID,TERMINAL)
		(SELECT	@vPREFIJO
			,@vCLIENTE_ID
			,@vRL_ID
			,@DOC
			,@NRO_LINEA
			,@vNAVE_ID
			,@vPOSICION_ID
			,@vOLD
			,@vNEW
			,@vQTY_OLD
			,@vQTY_NEW
			,GETDATE()
			,SU.USUARIO_ID
			,TUL.TERMINAL
		FROM	#TEMP_USUARIO_LOGGIN TUL 
				INNER JOIN SYS_USUARIO SU ON (TUL.USUARIO_ID = SU.USUARIO_ID))


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

ALTER     PROCEDURE [dbo].[SYS_DEV_I01_BULTOS]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
BEGIN

	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
begin 
	
	if @qty=0
      	   BEGIN 	
		insert into sys_dev_documento
		select	top 1
				sid.CLIENTE_ID, 
				'I02', 
				sid.CPTE_PREFIJO, 
				sid.CPTE_NUMERO, 
				d.FECHA_CPTE, 
				sid.FECHA_SOLICITUD_CPTE, 
				sid.AGENTE_ID, 
				sid.PESO_TOTAL, 
				sid.UNIDAD_PESO, 
				sid.VOLUMEN_TOTAL, 
				sid.UNIDAD_VOLUMEN, 
				sid.TOTAL_BULTOS, 
				sid.ORDEN_DE_COMPRA, 
				sid.OBSERVACIONES, 
				d.NRO_REMITO, 
				sid.NRO_DESPACHO_IMPORTACION, 
				sid.DOC_EXT, 
				sid.CODIGO_VIAJE, 
				sid.INFO_ADICIONAL_1, 
				sid.INFO_ADICIONAL_2, 
				sid.INFO_ADICIONAL_3, 
				d.TIPO_COMPROBANTE_id, 
				NULL, 
				NULL, 
				'P', 
				GETDATE(),
				NULL	--flg_movimiento	
		from	sys_int_documento sid
				inner join documento d on (sid.cliente_id=d.cliente_id)
				inner join det_documento dd on(d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2)
		where	sid.doc_ext=@doc_ext
	END
	
	insert into sys_dev_det_documento
	select	distinct
			sidd.DOC_EXT, 
			CAST(DD.DOCUMENTO_ID AS VARCHAR) + CAST(DD.NRO_LINEA AS VARCHAR), 
			sidd.CLIENTE_ID, 
			sidd.PRODUCTO_ID, 
			MAX(SIDD.CANTIDAD_SOLICITADA), 
			dd.CANTIDAD, 
			dd.EST_MERC_ID, 
			dd.CAT_LOG_ID_FINAL, 
			dd.NRO_BULTO, 
			dd.DESCRIPCION, 
			dd.NRO_LOTE, 
			dd.PROP1 AS NRO_PALLET, --NRO_PALLET 
			dd.FECHA_VENCIMIENTO, 
			dd.NRO_DESPACHO, 
			dd.NRO_PARTIDA, 
			sidd.UNIDAD_ID, 
			sidd.UNIDAD_CONTENEDORA_ID, 
			sidd.PESO, 
			sidd.UNIDAD_PESO, 
			sidd.VOLUMEN, 
			sidd.UNIDAD_VOLUMEN, 
			sidd.PROP1, 
			dd.PROP2, --NRO_LOTE 
			Isnull(sidd.PROP3,dd.nro_serie), 
			sidd.LARGO, 
			sidd.ALTO, 
			sidd.ANCHO, 
			sidd.DOC_BACK_ORDER, 
			NULL, 
			NULL, 
			'P', 
			GETDATE(), 
			DD.DOCUMENTO_ID, 
			dbo.get_nave_id(dd.documento_id,dd.nro_linea),
			dbo.get_nave_cod(dd.documento_id,dd.nro_linea), 	
			NULL	--flg_movimiento	
	from	sys_int_documento sid
			inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)
			inner join documento d on (sid.cliente_id=d.cliente_id and sidd.documento_id=d.documento_id)
			inner join det_documento dd on (d.documento_id=dd.documento_id and sid.doc_ext=dd.prop2 and sidd.producto_id = dd.producto_id)
	where	sid.doc_ext=@doc_ext 
			and sidd.estado_gt is not null 
			and dd.documento_id=@documento_id
			and sidd.documento_id=@documento_id
	group by
			SIDD.DOC_EXT					, CAST(DD.DOCUMENTO_ID AS VARCHAR) + CAST(DD.NRO_LINEA AS VARCHAR)	, SIDD.CLIENTE_ID
			, SIDD.PRODUCTO_ID				, DD.CANTIDAD						, DD.EST_MERC_ID				, DD.CAT_LOG_ID_FINAL
			, DD.NRO_BULTO					, DD.DESCRIPCION					, DD.NRO_LOTE					, DD.PROP1
			, DD.FECHA_VENCIMIENTO			, DD.NRO_DESPACHO					, DD.NRO_PARTIDA				, SIDD.UNIDAD_ID
			, SIDD.UNIDAD_CONTENEDORA_ID	, SIDD.PESO							, SIDD.UNIDAD_PESO				, SIDD.VOLUMEN
			, SIDD.UNIDAD_VOLUMEN			, SIDD.PROP1						, DD.PROP2						, ISNULL(SIDD.PROP3,DBO.FX_GETNROREMITODO(DD.DOCUMENTO_ID))			
			, SIDD.LARGO					, SIDD.ALTO							, SIDD.ANCHO					, SIDD.DOC_BACK_ORDER
			, DD.DOCUMENTO_ID				, DBO.GET_NAVE_ID(DD.DOCUMENTO_ID,DD.NRO_LINEA)						, DBO.GET_NAVE_COD(DD.DOCUMENTO_ID,DD.NRO_LINEA)
			, SIDD.PROP3					, DD.NRO_SERIE		
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

ALTER         PROCEDURE [dbo].[SYS_DEV_I08]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
set xact_abort on
BEGIN

	select @qty=count(*) from sys_dev_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento (nolock) where doc_ext = 'DEV' + CAST(@documento_id AS varchar(100))--@doc_ext

	BEGIN 
	
		IF @qty=0
			BEGIN 	
				INSERT INTO sys_dev_documento
				SELECT 
					 d.CLIENTE_ID
					,'I08'
					,d.CPTE_PREFIJO 
					,d.CPTE_NUMERO 
					,d.FECHA_CPTE
					,GetDate()
					,NULL
					,d.PESO_TOTAL
					,d.UNIDAD_PESO
					,d.VOLUMEN_TOTAL
					,d.UNIDAD_VOLUMEN
					,d.TOTAL_BULTOS
					,d.ORDEN_DE_COMPRA
					,d.OBSERVACIONES
					,d.NRO_REMITO
					,d.NRO_DESPACHO_IMPORTACION
					,'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))
					,NULL
					,NULL
					,NULL
					,NULL
					,d.TIPO_COMPROBANTE_ID
					,NULL
					,NULL
					,'P' --ESTADO_GT
					,GETDATE() 
					,null	--Flg_movimiento
				FROM documento d (nolock)
				WHERE d.documento_id = @documento_id
			END
	
			INSERT INTO sys_dev_det_documento
			SELECT 
				 'DEV' + CAST(d.DOCUMENTO_ID AS varchar(100))
				,isnull(@nro_lin,0) + dd.NRO_LINEA
				,d.CLIENTE_ID 
				,dd.PRODUCTO_ID
				,dd.CANTIDAD					--sidd.CANTIDAD_SOLICITADA
				,dd.CANTIDAD
				,dd.EST_MERC_ID
				,dd.CAT_LOG_ID_FINAL
				,dbo.Get_data_I08(dd.documento_id,dd.nro_linea,'3')
				,dd.DESCRIPCION
				,dd.NRO_LOTE
				,dd.PROP1 AS NRO_PALLET --NRO_PALLET 
				,dd.FECHA_VENCIMIENTO
				,dd.NRO_DESPACHO
				,dd.NRO_PARTIDA
				,dd.UNIDAD_ID 			--sidd.UNIDAD_ID
				,NULL					--sidd.UNIDAD_CONTENEDORA_ID
				,dd.PESO				--sidd.PESO
				,dd.UNIDAD_PESO 		--sidd.UNIDAD_PESO
				,dd.VOLUMEN				--sidd.VOLUMEN
				,dd.UNIDAD_VOLUMEN		--sidd.UNIDAD_VOLUMEN
				,dd.PROP1				--sidd.PROP1
				,dd.prop2
				,dd.prop3
				,dd.LARGO				--sidd.LARGO
				,dd.ALTO 				--sidd.ALTO 
				,dd.ANCHO				--sidd.ANCHO
				,dd.TRACE_BACK_ORDER	--sidd.DOC_BACK_ORDER
				,NULL
				,NULL
				,'P'
				,GETDATE()
				,DD.DOCUMENTO_ID
				,dbo.get_nave_id(dd.documento_id,dd.nro_linea)
				,dbo.get_nave_cod(dd.documento_id,dd.nro_linea)
				,null 	--Flg_movimiento 	
			FROM documento d (nolock)
				inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			WHERE dd.documento_id=@documento_id

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

ALTER     PROCEDURE [dbo].[SYS_DEV_I07]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
BEGIN

	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
begin 
	
	insert into sys_dev_documento
	select 
	d.CLIENTE_ID, 
	'I07', 
	d.CPTE_PREFIJO, 
	d.CPTE_NUMERO, 
	d.FECHA_CPTE, 
	null, 
	d.sucursal_origen, 
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	D.OBSERVACIONES, 
	d.NRO_REMITO, 
	D.NRO_DESPACHO_IMPORTACION, 
	'IM' + CAST(d.DOCUMENTO_ID AS varchar(100)),
	NULL,
	NULL,
	NULL,
	NULL,
	d.TIPO_COMPROBANTE_id, 
	NULL, 
	NULL, 
	'P', 
	GETDATE(),
	NULL --flg_movimiento
	from documento d 
	where d.documento_id=@documento_id	

	
	insert into sys_dev_det_documento
	select 
	'IM' + CAST(dd.DOCUMENTO_ID AS varchar(100)),
	dd.NRO_LINEA, 
	dd.CLIENTE_ID, 
	dd.PRODUCTO_ID, 
	dd.CANT_SOLICITADA, 
	dd.cantidad, 
	dd.EST_MERC_ID, 
	dd.CAT_LOG_ID_FINAL, 
	dd.NRO_BULTO, 
	dd.DESCRIPCION, 
	dd.NRO_LOTE, 
	dd.PROP1 AS NRO_PALLET, --NRO_PALLET 
	dd.FECHA_VENCIMIENTO, 
	dd.NRO_DESPACHO, 
	dd.NRO_PARTIDA, 
	dd.UNIDAD_ID, 
	null,
	null,
	null,
	null,
	null,
	dd.PROP1, 
	dd.PROP2, --NRO_LOTE 
	dd.PROP3, 
	null,
	null,
	null,
	null,
	NULL, 
	NULL, 
	'P', 
	GETDATE(), 
	DD.DOCUMENTO_ID, 
	dbo.get_nave_id(dd.documento_id,dd.nro_linea),
	dbo.get_nave_cod(dd.documento_id,dd.nro_linea),
	NULL --flg_movimiento
	from det_documento dd
	where dd.documento_id=@documento_id
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

ALTER          PROCEDURE [dbo].[SYS_DEV_BULTOS]
@documento_id AS NUMERIC(20,0) output,
@estado	as numeric(2,0) output
AS
DECLARE @doc_Ext AS varchar(100)
DECLARE @td AS varchar(20)
DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
DECLARE @tc AS varchar(15)
DECLARE @status AS varchar(5)
DECLARE @Cur as cursor
BEGIN
	

	SET @CUR = CURSOR FOR
		select	distinct
				prop2
				,tipo_comprobante_id
				,status 
		from	documento d inner join det_documento dd
				on(d.documento_id=dd.documento_id)
		where	d.documento_id=@documento_id
	OPEN @CUR 
	FETCH NEXT FROM @CUR INTO @doc_ext, @tc,@status
	While @@Fetch_Status=0
	begin
		select	@qty=count(*) 
		from	sys_dev_documento 
		where	doc_ext=@doc_ext

		select	@td=tipo_documento_id 
		from	sys_int_documento 
		where	doc_ext=@doc_ext

		select	@nro_lin=max(nro_linea) 
		from	sys_dev_det_documento 
		where doc_ext=@doc_ext
		
		IF (@doc_ext <> '' and @doc_ext is not null and @status='D40')
		BEGIN
			
			IF (@td='I01' and @estado=1 and @tc='DO')
			BEGIN
				 exec SYS_DEV_I01_BULTOS
				 @doc_ext=@doc_ext
				,@estado=1 
				,@documento_id=@documento_id
			END --IF

			IF (@td='I01' and @estado=3 and @tc='DO')
			BEGIN
	     			 exec sys_dev_I03
				 @doc_ext=@doc_ext
				,@estado=1 
						,@documento_id=@documento_id
			END --IF

			IF (@td='I04' and @estado=1 and @tc='PP')
			BEGIN
	     			 exec sys_dev_I04
				 @doc_ext=@doc_ext
				,@estado=1 
						,@documento_id=@documento_id
			END --IF

			IF (@td is null and @estado=1 and @tc='DE')
			BEGIN
	     			exec    sys_dev_I08
							 @doc_ext=@doc_ext
							,@estado=@estado 
							,@documento_id=@documento_id
					break;
			END --IF
			
			IF (@td='E04' and @estado=1 and @tc='DE')
			BEGIN
	     			exec    sys_dev_I08
							 @doc_ext=@doc_ext
							,@estado=@estado 
							,@documento_id=@documento_id
					break;
			END --IF

			IF (@td='I01' and @estado=1 and @tc='DE')
			BEGIN
	     			exec    sys_dev_I08
							 @doc_ext=@doc_ext
							,@estado=@estado 
							,@documento_id=@documento_id
					break;
			END --IF
			
			IF (@td is null and @estado=1 and @tc='IM')
			BEGIN
	     		exec sys_dev_I07
				 @doc_ext=@doc_ext
				,@estado=@estado 
				,@documento_id=@documento_id
			END --IF

	END --IF
	
	IF (@estado=1 and @tc='DE')
		BEGIN
	     	exec    sys_dev_I08
					@doc_ext=@doc_ext
					,@estado=@estado 
					,@documento_id=@documento_id
			return
	END --IF
			
		FETCH NEXT FROM @CUR INTO @doc_ext, @tc,@status	
	End
	close @cur
	deallocate @cur
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

ALTER     PROCEDURE [dbo].[SYS_DEV_I01]
 @doc_ext AS varchar(100)
,@estado as numeric(2,0)
,@documento_id numeric(20,0)
AS

DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
BEGIN

	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
begin 
	
	if @qty=0
      	   BEGIN 	
		insert into sys_dev_documento
		select top 1
		sid.CLIENTE_ID, 
		'I02', 
		sid.CPTE_PREFIJO, 
		sid.CPTE_NUMERO, 
		d.FECHA_CPTE, 
		sid.FECHA_SOLICITUD_CPTE, 
		sid.AGENTE_ID, 
		sid.PESO_TOTAL, 
		sid.UNIDAD_PESO, 
		sid.VOLUMEN_TOTAL, 
		sid.UNIDAD_VOLUMEN, 
		sid.TOTAL_BULTOS, 
		sid.ORDEN_DE_COMPRA, 
		sid.OBSERVACIONES, 
		d.NRO_REMITO, 
		sid.NRO_DESPACHO_IMPORTACION, 
		sid.DOC_EXT, 
		sid.CODIGO_VIAJE, 
		sid.INFO_ADICIONAL_1, 
		sid.INFO_ADICIONAL_2, 
		sid.INFO_ADICIONAL_3, 
		d.TIPO_COMPROBANTE_id, 
		NULL, 
		NULL, 
		'P', 
		GETDATE(),
		NULL	--flg_movimiento	
		from sys_int_documento sid
			inner join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_despacho_importacion)
		where sid.doc_ext=@doc_ext
	END
	
	insert into sys_dev_det_documento
	select	sidd.DOC_EXT, 
			cast(dd.documento_id as varchar) + cast(dd.nro_linea as varchar) as linea, 
			sidd.CLIENTE_ID, 
			sidd.PRODUCTO_ID, 
			MAX(SIDD.CANTIDAD_SOLICITADA), 
			dd.CANTIDAD, 
			dd.EST_MERC_ID, 
			dd.CAT_LOG_ID_FINAL, 
			dd.NRO_BULTO, 
			dd.DESCRIPCION, 
			dd.NRO_LOTE, 
			dd.PROP1 AS NRO_PALLET,
			dd.FECHA_VENCIMIENTO, 
			dd.NRO_DESPACHO, 
			dd.NRO_PARTIDA, 
			sidd.UNIDAD_ID, 
			sidd.UNIDAD_CONTENEDORA_ID, 
			sidd.PESO, 
			sidd.UNIDAD_PESO, 
			sidd.VOLUMEN, 
			sidd.UNIDAD_VOLUMEN, 
			sidd.PROP1, 
			dd.PROP2, --NRO_LOTE 
			Isnull(sidd.PROP3,Dbo.Fx_GetNroRemitoDO(DD.DOCUMENTO_ID)), 
			sidd.LARGO, 
			sidd.ALTO, 
			sidd.ANCHO, 
			sidd.DOC_BACK_ORDER, 
			NULL, 
			NULL, 
			'P', 
			GETDATE(), 
			DD.DOCUMENTO_ID, 
			dbo.get_nave_id(dd.documento_id,dd.nro_linea),
			dbo.get_nave_cod(dd.documento_id,dd.nro_linea), 	
			NULL	--flg_movimiento	
	from	sys_int_documento sid
			inner join sys_int_det_documento sidd on (sid.cliente_id=sidd.cliente_id and sid.doc_ext=sidd.doc_ext)
			inner join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_despacho_importacion)
			inner join det_documento dd on (d.documento_id=dd.documento_id and sidd.producto_id = dd.producto_id)
	where	sid.doc_ext=@doc_ext 
			and sidd.estado_gt is not null 
			and dd.documento_id=@documento_id
			and sidd.documento_id=@documento_id
	group by
			SIDD.DOC_EXT
			, CAST(DD.DOCUMENTO_ID AS VARCHAR) + CAST(DD.NRO_LINEA AS VARCHAR)
			, SIDD.CLIENTE_ID
			, SIDD.PRODUCTO_ID
			, DD.CANTIDAD
			, DD.EST_MERC_ID
			, DD.CAT_LOG_ID_FINAL
			, DD.NRO_BULTO
			, DD.DESCRIPCION
			, DD.NRO_LOTE
			, DD.PROP1
			, DD.FECHA_VENCIMIENTO
			, DD.NRO_DESPACHO
			, DD.NRO_PARTIDA
			, SIDD.UNIDAD_ID
			, SIDD.UNIDAD_CONTENEDORA_ID
			, SIDD.PESO
			, SIDD.UNIDAD_PESO
			, SIDD.VOLUMEN
			, SIDD.UNIDAD_VOLUMEN
			, SIDD.PROP1
			, DD.PROP2
			, ISNULL(SIDD.PROP3,DBO.FX_GETNROREMITODO(DD.DOCUMENTO_ID))
			, SIDD.LARGO
			, SIDD.ALTO
			, SIDD.ANCHO
			, SIDD.DOC_BACK_ORDER
			, DD.DOCUMENTO_ID
			, DBO.GET_NAVE_ID(DD.DOCUMENTO_ID,DD.NRO_LINEA)
			, DBO.GET_NAVE_COD(DD.DOCUMENTO_ID,DD.NRO_LINEA)		
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

ALTER          PROCEDURE [dbo].[SYS_DEV]
@documento_id AS NUMERIC(20,0) output,
@estado	as numeric(2,0) output
AS
DECLARE @doc_Ext AS varchar(100)
DECLARE @td AS varchar(20)
DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
DECLARE @tc AS varchar(15)
DECLARE @status AS varchar(5)
DECLARE @qty_doc_ext AS numeric(3,0)
DECLARE @tiene_cont AS numeric(3,0)
BEGIN
	select @doc_ext=nro_despacho_importacion,@tc=tipo_comprobante_id,@status=status from documento where documento_id=@documento_id
	select @qty=count(*) from sys_dev_documento where doc_ext=@doc_ext
	select @td=tipo_documento_id from sys_int_documento where doc_ext=@doc_ext
	select @nro_lin=max(nro_linea) from sys_dev_det_documento where doc_ext=@doc_ext
	
	select @qty_doc_ext=count(distinct prop2)from	documento d inner join det_documento dd
	on(d.documento_id=dd.documento_id)
	where	d.documento_id=@documento_id	
	
	select @tiene_cont=count(NRO_BULTO) from det_documento where documento_id = @documento_id	
	
IF (@qty_doc_ext>1 and @status='D40')
	BEGIN
		exec SYS_DEV_BULTOS @documento_id, @estado
		return
	END

 IF (@tiene_cont>0 and @status='D40')
	BEGIN
		exec SYS_DEV_BULTOS @documento_id, @estado
		return
	END	

IF (@doc_ext <> '' and @doc_ext is not null and @status='D40')
BEGIN
	
	IF (@td='I01' and @estado=1 and @tc='DO')
	BEGIN
	     exec sys_dev_I01
		 @doc_ext=@doc_ext
		,@estado=1 
                ,@documento_id=@documento_id
	END --IF

	IF (@td='I01' and @estado=3 and @tc='DO')
	BEGIN
	     	 exec sys_dev_I03
		 @doc_ext=@doc_ext
		,@estado=1 
                ,@documento_id=@documento_id
	END --IF

	IF (@td='I04' and @estado=1 and @tc='PP')
	BEGIN
	     	 exec sys_dev_I04
		 @doc_ext=@doc_ext
		,@estado=1 
                ,@documento_id=@documento_id
	END --IF

	IF (@td is null and @estado=1 and @tc='DE')
	BEGIN
	     	 exec    sys_dev_I08
					 @doc_ext=@doc_ext
					,@estado=@estado 
			        ,@documento_id=@documento_id
	END --IF
	
	IF (@td='I01' and @estado=1 and @tc='DE')
	BEGIN
	     	exec    sys_dev_I08
					@doc_ext=@doc_ext
					,@estado=@estado 
					,@documento_id=@documento_id
	END --IF
				
	IF (@td='E04' and @estado=1 and @tc='DE')
	BEGIN
	     	 exec    sys_dev_I08
					 @doc_ext=@doc_ext
					,@estado=@estado 
			        ,@documento_id=@documento_id
	END --IF
	
	SELECT @TD AS [TD]
	SELECT @ESTADO AS [ESTADO]
	SELECT @TC AS [TC]

	IF (@td is null and @estado=1 and @tc='IM')
	BEGIN
	     	 exec sys_dev_I07
		 @doc_ext=@doc_ext
		,@estado=@estado 
                ,@documento_id=@documento_id
	END --IF

END --IF

	IF (@td is null and @estado=1 and @tc='DE')
		BEGIN
	     	exec    sys_dev_I08
					@doc_ext=@doc_ext
					,@estado=@estado 
					,@documento_id=@documento_id
	END --IF


	IF (@td='I04' and @estado=2 and @tc='PP' and @status='D30')
	--Anula el pallet y genera un I06
    BEGIN
	     exec sys_dev_I04_D
		 @doc_ext=@doc_ext
		,@estado=2 
        ,@documento_id=@documento_id
	END --IF

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

ALTER  Procedure [dbo].[Sys_Dev_EgresoE10]
	@pviaje AS varchar(100) output
As
Begin
	Declare @Usuario	as Varchar(30)

	insert into sys_dev_documento
	select
	distinct 
	sid.CLIENTE_ID, 
	CASE WHEN sid.tipo_documento_id='E04' THEN 'E05' WHEN sid.tipo_documento_id='E08' THEN 'E09' ELSE sid.tipo_documento_id END, 
	sid.CPTE_PREFIJO, 
	sid.CPTE_NUMERO, 
	getdate(), --FECHA_CPTE, 
	sid.FECHA_SOLICITUD_CPTE, 
	sid.AGENTE_ID, 
	sid.PESO_TOTAL, 
	sid.UNIDAD_PESO, 
	sid.VOLUMEN_TOTAL, 
	sid.UNIDAD_VOLUMEN, 
	sid.TOTAL_BULTOS, 
	sid.ORDEN_DE_COMPRA, 
	sid.OBSERVACIONES, 
	cast(d.cpte_prefijo as varchar(20)) + cast(d.cpte_numero  as varchar(20)), 
	sid.NRO_DESPACHO_IMPORTACION, 
	sid.DOC_EXT, 
	sid.CODIGO_VIAJE, 
	sid.INFO_ADICIONAL_1, 
	sid.INFO_ADICIONAL_2, 
	sid.INFO_ADICIONAL_3, 
   	d.TIPO_COMPROBANTE_id, 	
	NULL, 
	NULL, 
	'P', 
	GETDATE(),
	Null --Flg_Movimiento
	from sys_int_documento sid
		left join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
	where sid.codigo_viaje=@pViaje


	insert into sys_dev_det_documento
	select	 d.nro_remito as doc_ext
			,(p.picking_id) as nro_linea
			,dd.cliente_id
			,dd.producto_id
			,dd.cant_solicitada
			,p.cant_confirmada
			,dd.est_merc_id
			,dd.cat_log_id_final
			,null as nro_bulto
			,dd.descripcion
			,dd.nro_lote
			,dd.prop1 as nro_pallet
			,dd.fecha_vencimiento
			,null as nro_despacho
			,dd.nro_partida
			,unidad_id
			,null as unidad_contenedora_id
			,null as peso
			,null as unidad_peso
			,null as volumen
			,null as unidad_volumen
			,Case  When D.Tipo_Comprobante_ID='E10'
				then  	DBO.GetValuesSysIntIA1(d.Documento_id, dd.Nro_linea) 
				Else 	dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1)
			 End As prop1
			,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,2) as prop2
			,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,3) as prop3
			,null as largo
			,null as alto
			,dd.nro_linea as ancho --nro de linea
			,null as doc_back_order
			,null as estado
			,null as fecha_estado
			,'P' as estado_gt
			,getdate() as fecha_estado_gt
			,p.documento_id
			,dbo.Aj_NaveCod_to_Nave_id(p.nave_cod) as nave_id
			,p.nave_cod	
			,Null --Flg_movimiento
	from 	det_documento dd
			inner join documento d on (dd.documento_id=d.documento_id)
			inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
	where
			p.Viaje_id=@pViaje

	Select @Usuario=Usuario_id from #Temp_Usuario_Loggin
	
	update 	picking 
		set 	facturado='1',
			fecha_control_Fac=Getdate(),
			Usuario_Control_fac=@Usuario,
			Terminal_Control_Fac=Host_Name()
	where 	viaje_id=@pViaje

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

ALTER PROCEDURE [dbo].[SYS_DEV_EGRESO]
 @pviaje AS varchar(100) output
AS
	declare @Qty as numeric(10,0)
	declare @ErrorSave int
	declare @AuxNroLinea bigint
	declare @ControlExpedicion char(1)
	declare @TipoComp	as varchar(5)
	declare @Usuario 	as varchar(20)
	declare @count		as smallint
	declare @controla	as char(1)
BEGIN
	begin try

		IF EXISTS (SELECT 1 FROM SYS_DEV_DOCUMENTO WHERE CODIGO_VIAJE = @pviaje)
			RETURN
		--Controlo que el viaje no este cerrado
		select @Qty=count(picking_id) from picking where viaje_id=@pViaje and facturado='1'
		if (@Qty>0) begin
			RAISERROR('El Picking/Viaje ya fue Cerrado!!!!',16,1)
			RETURN 
		end --if

		--Controlo que el viaje tenga todos los picking's cerrados
		set @Qty=0
		select @Qty=count(picking_id) from picking where (fin_picking in ('0','1') or fin_picking is null) and viaje_id=@pViaje
		if (@Qty>0) begin
			RAISERROR('Aun quedan Productos Pendientes por Pickear!!!!',16,1)
			RETURN 	
		end --if

		select	@Controla=isnull(flg_control_picking,'0')
		from	cliente_parametros 
		where	cliente_id=(select distinct cliente_id from picking(nolock) where viaje_id=@pviaje)
		if @controla='1'
		Begin
			SELECT	Distinct
					@count=count(pallet_controlado)
			From	picking p (nolock)
			Where 	P.viaje_id=@pviaje
					And pallet_controlado='0'
			if @count>0
			begin
				raiserror('Aun quedan pallets de picking por controlar',16,1)
				return
			end
		end

		---------------------------------------------------------------------------------------------------------------------
		--Controlo que el viaje este en el camion
		---------------------------------------------------------------------------------------------------------------------
		SELECT @TipoComp=TIPO_DOCUMENTO_ID FROM SYS_INT_DOCUMENTO WHERE CODIGO_VIAJE=@pviaje
		if @TipoComP ='E04'
		Begin

			select 	distinct 
					@ControlExpedicion=isnull(control_expedicion,'0')
			from 	documento d inner join tipo_comprobante tc
					on(d.tipo_comprobante_id=tc.tipo_comprobante_id)
			where	nro_despacho_importacion=ltrim(rtrim(Upper(@pViaje)))

		End
		Else
		Begin
			select @ControlExpedicion=control_expedicion from tipo_comprobante where tipo_comprobante_id=@TipoComp
		End


		set @Qty=0
		--control de expedicion parametrizable.
		SET @Controla=null

		Select	@Controla=isnull(c.flg_control_exp,'0')
		from	picking p inner join cliente_parametros c
				on(p.cliente_id=c.cliente_id)
		where	viaje_id=ltrim(rtrim(upper(@pViaje))) and st_control_exp='0'

		if @controla='1'
		begin
			Select @Qty=count(st_control_exp) from picking where viaje_id=ltrim(rtrim(upper(@pViaje))) and st_control_exp='0'
			if (@Qty>0 and @ControlExpedicion='1') begin
				RAISERROR('Aun quedan Pallets Pendientes de Cargar a Camion!!!!',16,1)
				RETURN 	
			end --if
		end
		--Controlo que no queden en sys_int_det_documento productos pendientes 
		set @Qty=0
		select @Qty=count(dd.doc_ext) 
		from sys_int_det_documento dd 
			inner join sys_int_documento d on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext)		
			inner join producto prod on (dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id)
		where dd.estado_gt is null and d.codigo_viaje=@pViaje
		if (@Qty>0) begin
			RAISERROR('El Picking/Viaje aun Tiene Productos Pendientes por Procesar!!!!',16,1)
			RETURN 	
		end --if

		If Dbo.GetTipoDocumento(@pViaje)='E10'
		Begin
			Exec Dbo.Sys_Dev_EgresoE10 @pViaje
			Return
		End
		   insert into sys_dev_documento
			select
			distinct 
			sid.CLIENTE_ID, 
			CASE WHEN sid.tipo_documento_id='E04' THEN 'E05' WHEN sid.tipo_documento_id='E08' THEN 'E09' ELSE sid.tipo_documento_id END, 
			sid.CPTE_PREFIJO, 
			sid.CPTE_NUMERO, 
			getdate(), --FECHA_CPTE, 
			sid.FECHA_SOLICITUD_CPTE, 
			sid.AGENTE_ID, 
			sid.PESO_TOTAL, 
			sid.UNIDAD_PESO, 
			sid.VOLUMEN_TOTAL, 
			sid.UNIDAD_VOLUMEN, 
			sid.TOTAL_BULTOS, 
			sid.ORDEN_DE_COMPRA, 
			sid.OBSERVACIONES, 
			cast(d.cpte_prefijo as varchar(20)) + cast(d.cpte_numero  as varchar(20)), 
			sid.NRO_DESPACHO_IMPORTACION, 
			sid.DOC_EXT, 
			sid.CODIGO_VIAJE, 
			sid.INFO_ADICIONAL_1, 
			sid.INFO_ADICIONAL_2, 
			sid.INFO_ADICIONAL_3, 
   			d.TIPO_COMPROBANTE_id, 	
			NULL, 
			NULL, 
			'P', 
			GETDATE(),
			Null --Flg_Movimiento 
			from	sys_int_documento sid
					left join documento d on (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
			where	sid.codigo_viaje=@pViaje
					and not exists (select	1 
									from	sys_dev_documento sd 
									where	sd.cliente_id=sid.cliente_id
											and sd.doc_ext=sid.doc_ext)
			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Documento, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END

			insert into sys_dev_det_documento
			select	 d.nro_remito as doc_ext
					,(p.picking_id) as nro_linea
					,dd.cliente_id
					,dd.producto_id
					,dd.cant_solicitada
					--,isnull(dd.cant_solicitada,sdd.CANTIDAD_SOLICITADA)
					,p.cant_confirmada
					,dd.est_merc_id
					,dd.cat_log_id_final
					,null as nro_bulto
					,dd.descripcion
					,dd.nro_lote
					,dd.prop1 as nro_pallet
					,dd.fecha_vencimiento
					,null as nro_despacho
					,dd.nro_partida
					,dd.unidad_id
					,null as unidad_contenedora_id
					,null as peso
					,null as unidad_peso
					,null as volumen
					,null as unidad_volumen
					,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,1) as prop1
					,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,2) as prop2
					,dbo.get_property(dd.cliente_id,d.nro_remito,dd.producto_id,3) as prop3
					,null as largo
					,null as alto
					,dd.nro_linea as ancho --nro de linea
					,null as doc_back_order
					,null as estado
					,null as fecha_estado
					,'P' as estado_gt
					,getdate() as fecha_estado_gt
					,p.documento_id
					,dbo.Aj_NaveCod_to_Nave_id(p.nave_cod) as nave_id
					,p.nave_cod	
					,Null		--Flg_Movimiento
			from 	det_documento dd
					inner join documento d on (dd.documento_id=d.documento_id)
					inner join picking p on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
					--JOIN AGREGADO PORQUE TREA MAS REGISTROS DADO QUE EXISTE OTRO DOCUMENTO EN LA TABLA DOCUMENTO
					--QUE TIENE EL MISMO 
					INNER JOIN sys_int_documento sid ON (sid.cliente_id=d.cliente_id and sid.doc_ext=d.nro_remito)
					--inner join SYS_INT_DET_DOCUMENTO sdd on(sdd.CLIENTE_ID=sid.CLIENTE_ID and sdd.DOC_EXT=sid.DOC_EXT and sdd.PRODUCTO_ID=dd.PRODUCTO_ID)
			where
					p.Viaje_id=@pViaje
					and not exists (select 1 from sys_dev_det_documento where sys_dev_det_documento.cliente_id = dd.cliente_id and sys_dev_det_documento.doc_Ext = d.nro_remito and sys_dev_det_documento.nro_linea = p.picking_id)

			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Det_Documento, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END
		  
		--Insert los productos que no ingresaron en el documento por falta de Stock
			insert into sys_dev_det_documento
			select dd.doc_ext
			,dd.nro_linea
			,dd.cliente_id
			,dd.producto_id
			,dd.cantidad_solicitada
			,0
			,dd.est_merc_id
			,dd.cat_log_id
			,dd.nro_bulto
			,dd.descripcion
			,dd.nro_lote
			,dd.nro_pallet
			,dd.fecha_vencimiento
			,dd.nro_despacho
			,dd.nro_partida
			,dd.unidad_id
			,dd.unidad_contenedora_id
			,dd.peso
			,dd.unidad_peso
			,dd.volumen
			,dd.unidad_volumen
			,dd.prop1
			,dd.prop2
			,dd.prop3
			,dd.largo
			,dd.alto
			,dd.ancho
			,dd.doc_back_order
			,null
			,null
			,dd.estado_gt
			,getdate()
			,dd.documento_id
			,dd.nave_id
			,dd.nave_cod
			,Null --Flg_Movimiento
			from 
			sys_int_det_documento dd
			inner join sys_int_documento d on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
			where cast(dd.doc_ext + dd.producto_id as varchar(400))  not in 
			(select cast(doc_ext + producto_id as varchar(400)) from sys_dev_det_documento)
			and d.codigo_viaje=@pViaje
			and not exists (select 1 from sys_dev_det_documento where sys_dev_det_documento.cliente_id = dd.cliente_id and sys_dev_det_documento.doc_Ext = dd.doc_ext and sys_dev_det_documento.nro_linea = dd.nro_linea)

			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error al Insertar en Sys_Dev_Det_Documento de los Productos Sin Stock, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END
		 	
			exec DBO.PedidoMultiProducto @pViaje

			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error Al Ejecutar la devolucion Pedido MultiProducto, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END

			
			--Si fue todo bien y no salto por error hago el update en facturado
			Select @Usuario=Usuario_id from #Temp_Usuario_Loggin
			
			update 	picking 
				set 	facturado='1',
					fecha_control_Fac=Getdate(),
					Usuario_Control_fac=@Usuario,
					Terminal_Control_Fac=Host_Name()
			where 	viaje_id=@pViaje


			IF @@ERROR <> 0 BEGIN
				SET @ErrorSave = @@ERROR
				RAISERROR('Error Al Realizar la Actualizacion en Cierre de Picking, Codigo_Error: %s',16,1,@ErrorSave)
				RETURN
			END
	end try
	begin catch
		exec usp_RethrowError
	end catch
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

ALTER /*ALTER*/ PROCEDURE [dbo].[SYS_INT_DET_DOC]
			@DOC_EXT as varchar(100)		OUTPUT,
			@NRO_LINEA as numeric			OUTPUT,
			@CLIENTE_ID as varchar(15)		OUTPUT,
			@PRODUCTO_ID as varchar(30)		OUTPUT,
			@CANTIDAD_SOLICITADA as numeric	OUTPUT,
			@CANTIDAD as numeric			OUTPUT,
			@EST_MERC_ID as varchar(50)		OUTPUT,
			@CAT_LOG_ID as varchar(50)		OUTPUT,
			@NRO_BULTO as varchar(100)		OUTPUT,
			@DESCRIPCION as varchar(500)	OUTPUT,
			@NRO_LOTE as varchar(100)		OUTPUT,
			@NRO_PALLET as varchar(100)		OUTPUT,
			@FECHA_VENCIMIENTO as datetime	OUTPUT,
			@NRO_DESPACHO as varchar(100)	OUTPUT,
			@NRO_PARTIDA as varchar(100)	OUTPUT,
			@UNIDAD_ID as varchar(5)		OUTPUT,
			@UNIDAD_CONTENEDORA_ID as varchar(5)OUTPUT,
			@PESO as numeric				OUTPUT,
			@UNIDAD_PESO as varchar(5)		OUTPUT,
			@VOLUMEN as numeric				OUTPUT,
			@UNIDAD_VOLUMEN as varchar(5)	OUTPUT,
			@PROP1 as varchar(100)			OUTPUT,
			@PROP2 as varchar(100)			OUTPUT,
			@PROP3 as varchar(100)			OUTPUT,
			@LARGO as numeric				OUTPUT,
			@ALTO as numeric				OUTPUT,
			@ANCHO as numeric				OUTPUT,
			@DOC_BACK_ORDER as varchar(100)	OUTPUT,
			@ESTADO as varchar(20)			OUTPUT,
			@FECHA_ESTADO as datetime		OUTPUT,
			@ESTADO_GT as varchar(20)		OUTPUT,
			@FECHA_ESTADO_GT as datetime	OUTPUT,
			@DOCUMENTO_ID as numeric		OUTPUT,		
			@NAVE_ID as numeric				OUTPUT,	
			@NAVE_COD as varchar(15)		OUTPUT

AS
BEGIN

	INSERT INTO [dbo].[SYS_INT_DET_DOCUMENTO]
           ([DOC_EXT],[NRO_LINEA],[CLIENTE_ID],[PRODUCTO_ID],[CANTIDAD_SOLICITADA],[CANTIDAD],[EST_MERC_ID],[CAT_LOG_ID]
           ,[NRO_BULTO],[DESCRIPCION],[NRO_LOTE],[NRO_PALLET],[FECHA_VENCIMIENTO],[NRO_DESPACHO],[NRO_PARTIDA],[UNIDAD_ID]
           ,[UNIDAD_CONTENEDORA_ID],[PESO],[UNIDAD_PESO],[VOLUMEN],[UNIDAD_VOLUMEN],[PROP1],[PROP2],[PROP3],[LARGO],[ALTO]
           ,[ANCHO],[DOC_BACK_ORDER],[ESTADO],[FECHA_ESTADO],[ESTADO_GT],[FECHA_ESTADO_GT],[DOCUMENTO_ID],[NAVE_ID],[NAVE_COD])
     VALUES
           (@DOC_EXT,@NRO_LINEA,@CLIENTE_ID,@PRODUCTO_ID,@CANTIDAD_SOLICITADA,@CANTIDAD,@EST_MERC_ID,@CAT_LOG_ID,
			@NRO_BULTO,@DESCRIPCION,@NRO_LOTE,@NRO_PALLET,CONVERT(DATETIME,@FECHA_VENCIMIENTO,103),@NRO_DESPACHO,
			@NRO_PARTIDA,@UNIDAD_ID,@UNIDAD_CONTENEDORA_ID,@PESO,@UNIDAD_PESO,@VOLUMEN,@UNIDAD_VOLUMEN,@PROP1,@PROP2,
			@PROP3,@LARGO,@ALTO,@ANCHO,@DOC_BACK_ORDER,@ESTADO,CONVERT(DATETIME,@FECHA_ESTADO,103),@ESTADO_GT,
			CONVERT(DATETIME,@FECHA_ESTADO_GT,103),@DOCUMENTO_ID,@NAVE_ID,@NAVE_COD)

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

/*CREATE*/ ALTER PROCEDURE [dbo].[SYS_INT_DOC]
		   @CLIENTE_ID as varchar(15)			OUTPUT,
           @TIPO_DOCUMENTO_ID as varchar(50)	OUTPUT,
           @CPTE_PREFIJO as varchar(6)			OUTPUT,
           @CPTE_NUMERO as varchar(20)			OUTPUT,
           @FECHA_CPTE as varchar(10)			OUTPUT,
           @FECHA_SOLICITUD_CPTE as varchar(10)	OUTPUT,
           @AGENTE_ID as varchar(20)			OUTPUT,
           @PESO_TOTAL as numeric				OUTPUT,
           @UNIDAD_PESO as varchar(5)			OUTPUT,
           @VOLUMEN_TOTAL as numeric			OUTPUT,
           @UNIDAD_VOLUMEN as varchar(5)		OUTPUT,
           @TOTAL_BULTOS as numeric				OUTPUT,
           @ORDEN_DE_COMPRA as varchar(100)		OUTPUT,
           @OBSERVACIONES as varchar(1000)		OUTPUT,
           @NRO_REMITO as varchar(50)			OUTPUT,
           @NRO_DESPACHO_IMPORTACION as varchar(50)OUTPUT,
           @DOC_EXT as varchar(100)				OUTPUT,
           @CODIGO_VIAJE as varchar(100)		OUTPUT,
           @INFO_ADICIONAL_1 as varchar(100)	OUTPUT,
           @INFO_ADICIONAL_2 as varchar(100)	OUTPUT,
           @INFO_ADICIONAL_3 as varchar(100)	OUTPUT,
           @TIPO_COMPROBANTE as varchar(5)		OUTPUT,
           @ESTADO as varchar(20)				OUTPUT,
           @FECHA_ESTADO as varchar(10)			OUTPUT,
           @ESTADO_GT as varchar(20)			OUTPUT,
           @FECHA_ESTADO_GT as varchar(10)		OUTPUT
AS
BEGIN
Declare @EXISTE		SmallInt

	SELECT	@EXISTE = COUNT(*)
			FROM	[dbo].[SYS_INT_DOCUMENTO]
			WHERE CLIENTE_ID = @CLIENTE_ID AND DOC_EXT = @DOC_EXT

	IF @EXISTE = 0
	BEGIN
		INSERT INTO [dbo].[SYS_INT_DOCUMENTO]
           ([CLIENTE_ID],[TIPO_DOCUMENTO_ID],[CPTE_PREFIJO],[CPTE_NUMERO],[FECHA_CPTE],[FECHA_SOLICITUD_CPTE],[AGENTE_ID]
           ,[PESO_TOTAL],[UNIDAD_PESO],[VOLUMEN_TOTAL],[UNIDAD_VOLUMEN],[TOTAL_BULTOS],[ORDEN_DE_COMPRA],[OBSERVACIONES]
           ,[NRO_REMITO],[NRO_DESPACHO_IMPORTACION],[DOC_EXT],[CODIGO_VIAJE],[INFO_ADICIONAL_1],[INFO_ADICIONAL_2],[INFO_ADICIONAL_3]
           ,[TIPO_COMPROBANTE],[ESTADO],[FECHA_ESTADO],[ESTADO_GT],[FECHA_ESTADO_GT])
		VALUES
           (@CLIENTE_ID,ltrim(rtrim(@TIPO_DOCUMENTO_ID)),@CPTE_PREFIJO,@CPTE_NUMERO,convert(datetime,@FECHA_CPTE,103),convert(datetime,@FECHA_SOLICITUD_CPTE,103),@AGENTE_ID, @PESO_TOTAL,@UNIDAD_PESO,@VOLUMEN_TOTAL,
			@UNIDAD_VOLUMEN,@TOTAL_BULTOS,@ORDEN_DE_COMPRA,@OBSERVACIONES,
		   @NRO_REMITO,@NRO_DESPACHO_IMPORTACION,@DOC_EXT,@CODIGO_VIAJE,@INFO_ADICIONAL_1,@INFO_ADICIONAL_2,@INFO_ADICIONAL_3,
		   @TIPO_COMPROBANTE,@ESTADO,convert(datetime,@FECHA_ESTADO,103),@ESTADO_GT,convert(datetime,@FECHA_ESTADO_GT,103))
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

ALTER Procedure [dbo].[sys_int_iex]

As
Begin
declare @vSuc						as varchar(20)
declare @vPed						as varchar(100)
declare @vnl						as numeric(20,0)
declare @vProducto_id				as varchar(30)
declare @vQty						as numeric(20,5)
declare @vNroControl				as varchar(100)
declare @vCodViaje					as varchar(100)
declare @vFecha						as varchar(100)
declare @CountReg                   as numeric(20,0)

declare @RsInf	as Cursor
SET NOCOUNT ON;

delete iex

Set @RsInf = Cursor For
	select 
	 d.agente_id 
	,d.doc_ext
	,dd.nro_linea
	,dd.producto_id
	,dd.cantidad_solicitada
	,dd.prop1
	,d.codigo_viaje
	,dbo.fx_DateTimeToAnsi(d.fecha_solicitud_cpte) as fecha 
	from sys_int_documento d
		inner join sys_int_det_documento dd on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
	where
		d.cliente_id='LEADER PRICE'
		and d.tipo_documento_id in ('E04','E03')
		and dd.estado is null
		and dd.estado_gt='P'
		and d.codigo_viaje in (select distinct ruta from picking p where p.fin_picking='2')
order by d.codigo_viaje desc

Open @RsInf
	Fetch Next From @RsInf into @vSuc,@vPed,@vnl,@vProducto_id,@vQty,@vNroControl,@vCodViaje,@vFecha 

While @@Fetch_Status=0
Begin	
	
	select 
	@CountReg=count(p.producto_id)
	from picking p
		inner join det_documento dd on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
		inner join documento d on (p.documento_id=d.documento_id)
	where p.fin_picking='2' and p.pallet_final is not null and ruta<>'ENVIADO' and
		  p.producto_id=@vProducto_id and d.sucursal_destino=@vSuc and d.nro_remito=@vPed

	if (@CountReg>0) begin
			insert into iex 
			select 
			@vNroControl,
			@vPed,
			@vFecha,
			p.producto_id,
			@vQty, --Cantidad Pedida
			p.cant_confirmada, --Cantidad Pickeada	
			0, --peso
			p.pallet_final,
			@vSuc,
			'LEADER PRICE',
			null
			from picking p
				inner join det_documento dd on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
				inner join documento d on (p.documento_id=d.documento_id)
			where p.fin_picking='2' and p.pallet_final is not null and ruta<>'ENVIADO' and
				  p.producto_id=@vProducto_id and d.sucursal_destino=@vSuc and d.nro_remito=@vPed
			
			update picking set ruta='ENVIADO' where picking_id in (	select p.picking_id		
							from picking p
								inner join det_documento dd on (p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
								inner join documento d on (p.documento_id=d.documento_id)
							where p.fin_picking='2' and p.pallet_final is not null and ruta<>'ENVIADO' and
								  p.producto_id=@vProducto_id and d.sucursal_destino=@vSuc and d.nro_remito=@vPed)
	end else begin
			insert into iex 
			values ( 
			@vNroControl,
			@vPed,
			@vFecha,
			@vProducto_id,
			@vQty, --Cantidad Pedida			
			0, --Cantidad Pickeada	
			0, --peso
			0,
			@vSuc,
			'LEADER PRICE',
			null)
	
	end --if
	update sys_int_det_documento set estado='INF',fecha_estado=getdate() where cliente_id='LEADER PRICE' and doc_ext=@vPed and Nro_linea=@vnl

	Fetch Next From @RsInf into @vSuc,@vPed,@vnl,@vProducto_id,@vQty,@vNroControl,@vCodViaje,@vFecha 
End	--End While @RsInf.

CLOSE @RsInf
DEALLOCATE @RsInf



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

ALTER                          procedure [dbo].[Sys_Int_Ingresa_Productos]
As
Begin


	-------------------------------------------------
	--TABLA SYS_INT_PRODUCTO.
	-------------------------------------------------
	Declare @Cliente_id			as varchar(15)			
	Declare @Producto_id			as varchar(30)
	Declare @Codigo_producto		as varchar(50)
	Declare @SubCodigo_1			as varchar(50)
	Declare @SubCodigo_2			as varchar(50)	
	Declare @Descripcion			as varchar(200)
	Declare @Marca				as varchar(60)		
	Declare @Fraccionable			as varchar(1)
	Declare @Unidad_Fraccion		as varchar(5)		
	Declare @Costo				as numeric(10,3)
	Declare @Unidad_id			as varchar(5)		
	Declare @SubFamilia_id			as varchar(30)
	Declare @Familia_id			as varchar(30)
	Declare @Observaciones			as varchar(400)
	Declare @Posiciones_Puras		as varchar(1)
	Declare @Moneda_Costo_Id		as varchar(20)
	Declare @Largo				as numeric(10,3)
	Declare @Alto				as numeric(10,3)
	Declare @Ancho				as numeric(10,3)
	Declare @Unidad_Volumen			as varchar(5)
	Declare @Peso				as numeric(20,5)
	Declare @Unidad_peso			as varchar(5)
	Declare @Lote_Automatico		as varchar(1)
	Declare @Pallet_Automatico		as varchar(1)
	Declare @Tolerancia_Min_Ingreso		as numeric(10,2)
	Declare @Tolerancia_Max_Ingreso		as numeric(10,2)
	Declare @Genera_Back_Order		as varchar(1)
	Declare @Clasificacion_Cot		as varchar(20)
	Declare @Codigo_Barra			as varchar(100)
	Declare @Ing_Cat_log_id			as varchar(50)
	Declare @Egr_Cat_log_id			as varchar(50)
	Declare @Producto_activo		as varchar(1)
	Declare @Cod_Tipo_Producto		as varchar(30)
	-------------------------------------------------
	--PRODUCTO.
	-------------------------------------------------
	Declare @IngresoT			as varchar(15)
	Declare	@EgresoT			as varchar(15)
	Declare @InventarioT			as varchar(15)
	Declare @Transferencia			as varchar(15)
	Declare @Ing_Cat_log_idP		as varchar(50)
	Declare @Egr_Cat_log_idP		as varchar(50)
	Declare @Pais_Id			as varchar(5)
	Declare @GrupoProducto			as varchar(5)
	Declare @TipoContenedora		as varchar(100)
	-------------------------------------------------
	--GENERICAS
	-------------------------------------------------
	Declare @Int				as Int
	Declare @Existe				as int
	-------------------------------------------------
	--CURSORES.
	-------------------------------------------------
	Declare @CurIngProd Cursor

		
	Set @CurIngProd=Cursor For
		Select 	 Cliente_id				,Producto_id
				,Codigo_producto		,SubCodigo_1
				,SubCodigo_2			,Descripcion
				,Marca				,Fraccionable
				,Unidad_Fraccion		,Costo
				,Unidad_id			,SubFamilia_id
				,Familia_id			,Observaciones
				,Posiciones_Puras		,Moneda_Costo_Id
				,Largo				,Alto
				,Ancho				,Unidad_Volumen
				,Peso				,Unidad_peso
				,Lote_Automatico		,Pallet_Automatico
				,Tolerancia_Min_Ingreso		,Tolerancia_Max_Ingreso
				,Genera_Back_Order		,Clasificacion_Cot
				,Codigo_Barra			,Ing_Cat_log_id
				,Egr_Cat_log_id			,Producto_activo
				,Cod_Tipo_Producto
		From	Sys_Int_Producto
		Where	(Ingresado='0' or Ingresado IS Null) --and fecha_carga is null

	Open @CurIngProd

	Fetch Next From @CurIngProd into @Cliente_id,@Producto_id,@Codigo_producto,@SubCodigo_1,@SubCodigo_2
									,@Descripcion,@Marca,@Fraccionable,@Unidad_Fraccion,@Costo,@Unidad_id
									,@SubFamilia_id,@Familia_id,@Observaciones,@Posiciones_Puras,@Moneda_Costo_Id
									,@Largo,@Alto,@Ancho,@Unidad_Volumen,@Peso,@Unidad_peso,@Lote_Automatico
									,@Pallet_Automatico,@Tolerancia_Min_Ingreso,@Tolerancia_Max_Ingreso
									,@Genera_Back_Order,@Clasificacion_Cot,@Codigo_Barra,@Ing_Cat_log_id
									,@Egr_Cat_log_id,@Producto_activo,@Cod_Tipo_Producto
	While @@Fetch_Status=0
	Begin
		select @Existe=dbo.Exist_Product(@Producto_id,@Cliente_id)
		
		if @Existe=0
		--Si el producto no existe, comienza la carga del mismo.
		Begin
			Select	@Int=Count(*)
			From	Producto
			where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id
	
			if @Int > 0
			Begin
				Select 	 @IngresoT			= Ingreso
						,@EgresoT		= Egreso
						,@InventarioT		= Inventario
						,@Transferencia		= Transferencia
						,@Ing_Cat_log_idP	= Ing_Cat_log_Id
						,@Egr_Cat_log_idP	= Egr_Cat_log_id
						,@Pais_Id		= Pais_id
						,@GrupoProducto		= Grupo_Producto
						,@TipoContenedora	= Tipo_Contenedora
				From	Producto
				Where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id
			End		
			Else
			Begin
				Select 	 @IngresoT			= Ingreso
						,@EgresoT		= Egreso
						,@InventarioT		= Inventario
						,@Transferencia		= Transferencia
						,@Ing_Cat_log_id	= Ing_Cat_log_Id
						,@Egr_Cat_log_id	= Egr_Cat_log_id
						,@Pais_Id		= Pais_id
						,@GrupoProducto		= Grupo_Producto
						,@TipoContenedora	= Tipo_Contenedora
				From	Producto
				Where	Cliente_id=@Cliente_id and Producto_id=@Familia_id;
			End
			IF LTRIM(RTRIM(@GrupoProducto))=''
			BEGIN
				SET @GrupoProducto=Null
			END
			Insert Into Producto (	 Cliente_id
									,Producto_id
									,Codigo_Producto
									,SubCodigo_1
									,SubCodigo_2
									,Descripcion
									,Nombre
									,Marca
									,Fraccionable
									,Unidad_Fraccion
									,Costo
									,Unidad_id
									,Tipo_Producto_id
									,Pais_Id
									,Familia_id
									,Criterio_id
									,Observaciones
									,Posiciones_Puras
									,Kit
									,Serie_Egr
									,Moneda_id
									,No_Agrupa_Items
									,Largo
									,Alto
									,Ancho
									,Unidad_Volumen
									,Volumen_Unitario
									,Peso
									,Unidad_peso
									,Peso_Unitario
									,Lote_Automatico
									,Pallet_Automatico
									,Ingreso
									,Egreso
									,Inventario
									,Transferencia
									,Tolerancia_Min
									,Tolerancia_Max
									,Back_Order
									,Clasificacion_Cot
									,Codigo_Barra
									,Ing_Cat_log_Id
									,Egr_Cat_log_id
									,Sub_Familia_id
									,Tipo_Contenedora
									,Grupo_Producto
									,Envase)
			Values(
									 @Cliente_id
									,@Producto_id
									,@Codigo_producto
									,@SubCodigo_1
									,@SubCodigo_2
									,@Descripcion
									,Null
									,@Marca
									,@Fraccionable
									,@Unidad_Fraccion
									,@Costo
									,@Unidad_id
									,@Cod_Tipo_Producto
									,isnull(@Pais_Id,'AR')
									,isnull(@Familia_id,'DEFAULT')
									,Null
									,@Observaciones
									,@Posiciones_Puras
									,0
									,0
									,@Moneda_Costo_Id
									,0
									,@Largo
									,@Alto
									,@Ancho
									,'M3'
									,0
									,@Peso
									,'KG'
									,0
									,ISNULL(@Lote_Automatico,'1')
									,ISNULL(@Pallet_Automatico,'1')
									,ISNULL(@IngresoT,'ING_PT')
									,ISNULL(@EgresoT,'PICK_PT')
									,ISNULL(@InventarioT,'INVENTARIO')
									,ISNULL(@Transferencia,'TRANSFERENCIAS')
									,@Tolerancia_Min_Ingreso
									,@Tolerancia_Max_Ingreso
									,ISNULL(@Genera_Back_Order,'1')
									,@Clasificacion_Cot
									,@Codigo_Barra
									,ISNULL(@Ing_Cat_log_idP,'CUA')
									,@Egr_Cat_log_idP
									,@SubFamilia_id
									,@TipoContenedora
									,ISNULL(@GrupoProducto,'SG')
									,'0'
					)

			--CRITERIOS DE LOCATOR.		
			Select	@Int=Count(*)
			From	Sys_Criterio_Locator
			where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;
			if @Int > 0
			Begin
				Insert into Sys_Criterio_Locator
					Select	@Cliente_id,@Producto_Id,Criterio_Id,Order_id,Forma_Id,Posicion_id
					from	Sys_Criterio_Locator
					where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;
			End		
			Else
			Begin
				Insert into Sys_Criterio_Locator
					Select	@Cliente_id,@Producto_Id,Criterio_Id,Order_id,Forma_Id,Posicion_id
					from	Sys_Criterio_Locator
					where	Cliente_id=@Cliente_id and Producto_id=@Familia_id
				-- Si no encuentra nada levanta esto.
				If @@Rowcount=0 
				Begin
					Insert into Sys_criterio_Locator Values(@Cliente_Id, @Producto_Id, 'FECHA_VENCIMIENTO','ASC','TO_DATE', 1)
					Insert into Sys_criterio_Locator Values(@Cliente_Id, @Producto_Id, 'ORDEN_PICKING', 'ASC','TO_NUMBER', 2)
				End
				
			End
			--FIN CRITERIOS DE LOCATOR.
	
			--POSICIONES Y NAVES.
			Select	@Int=Count(*)
			From	rl_producto_posicion_permitida
			where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;
			if @Int > 0
			Begin
				Insert into rl_producto_posicion_permitida
					Select	@Cliente_id,@Producto_id,nave_id,Posicion_id
					From	rl_producto_posicion_permitida
					where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;
			End		
			Else
			Begin
				Insert into rl_producto_posicion_permitida
					Select	@Cliente_id,@Producto_id,nave_id,Posicion_id
					From	rl_producto_posicion_permitida
					where	Cliente_id=@Cliente_id and Producto_id=@Familia_id
			End
			--mandatorios producto
			Select	@Int=Count(*)
			From	mandatorio_producto
			where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;
			if @Int > 0
			Begin
				Insert into mandatorio_producto
					Select	@Cliente_id,@Producto_id,tipo_operacion,campo
					From	mandatorio_producto
					where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;

			End		
			Else
			Begin
				Insert into mandatorio_producto
					Select	@Cliente_id,@Producto_id,tipo_operacion,campo
					From	mandatorio_producto
					where	Cliente_id=@Cliente_id and Producto_id=@Familia_id
				--Si no hay mandatorios heredables Carga por default.
				If @@RowCount=0
				Begin
					Insert into Mandatorio_Producto Values(@Cliente_Id,@Producto_Id,'EGR','CANTIDAD')
					Insert into Mandatorio_Producto Values(@Cliente_Id,@Producto_Id,'ING','CANTIDAD')
				End
			End
			--Producto Tratamiento
			Select	@Int=Count(*)
			From	rl_producto_tratamiento
			where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;
			if @Int > 0
			Begin
				Insert into rl_producto_tratamiento(cliente_id,producto_id,tipo_operacion_id,tipo_comprobante_id,transaccion_id)
					Select	@Cliente_id,@Producto_id,tipo_operacion_id,tipo_comprobante_id,transaccion_id
					From	rl_producto_tratamiento
					where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;

			End		
			Else
			Begin
				Insert into rl_producto_tratamiento(cliente_id,producto_id,tipo_operacion_id,tipo_comprobante_id,transaccion_id)
					Select	@Cliente_id,@Producto_id,tipo_operacion_id,tipo_comprobante_id,transaccion_id
					From	rl_producto_tratamiento
					where	Cliente_id=@Cliente_id and Producto_id=@Familia_id;

				If @@RowCount=0
				Begin
					Insert into Rl_Producto_Tratamiento(Cliente_Id,Producto_Id,Tipo_Operacion_Id, Tipo_Comprobante_Id,Transaccion_Id)
												Values(	@Cliente_Id, @Producto_Id,'EGR','E04','PICK_PT')

				
				End
			End

			--producto categoria logica
			Select	@Int=Count(*)
			From	rl_producto_tratamiento
			where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;
			if @Int > 0
			Begin
				Insert into rl_producto_catlog(cliente_id,producto_id,tipo_operacion_id,tipo_comprobante_id,Cat_log_id)
					Select	@Cliente_id,@Producto_id,tipo_operacion_id,tipo_comprobante_id,Cat_log_id
					From	rl_producto_catlog
					where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id;

			End		
			Else
			Begin
				Insert into rl_producto_catlog(cliente_id,producto_id,tipo_operacion_id,tipo_comprobante_id,Cat_log_id)
					Select	@Cliente_id,@Producto_id,tipo_operacion_id,tipo_comprobante_id,Cat_log_id
					From	rl_producto_catlog
					where	Cliente_id=@Cliente_id and Producto_id=@Familia_id;
			End

			--Levanto la configuracion de las etiquetas
			Select	@Int=Count(*)
			From	Etiqueta_Producto
			Where	cliente_Id=@Cliente_Id and Producto_Id=@SubFamilia_id;

			If @Int>0
			Begin
				Insert into Etiqueta_Producto 
					Select 	 @Cliente_Id, @Producto_Id, Tipo_Operacion_Id, Margh, Margv, Alto, Ancho, Impresora
							,Copias, Distentreetiquetas, QTYxLinea, Modo_Impresion,Terminal_Id
					From 	Etiqueta_Producto
					Where	Cliente_Id=@Cliente_Id and Producto_Id=@SubFamilia_id;
			End
			Else
			Begin
				Insert into Etiqueta_Producto 
					Select 	 @Cliente_Id, @Producto_Id, Tipo_Operacion_Id, Margh, Margv, Alto, Ancho, Impresora
							,Copias, Distentreetiquetas, QTYxLinea, Modo_Impresion,Terminal_Id
					From 	Etiqueta_Producto
					Where	Cliente_Id=@Cliente_Id and Producto_Id=@Familia_id;
			End
		End
		Else	--Desde aca es para los productos que ya estan ingresados.
		Begin

			Select	@Int=Count(*)
			From	Producto
			where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id
	
			if @Int > 0
			Begin
				Select 	 @IngresoT			= Ingreso
						,@EgresoT		= Egreso
						,@InventarioT		= Inventario
						,@Transferencia		= Transferencia
						,@Ing_Cat_log_idP	= Ing_Cat_log_Id
						,@Egr_Cat_log_idP	= Egr_Cat_log_id
						,@Pais_Id		= Pais_id
						,@GrupoProducto		= Grupo_Producto
						,@TipoContenedora	= Tipo_Contenedora
				From	Producto
				Where	Cliente_id=@Cliente_id and Producto_id=@SubFamilia_id
			End		
			Else
			Begin
				Select 	 @IngresoT			= Ingreso
						,@EgresoT		= Egreso
						,@InventarioT		= Inventario
						,@Transferencia		= Transferencia
						,@Ing_Cat_log_id	= Ing_Cat_log_Id
						,@Egr_Cat_log_id	= Egr_Cat_log_id
						,@Pais_Id		= Pais_id
						,@GrupoProducto		= Grupo_Producto
						,@TipoContenedora	= Tipo_Contenedora
				From	Producto
				Where	Cliente_id=@Cliente_id and Producto_id=@Familia_id;
			End
			IF LTRIM(RTRIM(@GrupoProducto))=''
			BEGIN
				SET @GrupoProducto=Null
			END
			
			Update producto Set
					 Codigo_Producto	=	@Codigo_producto
					,SubCodigo_1		=	@SubCodigo_1
					,SubCodigo_2		=	@SubCodigo_2
					,Descripcion			=	@Descripcion
					,Nombre				=	Null
					,Marca				=	@Marca
					,Fraccionable		=	@Fraccionable
					,Unidad_Fraccion	=	@Unidad_Fraccion
					,Costo				=	@Costo
					,Unidad_id			=	@Unidad_id
					,Tipo_Producto_id	=	@Cod_Tipo_Producto
					,Pais_Id				=	ISNULL(@Pais_Id,'AR')
					,Familia_id			=	ISNULL(@Familia_id,'DEFAULT')
					,Criterio_id			=	Null
					,Observaciones		=	@Observaciones
					,Posiciones_Puras	=	@Posiciones_Puras
					,Kit					=	0
					,Serie_Egr			=	0
					,Moneda_id			=	@Moneda_Costo_Id
					,No_Agrupa_Items	=	0
					,Largo				=	@Largo
					,Alto				=	@Alto
					,Ancho				=	@Ancho
					,Unidad_Volumen	=	'M3'
					,Volumen_Unitario	=	0
					,Peso				=	@Peso
					,Unidad_peso		=	'KG'
					,Peso_Unitario		=	0
					,Lote_Automatico	=	ISNULL(@Lote_Automatico,'1')
					,Pallet_Automatico	=	ISNULL(@Pallet_Automatico,'1')
					,Ingreso				=	ISNULL(@IngresoT,'ING_PT')
					,Egreso				=	ISNULL(@EgresoT,'PICK_PT')
					,Inventario			=	ISNULL(@InventarioT,'INVENTARIO')
					,Transferencia		=	ISNULL(@Transferencia,'TRANSFERENCIAS')
					,Tolerancia_Min		=	@Tolerancia_Min_Ingreso
					,Tolerancia_Max		=	@Tolerancia_Max_Ingreso
					,Back_Order			=	@Genera_Back_Order
					,Clasificacion_Cot	=	@Clasificacion_Cot
					,Codigo_Barra		=	@Codigo_Barra
					,Ing_Cat_log_Id		=	ISNULL(@Ing_Cat_log_idP,'CUA')
					,Egr_Cat_log_id		=	@Egr_Cat_log_idP
					,Sub_Familia_id		=	@SubFamilia_id
					,Tipo_Contenedora	=	@TipoContenedora
					,Grupo_Producto		=	IsNull(@GrupoProducto,'SG')
			WHERE 	Cliente_id=@Cliente_id AND Producto_id=@Producto_id
		End
		If @@error=0
		Begin
			update sys_int_producto set ingresado=1,fecha_carga=getdate() where cliente_id=@cliente_id and producto_id=@producto_id
		End

		Fetch Next From @CurIngProd into @Cliente_id,@Producto_id,@Codigo_producto,@SubCodigo_1,@SubCodigo_2
										,@Descripcion,@Marca,@Fraccionable,@Unidad_Fraccion,@Costo,@Unidad_id
										,@SubFamilia_id,@Familia_id,@Observaciones,@Posiciones_Puras,@Moneda_Costo_Id
										,@Largo,@Alto,@Ancho,@Unidad_Volumen,@Peso,@Unidad_peso,@Lote_Automatico
										,@Pallet_Automatico,@Tolerancia_Min_Ingreso,@Tolerancia_Max_Ingreso
										,@Genera_Back_Order,@Clasificacion_Cot,@Codigo_Barra,@Ing_Cat_log_id
										,@Egr_Cat_log_id,@Producto_activo,@Cod_Tipo_Producto
	End					
	Close @CurIngProd
	Deallocate @CurIngProd
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

ALTER   PROCEDURE [dbo].[Sys_WriteLocking]
@pid 						as	varchar(200) output,
@pError					as varchar(4000) output

AS

BEGIN

	--Borro las sessiones que detecto que no estan activas y la que corre este procedure
	delete Sys_Session_Login where session_id not in (select spid from  master.dbo.sysprocesses)
	delete Sys_Session_Login where session_id in (select @@spid)

	insert into eventslock	
	select distinct 
		@pid,	
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
		getdate() as fecha_registro,
		@pError
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

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[TAREAS_PICKING_D]
	@USUARIO 			AS VARCHAR(30),
	@VIAJE_ID 			AS VARCHAR(100),
	@PALLET_I			AS VARCHAR(30),
	@RUTA_I				AS VARCHAR(50),
	@CLIENTE			AS VARCHAR(30)=NULL,
	@VH					AS VARCHAR(40)=NULL,
	@PALLETCOMPLETO		AS NUMERIC(10),
	@NAVECALLE			AS VARCHAR(50)=NULL
AS
	DECLARE @VERIFICACION 	AS NUMERIC(20)
	DECLARE @VIAJEID 		AS VARCHAR(100)
	DECLARE @PRODUCTO_ID	AS VARCHAR(50)
	DECLARE @DESCRIPCION	AS VARCHAR(200)
	DECLARE @QTY			AS NUMERIC(20,5)
	DECLARE @POSICION_COD	AS VARCHAR(45)
	DECLARE @PALLET			AS VARCHAR(100)
	DECLARE @RUTA			AS VARCHAR(50)--SE USA INTERNAMENTE Y SE DEVUELVE A LA APLICACION
	DECLARE @UNIDAD_ID		AS VARCHAR(5)
	DECLARE @TQUERY			AS VARCHAR(1)
	DECLARE @PICKING_ID		AS INT
	DECLARE @VAL_COD_EGR	AS CHAR(1)
	DECLARE @CLIENTE_ID		AS VARCHAR(15)
	DECLARE @NRO_LOTE		AS VARCHAR(50)
	DECLARE @TOMARUTA		AS CHAR(1)
	DECLARE @PALLETCOMP		AS CHAR(1)
	DECLARE @NRO_LINEA		AS NUMERIC(20,0)
	DECLARE @NRO_CONTENEDORA AS VARCHAR(50)
	DECLARE @DOCUMENTO_ID AS NUMERIC(20,0)
	DECLARE @LOTEPROVEEDOR	AS VARCHAR(100)
	DECLARE @NRO_PARTIDA	AS VARCHAR(100)
	DECLARE @NRO_SERIE		AS VARCHAR(50)
	DECLARE @NRO_SERIE_STOCK AS VARCHAR(50)

	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ

	BEGIN

	--DETERMINO SI TOMO TODA LA RUTA.
	SELECT	@TOMARUTA=ISNULL(FLG_TOMAR_RUTA,'0')
	FROM	CLIENTE_PARAMETROS
	WHERE	CLIENTE_ID=@CLIENTE

	SELECT	@PALLETCOMP=ISNULL(FLG_ACTIVA_PC_PN,'0')
	FROM	CLIENTE_PARAMETROS
	WHERE	CLIENTE_ID=@CLIENTE

	IF @VIAJE_ID <> '0'	
	BEGIN
		SELECT @VERIFICACION= DBO.VERIFICA_FIN_VIAJES(@VIAJE_ID)
		IF @VERIFICACION=1	
		BEGIN
			RAISERROR ('1', 16, 1)
			Return(99)
		END
		ELSE
		BEGIN
			SELECT @VERIFICACION= Dbo.Fx_Fin_Viaje_Usuario(@VIAJE_ID,@USUARIO)
			IF @VERIFICACION=1	
			BEGIN
				RAISERROR ('1', 16, 1)
				Return(99)
			END
		END
		IF @RUTA_I<>'0' AND @RUTA_I IS NOT NULL
		BEGIN
			SELECT @VERIFICACION= DBO.FX_FIN_RUTA(@VIAJE_ID,@RUTA_I)
			IF @VERIFICACION=1	
			BEGIN
				RAISERROR('2',16,1)
				Return(99)
			END

			ELSE
			BEGIN
				SELECT @VERIFICACION= DBO.FX_FIN_RUTA_USUARIO(@VIAJE_ID,@RUTA_I,@USUARIO)
				IF (@VERIFICACION=1)
				BEGIN
					IF (@TOMARUTA='1')
					BEGIN
						SET @RUTA_I= NULL
					END
					ELSE
					BEGIN
						IF @TOMARUTA='0'
						BEGIN
							Set @RUTA_I=null
						END
						ELSE
						BEGIN
							RAISERROR('2',16,1)
							Return(99)
						END
					END
				END
			END	
		END
	END --FIN VERIFICACIONES.

 	IF @VIAJE_ID='0'	
		BEGIN	
			SET @TQUERY='1'
		END
	ELSE
		BEGIN
			IF @VIAJE_ID IS NOT NULL AND @RUTA_I IS NOT NULL BEGIN
					SET @TQUERY='2'
				END
			ELSE BEGIN
					IF @VIAJE_ID IS NOT NULL AND @RUTA_I IS NULL AND 0=0 --Fin Ruta
						BEGIN
							SET @TQUERY='3'
						END
			END
		END --FIN TQUERY

	IF @TQUERY='1' BEGIN--Por aca pase y termine
		
			SELECT 	TOP 1
					@VIAJEID=SP.VIAJE_ID, @PRODUCTO_ID=SP.PRODUCTO_ID,@DESCRIPCION=SP.DESCRIPCION, 
					@QTY=SUM(SP.CANTIDAD),@POSICION_COD= SP.POSICION_COD,@PALLET=SP.PROP1,@RUTA=SP.RUTA,
					@UNIDAD_ID=PROD.UNIDAD_ID, @VAL_COD_EGR=PROD.VAL_COD_EGR,@CLIENTE_ID=SP.CLIENTE_ID,
					@NRO_LOTE=CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,
					@NRO_SERIE_STOCK = SP.NRO_SERIE,--@NRO_LINEA= SP.NRO_LINEA, --LO AGREGUE PARA QUE ACTUALICE POR NRO_LINEA Y NO TODAS LAS TAREAS
					@NRO_CONTENEDORA =DD.NRO_BULTO,@LOTEPROVEEDOR = DD.NRO_LOTE, @NRO_PARTIDA = DD.NRO_PARTIDA
					,@NRO_SERIE = DD.NRO_SERIE
			FROM 	PICKING SP
					LEFT JOIN POSICION POS ON(SP.POSICION_COD=POS.POSICION_COD)
					INNER JOIN PRIORIDAD_VIAJE SPV
					ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
					INNER JOIN PRODUCTO PROD
					ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
					INNER JOIN RL_SYS_CLIENTE_USUARIO SU ON(SP.CLIENTE_ID=SU.CLIENTE_ID)
					INNER JOIN DET_DOCUMENTO DD ON(SP.DOCUMENTO_ID=DD.DOCUMENTO_ID AND SP.NRO_LINEA=DD.NRO_LINEA)
					INNER JOIN CLIENTE C ON(SP.CLIENTE_ID=C.CLIENTE_ID)
					INNER JOIN CLIENTE_PARAMETROS CP ON(C.CLIENTE_ID=CP.CLIENTE_ID)
			WHERE 	SPV.PRIORIDAD = (	SELECT 	MIN(PRIORIDAD)
										FROM	PRIORIDAD_VIAJE
										WHERE	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID)))											)								
												AND SU.USUARIO_ID=@USUARIO
					AND ((CP.FLG_ACTIVA_PC_PN='0') OR (DBO.VERIFICA_PALLET_FINAL(SP.POSICION_COD,SP.VIAJE_ID,SP.RUTA, SP.PROP1)=@PALLETCOMPLETO))
					AND SP.FLG_PALLET_HOMBRE = SP.TRANSF_TERMINADA -- Agregado Privitera Maximiliano 06/01/2010
					AND	SP.FECHA_INICIO IS NULL
					AND	SP.FECHA_FIN IS NULL			
					AND	SP.USUARIO IS NULL
					AND	SP.CANT_CONFIRMADA IS NULL 
					AND	SP.VIAJE_ID IN (SELECT 	VIAJE_ID
										FROM  	RL_VIAJE_USUARIO
										WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID)))
												AND	LTRIM(RTRIM(UPPER(USUARIO_ID))) =LTRIM(RTRIM(UPPER(@USUARIO)))
					AND SP.NAVE_COD	IN(	SELECT 	NAVE_COD
										FROM 	NAVE N INNER JOIN RL_USUARIO_NAVE RLNU
												ON(N.NAVE_ID=RLNU.NAVE_ID)
										WHERE	N.NAVE_COD=SP.NAVE_COD
												AND LTRIM(RTRIM(UPPER(RLNU.USUARIO_ID)))=LTRIM(RTRIM(UPPER(@USUARIO)))
										)
										)
					AND SP.FIN_PICKING <>'2'
					AND ((@CLIENTE IS NULL) OR(SP.CLIENTE_ID=@CLIENTE))
					AND	((@VH IS NULL)OR(SP.POSICION_COD IN(SELECT 	POSICION_COD
															FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																	INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																	INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
															WHERE 	VEHICULO_ID=@VH
																	AND((CP.FLG_PICKING_CN='0')OR (CN.CALLE_COD=@NAVECALLE))
															UNION 
															SELECT 	NAVE_COD AS POSICION_COD
															FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																	ON(V.NAVE_ID=N2.NAVE_ID)
															WHERE	VEHICULO_ID=@VH
																	AND((CP.FLG_PICKING_CN='0')OR(N2.NAVE_COD=@NAVECALLE))
																	)))
			GROUP BY	
					SP.VIAJE_ID, SP.PRODUCTO_ID,SP.DESCRIPCION, SP.RUTA,SP.POSICION_COD,SP.TIPO_CAJA,SP.PROP1,PROD.UNIDAD_ID,
					SPV.PRIORIDAD, PROD.VAL_COD_EGR,SP.CLIENTE_ID, POS.ORDEN_PICKING,CP.FLG_SOLICITA_LOTE, 
					CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,SP.NRO_SERIE,DD.NRO_BULTO
					,DD.NRO_LOTE,DD.NRO_PARTIDA,DD.NRO_SERIE
			ORDER BY
					SPV.PRIORIDAD ASC,SP.RUTA, CAST(ISNULL(SP.TIPO_CAJA,0) AS NUMERIC(10,1)) DESC,POS.ORDEN_PICKING, SP.POSICION_COD ASC, SP.PRODUCTO_ID


			UPDATE 	PICKING SET FECHA_INICIO = GETDATE(),USUARIO=UPPER(LTRIM(RTRIM(@USUARIO))),PALLET_PICKING=@PALLET_I,
					VEHICULO_ID=@VH		
			FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
					ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)			
			WHERE  	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID )))
					AND LTRIM(RTRIM(UPPER(P.PRODUCTO_ID)))=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
					AND LTRIM(RTRIM(UPPER(P.DESCRIPCION)))=LTRIM(RTRIM(UPPER(@DESCRIPCION)))
					AND FLG_PALLET_HOMBRE = TRANSF_TERMINADA -- Agregado Privitera Maximiliano 06/01/2010
					AND LTRIM(RTRIM(UPPER(P.POSICION_COD)))=LTRIM(RTRIM(UPPER(@POSICION_COD)))
					AND ((@PALLET IS NULL) OR(LTRIM(RTRIM(UPPER(P.PROP1))) = LTRIM(RTRIM(UPPER(@PALLET)))))
					AND LTRIM(RTRIM(UPPER(P.RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
					AND ((@LOTEPROVEEDOR IS NULL OR @LOTEPROVEEDOR = '') OR (DD.NRO_LOTE=@LOTEPROVEEDOR))
					AND ((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA = '') OR (DD.NRO_PARTIDA=@NRO_PARTIDA))
					AND ((@VH IS NULL OR @VH='') 
							OR(	POSICION_COD IN(	SELECT 	POSICION_COD
													FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
															INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
															INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
													WHERE 	VEHICULO_ID=@VH
													UNION 
													SELECT 	NAVE_COD AS POSICION_COD
													FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
															ON(V.NAVE_ID=N2.NAVE_ID)
													WHERE	VEHICULO_ID=@VH)))
					AND (@NRO_SERIE_STOCK IS NULL OR P.NRO_SERIE = @NRO_SERIE_STOCK)
					AND (@NRO_CONTENEDORA IS NULL OR DD.NRO_BULTO = @NRO_CONTENEDORA)

					
					--AND P.NRO_LINEA =@NRO_LINEA 
					--Catalina Castillo.Tracker 4741
					--AND P.PICKING_ID=@PICKING_ID 
			if @tomaruta='1'
			begin --Comienzo a tomar toda la ruta.
				DECLARE T_RUTA CURSOR FOR
				SELECT 	PICKING_ID
				FROM	PICKING P
				WHERE	((@PALLETCOMP='0') OR (DBO.VERIFICA_PALLET_FINAL(P.POSICION_COD,P.VIAJE_ID,P.RUTA, P.PROP1)=@PALLETCOMPLETO))
						AND P.NAVE_COD IN(	SELECT	NAVE_COD
											FROM 	NAVE N INNER JOIN RL_USUARIO_NAVE RLNU
													ON(N.NAVE_ID=RLNU.NAVE_ID)
											WHERE	N.NAVE_COD=P.NAVE_COD
													AND RLNU.USUARIO_ID=@USUARIO)
													AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
													AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
													AND FECHA_INICIO IS NULL AND FECHA_FIN  IS NULL AND USUARIO IS NULL
													AND	((@VH IS NULL) OR(	POSICION_COD IN(	SELECT 	POSICION_COD
																								FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																										INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																										INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																								WHERE 	VEHICULO_ID=@VH
																								UNION 
																								SELECT 	NAVE_COD AS POSICION_COD
																								FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																										ON(V.NAVE_ID=N2.NAVE_ID)
																								WHERE	VEHICULO_ID=@VH)))
				OPEN T_RUTA

				FETCH NEXT FROM T_RUTA INTO @PICKING_ID
				WHILE @@FETCH_STATUS=0 
					BEGIN
					If 0=0 
						Begin
							UPDATE	PICKING SET USUARIO =@USUARIO 
							FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
									ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
							WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID))) 
									AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA))) 
									AND PICKING_ID=@PICKING_ID
									--AND (DBO.VERIFICA_PALLET_FINAL(@POSICION_COD,@VIAJEID,@RUTA, @PALLET)=@PALLETCOMPLETO)
									AND FECHA_INICIO IS NULL AND FECHA_FIN IS NULL AND CANT_CONFIRMADA IS NULL AND PALLET_PICKING IS NULL
									AND USUARIO IS NULL
									AND
									((@VH IS NULL) OR(	POSICION_COD IN(	SELECT 	POSICION_COD
																			FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																					INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																					INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																			WHERE 	VEHICULO_ID=@VH
																			UNION 
																			SELECT 	NAVE_COD AS POSICION_COD
																			FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																					ON(V.NAVE_ID=N2.NAVE_ID)
																			WHERE	VEHICULO_ID=@VH)))
							FETCH NEXT FROM T_RUTA INTO @PICKING_ID
						End
					END
				CLOSE T_RUTA
				DEALLOCATE T_RUTA
			End
			IF @PRODUCTO_ID IS NOT NULL 
			BEGIN
				SELECT 	@VIAJEID AS VIAJE_ID,@PRODUCTO_ID AS PRODUCTO_ID, @DESCRIPCION AS DESCRIPCION, 
						@QTY AS QTY, @POSICION_COD AS POSICION_COD, @PALLET AS PALLET,@RUTA AS RUTA,
						@UNIDAD_ID AS UNIDAD_ID,@VAL_COD_EGR AS VAL_COD_EGR,@CLIENTE_ID AS CLIENTE_ID,
						@NRO_LOTE AS NRO_LOTE,
						@NRO_SERIE_STOCK AS NRO_SERIE_STOCK,
						@NRO_CONTENEDORA AS NRO_CONTENEDORA,	
						@LOTEPROVEEDOR AS LOTE_PROVEEDOR,@NRO_PARTIDA AS NRO_PARTIDA,
						@NRO_SERIE AS NRO_SERIE
				RETURN
			END
		END --FIN TQUERY=1
	ELSE
		BEGIN
			IF @TQUERY='2'
				BEGIN
					SELECT 	TOP 1
							@VIAJEID=SP.VIAJE_ID, @PRODUCTO_ID=SP.PRODUCTO_ID, 
							@DESCRIPCION=SP.DESCRIPCION, @QTY=SUM(SP.CANTIDAD),@POSICION_COD= SP.POSICION_COD,
							@PALLET = SP.PROP1,@RUTA=SP.RUTA,@UNIDAD_ID=PROD.UNIDAD_ID, @VAL_COD_EGR=PROD.VAL_COD_EGR,
							@CLIENTE_ID = SP.CLIENTE_ID,
							@NRO_LOTE=CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,
							@NRO_SERIE_STOCK = SP.NRO_SERIE,--@NRO_LINEA= SP.NRO_LINEA, --LO AGREGUE PARA QUE ACTUALICE POR NRO_LINEA Y NO TODAS LAS TAREAS
							@NRO_CONTENEDORA =DD.NRO_BULTO,@LOTEPROVEEDOR = DD.NRO_LOTE, @NRO_PARTIDA = DD.NRO_PARTIDA,
							@NRO_SERIE = DD.NRO_SERIE
					FROM 	PICKING SP 
							LEFT JOIN POSICION POS ON(SP.POSICION_COD=POS.POSICION_COD)
							INNER JOIN PRIORIDAD_VIAJE SPV
							ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
							INNER JOIN PRODUCTO PROD
							ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
							INNER JOIN RL_SYS_CLIENTE_USUARIO SU ON(SP.CLIENTE_ID=SU.CLIENTE_ID)
							INNER JOIN DET_DOCUMENTO DD ON(SP.DOCUMENTO_ID=DD.DOCUMENTO_ID AND SP.NRO_LINEA=DD.NRO_LINEA)
							INNER JOIN CLIENTE C ON(SP.CLIENTE_ID=C.CLIENTE_ID)
							INNER JOIN CLIENTE_PARAMETROS CP ON(C.CLIENTE_ID=CP.CLIENTE_ID)
					WHERE 					
							SP.FECHA_INICIO IS NULL
							AND ((CP.FLG_ACTIVA_PC_PN='0') OR (DBO.VERIFICA_PALLET_FINAL(SP.POSICION_COD,SP.VIAJE_ID,SP.RUTA, SP.PROP1)=@PALLETCOMPLETO))
							AND	SP.FECHA_FIN IS NULL			
							AND SP.CANT_CONFIRMADA IS NULL
							AND SU.USUARIO_ID=@USUARIO
							AND SP.FLG_PALLET_HOMBRE = SP.TRANSF_TERMINADA -- Agregado Privitera Maximiliano 06/01/2010
							--AND UPPER(LTRIM(RTRIM(SP.USUARIO)))=Ltrim(Rtrim(Upper(@Usuario)))
							AND	UPPER(LTRIM(RTRIM(SP.VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJE_ID)))
							and
							SP.VIAJE_ID IN (SELECT 	VIAJE_ID
											FROM  	RL_VIAJE_USUARIO
											WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID)))													AND
													LTRIM(RTRIM(UPPER(USUARIO_ID))) =LTRIM(RTRIM(UPPER(@USUARIO))))							AND SP.SALTO_PICKING = (	SELECT 	MIN(ISNULL(SALTO_PICKING,0))
														FROM 	PICKING 
														WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJE_ID)))
																AND FECHA_INICIO IS NULL
																--AND USUARIO=SP.USUARIO
																AND FECHA_FIN IS NULL
																AND CANT_CONFIRMADA IS NULL
																AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA_I)))
													)
		
							AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA_I)))
							AND ((@CLIENTE IS NULL) OR (SP.CLIENTE_ID=@CLIENTE))
							AND
							((@VH IS NULL) OR(SP.POSICION_COD IN(	SELECT 	POSICION_COD
																	FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																			INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																			INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																	WHERE 	VEHICULO_ID=@VH
																	UNION 
																	SELECT 	NAVE_COD AS POSICION_COD
																	FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																			ON(V.NAVE_ID=N2.NAVE_ID)
																	WHERE	VEHICULO_ID=@VH)))

				GROUP BY	SP.VIAJE_ID, SP.PRODUCTO_ID,SP.DESCRIPCION, SP.RUTA,SP.POSICION_COD,SP.TIPO_CAJA,SP.PROP1,PROD.UNIDAD_ID, PROD.VAL_COD_EGR,SP.CLIENTE_ID,POS.ORDEN_PICKING,
							CP.FLG_SOLICITA_LOTE
							,CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,SP.NRO_SERIE,DD.NRO_BULTO
							,DD.NRO_LOTE, DD.NRO_PARTIDA, DD.NRO_SERIE
				ORDER BY	SP.RUTA,CAST(SP.TIPO_CAJA AS NUMERIC(10,1)) DESC,POS.ORDEN_PICKING, SP.POSICION_COD ASC, SP.PRODUCTO_ID
				

				UPDATE 	PICKING SET FECHA_INICIO = GETDATE(),USUARIO=UPPER(LTRIM(RTRIM(@USUARIO))),PALLET_PICKING=@PALLET_I,
						VEHICULO_ID=@VH	
				FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
						ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
				WHERE  	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID))) AND P.PRODUCTO_ID=@PRODUCTO_ID 
						AND P.DESCRIPCION=@DESCRIPCION AND POSICION_COD=@POSICION_COD
						AND FLG_PALLET_HOMBRE = TRANSF_TERMINADA -- Agregado Privitera Maximiliano 06/01/2010
						AND ((@PALLET IS NULL) OR(LTRIM(RTRIM(UPPER(P.PROP1))) = LTRIM(RTRIM(UPPER(@PALLET)))))
						AND ((@LOTEPROVEEDOR IS NULL OR @LOTEPROVEEDOR = '') OR (DD.NRO_LOTE=@LOTEPROVEEDOR))
						AND ((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA = '') OR (DD.NRO_PARTIDA=@NRO_PARTIDA))
						AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
						AND FECHA_INICIO IS NULL AND FECHA_FIN IS NULL AND CANT_CONFIRMADA IS NULL AND PALLET_PICKING IS NULL
						AND
						((@VH IS NULL OR @VH = '') OR(	POSICION_COD IN(	SELECT 	POSICION_COD
																FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																		INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																		INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																WHERE 	VEHICULO_ID=@VH
																UNION 
																SELECT 	NAVE_COD AS POSICION_COD
																FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																		ON(V.NAVE_ID=N2.NAVE_ID)
																WHERE	VEHICULO_ID=@VH)))
						AND (@NRO_SERIE_STOCK IS NULL OR P.NRO_SERIE = @NRO_SERIE_STOCK)
						AND (@NRO_CONTENEDORA IS NULL OR (DD.NRO_BULTO = @NRO_CONTENEDORA))
						--AND P.NRO_LINEA =@NRO_LINEA
						--Catalina Castillo.Tracker 4741
						--AND P.PICKING_ID=@PICKING_ID 


				IF @PRODUCTO_ID IS NOT NULL 
					BEGIN
						SELECT 	@VIAJEID AS VIAJE_ID,@PRODUCTO_ID AS PRODUCTO_ID, @DESCRIPCION AS DESCRIPCION, 
								@QTY AS QTY, @POSICION_COD AS POSICION_COD, @PALLET AS PALLET,@RUTA AS RUTA,
								@UNIDAD_ID AS UNIDAD_ID, @VAL_COD_EGR AS VAL_COD_EGR,@CLIENTE_ID AS CLIENTE_ID,
								@NRO_LOTE AS NRO_LOTE,
								@NRO_SERIE_STOCK AS NRO_SERIE_STOCK,	
								@NRO_CONTENEDORA AS NRO_CONTENEDORA,
								@LOTEPROVEEDOR AS LOTE_PROVEEDOR,
								@NRO_PARTIDA AS NRO_PARTIDA,
								@NRO_SERIE AS NRO_SERIE
						RETURN
					END
						
				END --FIN TQUERY=2
			ELSE 
				BEGIN
					IF @TQUERY='3'
						BEGIN
							SELECT 		TOP 1
										@VIAJEID=SP.VIAJE_ID, @PRODUCTO_ID=SP.PRODUCTO_ID, 
										@DESCRIPCION=SP.DESCRIPCION, @QTY=SUM(SP.CANTIDAD),@POSICION_COD= SP.POSICION_COD,
										@PALLET = SP.PROP1,@RUTA=SP.RUTA,@UNIDAD_ID=PROD.UNIDAD_ID, @VAL_COD_EGR=PROD.VAL_COD_EGR,
										@CLIENTE_ID = SP.CLIENTE_ID,
										@NRO_LOTE=CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,
										@NRO_SERIE_STOCK = SP.NRO_SERIE,--@NRO_LINEA= SP.NRO_LINEA, --LO AGREGUE PARA QUE ACTUALICE POR NRO_LINEA Y NO TODAS LAS TAREAS
										@NRO_CONTENEDORA =DD.NRO_BULTO
										,@LOTEPROVEEDOR = DD.NRO_LOTE,@NRO_PARTIDA=DD.NRO_PARTIDA, @NRO_SERIE = DD.NRO_SERIE
							FROM 		PICKING SP
										LEFT JOIN POSICION POS ON(SP.POSICION_COD=POS.POSICION_COD)
										INNER JOIN PRIORIDAD_VIAJE SPV
										ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
										INNER JOIN PRODUCTO PROD
										ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
										INNER JOIN RL_SYS_CLIENTE_USUARIO SU ON(SP.CLIENTE_ID=SU.CLIENTE_ID)
										INNER JOIN DET_DOCUMENTO DD ON(SP.DOCUMENTO_ID=DD.DOCUMENTO_ID AND SP.NRO_LINEA=DD.NRO_LINEA)
										INNER JOIN CLIENTE C ON(SP.CLIENTE_ID=C.CLIENTE_ID)
										INNER JOIN CLIENTE_PARAMETROS CP ON(C.CLIENTE_ID=CP.CLIENTE_ID)
							WHERE 		SP.FECHA_INICIO IS NULL
										AND ((CP.FLG_ACTIVA_PC_PN='0') OR (DBO.VERIFICA_PALLET_FINAL(SP.POSICION_COD,SP.VIAJE_ID,SP.RUTA, SP.PROP1)=@PALLETCOMPLETO))
										AND SP.FLG_PALLET_HOMBRE = SP.TRANSF_TERMINADA -- Agregado Privitera Maximiliano 06/01/2010
										AND	SP.FECHA_FIN IS NULL			
										AND	SP.USUARIO IS NULL
										AND	SP.CANT_CONFIRMADA IS NULL
										AND SU.USUARIO_ID=@USUARIO
										AND	SP.VIAJE_ID IN (SELECT 	VIAJE_ID
															FROM  	RL_VIAJE_USUARIO
															WHERE 	VIAJE_ID=SP.VIAJE_ID
																	AND
																	USUARIO_ID =@USUARIO)
										AND SP.NAVE_COD	IN(	SELECT 	NAVE_COD
															FROM 	NAVE N INNER JOIN RL_USUARIO_NAVE RLNU
																	ON(N.NAVE_ID=RLNU.NAVE_ID)
															WHERE	N.NAVE_COD=SP.NAVE_COD
																	AND RLNU.USUARIO_ID=@USUARIO
															)
										AND LTRIM(RTRIM(UPPER(SP.VIAJE_ID)))=UPPER(LTRIM(RTRIM(@VIAJE_ID)))
										AND ((@CLIENTE IS NULL) OR (SP.CLIENTE_ID=@CLIENTE))
										AND
										((@VH IS NULL) OR(SP.POSICION_COD IN(	SELECT 	POSICION_COD
																				FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																						INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																						INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																				WHERE 	VEHICULO_ID=@VH
																				UNION 
																				SELECT 	NAVE_COD AS POSICION_COD
																				FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																						ON(V.NAVE_ID=N2.NAVE_ID)
																				WHERE	VEHICULO_ID=@VH)))

							GROUP BY	SP.VIAJE_ID, SP.PRODUCTO_ID, SP.DESCRIPCION, SP.RUTA, SP.POSICION_COD, SP.TIPO_CAJA, SP.PROP1,PROD.UNIDAD_ID, PROD.VAL_COD_EGR, SP.CLIENTE_ID,POS.ORDEN_PICKING,
										CP.FLG_SOLICITA_LOTE, --DD.PROP2
										CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END,SP.NRO_SERIE,DD.NRO_BULTO
										,DD.NRO_LOTE,DD.NRO_PARTIDA,DD.NRO_SERIE
							ORDER BY	SP.RUTA,CAST(SP.TIPO_CAJA AS NUMERIC(10,1)) DESC,POS.ORDEN_PICKING, SP.POSICION_COD ASC, SP.PRODUCTO_ID
							if @tomaruta='1'
							begin
							DECLARE T_RUTA CURSOR FOR
								SELECT 	PICKING_ID
								FROM	PICKING P
								WHERE	P.NAVE_COD IN(	SELECT 	NAVE_COD
														FROM 	NAVE N INNER JOIN RL_USUARIO_NAVE RLNU
																ON(N.NAVE_ID=RLNU.NAVE_ID)
														WHERE	N.NAVE_COD=P.NAVE_COD
																AND RLNU.USUARIO_ID=@USUARIO)
																AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
																AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
																AND ((@PALLETCOMP='0') OR (DBO.VERIFICA_PALLET_FINAL(P.POSICION_COD,P.VIAJE_ID,P.RUTA, P.PROP1)=@PALLETCOMPLETO))
																AND	((@VH IS NULL) OR(	POSICION_COD IN(SELECT 	POSICION_COD
																										FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																												INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																												INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																										WHERE 	VEHICULO_ID=@VH
																										UNION 
																										SELECT 	NAVE_COD AS POSICION_COD
																										FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																												ON(V.NAVE_ID=N2.NAVE_ID)
																										WHERE	VEHICULO_ID=@VH)))
				
							OPEN T_RUTA
							FETCH NEXT FROM T_RUTA INTO @PICKING_ID
							WHILE @@FETCH_STATUS=0 
								BEGIN
								If 0=0 
									Begin
										UPDATE PICKING SET USUARIO =@USUARIO 
										WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
												AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
												AND PICKING_ID=@PICKING_ID
												AND
												((@VH IS NULL) OR(	POSICION_COD IN(	SELECT 	POSICION_COD
																						FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																								INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																								INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																						WHERE 	VEHICULO_ID=@VH
																						UNION 
																						SELECT 	NAVE_COD AS POSICION_COD
																						FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																								ON(V.NAVE_ID=N2.NAVE_ID)
																						WHERE	VEHICULO_ID=@VH)))

										FETCH NEXT FROM T_RUTA INTO @PICKING_ID
									End
								END
				
								CLOSE T_RUTA
								DEALLOCATE T_RUTA
							END
							UPDATE 	PICKING SET FECHA_INICIO = GETDATE(),USUARIO=UPPER(LTRIM(RTRIM(@USUARIO))),PALLET_PICKING=@PALLET_I,
									VEHICULO_ID=@VH	
							FROM	PICKING P INNER JOIN DET_DOCUMENTO DD
									ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
							WHERE  	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
									AND FLG_PALLET_HOMBRE = TRANSF_TERMINADA -- Agregado Privitera Maximiliano 06/01/2010
									AND P.PRODUCTO_ID=@PRODUCTO_ID 
									AND P.DESCRIPCION=@DESCRIPCION 
									AND POSICION_COD=@POSICION_COD
									AND ((@LOTEPROVEEDOR IS NULL OR @LOTEPROVEEDOR = '') OR (DD.NRO_LOTE=@LOTEPROVEEDOR))
									AND ((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA = '') OR (DD.NRO_PARTIDA=@NRO_PARTIDA))
									AND ((@PALLET IS NULL) OR(LTRIM(RTRIM(UPPER(P.PROP1))) = LTRIM(RTRIM(UPPER(@PALLET)))))
									AND LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))
									AND
									((@VH IS NULL OR @VH='') OR(	POSICION_COD IN(	SELECT 	POSICION_COD
																			FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
																					INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
																					INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
																			WHERE 	VEHICULO_ID=@VH
																			UNION 
																			SELECT 	NAVE_COD AS POSICION_COD
																			FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
																					ON(V.NAVE_ID=N2.NAVE_ID)
																			WHERE	VEHICULO_ID=@VH)))
									AND (@NRO_SERIE_STOCK IS NULL OR P.NRO_SERIE = @NRO_SERIE_STOCK)
									AND (@NRO_CONTENEDORA IS NULL OR DD.NRO_BULTO = @NRO_CONTENEDORA)
									--AND P.PICKING_ID =@PICKING_ID


							IF @PRODUCTO_ID IS NOT NULL 
							BEGIN
								SELECT 	@VIAJEID AS VIAJE_ID,@PRODUCTO_ID AS PRODUCTO_ID, @DESCRIPCION AS DESCRIPCION, 
										@QTY AS QTY, @POSICION_COD AS POSICION_COD, @PALLET AS PALLET,@RUTA AS RUTA,
										@UNIDAD_ID AS UNIDAD_ID, @VAL_COD_EGR AS VAL_COD_EGR,@CLIENTE_ID AS CLIENTE_ID,
										@NRO_LOTE AS NRO_LOTE,
										@NRO_SERIE_STOCK AS NRO_SERIE_STOCK,	
										@NRO_CONTENEDORA AS NRO_CONTENEDORA
										,@LOTEPROVEEDOR AS LOTE_PROVEEDOR
										,@NRO_PARTIDA AS NRO_PARTIDA
										,@NRO_SERIE AS NRO_SERIE										
					END
				END
			END 
		END--END ELSE
	
END-- FIN PROCEDURE
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

ALTER  PROCEDURE [dbo].[test_DEFAULT]
@deposito_default varchar(30) OUTPUT
---sirve para averiguar el deposito default ya que no se puede acceder a tablas temporales
---desde una funcion


AS

set @deposito_default=(SELECT top 1    USUARIO_ID
FROM         SYS_USUARIO)
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

ALTER   Procedure [dbo].[Test_VerificaExistencias]
As
Begin
	
	Declare @Qty 	as Float
	
	Create TABLE #temp_existencia (
	clienteid         	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	productoid        	VARCHAR(30)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cantidad          	NUMERIC(20,5) 	NULL,
	nro_serie         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_lote          	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	fecha_vencimiento DATETIME      	NULL,
	nro_despacho     VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_bulto         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_partida       	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	peso              		NUMERIC(20,5) 	NULL,
	volumen           	NUMERIC(20,5) 	NULL,
	tie_in            		CHAR(1)        	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	STORAGE           	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	naveid            	NUMERIC(20,0) 	NULL,
	callecod          	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	calleid           		NUMERIC(20,0) 	NULL,
	columnacod        	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	columnaid         	NUMERIC(20,0) 	NULL,
	nivelcod          	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nivelid           		NUMERIC(20,0) 	NULL,
	categlogid        	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop1             	VARCHAR(100)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop2             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop3             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_id         	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_peso       	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_volumen    VARCHAR(5)     COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	est_merc_id       	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	moneda_id         	VARCHAR(20)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	costo             		NUMERIC(10,3) 	NULL 
	)

	CREATE TABLE #temp_existencia_doc (
	clienteid         	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	productoid        	VARCHAR(30)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cantidad          	NUMERIC(20,5) 	NULL,
	nro_serie         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_lote          	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	fecha_vencimiento DATETIME      	NULL,
	nro_despacho     VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_bulto         	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	nro_partida       	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	peso              		NUMERIC(20,5) 	NULL,
	volumen           	NUMERIC(20,5) 	NULL,
	tie_in            		CHAR(1)        	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cantidad_disp     	NUMERIC(20,5) 	NULL,
	code              		CHAR(1)        	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	description       	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cat_log_id        	VARCHAR(50)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop1             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop2             	VARCHAR(100)   	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	prop3             	VARCHAR(100)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_id         	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_peso       	VARCHAR(5)     	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	unidad_volumen    VARCHAR(5)     COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	est_merc_id       	VARCHAR(15)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	moneda_id         	VARCHAR(20)    	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	costo             		NUMERIC(10,3) 	NULL,
	orden             	NUMERIC(20,0) 	NULL
	)

	CREATE TABLE #temp_rl_existencia_doc (
	rl_id 			NUMERIC(20,5) NULL
	)
	
	Exec  Funciones_Frontera_Api#VerificaExistencias
			@xCliente_id	='10202',
			@xProducto_id	='10347',
			@xCantidad		= @QTY Output

	Select @QTY as cantidad

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

ALTER  procedure [dbo].[TestCursorDin]
As
Begin
	Declare @xSQL as nvarchar(4000)
	Declare @Cliente_id as varchar(15)
	Declare @Producto_id as varchar(30)

	DECLARE @my_cur CURSOR
    EXEC sp_executesql
          N'SET @my_cur = CURSOR FOR SELECT Cliente_id,Producto_id FROM producto; OPEN @my_cur',
          N'@my_cur cursor OUTPUT', @my_cur OUTPUT

    FETCH NEXT FROM @my_cur into @Cliente_id,@Producto_id
	While @@Fetch_Status=0
	Begin
		Select @Cliente_id as Cliente, @Producto_id as Producto
	    FETCH NEXT FROM @my_cur into @Cliente_id,@Producto_id
	End
	close @my_Cur
	Deallocate @My_cur
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

ALTER PROCEDURE [dbo].[TOLERANCIA] --Control de tolerancia de productos Minima y Maxima
	@CLIENTE_ID		VARCHAR(15),
	@OC				VARCHAR(100),
	@PRODUCTO_ID	VARCHAR(30),
	@TolMax			Float output,
	@TolMin			Float output	
AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
	
	DECLARE @ToleranciaMax		Float
	DECLARE @ToleranciaMin		Float
	DECLARE @DOC_EXT			VARCHAR(100)
	DECLARE @SUCURSAL_ORIGEN	VARCHAR(20)
	DECLARE @qtyBO				Float
	
	SELECT @ToleranciaMax=isnull(TOLERANCIA_MAX,0), @ToleranciaMin=isnull(TOLERANCIA_MIN,0) from producto where cliente_id=@cliente_id and producto_id=@producto_id
	
	SELECT 	TOP 1
				@DOC_EXT=SD.DOC_EXT,@SUCURSAL_ORIGEN=AGENTE_ID
		FROM 	SYS_INT_DOCUMENTO SD INNER JOIN SYS_INT_DET_DOCUMENTO SDD  ON(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)
		WHERE 	ORDEN_DE_COMPRA=@OC
				AND PRODUCTO_ID=@PRODUCTO_ID
				AND SD.CLIENTE_ID=@CLIENTE_ID
				and SDD.fecha_estado_gt is null
				and SDD.estado_gt is null
				
	Select 	@qtyBO=sum(cantidad_solicitada)
		from	sys_int_det_documento
		where	doc_ext=@doc_ext
				and fecha_estado_gt is null
				and estado_gt is null
				
	set @ToleranciaMax= @qtyBO + ((@qtyBO * @ToleranciaMax)/100)
	set @ToleranciaMin= @qtyBO - ((@qtyBO * @ToleranciaMin)/100)
	
	set @TolMax = @ToleranciaMax
	set @TolMin	= @ToleranciaMin

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

ALTER PROCEDURE [dbo].[TOMA_VH]
@VEHICULO_ID	VARCHAR(50)
AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
	
	DECLARE @USUARIO	VARCHAR(20)
	DECLARE @COUNT	INT
	
	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	
	SELECT 	@COUNT=COUNT(*)
	FROM	RL_USUARIO_VEHICULO
	WHERE	VEHICULO_ID=@VEHICULO_ID

	IF @COUNT>0
	BEGIN
		-- si es mayor a 0 es porque esta tomado por alguien
		DELETE FROM RL_USUARIO_VEHICULO WHERE VEHICULO_ID=@VEHICULO_ID
	END
	DELETE FROM RL_USUARIO_VEHICULO WHERE USUARIO_ID=@USUARIO
	INSERT INTO RL_USUARIO_VEHICULO VALUES (@USUARIO, @VEHICULO_ID, GETDATE())

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

ALTER         PROCEDURE [dbo].[TRD_ACT_NRO_LINEA_PALLET]
	@Doc_Trans_Id	as Numeric(20,0) output,
	@PalletD		as Varchar(100)
As
Begin
	Declare @Doc_id 		as Numeric(20,0)
	Declare @PalletOrigen	as Varchar(100)
	Declare @PosCodDest		as Varchar(45)
	Declare @Pallet_E		as Varchar(45)
	Declare @NroLinea		as numeric(10,0)

	SELECT 	@Doc_id=DD.Documento_id,@PosCodDest=p.posicion_cod,@Pallet_E=dd.Prop1,
			@NroLinea=dd.nro_linea
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

	Update 	Det_Documento set Prop1=Ltrim(Rtrim(Upper(@PalletD)))
	where	Documento_id=@Doc_id 
			and nro_linea=@NroLinea

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

ALTER    PROCEDURE [dbo].[TRD_ACT_PALLET]
	@Doc_Trans_Id	as Numeric(20,0) output
As
Begin
		

	EXEC actualiza_pos_picking_desk @Doc_Trans_Id
	if @@error<>0
	Begin
		raiserror('Fallo al ejecutar actualiza_pos_picking_desk Sp.',16,1)
		Return(99)
	End
	Declare @Doc_id 		as Numeric(20,0)
	Declare @PalletOrigen	as Varchar(100)
	Declare @PosCodDest		as Varchar(45)
	Declare @Pallet_E		as Varchar(45)

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

ALTER       PROCEDURE [dbo].[TRD_GET_PALLETS_BY_POS]
@POSICION AS VARCHAR(45),
@PRODUCTO AS VARCHAR(30)
AS

BEGIN
	
	DECLARE @EXISTE 	AS INT

	SELECT 	@EXISTE=COUNT(POSICION_ID)
	FROM 	POSICION
	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION)))

	IF @EXISTE=0
		BEGIN
			SELECT 	@EXISTE=COUNT(NAVE_ID)
			FROM 	NAVE
			WHERE	NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION)))

			IF @EXISTE=0
				BEGIN
					RAISERROR('La ubicacion es inexistente',16,1)
					Return
				END
		END	

	SELECT DISTINCT X.*
	FROM(
			SELECT 	DD.PROP1
			FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	P.POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION)))
					--AND DD.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO)))
			
			UNION ALL
	
			SELECT 	DD.PROP1
			FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
			WHERE	N.NAVE_COD=LTRIM(RTRIM(UPPER(@POSICION)))
					--AND DD.PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO)))
	) AS X				
	ORDER BY PROP1

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

ALTER         PROCEDURE [dbo].[TRD_GetProdByPallet]
@Pallet as varchar(100)
As
Begin
	SELECT	 DISTINCT
			 DD.PRODUCTO_ID 					
			,PROD.DESCRIPCION					
			,PROD.UNIDAD_ID						
			,SUM(CAST(RL.CANTIDAD AS INT))		AS QTY
			,DD.NRO_LOTE						
			,ISNULL(P.POSICION_COD,N.NAVE_COD)	AS UBICACION
	FROM	RL_DET_DOC_TRANS_POSICION RL INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			INNER JOIN PRODUCTO PROD
			ON(DD.PRODUCTO_ID=PROD.PRODUCTO_ID AND DD.CLIENTE_ID=PROD.CLIENTE_ID)
			LEFT JOIN POSICION P
			ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			LEFT JOIN NAVE N
			ON(RL.NAVE_ACTUAL=N.NAVE_ID)
			INNER JOIN DOCUMENTO D
			ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
	WHERE	DD.PROP1=Ltrim(Rtrim(Upper(@Pallet)))
			AND D.STATUS='D40'
	GROUP 
	BY 		DD.PRODUCTO_ID,PROD.DESCRIPCION,P.POSICION_COD,NAVE_COD,DD.NRO_LOTE,PROD.UNIDAD_ID,DD.PROP1,DD.NRO_LINEA

	IF @@ROWCOUNT=0
		BEGIN
			RAISERROR('No hay registros para el pallet ingresado.',16,1)
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

ALTER procedure [dbo].[Val_Prod]
@Cliente	varchar(20)=null,
@Codigo		varchar(50)=null,
@ProductoID	varchar(30) Output
As
Begin
	Declare @Count		SmallInt

	Select	@Count=Count(*)
	from	Producto
	Where	Cliente_ID=@Cliente and Producto_Id=@Codigo
	
	If @Count=1
	Begin
		Set @ProductoID=@Codigo
	End
	If @Count=0
	Begin
		Set @Count=Null
		Select	@Count=Count(*)
		From	Rl_Producto_Codigos
		Where	Cliente_id=@Cliente and Codigo=@Codigo

		If @Count=1
		Begin
			Select	@ProductoID=Producto_id
			From	rl_producto_codigos
			Where	Cliente_id=@Cliente and codigo=@Codigo
		End
		If @Count=0
		Begin
			raiserror('No Existe el producto para el codigo %s, cliente %s',16,1,@codigo,@cliente)
			Return
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