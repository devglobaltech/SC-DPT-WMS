
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 05:24 p.m.
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

ALTER      PROCEDURE [dbo].[MobBuscarPosicion]
@POSICION_COD AS VARCHAR(45)
AS

declare @Mensaje  VARCHAR(200)
/*
SELECT     POSICION_ID
FROM         POSICION
WHERE     (POSICION_COD = @POSICION_COD)*/
	SELECT X.*
	FROM
	(
		SELECT     POSICION_ID,'POS' AS TIPO
		FROM       POSICION
		WHERE     (POSICION_COD = @POSICION_COD)
		UNION ALL
		SELECT     NAVE_ID, 'NAVE' AS TIPO
		FROM       NAVE
		WHERE     (NAVE_COD = @POSICION_COD)
	) AS X

IF @@ROWCOUNT =0
BEGIN


	----RAISERROR ('La posición ingresada no es una ubicación válida', 16, 1)

	set @Mensaje= 'La posición:  ' + @POSICION_COD + ' no es una ubicación válida'
	RAISERROR (@Mensaje, 16, 1)
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

ALTER Procedure [dbo].[OrderByNroLinea]
@Documento_Id	Varchar(30)
As
Begin
	Declare @tCur		Cursor
	Declare @Nro_Linea	Numeric(10,0)
	Declare @vNro_Linea	Numeric(10,0)

	Set @tCur = Cursor for
		SELECT 	nro_linea 
		FROM 	det_documento_aux
		WHERE 	documento_id=@Documento_Id
		ORDER BY 
				nro_linea		

	Open 	@tCur
	Set 	@vNro_Linea=0 

	Fetch Next From @tCur Into 	@Nro_Linea
	While @@Fetch_Status=0
	Begin
		Set @vNro_Linea=@vNro_Linea +1
		
		Update Consumo_locator_Egr 	Set Nro_linea=@vNro_Linea Where	Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea
		Update det_documento_aux 	Set Nro_linea=@vNro_Linea Where	Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea

		Fetch Next From @tCur Into 	@Nro_Linea
	End

	Close @tCur
	Deallocate @tCur

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

ALTER PROCEDURE [dbo].[PARAMETROS_AUDITORIA_UPDATE]
@PARAM_ID	NUMERIC(38) OUTPUT,
@VALOR		CHAR(1) OUTPUT
AS
BEGIN
	DECLARE @CONTROL 	AS CHAR(1)
	DECLARE @USUARIO_ID AS VARCHAR(20)

	SELECT @CONTROL= AUDITABLE FROM PARAMETROS_AUDITORIA WHERE TIPO_AUDITORIA_ID=@PARAM_ID;
	
	IF @VALOR<>@CONTROL
	BEGIN
		SELECT @USUARIO_ID=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN;
	
		UPDATE PARAMETROS_AUDITORIA SET AUDITABLE=@VALOR WHERE TIPO_AUDITORIA_ID=@PARAM_ID;
	
		INSERT INTO SYS_AUDITORIA_PARAMETROS (TIPO_AUDITORIA_ID, AUDITABLE, USUARIO_ID, TERMINAL, FECHA)
		VALUES (@PARAM_ID, @VALOR, @USUARIO_ID, HOST_NAME(), GETDATE())
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