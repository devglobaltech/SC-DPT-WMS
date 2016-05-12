
GO

/*
Script created by Quest Change Director for SQL Server at 17/12/2012 01:32 p.m.
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
SET QUOTED_IDENTIFIER OFF
GO

ALTER        PROCEDURE [dbo].[Aux_det_doc_insert]
@documento_id			numeric(20,0)  output,
@nro_linea				numeric(10,0)  output,	
@motivo_id				varchar(15)  output,
@usuario					varchar(20)  output,
@terminal				varchar(20)  output,
@observacion			varchar(100)  output,
@documento_id_orig	numeric(20,0)  output,
@nro_linea_orig		numeric(10,0)  output	
AS
BEGIN

	Declare @QtyOriginal		as Float
	Declare @QtyNueva		as Float

	Select 	@QtyOriginal=Sum(Cant_Confirmada)
	From	vPicking
	Where	Documento_id=@documento_id_orig
			And nro_linea=@nro_linea_orig
	
	Select 	@QtyNueva=Cantidad
	from	det_documento
	where	documento_id=@documento_id
			and nro_linea=@nro_linea



	If @QtyNueva>@QtyOriginal
	Begin
		raiserror('Se esta tratando de ingresar mas de lo que egreso.',16,1)
		return
	End

     insert into aux_det_documento values(@documento_id,
					  @nro_linea,
					  @motivo_id,
					  getdate(),
					  @usuario,
					  @terminal,
					  @observacion,
					  @documento_id_orig,
					  @nro_linea_orig)
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