
GO

/*
Script created by Quest Change Director for SQL Server at 17/12/2012 01:29 p.m.
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

ALTER     PROCEDURE [dbo].[Ajuste_Propiedades]
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
@pfecha_vencimiento	varchar(100)  output,
@pnro_despacho	varchar(100)  output,
@pnro_partida		varchar(100)  output,
@punidad_id		varchar(100)  output,
@pnave_id		varchar(100)  output,
@pnave_cod		varchar(100)  output,
@pNew_Value		varchar(100)  output,
@pLoteP		varchar(100)  output,
@pBulto 		varchar(50)    output,
@pProp3		varchar(100)  output,
@pNRO_TIE_IN		varchar(100)  output,
@pNRO_TIE_IN_PADRE		varchar(100)  output

AS

BEGIN
	DECLARE @DocId_PreFix	AS VARCHAR(20)


	If @pTipo_Documento = 'ST04'
		Begin
			Set @DocId_PreFix= 'AJ_F_VTO'
		End
	If @pTipo_Documento = 'ST05'
		Begin
			Set @DocId_PreFix= 'AJ_LOTE_PRO'
		End
	If @pTipo_Documento = 'ST06'
		Begin
			Set @DocId_PreFix= 'AJ_PALLET'
		End
	If @pTipo_Documento = 'ST07'
		Begin
			Set @DocId_PreFix= 'AJ_BULTO'
		End
	If @pTipo_Documento = 'ST08'
		Begin
			Set @DocId_PreFix= 'AJ_LOTE_INT'
		End
	If @pTipo_Documento = 'ST09'
		Begin
			Set @DocId_PreFix= 'AJ_PROP3'
		End
	If @pTipo_Documento = 'ST10'
		Begin
			Set @DocId_PreFix= 'AJ_NROTIEIN'
		End
	If @pTipo_Documento = 'ST11'
		Begin
			Set @DocId_PreFix= 'AJ_NROTIEINPADRE'
		End

	Insert Into sys_dev_documento (
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
		getdate(),
		@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
		'P' , --Estado_GT
		getdate(),
		Null -- Flg_movimiento
		 )

	IF @@ERROR <> 0 BEGIN
		RAISERROR('Error al Registrar el Cambio de Categoria Logica: CABECERA',16,1)
		RETURN --PARA QUENO SIGA EJECUTANDO CODIGO
	END

---------------NEGATIVO---------------------------------------
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
		,prop2
		,nro_bulto
		,prop3
		,Flg_Movimiento)
	values(
		@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
		1, --nro_linea
		@pCliente_id,
		@pproducto_id,
		(@pcantidad * -1),
		(@pcantidad * -1),
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
		@pLoteP,
		@pBulto,
		@pProp3,
		Null)	

	IF @@ERROR <> 0 BEGIN
		RAISERROR('Error al Registrar el Cambio de Categoria Logica: NEGATIVO',16,1)
		RETURN
	END

---------------POSITIVO---------------------------------------
	If @pTipo_Documento = 'ST04'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)

			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
				@pCliente_id,
				@pproducto_id,
				@pcantidad,
				@pcantidad,
				@pest_merc_id,
				@pcat_log_id,
				@pdescripcion,
				@pnro_lote,
				@pnro_pallet,
				@pNew_Value,
				@pnro_despacho,
				@pnro_partida,
				@punidad_id,
				@pTipo_Documento,		
				'P', --estado_gt
				getdate(), --fecha_estado_gt
				dbo.Aj_NaveCod_to_Nave_id(@pnave_cod),
				@pnave_cod,
				@pLoteP,
				@pBulto,
				@pProp3,
				Null
				)
		End
	If @pTipo_Documento = 'ST05'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)

			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
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
				@pNew_Value,
				@pBulto,
				@pProp3,
				Null
				)
		End
	If @pTipo_Documento = 'ST06'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)

			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
				@pCliente_id,
				@pproducto_id,
				@pcantidad,
				@pcantidad,
				@pest_merc_id,
				@pcat_log_id,
				@pdescripcion,
				@pnro_lote,
				@pNew_Value,
				@pfecha_vencimiento,
				@pnro_despacho,
				@pnro_partida,
				@punidad_id,
				@pTipo_Documento,		
				'P', --estado_gt
				getdate(), --fecha_estado_gt
				dbo.Aj_NaveCod_to_Nave_id(@pnave_cod),
				@pnave_cod,
				@pLoteP,
				@pBulto,
				@pProp3,
				Null
				)
		End
	If @pTipo_Documento = 'ST07'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)

			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
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
				@pLoteP,
				@pNew_Value,
				@pProp3,
				Null)
		End
	If @pTipo_Documento = 'ST08'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)

			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
				@pCliente_id,
				@pproducto_id,
				@pcantidad,
				@pcantidad,
				@pest_merc_id,
				@pcat_log_id,
				@pdescripcion,
				@pNew_Value,
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
				@pLoteP,
				@pBulto,
				@pProp3,
				Null)
		End
	If @pTipo_Documento = 'ST09'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)
			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
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
				@pLoteP,
				@pBulto,
				@pNew_Value,
				Null)
		End
	If @pTipo_Documento = 'ST10'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)
			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
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
				@pLoteP,
				@pBulto,
				@pProp3,
				Null)
		End
	If @pTipo_Documento = 'ST11'
		Begin
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
				,prop2
				,nro_bulto
				,prop3
				,Flg_Movimiento)
			values(
				@DocId_PreFix + CAST(@pDoc_ext AS varchar(100)),
				2, --nro_linea
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
				@pLoteP,
				@pBulto,
				@pProp3,
				Null)
		End



	IF @@ERROR <> 0 BEGIN
		RAISERROR('Error al Registrar el Cambio de Categoria Logica: POSITIVO',16,1)
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