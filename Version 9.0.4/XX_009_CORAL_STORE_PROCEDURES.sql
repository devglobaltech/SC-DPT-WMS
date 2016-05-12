
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 04:01 p.m.
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

-- =============================================
-- Author:		LRojas
-- Create date: 19/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[eliminar_caja_contenedora_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(30) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    DECLARE @PRODUCTO_ID as varchar(30),
            @CANT_CONTROLADA as numeric(20,5),
			@NRO_LOTE AS VARCHAR(100),
			@NRO_PARTIDA AS VARCHAR(100),
			@NRO_SERIE AS VARCHAR(50)
	
	DECLARE cur_eliminador CURSOR FOR
    SELECT P.PRODUCTO_ID, ISNULL(P.NRO_LOTE,''), ISNULL(P.NRO_PARTIDA,''), ISNULL(P.NRO_SERIE,''), P.CANT_CONFIRMADA
    FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
    WHERE D.CLIENTE_ID = @CLIENTE_ID AND D.NRO_REMITO = @PEDIDO_ID AND P.PALLET_PICKING = @NRO_CONTENEDORA
    AND P.PALLET_CONTROLADO='1'
    
    OPEN cur_eliminador
    FETCH cur_eliminador 
    INTO @PRODUCTO_ID, @NRO_LOTE, @NRO_PARTIDA, @NRO_SERIE, @CANT_CONTROLADA
    
    WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC quitar_producto_empaque @CLIENTE_ID, @PEDIDO_ID, @NRO_LOTE, @NRO_PARTIDA, @NRO_SERIE, @PRODUCTO_ID, @NRO_CONTENEDORA, @CANT_CONTROLADA
            
            FETCH cur_eliminador INTO @PRODUCTO_ID, @NRO_LOTE, @NRO_PARTIDA, @NRO_SERIE, @CANT_CONTROLADA
        END
    CLOSE cur_eliminador
    DEALLOCATE cur_eliminador
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