
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 05:19 p.m.
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

ALTER    PROCEDURE [dbo].[GETDATAFORPALLETtest]
@NROPALLET AS VARCHAR(100),
@USUARIO AS VARCHAR(20)
AS
DECLARE @DOCUMENTO_ID AS NUMERIC(20,0)
DECLARE @NRO_LINEA   AS NUMERIC(10,0)
DECLARE @TRANSACCION_ID   AS VARCHAR(15)
DECLARE @TIPO_OPERACION_ID AS VARCHAR(5)
DECLARE @DOC_TRANS_ID AS NUMERIC(20,0)
DECLARE @VCANT AS NUMERIC(20)
DECLARE @ESTACION AS VARCHAR(15)

	BEGIN
		SELECT @DOCUMENTO_ID=X.DOCUMENTO_ID,@NRO_LINEA=NRO_LINEA, @DOC_TRANS_ID=DOC_TRANS_ID
		FROM(
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN NAVE N
					ON(RL.NAVE_ACTUAL = N.NAVE_ID)
			WHERE 	DD.PROP1=@NROPALLET AND D.STATUS='D30'
					AND N.PRE_INGRESO='1'
		
			UNION ALL
		
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE 	DD.PROP1=@NROPALLET AND D.STATUS='D30'
					AND	P.INTERMEDIA='1'
		
			UNION ALL
		
			SELECT 
					DD.PROP1,DD.DOCUMENTO_ID,DD.NRO_LINEA,RL.POSICION_ACTUAL,RL.NAVE_ACTUAL, DDT.DOC_TRANS_ID
			FROM 	DET_DOCUMENTO DD INNER JOIN DOCUMENTO D
					ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA =DDT.NRO_LINEA_DOC)
					INNER JOIN RL_DET_DOC_TRANS_POSICION RL
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
					INNER JOIN NAVE N
					ON(N.NAVE_ID=RL.NAVE_ACTUAL)
			WHERE 	DD.PROP1=@NROPALLET AND D.STATUS='D30'
					AND	N.INTERMEDIA='1'
		) AS X

	END
---Ac  validamos que el usuario tenga permiso para tomar el pallet

	IF @@ROWCOUNT =0
		BEGIN
			RAISERROR ('EL PALLET SOLICITADO NO ESTA DISPONIBLE.', 16, 1)
		END

	ELSE
	BEGIN

	SELECT 	@TRANSACCION_ID=DT.TRANSACCION_ID,@TIPO_OPERACION_ID=DT.TIPO_OPERACION_ID,@ESTACION=DT.ESTACION_ACTUAL
	FROM 	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID= DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN DOCUMENTO_TRANSACCION DT
			ON(DDT.DOC_TRANS_ID=DT.DOC_TRANS_ID)
	WHERE 	DD.DOCUMENTO_ID=@DOCUMENTO_ID 
			AND DD.NRO_LINEA=@NRO_LINEA
	
					
	
	SELECT
	       @VCANT=COUNT(D.DOCUMENTO_ID)
	
	FROM 	DET_DOCUMENTO_TRANSACCION DDT
	      	INNER JOIN DOCUMENTO_TRANSACCION DT ON (DT.DOC_TRANS_ID=DDT.DOC_TRANS_ID)
	      	INNER JOIN DET_DOCUMENTO         DD ON DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA
	      	LEFT JOIN DOCUMENTO               D ON DDT.DOCUMENTO_ID=D.DOCUMENTO_ID
	      	INNER JOIN TRANSACCION              TR ON (TR.TRANSACCION_ID = DT.TRANSACCION_ID)
	      	INNER JOIN RL_TRANSACCION_ESTACION  R1 ON (DT.ESTACION_ACTUAL = R1.ESTACION_ID AND DT.TRANSACCION_ID = R1.TRANSACCION_ID)
	WHERE 	1 <> 0
			AND DT.ESTACION_ACTUAL = @ESTACION --'PRUEBA_1'
			AND DT.TIPO_OPERACION_ID = @TIPO_OPERACION_ID--'ING'
			AND DT.TRANSACCION_ID=@TRANSACCION_ID --'TING_MONO'
			AND DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.NRO_LINEA=@NRO_LINEA
		    AND DDT.CLIENTE_ID IN (
								SELECT CLIENTE_ID 	
								FROM CLIENTE
	                            WHERE
									(SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END
										FROM   rl_sys_cliente_usuario
										WHERE  cliente_id = dd.cliente_id
										And    usuario_id = @USUARIO) = 1)


		IF @VCANT=0 --@@ROWCOUNT =0
		BEGIN
			RAISERROR ('NO TIENE PERMISOS SOBRE EL PALLET SELECCIONADO', 16, 1)
		END
		ELSE
		BEGIN 
		SELECT @DOCUMENTO_ID AS DOCUMENTO_ID, @NRO_LINEA AS NRO_LINEA --FROM X
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

ALTER  PROCEDURE [dbo].[GetPalletByPos]
@Posicion 	as varchar(45),
@PalletOut	as varchar(100) Output
As
Begin

	select 	@PalletOut=dd.prop1
	from	rl_det_doc_trans_posicion rl 
			inner join det_documento_transaccion ddt
			on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			left join nave n
			on(n.nave_id=rl.nave_actual)
			left join posicion p
			on(p.posicion_id=rl.posicion_actual)
			inner join det_documento dd
			on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
	where	p.posicion_cod=Ltrim(Rtrim(Upper(@Posicion)))
			or n.nave_cod=Ltrim(Rtrim(Upper(@Posicion)))


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

ALTER Procedure [dbo].[GetTipoDoc_Sucursal]
	@Cliente_id		as Varchar(15) output,
	@Suc_id		as Varchar(20) output,
	@vTipoDOC		as Varchar(50) output
as
Begin

	Select	@vTipoDOC = tipo_documento_id_F 
	From 	sucursal 
	Where 	cliente_id = @Cliente_id 
			and ltrim(rtrim(upper(Sucursal_id))) =  ltrim(rtrim(upper(@Suc_id)))

	Select @vTipoDOC

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

ALTER PROCEDURE [dbo].[GetValuesForFTP]
AS
BEGIN
	SELECT 	PARAMETRO_ID, VALOR 
	FROM 	SYS_PARAMETRO_PROCESO 
	WHERE 	PROCESO_ID='WARP' AND SUBPROCESO_ID='FTP'
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
  
 DECLARE @DOC_ID    NUMERIC(20,0)  
 DECLARE @DOC_TRANS_ID  NUMERIC(20,0)  
 DECLARE @DOC_EXT   VARCHAR(100)  
 DECLARE @SUCURSAL_ORIGEN VARCHAR(20)  
 DECLARE @CAT_LOG_ID   VARCHAR(50)  
 DECLARE @DESCRIPCION  VARCHAR(30)  
 DECLARE @UNIDAD_ID   VARCHAR(15)  
 DECLARE @NRO_PARTIDA  VARCHAR(100)  
 DECLARE @LOTE_AT   VARCHAR(50)  
 DECLARE @Preing    VARCHAR(45)  
 DECLARE @CatLogId   Varchar(50)  
 DECLARE @LineBO    Float  
 DECLARE @qtyBO    Float  
 DECLARE @ToleranciaMax  Float  
 DECLARE @QtyIngresada  Float  
 DECLARE @tmax    Float  
 DECLARE @MAXP    VARCHAR(50)  
 DECLARE @NROLINEA   INTEGER  
 DECLARE @cantidad   numeric(20,5)  
 DECLARE @fecha    datetime   
 DECLARE @PRODUCTO_ID  VARCHAR(30)  
 DECLARE @PALLET_AUTOMATICO VARCHAR(1)  
 declare @lote    VARCHAR(1)  
 DECLARE @NRO_PALLET   VARCHAR(100)  
 -- Catalina Castillo.25/01/2012.Se agrega variable para saber si tiene registros de contenedoras, el producto   
 DECLARE @NRO_REG_CONTENEDORAS INTEGER  
 DECLARE @NROBULTO   INTEGER  
 DECLARE @NRO_LINEA_CONT  INTEGER  
        declare @cpte_prefijo varchar(10)  
        declare @cpte_numero varchar(20)  
 -- LRojas TrackerID 3851 29/03/2012: Control, si el producto genera Back Order se crea un nuevo ingreso, de lo contrario no  
 DECLARE @GENERA_BO          VARCHAR(1)  
  DECLARE @NRO_LOTE   VARCHAR(100)  
 DECLARE @INGLOTEPROVEEDOR VARCHAR(1)  
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
    
  /*select @qtyBO=sum(sdd.cantidad_solicitada)  
  from sys_int_documento sd  
  inner join sys_int_det_documento sdd on(sd.cliente_id=sdd.cliente_id and sd.doc_ext=sdd.doc_ext)  
  where sd.orden_de_compra=@OC and sdd.producto_id=@producto_id  
  and sd.fecha_estado_gt is null  
  and sd.estado_gt is null*/  
  
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
      
  UPDATE SYS_INT_DOCUMENTO SET ESTADO_GT='P' ,FECHA_ESTADO_GT=getdate() WHERE DOC_EXT=@doc_ext  
    
  UPDATE SYS_INT_DET_DOCUMENTO SET ESTADO_GT='P', DOC_BACK_ORDER=@doc_ext,FECHA_ESTADO_GT=getdate(), DOCUMENTO_ID=@Doc_ID, NRO_LOTE = @NRO_LOTE, NRO_PARTIDA = @NRO_PARTIDA  
  WHERE  DOC_EXT=@doc_ext and documento_id is null  
  
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