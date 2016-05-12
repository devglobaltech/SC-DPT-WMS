USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 03:29 p.m.
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

ALTER    FUNCTION [dbo].[GetQtySol](
@pDocumento_id		as numeric(20,0),
@pNroLinea			as numeric(20,0),
@pCliente_id 		as varchar(15)
) RETURNS NUMERIC(20,5)
AS
BEGIN
	declare @vQty 		as numeric(20,5)
	declare @vCliente_id 	as varchar(15)

	declare @vProducto_id 	as varchar(15)
   
	SELECT 	@vCliente_id=cliente_id,@vProducto_id=producto_id,@vQty=sum(cantidad)
	FROM 	det_documento
	WHERE 	documento_id=@pDocumento_id and cliente_id=@pCliente_id
			and nro_linea = @pNroLinea
	group by 
		cliente_id,producto_id

	RETURN @vQty
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

ALTER  FUNCTION [dbo].[Picking_InProcess](@Picking_Id as Numeric(20,0)) Returns Int
As
Begin
	Declare @Cont 	As Int
	Declare @Return	As Int

	Select 	@Cont=Count(*)
	From	Picking 
	Where	Picking_id=@Picking_Id
			And Fecha_Inicio 	is not null
			And Fecha_Fin		is null
			And Cant_confirmada	is null

	If @Cont=0 --La tarea no esta tomada.
	Begin
		Set @Return=0
	End
	Else
	Begin
		Set @Return=1
	End
	
	Return @Return

End --Fin FX
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

ALTER     FUNCTION [dbo].[PICKING_VER_AFECTACION](
@USUARIO_ID 			AS VARCHAR(30),
@VIAJE_ID 			AS VARCHAR(100)
)returns int
Begin
	Declare @Return as Int
	Declare @Cont	as int

	--Saco Vinculacion con el Viaje
	Select 	@Cont=Count(*)
	from	rl_viaje_usuario
	where	LTRIM(RTRIM(UPPER(viaje_id)))=ltrim(rtrim(Upper(@viaje_id)))
			and usuario_id=ltrim(rtrim(upper(@usuario_ID)))

	if @Cont=0
	Begin
		set @Return=0	
	End
	Else
	Begin
		set @Return=1
	End
	return @Return
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

ALTER     FUNCTION [dbo].[QTY_DIFPICKING](
@pViaje_id			as varchar(100), 
@pCliente_id 		as varchar(15),
@pProducto_id		as varchar(30), 
@pTipo				as varchar(1) 

) RETURNS NUMERIC(20,5)
AS
BEGIN
	declare @vQty 		as numeric(20,5)   
	declare @vQty1 		as numeric(20,5)   
	declare @vQty2 		as numeric(20,5)   

/*
@pTipo:

1 - Sys_int_det_documento (lo pedido)
2 - Asignada (lo Asignado)
3 - Sys_dev_det_documento (lo devuelto a JDE)
4 - Pickeado (lo pikeado)
5 - Existencia disponible Warp

*/
	if (@pTipo=1) begin
		SELECT @vQty=isnull(sum(isnull(cantidad_solicitada,0)),0)
		from sys_int_det_documento dd (nolock) inner join sys_int_documento d (nolock) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext) 
		where
		d.codigo_viaje=@pViaje_id
		and dd.cliente_id=@pCliente_id 
		and producto_id=@pProducto_id
	end --if

	if (@pTipo=2) begin
		select @vQty=isnull(sum(isnull(cantidad,0)),0) from picking p (nolock)
		where 
		p.viaje_id=@pViaje_id
		and p.cliente_id=@pCliente_id
		and p.producto_id=@pProducto_id
	end --if

	if (@pTipo=3) begin
		SELECT @vQty=isnull(sum(isnull(cantidad,0)),0)
		from sys_dev_det_documento dd (nolock) inner join sys_dev_documento d (nolock) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext) 
		where
		d.codigo_viaje=@pViaje_id
		and dd.cliente_id=@pCliente_id 
		and producto_id=@pProducto_id
	end --if

	if (@pTipo=4) begin
		select @vQty=isnull(sum(isnull(cant_confirmada,0)),0) from picking p (nolock)
		where 
		p.viaje_id=@pViaje_id
		and p.cliente_id=@pCliente_id
		and p.producto_id=@pProducto_id
	end --if

	if (@pTipo=5) begin  		
		
		SELECT
		@vQty1=rl.cantidad
		FROM rl_det_doc_trans_posicion rl (nolock)
			inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
		   inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
			inner join posicion p (nolock) on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
			left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
		WHERE
			rl.doc_trans_id_egr is null
			and rl.nro_linea_trans_egr is null
			and rl.disponible='1'
			and isnull(em.disp_egreso,'1')='1'
			and isnull(em.picking,'1')='1'
			and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
			and dd.cliente_id=@pCliente_id
			and dd.producto_id=@pProducto_id
	
		SELECT
		@vQty2=rl.cantidad
		FROM rl_det_doc_trans_posicion rl (nolock)
			inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
		   inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
			inner join nave n (nolock) on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
			left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
		WHERE
			rl.doc_trans_id_egr is null
			and rl.nro_linea_trans_egr is null
			and rl.disponible='1'
			and isnull(em.disp_egreso,'1')='1'
			and isnull(em.picking,'1')='1'
			and rl.cat_log_id<>'TRAN_EGR'
			and dd.cliente_id=@pCliente_id
			and dd.producto_id=@pProducto_id

		set @vQty=isnull(@vQty1,0)+isnull(@vQty2,0)	
	end --if


	RETURN @vQty
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

ALTER  FUNCTION [dbo].[QTY_DIFPICKING_NVLCumpl](
@pViaje_id			as varchar(100), 
@pCliente_id 			as varchar(15),
@pProducto_id			as varchar(30), 
@Nro_Remito			as varchar(30),
@pTipo				as varchar(1) 

) RETURNS NUMERIC(20,5)
AS
BEGIN
	declare @vQty 		as numeric(20,5)   
	declare @vQty1 		as numeric(20,5)   
	declare @vQty2 		as numeric(20,5)   

/*
@pTipo:

1 - Sys_int_det_documento (lo pedido)
2 - Asignada (lo Asignado)
3 - Sys_dev_det_documento (lo devuelto a JDE)
4 - Pickeado (lo pikeado)
5 - Existencia disponible Warp

*/

	if (@pTipo=1) begin
		SELECT	@vQty=isnull(sum(isnull(cantidad_solicitada,0)),0)
		from 	sys_int_det_documento dd (nolock)
			inner join sys_int_documento d (nolock) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext) 
		where	d.codigo_viaje= @pViaje_id
			and dd.cliente_id= @pCliente_id 
			and producto_id= @pProducto_id
			and d.doc_ext = @nro_remito
	end --if

	if (@pTipo=2) begin
		select	@vQty=isnull(sum(isnull(cantidad,0)),0)
		from	picking p (nolock)
			inner join documento d (nolock) on (p.documento_id = d.documento_id)
		where	p.viaje_id= @pViaje_id
			and p.cliente_id= @pCliente_id
			and p.producto_id= @pProducto_id
			and d.nro_remito = @nro_remito
	end --if

	if (@pTipo=3) begin
		SELECT	@vQty=isnull(sum(isnull(cantidad,0)),0)
		from 	sys_dev_det_documento dd (nolock) 
			inner join sys_dev_documento d (nolock) on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext) 
		where	d.codigo_viaje= @pViaje_id
			and dd.cliente_id= @pCliente_id 
			and producto_id= @pProducto_id
			and d.doc_ext = @nro_remito
	end --if

	if (@pTipo=4) begin
		select	@vQty=isnull(sum(isnull(cant_confirmada,0)),0) 
		from	picking p (nolock)
			inner join documento d (nolock) on (p.documento_id = d.documento_id)
		where	p.viaje_id=@pViaje_id
			and p.cliente_id=@pCliente_id
			and p.producto_id=@pProducto_id
			and d.nro_remito = @nro_remito
	end --if

	if (@pTipo=5) begin  		
		
		SELECT
		@vQty1=rl.cantidad
		FROM rl_det_doc_trans_posicion rl (nolock)
			inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
		   inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
			inner join posicion p (nolock) on (rl.posicion_actual=p.posicion_id and p.pos_lockeada='0' and p.picking='1')
			left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
		WHERE
			rl.doc_trans_id_egr is null
			and rl.nro_linea_trans_egr is null
			and rl.disponible='1'
			and isnull(em.disp_egreso,'1')='1'
			and isnull(em.picking,'1')='1'
			and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
			and dd.cliente_id=@pCliente_id
			and dd.producto_id=@pProducto_id
	
		SELECT
		@vQty2=rl.cantidad
		FROM rl_det_doc_trans_posicion rl (nolock)
			inner join det_documento_transaccion ddt (nolock) on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
		   inner join det_documento dd (nolock) ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			inner join categoria_logica cl (nolock) on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id and cl.disp_egreso='1' and cl.picking='1')
			inner join nave n (nolock) on (rl.nave_actual=n.nave_id and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1')
			left join estado_mercaderia_rl em (nolock) on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
		WHERE
			rl.doc_trans_id_egr is null
			and rl.nro_linea_trans_egr is null
			and rl.disponible='1'
			and isnull(em.disp_egreso,'1')='1'
			and isnull(em.picking,'1')='1'
			and rl.cat_log_id<>'TRAN_EGR'
			and dd.cliente_id=@pCliente_id
			and dd.producto_id=@pProducto_id

		set @vQty=isnull(@vQty1,0)+isnull(@vQty2,0)	
	end --if


	RETURN @vQty
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

ALTER FUNCTION [dbo].[RECEPCION_PROCESADA](@CLIENTE_ID AS VARCHAR(15), @DOC_EXT AS VARCHAR(100)) RETURNS FLOAT
AS
BEGIN
	DECLARE @RETORNO	FLOAT
	DECLARE	@TOTAL		FLOAT
	DECLARE	@PROCESADOS	FLOAT

	SELECT 	@TOTAL=COUNT(*)
	FROM	SYS_INT_DET_DOCUMENTO 
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND DOC_EXT=@DOC_EXT

	SELECT 	@PROCESADOS=COUNT(*)
	FROM	SYS_INT_DET_DOCUMENTO 
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND DOC_EXT=@DOC_EXT
			AND ESTADO_GT IS NOT NULL
			AND FECHA_ESTADO_GT IS NOT NULL
	
	IF (@TOTAL=@PROCESADOS)
	BEGIN
		SET @RETORNO=1
	END
	ELSE
	BEGIN
		SET @RETORNO=0
	END
	RETURN @RETORNO

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

ALTER FUNCTION [dbo].[SOLICITA_VERIFICACION](@ID NUMERIC(20,0))RETURNS SMALLINT
BEGIN
	DECLARE @RET 		SMALLINT
	DECLARE @COUNT	FLOAT

	SELECT 	@COUNT=COUNT(*)
	FROM	PICKING
	WHERE	HIJO=@ID
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL

	IF  @COUNT >0
	BEGIN
		SET @RET=0
	END
	ELSE
	BEGIN
		SET @RET=1
	END

	RETURN @RET
END--FIN FUNCION
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

ALTER  FUNCTION [dbo].[STATUS_CONTROL_PICKING]
( @PALLET_PICK AS NUMERIC(20,0) ) RETURNS FLOAT(2)
AS
BEGIN
	DECLARE @TOTAL 		AS NUMERIC(20,0)
	DECLARE @TOTAL_FIN 	AS NUMERIC(20,0)
	DECLARE @RETORNO 	AS NUMERIC(20,0)

	SELECT 	@TOTAL=ISNULL(COUNT(PICKING_ID),0)
	FROM 	PICKING
	WHERE 	PALLET_PICKING=@PALLET_PICK

	SELECT 	@TOTAL_FIN=COUNT(PICKING_ID)
	FROM 	PICKING
	WHERE	PALLET_PICKING=@PALLET_PICK
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
			AND PALLET_CONTROLADO='1'


	IF @TOTAL_FIN>0
		BEGIN
			IF @TOTAL <> @TOTAL_FIN
				BEGIN
					SET @RETORNO=0
				END
			ELSE
				BEGIN
					SET @RETORNO=1
				END
		END
	ELSE
		BEGIN
			SET @RETORNO=0
		END		
	RETURN @RETORNO
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

ALTER   FUNCTION [dbo].[STATUS_EXPEDICION]
( @VIAJE_ID AS VARCHAR(100) ) RETURNS FLOAT(2)
AS
BEGIN
	DECLARE @TOTAL 		AS NUMERIC(20,0)
	DECLARE @TOTAL_FIN 	AS NUMERIC(20,0)
	DECLARE @RETORNO 	AS NUMERIC(20,0)

	SELECT 	@TOTAL=ISNULL(COUNT(PICKING_ID),0)
	FROM 	PICKING (nolock)
	WHERE 	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))

	SELECT 	@TOTAL_FIN=ISNULL(COUNT(PICKING_ID),0)
	FROM 	PICKING (nolock)
	WHERE	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
			AND ST_CONTROL_EXP='1'


	IF @TOTAL_FIN>0
		BEGIN
			IF @TOTAL <> @TOTAL_FIN
				BEGIN
					SET @RETORNO=0
				END
			ELSE
				BEGIN
					SET @RETORNO=1
				END
		END
	ELSE
		BEGIN
			SET @RETORNO=0
		END		
	RETURN @RETORNO
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

ALTER   FUNCTION [dbo].[STATUS_PICKING]
( @VIAJE_ID AS VARCHAR(100) ) RETURNS FLOAT(2)
AS
BEGIN
	DECLARE @TOTAL 		AS NUMERIC(20,0)
	DECLARE @TOTAL_FIN 	AS NUMERIC(20,0)
	DECLARE @RETORNO 	AS NUMERIC(20,0)

	SELECT 	@TOTAL=ISNULL(COUNT(PICKING_ID),0)
	FROM 	PICKING
	WHERE 	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))

	SELECT 	@TOTAL_FIN=COUNT(PICKING_ID)
	FROM 	PICKING
	WHERE	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
	IF @TOTAL_FIN>0
		BEGIN
			IF @TOTAL <> @TOTAL_FIN
				BEGIN
					SET @RETORNO=1
				END
			ELSE
				BEGIN
					SET @RETORNO=2
				END
		END
	ELSE
		BEGIN
			SET @RETORNO=0
		END		
	RETURN @RETORNO
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

ALTER Function [dbo].[Sys_Obj_Locking]
(@pSession 				numeric(20,0),
 @pCampo 				varchar(2)
) returns varchar(200)
As

Begin
	declare @Retorno 				as varchar(200)
	declare @vStatus				as varchar(200)
	declare @vHostname			as varchar(200)
	declare @vProgram_Name		as varchar(200)
	declare @vCmd					as varchar(200)
	declare @vLoginName			as varchar(200)
	declare @vFechaLock			as varchar(200)
	declare @vdbName				as varchar(200)

SELECT
  @vStatus=status
 ,@vHostname=hostname
 ,@vProgram_Name=program_name
 ,@vCmd=cmd
 ,@vLoginName=convert(sysname, rtrim(loginame))
 ,@vdbName = case
		when dbid = 0 then null
		when dbid <> 0 then db_name(dbid)
	end      
 ,@vFechaLock=substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
   
from master.dbo.sysprocesses   (nolock)
where (hostname is not null and hostname<>'') and spid=@pSession

	if (@pCampo='1') begin
		set @Retorno=@vStatus			
	end --if

	if (@pCampo='2') begin
		set @Retorno=@vHostname			
	end --if

	if (@pCampo='3') begin
		set @Retorno=@vProgram_Name			
	end --if

	if (@pCampo='4') begin
		set @Retorno=@vCmd			
	end --if

	if (@pCampo='5') begin
		set @Retorno=@vLoginName			
	end --if

	if (@pCampo='6') begin
		set @Retorno=@vFechaLock			
	end --if
	
	if (@pCampo='7') begin
		set @Retorno=@vdbName			
	end --if
	return @Retorno
End --procedure
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

ALTER  FUNCTION [dbo].[UBICADO] (@doc_trans_id integer, @nro_linea_trans integer, 
@CAT_LOG_ID varchar(50), @EST_MERC_ID varchar(15), @DEPOSITO_DEFAULT varchar(15))

RETURNS int 

AS
BEGIN
DECLARE @iValor int

IF (@CAT_LOG_ID IS NULL or @CAT_LOG_ID='') AND  (@EST_MERC_ID IS NULL  or  @EST_MERC_ID ='')
	BEGIN

	SET @iValor = (SELECT CASE COUNT(RL.RL_ID) WHEN 0 THEN 1 ELSE 0 END AS UBICADO 
	FROM RL_DET_DOC_TRANS_POSICION RL 
	WHERE RL.DOC_TRANS_ID  = @doc_trans_id
	AND RL.NRO_LINEA_TRANS  = @nro_linea_trans
	AND RL.NAVE_ACTUAL  IN(SELECT NAVE_ID 
	FROM  NAVE 
	WHERE	 PRE_INGRESO  = 1 
	AND	DEPOSITO_ID  = @DEPOSITO_DEFAULT	
	)
	AND RL.CAT_LOG_ID_FINAL IS NULL AND 1 = 1	
	AND RL.EST_MERC_ID IS NULL AND 1 = 1 )
	
	END
ELSE
	BEGIN
		IF @CAT_LOG_ID IS NOT NULL AND  @EST_MERC_ID IS NOT NULL
			BEGIN
			SET @iValor= (SELECT CASE COUNT(RL.RL_ID) WHEN 0 THEN 1 ELSE 0 END AS UBICADO 
				FROM RL_DET_DOC_TRANS_POSICION RL 
				WHERE RL.DOC_TRANS_ID  = @doc_trans_id
				AND RL.NRO_LINEA_TRANS  = @nro_linea_trans
				AND RL.NAVE_ACTUAL  IN(SELECT NAVE_ID 
				FROM  NAVE 
				WHERE	 PRE_INGRESO  = 1 
				AND	DEPOSITO_ID  = @DEPOSITO_DEFAULT					
				)
				AND RL.CAT_LOG_ID_FINAL = @CAT_LOG_ID
				AND RL.EST_MERC_ID =  @EST_MERC_ID)
			END	
			ELSE
			BEGIN
				IF (@CAT_LOG_ID IS NULL or @CAT_LOG_ID='')  AND  @EST_MERC_ID IS NOT NULL
				BEGIN
				SET @iValor= (SELECT CASE COUNT(RL.RL_ID) WHEN 0 THEN 1 ELSE 0 END AS UBICADO 
					FROM RL_DET_DOC_TRANS_POSICION RL 
					WHERE RL.DOC_TRANS_ID  = @doc_trans_id
					AND RL.NRO_LINEA_TRANS  = @nro_linea_trans
					AND RL.NAVE_ACTUAL  IN(SELECT NAVE_ID 
					FROM  NAVE 
					WHERE	 PRE_INGRESO  = 1 
					AND	DEPOSITO_ID  = @DEPOSITO_DEFAULT						
					)
					AND RL.CAT_LOG_ID_FINAL IS NULL AND 1 = 1
					AND RL.EST_MERC_ID =  @EST_MERC_ID)
				END
	
				ELSE
				BEGIN
		
				IF @CAT_LOG_ID IS NOT NULL AND  (@EST_MERC_ID IS NULL or @EST_MERC_ID='')
				BEGIN
				SET @iValor= (SELECT CASE COUNT(RL.RL_ID) WHEN 0 THEN 1 ELSE 0 END AS UBICADO 
					FROM RL_DET_DOC_TRANS_POSICION RL 
					WHERE RL.DOC_TRANS_ID  = @doc_trans_id
					AND RL.NRO_LINEA_TRANS  = @nro_linea_trans
					AND RL.NAVE_ACTUAL  IN(SELECT NAVE_ID 
					FROM  NAVE 
					WHERE	 PRE_INGRESO  = 1 
					AND	DEPOSITO_ID  = @DEPOSITO_DEFAULT						
					)
					AND RL.CAT_LOG_ID_FINAL = @CAT_LOG_ID
					AND RL.EST_MERC_ID IS NULL AND 1 = 1 )
				END
			END
	END

END
   RETURN(@iValor)
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

ALTER Function [dbo].[Verifica_Cambio_Nave](@Rl NUMERIC(20,0)) RETURNS INT
As
Begin
	Declare @Return 	as int
	Declare @Anterior 	as Numeric(20,0)
	Declare @Actual 	as Numeric(20,0)

	--Saco la Nave Actual
	Select Distinct @Actual=X.Nave_Id
	From(
			Select 	Nave_id
			from	rl_det_doc_trans_posicion Rl
					inner join Nave N
					On(Rl.Nave_Actual=N.Nave_id)
			Where	Rl.Rl_Id=@Rl
			Union All
			Select 	Nave_id
			From	Rl_Det_Doc_Trans_Posicion Rl
					inner join Posicion P
					On(Rl.Posicion_Actual=P.Posicion_Id)
			Where	Rl.Rl_Id=@Rl
		)As X

	--Saco la Nave Anterior	
	Select Distinct @Anterior=X.Nave_Id
	From(
			Select 	Nave_id
			from	rl_det_doc_trans_posicion Rl
					inner join Nave N
					On(Rl.Nave_Anterior=N.Nave_id)
			Where	Rl.Rl_Id=@Rl
			Union All
			Select 	Nave_id
			From	Rl_Det_Doc_Trans_Posicion Rl
					inner join Posicion P
					On(Rl.Posicion_Anterior=P.Posicion_Id)
			Where	Rl.Rl_Id=@Rl
		)As X

	If @Actual <> @Anterior
	Begin
		Set @Return=1
	End 
	Else
	Begin
		Set @Return=0
	End

	Return @Return
End	--Fin Fx
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

ALTER FUNCTION [dbo].[VERIFICA_FIN_CALLE](
	@VIAJE_ID	AS VARCHAR(100),
	@NAVE		AS VARCHAR(50),
	@CALLE		AS VARCHAR(50),
	@VEHICULO	AS VARCHAR(50)
) RETURNS INTEGER
AS
BEGIN
	DECLARE @CONTROL	BIGINT
	DECLARE @RET		SMALLINT

	SELECT 	@CONTROL=COUNT(PICK.PICKING_ID)
	FROM	PICKING PICK LEFT JOIN POSICION P
			ON(PICK.POSICION_COD=P.POSICION_COD)
			LEFT JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
			LEFT JOIN NAVE N ON(P.NAVE_ID=N.NAVE_ID)
			LEFT JOIN RL_VEHICULO_POSICION RL ON(P.POSICION_ID=RL.POSICION_ID)
	WHERE	1=1
			AND RL.VEHICULO_ID=@VEHICULO
			AND VIAJE_ID=@VIAJE_ID
			AND CN.CALLE_COD=@CALLE
			AND N.NAVE_COD=@NAVE
			AND PICK.FECHA_INICIO IS NULL

	IF @CONTROL>0
	BEGIN
		SET @RET=1
	END
	ELSE
	BEGIN
		SET @RET=0
	END
	RETURN @RET
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

ALTER      FUNCTION [dbo].[VERIFICA_FIN_VIAJES](@VIAJE AS VARCHAR(30))RETURNS INTEGER
AS
	
BEGIN
	DECLARE @CANT AS NUMERIC(20)
	DECLARE @RETORNO AS NUMERIC(20)

	SELECT 	@CANT=COUNT(*)
	FROM 	PICKING
	WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID))) = LTRIM(RTRIM(UPPER(@VIAJE)))
			AND FECHA_INICIO IS NULL
			AND FECHA_FIN IS NULL
			AND CANT_CONFIRMADA IS NULL
	IF @CANT >= 1
		BEGIN
			SET @RETORNO = 0
		END 
	ELSE
		BEGIN
			SET @RETORNO = 1
		END
	RETURN @RETORNO
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

ALTER  Function [dbo].[VerificaDocExt](
@Cliente_id 	as varchar(15),
@Doc_Ext	as varchar(100)
)Returns SmallInt
Begin
	Declare @Ret	as smallint
	Declare @Total	as int
	Declare @Proc	as Int

	SELECT 	@Total=COUNT(DOC_EXT)
	FROM	SYS_INT_DET_DOCUMENTO
	WHERE	Cliente_ID=@Cliente_ID
			And DOC_EXT=@Doc_Ext
	
	SELECT 	@Proc=COUNT(DOC_EXT)
	FROM	SYS_INT_DET_DOCUMENTO
	WHERE	Cliente_Id=@Cliente_id
			And DOC_EXT=@Doc_Ext
			AND ESTADO_GT IS NOT NULL
	If (@Total=@Proc)
	Begin
		Set @Ret=1
	End
	Else
	Begin
		Set @Ret=0
	End	
	Return @Ret		
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

ALTER    Function [dbo].[VerificaMovDocExt](
@Cliente_id 	as varchar(15),
@Doc_Ext	as varchar(100)
)Returns SmallInt
Begin
	Declare @Ret	as smallint
	Declare @Total	as int
	Declare @Proc	as Int

	SELECT 	@Total=COUNT(S.DOC_EXT)
	FROM	SYS_DEV_DET_DOCUMENTO S
	WHERE	s.Cliente_ID=@Cliente_ID
			And DOC_EXT=@Doc_Ext

	SELECT 	@Proc=COUNT(DOC_EXT)
	FROM	SYS_DEV_DET_DOCUMENTO
	WHERE	Cliente_Id=@Cliente_id
			And DOC_EXT=@Doc_Ext
			and flg_movimiento='1'

	If (@Total=@Proc)
	Begin
		Set @Ret=1
	End
	Else
	Begin
		Set @Ret=0
	End	
	Return @Ret		
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

ALTER    Function [dbo].[VerificaPenDocExt](
@Cliente_id 	as varchar(15),
@Doc_Ext	as varchar(100)
)Returns SmallInt
Begin
	Declare @Ret	as smallint
	Declare @Total	as int
	Declare @Proc	as Int

	SELECT 	@Total=COUNT(*)
	FROM	SYS_INT_DET_DOCUMENTO 
	WHERE	CLIENTE_ID=@Cliente_id
			AND DOC_EXT=@Doc_Ext
	
	SELECT 	@Proc=COUNT(*)
	FROM	SYS_INT_DET_DOCUMENTO
	WHERE	CLIENTE_ID=@Cliente_id
			AND DOC_EXT=@Doc_Ext
			AND ESTADO_GT IS NOT NULL


	If (@Total=@Proc)
	Begin
		Set @Ret=1
	End
	Else
	Begin
		Set @Ret=0
	End	
	Return @Ret		
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

ALTER function  [dbo].[Split] (
@StringToSplit varchar(2048),
@Separator varchar(128))
returns table as return
with indices as
(
select 0 S, 1 E
union all
select E, charindex(@Separator, @StringToSplit, E) + len(@Separator)
from indices
where E > S
)
select substring(@StringToSplit,S,
case when E > len(@Separator) then e-s-len(@Separator) else len(@StringToSplit) - s + 1 end) String
,S StartIndex        
from indices where S >0
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