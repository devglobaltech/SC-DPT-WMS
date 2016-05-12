USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 03:23 p.m.
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

ALTER FUNCTION [dbo].[Aj_NaveCod_to_Nave_id](
@PARAM AS VARCHAR(45)) RETURNS FLOAT
AS
BEGIN
	DECLARE @RETURN AS NUMERIC(20,0)

	SELECT 	@RETURN=NAVE_ID
	FROM	NAVE
	WHERE	NAVE_COD=LTRIM(RTRIM(UPPER(@PARAM)))
	RETURN @RETURN

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

ALTER  Function [dbo].[am_funciones_estacion2_api#GetOrdenEstacionForDocTrID](
	@Doc_trans_Id as Numeric(20,0)
) Returns  Int
As
Begin
	Declare @Return as Int

	select 	@return=orden_estacion
	from	documento_transaccion
	where 	doc_trans_id=@doc_trans_id

	Return @Return

End --Fin FX.
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

ALTER  FUNCTION [dbo].[CatLogDefaultProducto](
@pCliente_id 		as varchar(15),
@pProducto_id		as varchar(30) 

) RETURNS varchar(50)
AS
BEGIN
	declare @vCatLog 	as varchar(50)
   
	select @vCatLog=ing_cat_log_id from producto where cliente_id=@pCliente_id and producto_id=@pProducto_id
	
	RETURN isnull(@vCatLog,'NO ENCUENTRO')
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

ALTER     FUNCTION [dbo].[CLIENTE_VIAJERUTA](
@pViaje_id			as varchar(100), 
@pRuta	 			as varchar(30)
) RETURNS VARCHAR (300)
AS
BEGIN
	Declare @sString as varchar (200)
	Declare @rString as varchar (200)

	Set @rString = ''
	
	DECLARE dcursor CURSOR FOR 	
		SELECT	s.nombre
		FROM	picking p (nolock)
			inner join documento d (nolock) on (p.documento_id = d.documento_id)	
			inner join sucursal s (nolock) on (d.sucursal_destino = s.sucursal_id)
		WHERE 	viaje_id = @pViaje_id
			AND ruta = @pRuta
		Group by s.sucursal_id, s.nombre
		Order by s.sucursal_id
	
	open dcursor
	fetch next from dcursor into @sString
	WHILE @@FETCH_STATUS = 0
	BEGIN
     		If @rString = ''
			Begin
				set @rString = @sString
			End
		Else
			Begin
				set @rString = @rString + ' - ' + @sString
			End

     		fetch next from dcursor into @sString
	END

CLOSE dcursor
DEALLOCATE dcursor

RETURN @rSTRING

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

ALTER  FUNCTION [dbo].[DATE_PICKING]
( @VIAJE_ID AS VARCHAR(50),@VALUE AS CHAR(1)) RETURNS DATETIME
AS
BEGIN
	DECLARE @RETORNO AS DATETIME
	IF @VALUE='1'
		BEGIN
			SELECT 	@RETORNO=MIN(FECHA_INICIO)
			FROM 	PICKING (nolock)
			WHERE 	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))
		END
	ELSE	
		BEGIN
			SELECT 	@RETORNO=MAX(FECHA_FIN)
			FROM 	PICKING (nolock)
			WHERE 	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))
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
SET QUOTED_IDENTIFIER OFF
GO

ALTER Function [dbo].[ent_documento_api#Ya_Existe_Nro_Comprobante] 
			(
				@P_Cliente_Id varchar(15), 
				@P_Tipo_Comprobante varchar(5),
				@P_Cpte_Prefijo varchar(6),
				@P_Cpte_Numero varchar(20),
				@PDocumento_Id_Actual numeric(20,0),
				@P_Sucursal_Origen varchar(20)
			) RETURNS numeric(1,0)
As
Begin
	Declare @Total numeric(10,0)
	Declare @ReturnValue numeric(1,0)

	If @PDocumento_Id_Actual is not null
		Begin
			If @P_Sucursal_Origen is null
				Begin
					Select @Total = isnull(Count(documento_id),0)
					From documento
					Where cliente_id = @P_Cliente_Id
						and cpte_prefijo = @P_Cpte_Prefijo 
						and cpte_numero =  @P_Cpte_Numero 
						and tipo_comprobante_id= @P_Tipo_Comprobante 
						and documento_id <> @PDocumento_Id_Actual 
						and status <> 'D99' 
						and sucursal_origen is null 
				End
			Else
				Begin
					Select @Total = isnull(Count(documento_id),0) 
					From documento
					Where cliente_id = @P_Cliente_Id 
						and cpte_prefijo = @P_Cpte_Prefijo 
						and cpte_numero = @P_Cpte_Numero 
						and tipo_comprobante_id= @P_Tipo_Comprobante 
						and documento_id <> @PDocumento_Id_Actual 
						and status <> 'D99' 
						and sucursal_origen = @P_Sucursal_Origen
				End
		End
    Else
		Begin
			If @P_Sucursal_Origen is null
				Begin
					Select @Total = isnull(Count(documento_id),0)
					From documento
					Where cliente_id = @P_Cliente_Id 
						and cpte_prefijo = @P_Cpte_Prefijo 
						and cpte_numero = @P_Cpte_Numero
						and tipo_comprobante_id= @P_Tipo_Comprobante
						and documento_id is not null 
						and status <> 'D99' 
						and sucursal_origen is null 
				End 
			Else
				Begin	
					Select @Total = isnull(Count(documento_id),0) 
					From documento
					Where cliente_id = @P_CLiente_Id
						and cpte_prefijo = @P_Cpte_Prefijo
						and cpte_numero = @P_Cpte_Numero
						and tipo_comprobante_id= @P_Tipo_Comprobante
						and documento_id is not null
						and status <> 'D99' 
						and sucursal_origen = @P_Sucursal_Origen
				End
		End

    
	If @Total = 0
		Begin
			Set @ReturnValue = 0
		End
    Else
		Begin
			Set @ReturnValue = 1
		End

	RETURN(@ReturnValue)

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

ALTER Function [dbo].[ent_documento_api#Ya_Existe_Orden_de_Compra]
			(
				@P_Cliente_Id varchar(15),
				@P_Tipo_Comprobante varchar(5),
				@P_Tipo_Operacion_Id varchar(5),
				@P_Orden_De_Compra varchar(20),
				@P_Documento_Id_Actual numeric(20,0)
			) RETURNS numeric(1,0)
As
Begin
	Declare @Cant_Docs numeric(10,0)
	Declare @ReturnValue numeric(1,0)

	If @P_Orden_De_Compra is not null
		Begin
			If @P_Documento_Id_Actual is not null 
				Begin
					SELECT @Cant_Docs = Count(documento_id)
					From documento
					Where cliente_id = @P_Cliente_Id 
						AND tipo_comprobante_id= @P_Tipo_Comprobante 
						AND tipo_operacion_id = @P_Tipo_Operacion_Id
						AND orden_de_compra= @P_Orden_De_Compra 
						and documento_id <> @P_Documento_Id_Actual 
						and status <> 'D99'   
				End
			Else
				Begin
					SELECT @Cant_Docs = Count(documento_id)
					From documento
					Where cliente_id =@P_Cliente_Id
						AND tipo_comprobante_id=@P_Tipo_Comprobante
						AND tipo_operacion_id =@P_Tipo_Operacion_Id
						AND orden_de_compra=@P_Orden_De_Compra
						and status <> 'D99' 
				End
		End
	Else
		Begin
			Set @Cant_Docs = 0
		End
    
	If @Cant_Docs = 0
		Begin
			Set @ReturnValue = 0
		End
    Else
		Begin
			Set @ReturnValue = 1
		End

	RETURN(@ReturnValue)

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

ALTER FUNCTION [dbo].[EvaluarTest] (@sParametro varchar(20))
RETURNS varchar(50)
AS
BEGIN
DECLARE @iValor varchar(50)

  set @iValor= (select password from sys_usuario where usuario_id=@sParametro)
 	
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

ALTER  Function [dbo].[Exist_Product](
@Producto_Id as varchar(30),
@Cliente_id as varchar(30)
)returns Int
As
Begin
	Declare @Return as int
	Declare @Cont	as int


	Select	@Cont=Count(*)
	from 	producto
	where	producto_id=ltrim(rtrim(upper(@producto_id)))
		and Cliente_id=ltrim(rtrim(upper(@cliente_id)))

	if @Cont=1
	Begin
		set @return=1
	end
	Else
	Begin
		set @return=0
	End	

	Return @Return
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

ALTER   FUNCTION [dbo].[fecha_de_corrido]
(
	--@FDESDE DATETIME ,
	@dias   VARCHAR(4000) 
)
RETURNS VARCHAR(4000) 
AS 
	BEGIN

		DECLARE @fecha  VARCHAR(3) 
/*
		IF @FDESDE IS NULL 
		BEGIN 
			RETURN null
		END
   */
		SELECT @fecha  = '999'

--CONVERT(DATETIME, CONVERT(VARCHAR (23), @FDESDE, 103), 103)+ @dias - CONVERT(DATETIME, CONVERT(VARCHAR (23), DBO.ADV_getCurrentDate(), 103), 103)
		
		RETURN (@fecha)

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

ALTER    FUNCTION [dbo].[FUNCIONES_GENERALES_API_UBICADO]
(
/*
	@CDOC_TRANS_ID     NUMERIC(20,0) ,
	@CNRO_LINEA_TRANS  NUMERIC(10,0) ,
	@PCAT_LOG_ID       VARCHAR(50) ,
	@PEST_MERC_ID      VARCHAR(15) 
*/
)
RETURNS INTEGER 
AS 
	BEGIN

		DECLARE @THE_NUMBER                               INTEGER 
		
		SELECT @THE_NUMBER = CASE COUNT(RL.RL_ID) WHEN 0 THEN 1 ELSE 0 END 
		FROM RL_DET_DOC_TRANS_POSICION  RL 
		WHERE RL.DOC_TRANS_ID = 27
		      AND RL.NRO_LINEA_TRANS = 1
		      AND RL.NAVE_ACTUAL IN ( SELECT NAVE_ID 
		                              FROM NAVE 
		                              WHERE PRE_INGRESO = 1 
		                              --AND DEPOSITO_ID = (SELECT DEPOSITO_DEFAULT 
		                              --                   FROM #TEMP_USUARIO_LOGGIN ) 
		                        ) 
		 AND RL.CAT_LOG_ID_FINAL = 'TRAN_ING'
		 AND RL.EST_MERC_ID =  NULL


		RETURN @THE_NUMBER
	END

/*
SELECT FUNCIONES_GENERALES_API_UBICADO
@CDOC_TRANS_ID=27,
@CNRO_LINEA_TRANS=1,
@PCAT_LOG_ID=null,
@PEST_MERC_ID=null

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

ALTER function [dbo].[fx_DateTimeToAnsi](@Value as datetime) Returns Varchar(10)
As
Begin
	Declare @Month as varchar(2)
	Declare @Day as varchar(2)
	Declare @Year as varchar(4)
	Declare @Return as varchar(8)

	Select @Year=Year(@Value)
	Select @Month=Month(@Value)
	Select @Day=Month(@Value)

	if len(@Month)=1
	begin
		set @Month='0' + @Month
	end
	If len(@Day)=1
	begin
		set @Day='0' + @Day
	End
	Set @Return=@year + @Month + @Day
	Return @Return
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

ALTER    function [dbo].[Fx_Fin_Ruta](
	@Viaje_id 	as Varchar(100),
	@Ruta		as Varchar(50)
)Returns int
As
Begin
	Declare @vCant 	as int --Saco el total.
	Declare @vTotal as int --Para el control.
	Declare @Return as int --Para el retorno.

	Select 	@vCant=Count(picking_id)
	From	Picking
	where	ltrim(rtrim(upper(Viaje_Id)))=Ltrim(Rtrim(Upper(@Viaje_id)))
			and
			ltrim(rtrim(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))

	Select 	@vTotal=Count(picking_id)
	From	Picking
	Where	fecha_inicio is not null
			and 
			fecha_Fin is not null
			and
			Cant_confirmada is not null
			and 
			Usuario is not null
			and
			ltrim(rtrim(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))
			and
			ltrim(rtrim(upper(viaje_id)))=ltrim(rtrim(upper(@Viaje_id)))

	If @vCant=@vTotal
		Begin
			Set @Return=1
		End
	Else
		Begin
			Set @Return=0
		End
	Return @Return
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

ALTER        function [dbo].[Fx_Fin_Ruta_Usuario](
	@Viaje_id 	as Varchar(100),
	@Ruta		as Varchar(50),
	@Usuario	as varchar(30)
)Returns int
As
Begin
	Declare @vCant 	as int --Saco el total.
	Declare @vTotal as int --Para el control.
	Declare @Return as int --Para el retorno.

	Select 	@vCant=Count(picking_id)
	From	Picking
	where	ltrim(rtrim(upper(Viaje_Id)))=Ltrim(Rtrim(Upper(@Viaje_id)))
			and	ltrim(rtrim(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))
			and usuario=ltrim(rtrim(upper(@Usuario)))
	
	Select 	@vTotal=Count(picking_id)
	From	Picking
	Where	fecha_inicio is not null
			and 
			fecha_Fin is not null
			and
			Cant_confirmada is not null
			and	ltrim(upper(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))
			and	ltrim(rtrim(upper(viaje_id)))=ltrim(rtrim(upper(@Viaje_id)))
			and usuario=ltrim(rtrim(upper(@Usuario)))

	If @vCant=@vTotal
		Begin
			Set @Return=1
		End
	Else
		Begin
			Set @Return=0
		End
	Return @Return
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

ALTER               function [dbo].[Fx_Fin_Viaje_Usuario](
	@Viaje_id 	as Varchar(100),
	@Usuario	as varchar(50)
)Returns int
As
Begin
	Declare @vCant 		as int --Saco el total.
	Declare @vTotal 	as int --Para el control.
	Declare @Return 	as int --Para el retorno.
	Declare @CountRuta	as Int --Para saber si puede tomar otra ruta

	Select 	@vCant=Count(picking_id)
	From	Picking
	where	Ltrim(Rtrim(Upper(Viaje_Id)))=Ltrim(Rtrim(Upper(@Viaje_id)))
			and usuario=ltrim(rtrim(upper(@usuario)))

	Select 	@vTotal=Count(picking_id)
	From	Picking sp
	Where	fecha_inicio is not null
			and 
			fecha_Fin is not null
			and
			Cant_confirmada is not null
			and	Ltrim(Rtrim(Upper(viaje_id)))=ltrim(rtrim(upper(@Viaje_id)))
			and usuario=ltrim(rtrim(upper(@usuario)))
			and nave_cod in(select 	
									nave_cod
							from 	nave n inner join rl_usuario_nave rlnu
									on(n.nave_id=rlnu.nave_id)
							where	n.nave_cod=sp.nave_cod
									and rlnu.usuario_id=@usuario)
	SELECT 			TOP 1
					@CountRuta=count(ISNULL(SP.RUTA,0))
	FROM 			PICKING SP 
					INNER JOIN PRIORIDAD_VIAJE SPV
					ON(SPV.VIAJE_ID=SP.VIAJE_ID)
					INNER JOIN PRODUCTO PROD
					ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
	WHERE 			
					SPV.PRIORIDAD = (
										SELECT 	MIN(PRIORIDAD)
										FROM	PRIORIDAD_VIAJE
										WHERE	VIAJE_ID=SP.VIAJE_ID
									)								
					AND	SP.FECHA_INICIO IS NULL
					AND	SP.FECHA_FIN IS NULL			
					AND	SP.USUARIO IS NULL
					AND	SP.CANT_CONFIRMADA IS NULL 
					AND	SP.VIAJE_ID IN (SELECT 	VIAJE_ID
										FROM  	RL_VIAJE_USUARIO
										WHERE 	VIAJE_ID=SP.VIAJE_ID
												AND
												USUARIO_ID =@Usuario
					AND SP.NAVE_COD	IN(	SELECT 	
												NAVE_COD
										FROM 	NAVE N INNER JOIN RL_USUARIO_NAVE RLNU
												ON(N.NAVE_ID=RLNU.NAVE_ID)
										WHERE	N.NAVE_COD=SP.NAVE_COD
												AND RLNU.USUARIO_ID=@Usuario
										)
										)
					AND Ltrim(Rtrim(Upper(sp.VIAJE_ID)))=Ltrim(Rtrim(Upper(@Viaje_id)))
					AND SP.FIN_PICKING <>'2'
	GROUP BY	SP.VIAJE_ID, SP.PRODUCTO_ID,SP.DESCRIPCION, SP.RUTA,SP.POSICION_COD,SP.TIPO_CAJA,SP.PROP1,PROD.UNIDAD_ID,SPV.PRIORIDAD
	ORDER BY	SPV.PRIORIDAD ASC,SP.TIPO_CAJA DESC, SP.POSICION_COD ASC

	If @vCant=@vTotal
		Begin
			if @CountRuta=0 Or @CountRuta is null
			Begin
				Set @Return=1
			End
			Else
			Begin
				Set @Return=0
			End
		End
	Else
		Begin
			Set @Return=0
		End
	Return @Return
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

ALTER Function [dbo].[Fx_GetNroRemitoDO](@Documento_ID  Numeric(20,0)) Returns Varchar(30)
As
Begin
	Declare @TipoComprobante	as Varchar(5)
	Declare @Retorno			as Varchar(30)

	Select 	@TipoComprobante=Tipo_Comprobante_ID
	From	Documento(Nolock)
	Where	Documento_ID=@Documento_ID

	If  @TipoComprobante='DO'
	Begin
		Select 	@Retorno=Nro_Remito
		From	Documento(Nolock)
		Where	Documento_ID=@Documento_ID		
	End
	Else
	Begin
		Set @Retorno=Null
	End

	Return @Retorno
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

ALTER   Function [dbo].[FX_GetPalletByPos]
(@Posicion 	as varchar(45))Returns Varchar(100)
As
Begin
	
	Declare @PalletOut as Varchar(100)
	
	Select 	Top 1
			@PalletOut=dd.prop1
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

	Return @PalletOut
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

ALTER  Function [dbo].[Fx_Procesados]
(@ViajeId	as Varchar(100)) Returns Int
As
Begin
	Declare @Return		as Int
	Declare @Cont		as Int

	select @Cont=count(dd.doc_ext) 
	from sys_int_det_documento dd (nolock)
		inner join sys_int_documento d (nolock) on (dd.cliente_id=d.cliente_id and dd.doc_ext=d.doc_ext)		
		inner join producto prod (nolock) on (dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id)
	where dd.estado_gt is null and d.codigo_viaje=@ViajeId
	
	If @Cont>0
	Begin
		Set @Return= 1
	End
	Else
	Begin
		Set @Return= 0
	End

	Return @Return
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

ALTER  FUNCTION [dbo].[FX_TRUNC_FECHA] (@sParametro DATETIME)
RETURNS varchar(20)
AS
BEGIN
DECLARE @iValor varchar(20)

	set @iValor = CONVERT(VARCHAR, @sParametro ,101)
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

ALTER function [dbo].[FxTimebyDetime](
@Arg datetime
)returns varchar(30)
as
Begin
	declare @Hour	as varchar(2)
	declare @Min	as varchar(2)
	declare @Sec	as varchar(2)
	declare @ret	as varchar(20)

	Set @Hour	=datepart(hh,@Arg)
	Set @Min	=datepart(mm,@Arg)
	Set @Sec	=datepart(ss,@Arg)
	--Hours
	if len(@hour)=1
	begin
		set @ret='0'+@hour
	end
	else
	begin
		set @ret=@hour
	end
	--Minutes
	if len(@min)=1
	begin
		set @ret=@ret + ':0'+ @min
	end
	else
	begin
		set @ret=@ret+ ':' + @min
	end
	--Seconds
	if len(@Sec)=1
	begin
		set @ret=@ret + ':0'+ @Sec
	end
	else
	begin
		set @ret=@ret+ ':' + @Sec
	end
	return @ret
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

ALTER      Function [dbo].[Get_Agente_Desc]
(@documento_id numeric(20,0)
,@nro_linea 	numeric(10,0)
,@Tipo			varchar(1)
) returns varchar(200)
As
Begin
	Declare @doc_id 		as numeric(20,0)
	declare @Sucursal_id 	as varchar(100)
	declare @NroPedido 		as varchar(100)
	declare @Retorno 		as varchar(100)
	declare @Motivo			as varchar(100)
	declare @Razon_Social	as varchar(100)

/* Parametro @Tipo
1:Razon_Social
2:@NroPedido 
3:Motivo
*/
	
	select 	@doc_id=documento_id_orig
	from 	aux_det_documento (nolock)
	where 	documento_id=@documento_id
			and nro_linea=@nro_linea

	select 	@Razon_Social=nombre
	from 	vdocumento d (nolock) inner join sucursal s (nolock)
			on(d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
	where 	documento_id=@doc_id

	if (@Tipo='1') begin
		set @Retorno=@Razon_Social			
	end --if


	Return @Retorno		
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

ALTER     Function [dbo].[Get_data_I08]
(@documento_id numeric(20,0)
,@nro_linea 	numeric(10,0)
,@Tipo			varchar(1)
) returns varchar(200)
As
Begin
	Declare @doc_id 		as numeric(20,0)
	declare @Sucursal_id as varchar(100)
	declare @NroPedido 	as varchar(100)
	declare @Retorno 		as varchar(100)
	declare @Motivo		as varchar(100)

/* Parametro @Tipo
1:Sucursal_id
2:@NroPedido 
3:Motivo
*/
	
	select 	@doc_id=documento_id_orig,@Motivo=motivo_id
	from 	aux_det_documento (nolock)
	where 	documento_id=@documento_id
			and nro_linea=@nro_linea

	select 	@Sucursal_id=sucursal_destino,@NroPedido=nro_remito 
	from 	vdocumento (nolock) 
	where 	documento_id=@doc_id

	if (@Tipo='1') begin
		set @Retorno=@Sucursal_id			
	end --if

	if (@Tipo='2') begin
		set @Retorno=@NroPedido			
	end --if
	
	if (@Tipo='3') begin
		set @Retorno=@Motivo			
	end --if

	Return @Retorno		
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

ALTER  Function [dbo].[Get_data_Session_Login]
(@pSession_id		as numeric(30,0),
 @pCampo   			as varchar(2)
) returns varchar(200)
As
Begin
	Declare @vUsuario_id 				as varchar(200)
	Declare @vNombre_Usuario 			as varchar(200)
	Declare @vTerminal			 		as varchar(200)
	declare @vFLogin						as varchar(200)
	declare @Retorno						as varchar(200)

/* Parametro @pCampo
1:Usuario_id
2:Nombre_Usuario
3:Terminal
4:FechaLogin 
*/
	
	select @vUsuario_id=usuario_id,@vNombre_Usuario=nombre_usuario,@vTerminal=terminal,@vFLogin=fecha_login from sys_session_login where session_id=@pSession_id
	
	if (@pCampo='1') begin
		set @Retorno=@vUsuario_id			
	end --if

	if (@pCampo='2') begin
		set @Retorno=@vNombre_Usuario			
	end --if

	if (@pCampo='3') begin
		set @Retorno=@vTerminal			
	end --if

	if (@pCampo='4') begin
		set @Retorno=@vFLogin			
	end --if

	Return @Retorno		
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

ALTER  Function [dbo].[Get_Descripcion](@Cliente_Id varchar(15),@Producto_id varchar(30)) returns varchar(200)
As
Begin
	Declare @Descripcion as varchar(200)

	select 	@Descripcion=Descripcion
	from	producto
	Where 	Cliente_Id=Rtrim(Ltrim(Upper(@Cliente_Id)))
			and Producto_id=Rtrim(Ltrim(Upper(@Producto_id)))

	Return @Descripcion			

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

ALTER FUNCTION [dbo].[GET_NAVE_COD](
@documento_id AS numeric(20,0)
,@nro_linea as numeric(10,0)

)RETURNS VARCHAR(50)
AS
	
BEGIN
	DECLARE @RETORNO AS NUMERIC(20,0)
	DECLARE @RETORNO_COD AS VARCHAR(50)
	DECLARE @nave_id AS NUMERIC(20,0)
	DECLARE @posicion_id AS NUMERIC(20,0)
	
	SELECT
		@nave_id=rl.nave_actual,
		@posicion_id=rl.posicion_actual
	FROM rl_det_doc_trans_posicion rl
		inner join det_documento_transaccion ddt on (rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
		inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
	WHERE
		ddt.documento_id=@documento_id and nro_linea_doc=@nro_linea

	IF @nave_id is not null
		BEGIN
			SET @RETORNO = @nave_id
		END 
	ELSE
		BEGIN
			select @nave_id=nave_id from posicion where posicion_id=@posicion_id
			SET @RETORNO=@nave_id 
		END
	
	select @RETORNO_COD=nave_cod from nave where nave_id=@nave_id
	
	RETURN @RETORNO_cod
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

ALTER FUNCTION [dbo].[GET_NAVE_ID](
@documento_id AS numeric(20,0)
,@nro_linea as numeric(10,0)

)RETURNS INTEGER
AS
	
BEGIN
	DECLARE @RETORNO AS NUMERIC(20,0)
	DECLARE @nave_id AS NUMERIC(20,0)
	DECLARE @posicion_id AS NUMERIC(20,0)
	
	SELECT
		@nave_id=rl.nave_actual,
		@posicion_id=rl.posicion_actual
	FROM rl_det_doc_trans_posicion rl
		inner join det_documento_transaccion ddt on (rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
		inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
	WHERE
		ddt.documento_id=@documento_id and nro_linea_doc=@nro_linea

	IF @nave_id is not null
		BEGIN
			SET @RETORNO = @nave_id
		END 
	ELSE
		BEGIN
			select @nave_id=nave_id from posicion where posicion_id=@posicion_id
			SET @RETORNO=@nave_id 
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

ALTER  FUNCTION [dbo].[GET_NAVE_ID_TR](
@PARAM AS VARCHAR(45)) RETURNS FLOAT
AS
BEGIN
	DECLARE @RETURN AS NUMERIC(20,0)

	SELECT 	@RETURN=NAVE_ID
	FROM	NAVE
	WHERE	NAVE_COD=LTRIM(RTRIM(UPPER(@PARAM)))
			AND NAVE_TIENE_LAYOUT='0'

	RETURN @RETURN

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

ALTER FUNCTION [dbo].[GET_PICKING_INFO]
(@VIAJE_ID AS VARCHAR(30),
@TIPO AS CHAR(1))RETURNS float(10)

AS
BEGIN
	DECLARE @VCANT AS NUMERIC(20,0)

	IF @TIPO='1'
		BEGIN
			SELECT 	
					@VCANT=COUNT(DOCUMENTO_ID)
			FROM 	PICKING
			WHERE 	FECHA_INICIO IS NOT NULL AND
					FECHA_FIN IS NOT NULL AND
					USUARIO IS NOT NULL AND 
					PALLET_PICKING IS NOT NULL AND
					CANT_CONFIRMADA IS NOT NULL
					AND VIAJE_ID=RTRIM(LTRIM(UPPER(@VIAJE_ID)))
			GROUP BY DOCUMENTO_ID
		END	
	ELSE
		BEGIN	
			SELECT 	@VCANT=COUNT(PRODUCTO_ID)
			FROM 	PICKING
			WHERE 	FECHA_INICIO IS NOT NULL AND
					FECHA_FIN IS NOT NULL AND
					USUARIO IS NOT NULL AND 
					PALLET_PICKING IS NOT NULL AND
					CANT_CONFIRMADA IS NOT NULL
					AND VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))
		END
	RETURN CAST(isnull(@VCANT,0) AS float(10))
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

ALTER  FUNCTION [dbo].[GET_POS_ID_TR](
@PARAM AS VARCHAR(45)) RETURNS FLOAT
AS
BEGIN
	DECLARE @RETURN AS NUMERIC(20,0)

	SELECT 	@RETURN=POSICION_ID
	FROM	POSICION
	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@PARAM)))

	RETURN @RETURN

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

ALTER   FUNCTION [dbo].[GET_POSICION_ID](
	@POSICION_COD	AS VARCHAR(45) 
) RETURNS NUMERIC(20,0)
AS
BEGIN
   DECLARE @POSICION_ID AS  NUMERIC(20,0)
   
   SELECT @POSICION_ID=POSICION_ID
   FROM POSICION
   WHERE POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD)))
   
  RETURN @POSICION_ID
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

ALTER FUNCTION [dbo].[GET_PROPERTY](
 @pcliente_id AS varchar(15)
,@pdoc_ext as varchar(100)
,@pproducto as varchar(100)
,@pprop	as numeric(1,0)

)RETURNS varchar(100)
AS
	
BEGIN
	DECLARE @vretorno AS varchar(100)
	declare @vprop1 as varchar(100)
	declare @vprop2 as varchar(100)
	declare @vprop3 as varchar(100)
	
    select @vprop1=prop1,
		   @vprop2=prop2,
           @vprop3=prop3 
    from sys_int_det_documento where cliente_id=@pcliente_id and doc_ext=@pdoc_ext and producto_id=@pproducto
	
	if (@pprop=1) begin
		set @vretorno=@vprop1		
	end --if

	if (@pprop=2) begin
		set @vretorno=@vprop2		
	end --if

	if (@pprop=3) begin
		set @vretorno=@vprop3
	end --if

	RETURN @vretorno
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

ALTER   FUNCTION [dbo].[GET_SALDO_INICIAL](
	@CLIENTE_ID 	VARCHAR(15),
	@PRODUCTO_ID	VARCHAR(30),
	@FECHA			VARCHAR(8),
	@FECHAD			VARCHAR(8)
)RETURNS FLOAT
AS
BEGIN
	DECLARE @RETORNO 	AS FLOAT
	DECLARE @MINDATE	AS DATETIME

	IF @FECHA IS NULL
	BEGIN
		SET @RETORNO=0
	END
	ELSE
	BEGIN

		SELECT 	@MINDATE=MIN(FECHA) 
		FROM 	PRODUCTO_AGRUPADO_HISTORICO (NOLOCK)
		WHERE 	CLIENTE_ID=@CLIENTE_ID
				AND PRODUCTO_ID=@PRODUCTO_ID
				AND FECHA BETWEEN @FECHA AND DATEADD(DD,1,@FECHAD)

		SELECT  @RETORNO=CANTIDAD
		FROM	PRODUCTO_AGRUPADO_HISTORICO(NOLOCK)
		WHERE 	CLIENTE_ID=@CLIENTE_ID
				AND PRODUCTO_ID=@PRODUCTO_ID
				AND FECHA=@MINDATE

	END
	RETURN 	@RETORNO
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

ALTER  FUNCTION [dbo].[Get_Tipo_Documento_id](
@pCliente_id as varchar(100)
,@pdoc_ext AS varchar(100)

)RETURNS VARCHAR(50)
AS
	
BEGIN
	DECLARE @vRetorno AS VARCHAR(50)
	select distinct @vRetorno=tipo_documento_id from sys_int_documento where cliente_id=@pCliente_id and doc_ext=@pdoc_ext
	RETURN isnull(@vRetorno,'')
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

ALTER FUNCTION [dbo].[GET_UNIDAD_ID](@Cliente_id varchar(15),@Producto_id Varchar(30)) Returns Varchar(5)
As
Begin
	Declare @Unidad as varchar(5)
	
	Select 	@Unidad=Unidad_id
	From	Producto
	Where	Cliente_id=Rtrim(Ltrim(Upper(@Cliente_id)))
			and Producto_Id=Rtrim(Ltrim(Upper(@Producto_id)))

	Return @Unidad

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

ALTER  FUNCTION [dbo].[GET_USUARIO_PEDIDO](@CLIENTE_ID VARCHAR(30), @PARAM VARCHAR(100)) RETURNS VARCHAR(100)
AS
BEGIN
	DECLARE @RETORNO		AS VARCHAR(100)
	DECLARE @FECHA			AS DATETIME
	DECLARE @USR_TERMINAL	AS VARCHAR(100)
	DECLARE @TERMINAL		AS VARCHAR(100)
	DECLARE @MYPOS			AS INT
	DECLARE @USUARIO_ID		AS VARCHAR(15)
	DECLARE @NOMBRE			AS VARCHAR(100)

	SELECT 	@FECHA=FECHA_SOLICITUD_CPTE, @USR_TERMINAL=OBSERVACIONES
	FROM	SYS_INT_DOCUMENTO (NOLOCK)
	WHERE 	CLIENTE_ID=@CLIENTE_ID
			AND DOC_EXT=@PARAM

	SELECT @MYPOS=CHARINDEX('|',@USR_TERMINAL)

	IF @MYPOS IS NOT NULL
	BEGIN
		SET @USUARIO_ID	=SUBSTRING(@USR_TERMINAL,0,@MYPOS)
		SET @TERMINAL	=SUBSTRING(@USR_TERMINAL,@MYPOS + 1,Len(@USR_TERMINAL))
		SELECT @NOMBRE=NOMBRE FROM SYS_USUARIO (NOLOCK) WHERE USUARIO_ID=@USUARIO_ID
		SET @RETORNO='Usuario Originador: ' + @USUARIO_ID + '-' + @NOMBRE
		SET @RETORNO=@RETORNO + ' | ' + 'Terminal: ' + @TERMINAL
		SET @RETORNO=@RETORNO + ' | ' + 'Fecha Pedido: ' + CONVERT(VARCHAR,@FECHA,103)

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

ALTER Function [dbo].[GetCalle](@NaveCod varchar(15), @CalleCod Varchar(15)) returns Bigint
As
Begin

	Declare @Retorno as bigint 
	
	Select 	@Retorno =Cn.Calle_ID 
	from 	nave n inner join Calle_Nave Cn
			on(Cn.nave_id=n.nave_id)
	where 	n.nave_cod=@NaveCod
			and Cn.Calle_Cod=@callecod
	return @retorno

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

ALTER Function [dbo].[GetColumna](@NaveCod varchar(15), @CalleCod Varchar(15), @ColCod varchar(15)) returns Bigint
As
Begin

	Declare @Retorno as bigint 
	
	Select 	@Retorno =Cnav.Columna_ID
	from 	nave n inner join Calle_Nave Cn
			on(Cn.nave_id=n.nave_id)
			inner join Columna_Nave CNav
			on(Cnav.Calle_ID=Cn.Calle_ID)
	where 	n.nave_cod=@NaveCod
			and Cn.Calle_Cod=@callecod
			and CNav.Columna_Cod=@ColCod

	return @retorno

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

ALTER Function [dbo].[GetNave](@NaveCod varchar(100)) returns Bigint
As
Begin
	Declare @Retorno as bigint 
	
	Select @Retorno =Nave_ID from nave where nave_cod=@NaveCod

	return @retorno
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

ALTER  FUNCTION [dbo].[GETORDENESTACIONFORDOCTRID]
(@PDOCTR NUMERIC(20,0)) RETURNS FLOAT
AS
BEGIN
	DECLARE @RETORNO AS NUMERIC(3,0)	

	SELECT 	@RETORNO=ORDEN_ESTACION
	FROM  	DOCUMENTO_TRANSACCION
	WHERE 	DOC_TRANS_ID=@PDOCTR

	RETURN 	@RETORNO
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

ALTER   FUNCTION [dbo].[GETPICKERMANS]
(@VIAJE_ID AS VARCHAR(100))
RETURNS VARCHAR(400) 

AS
BEGIN
	DECLARE @USUARIO VARCHAR(50)
	DECLARE @FINAL VARCHAR(400)
	DECLARE @SEP   VARCHAR(1)
	DECLARE PCUR CURSOR FOR
		SELECT 	 U.USUARIO_ID + '-' + NOMBRE AS USUARIO
		FROM 	RL_VIAJE_USUARIO RL (nolock)
			INNER JOIN SYS_USUARIO U (nolock) ON (RL.USUARIO_ID=U.USUARIO_ID)
		WHERE 	
		VIAJE_ID=@VIAJE_ID

	OPEN PCUR
	FETCH NEXT FROM PCUR INTO @USUARIO
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF (ISNULL(@FINAL,'')='')
		BEGIN
		   SET @SEP=''
		END
		ELSE
	        BEGIN   
		   SET @SEP=';'	
		END
		SET @FINAL=CAST(ISNULL(@FINAL,'') + @SEP + @USUARIO AS VARCHAR(400))
		FETCH NEXT FROM PCUR INTO @USUARIO
	END
	CLOSE PCUR
	DEALLOCATE PCUR
	RETURN (ISNULL(CAST(@FINAL AS VARCHAR(400)),''))
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

ALTER   FUNCTION [dbo].[GETPICKINGID](
@VIAJEID			VARCHAR(100),
@PRODUCTO_ID		VARCHAR(30),
@POSICION_COD		VARCHAR(50),
@PALLET			VARCHAR(100),
@RUTA				VARCHAR(100)
)RETURNS BIGINT
AS
BEGIN
	DECLARE @MINID AS BIGINT
	DECLARE @RETURN AS BIGINT

	SELECT 	@MINID=MIN(Picking_ID)
	FROM	PICKING
	WHERE	VIAJE_ID=@VIAJEID
			AND PRODUCTO_ID=@PRODUCTO_ID
			AND POSICION_COD=@POSICION_COD
			AND PROP1=@PALLET
			AND RUTA=@RUTA

	

	SET @RETURN=@MINID

	RETURN @RETURN

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

ALTER Function [dbo].[GetTipoDocumento](@PViaje	varchar(100))Returns Varchar(5)
As
Begin
	Declare @Retorno	Varchar(5)
	
	Select 	Distinct @Retorno=Tipo_Comprobante_ID
	From	Documento
	Where	Nro_Despacho_Importacion=@pViaje

	Return @Retorno


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

/*#10102008#*/
ALTER       FUNCTION [dbo].[GetValuesSysIntIA1]
( @Documento_ID Numeric(20,0) , @Nro_Linea numeric(10,0)) Returns Varchar(100)
As
Begin
	Declare @Doc_Ext	as Varchar(100)
	Declare @Cliente_ID	as Varchar(15)
	Declare @TC			as Varchar(5)
	Declare @Retorno	as Varchar(100)

	Select 	@TC=Tipo_Comprobante_Id
	From	Documento
	Where	Documento_ID=@Documento_ID


	If @TC='E10'
	Begin
		Select 	@Doc_Ext=Prop1
		From	Det_Documento
		Where	Documento_ID=@Documento_ID
				And Nro_Linea=@Nro_Linea

		Select 	@Retorno=Info_Adicional_1
		From	Sys_Int_Documento
		Where	Doc_Ext=@Doc_Ext		
	End
	Else
	Begin
		Set @Retorno='-1'
	End

	Return @Retorno

End --Fin Fx.
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

ALTER FUNCTION [dbo].[IsPosPicking](@Posicion as Varchar(45))returns int
As
Begin
	Declare @Retorno 	as int
	Declare @Cont		as int

	Select 	@Cont	=Count(posicion_id)
	from	posicion
	where	Posicion_cod=@Posicion
			And Picking=1

	If @Cont=0
	Begin
		Select 	@Cont=Count(Nave_Id)
		from	Nave
		Where	Nave_cod=@Posicion
				And Picking=1

		If @Cont=0
		Begin
			set @Retorno= 0
		End
		If @Cont>0
		Begin
			Set @Retorno=1
		End
	end
	Else
	Begin
		Set @Retorno=1
	End
	return @Retorno
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

ALTER function [dbo].[PADL](@value varchar(8000), @length int,@replaceChar char(1))
returns varchar(8000)
as

begin
if @value is null
   set @value = ''
declare @spaces as varchar(8000)
set @spaces = space(@length)
if @replaceChar <> ' '
   set @spaces = replace(@spaces, ' ', @replaceChar)

return  RIGHT(@spaces + @value,@length)

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
SET QUOTED_IDENTIFIER OFF
GO

ALTER  function [dbo].[PADR](@value varchar(8000), @length int,@replaceChar char(1))
returns varchar(8000)
as

begin
if @value is null
   set @value = ''
declare @spaces as varchar(8000)
set @spaces = space(@length)
if @replaceChar <> ' '
   set @spaces = replace(@spaces, ' ', @replaceChar)

--set @value = ltrim(rtrim(@value))

return  LEFT(@value + @spaces  ,@length)

end
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