
GO

/*
Script created by Quest Change Director for SQL Server at 13/12/2012 04:36 p.m.
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

-- =============================================
-- Author:		Catalina Castillo Puentes
-- Create date: 03/04/2012
-- Description:	Función para retornar cantidades libres o cantidades ocupadas
-- =============================================
ALTER FUNCTION [dbo].[Fx_Algoritmo_Posiciones]
(
	-- Add the parameters for the function here
	@Posicion_Id AS NUMERIC(20,0),@Hija_De AS NUMERIC(20,0), @Producto_Id AS VARCHAR(30),@FiltroResultado AS VARCHAR(8)
)
RETURNS INT
AS
BEGIN
	--DEFINICION DE NIVEL DE BUSQUEDA DE LAYOUT
	
	--NIVEL		  1
	--COLUMNA	  2
	--CALLE		  3
	--NAVE		  4
	


	-- VARIABLES DE LAYOUT
	DECLARE @CANTIDAD_POSICIONES AS INT
		SET @CANTIDAD_POSICIONES=0
	DECLARE @CANTIDAD_POS_OCUPADAS AS INT
		SET @CANTIDAD_POS_OCUPADAS=0
	DECLARE @CANTIDAD_POS_DISPONIBLES AS INT
		SET @CANTIDAD_POS_DISPONIBLES=0
	DECLARE @NIVEL_DE_BUSQUEDA AS INT
		SET @NIVEL_DE_BUSQUEDA=0
	DECLARE @NIVEL_ID AS NUMERIC(20,0)
	DECLARE @COLUMNA_ID AS NUMERIC(20,0)
	DECLARE @CALLE_ID AS NUMERIC(20,0)
	DECLARE @NAVE_ID AS NUMERIC(20,0)
	
	--CONTADORES
	DECLARE @CONTADOR_PROFUNDIDAD AS INT
	DECLARE @CONTADOR_NIVEL AS INT

	--VARIABLES DE NEGOCIO
			
	
	SELECT @NIVEL_ID=NIVEL_ID, @COLUMNA_ID=COLUMNA_ID,@CALLE_ID=CALLE_ID,@NAVE_ID=NAVE_ID
		FROM POSICION WHERE POSICION_ID=@Posicion_Id


	--Inicia Busqueda por Nivel,verificando si para ese nivel existe profundidad
	IF @HIJA_DE>0
		BEGIN			
				SELECT @CANTIDAD_POS_OCUPADAS = COUNT(A.POSICION) FROM
					(
				 SELECT RL.POSICION_ACTUAL  POSICION
				 FROM RL_DET_DOC_TRANS_POSICION RL
				 INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
				 ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID
				 INNER JOIN DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID
				 AND DD.PRODUCTO_ID=@Producto_Id
				 INTERSECT
				 SELECT P.POSICION_ID POSICION
					 FROM POSICION P INNER JOIN NIVEL_NAVE NN
						ON P.NIVEL_ID = NN.NIVEL_ID
				  WHERE NN.HIJA_DE=@HIJA_DE
					)A
		
		  
				SELECT @CANTIDAD_POS_DISPONIBLES=COUNT(B.POSICION) 
				FROM (
				SELECT T.POSICION_ID POSICION
					FROM POSICION T INNER JOIN NIVEL_NAVE N
					ON T.NIVEL_ID = N.NIVEL_ID
				WHERE N.HIJA_DE=@HIJA_DE
				EXCEPT	
				
				SELECT  A.POSICION FROM
					(
				 SELECT RL.POSICION_ACTUAL  POSICION
				 FROM RL_DET_DOC_TRANS_POSICION RL
				 INTERSECT
				 SELECT P.POSICION_ID POSICION
					 FROM POSICION P INNER JOIN NIVEL_NAVE NN
						ON P.NIVEL_ID = NN.NIVEL_ID
				  WHERE NN.HIJA_DE=@HIJA_DE
					)A    
				 )B
			
		SET @NIVEL_DE_BUSQUEDA=1

		END
		
		IF (@CANTIDAD_POS_OCUPADAS = 0 AND @CANTIDAD_POS_DISPONIBLES=0) OR (@CANTIDAD_POS_DISPONIBLES=0)
			BEGIN --No tiene profundidad el nivel, se pasa a buscar en otro nivel de la misma columna
					SELECT @CANTIDAD_POS_OCUPADAS= COUNT(A.POSICION) FROM
						(
					 SELECT RL.POSICION_ACTUAL  POSICION
					 FROM RL_DET_DOC_TRANS_POSICION RL
					 INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
					 ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID
					 INNER JOIN DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID
					 AND DD.PRODUCTO_ID=@Producto_Id
					 INTERSECT
					 SELECT P.POSICION_ID POSICION
						 FROM POSICION P INNER JOIN NIVEL_NAVE NN
							ON P.NIVEL_ID = NN.NIVEL_ID
					  WHERE NN.COLUMNA_ID=@COLUMNA_ID 
						)A
						
					SELECT @CANTIDAD_POS_DISPONIBLES=COUNT(B.POSICION)
					FROM (
					SELECT T.POSICION_ID POSICION
						FROM POSICION T INNER JOIN NIVEL_NAVE N
						ON T.NIVEL_ID = N.NIVEL_ID
					WHERE N.COLUMNA_ID=@COLUMNA_ID 
					EXCEPT	
	                
					SELECT  A.POSICION FROM
						(
					 SELECT RL.POSICION_ACTUAL  POSICION
					 FROM RL_DET_DOC_TRANS_POSICION RL
					 INTERSECT
					 SELECT P.POSICION_ID POSICION
						 FROM POSICION P INNER JOIN NIVEL_NAVE NN
							ON P.NIVEL_ID = NN.NIVEL_ID
					  WHERE NN.COLUMNA_ID=@COLUMNA_ID 
						)A    
					 )B
				SET @NIVEL_DE_BUSQUEDA=2
			END
			
		IF (@CANTIDAD_POS_OCUPADAS = 0 AND @CANTIDAD_POS_DISPONIBLES=0) OR (@CANTIDAD_POS_DISPONIBLES=0)
			BEGIN -- No tiene ocupaciones en ningun nivel de la columna, se busca por calle en las columnas
				SELECT @CANTIDAD_POS_OCUPADAS= COUNT(A.POSICION) FROM
						(
					 SELECT RL.POSICION_ACTUAL  POSICION
					 FROM RL_DET_DOC_TRANS_POSICION RL
					 INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
					 ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID
					 INNER JOIN DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID
					 AND DD.PRODUCTO_ID=@Producto_Id
					 INTERSECT
					 SELECT P.POSICION_ID POSICION
						 FROM POSICION P INNER JOIN NIVEL_NAVE NN
							ON P.NIVEL_ID = NN.NIVEL_ID
					  WHERE NN.CALLE_ID=@CALLE_ID 
						)A
						
					SELECT @CANTIDAD_POS_DISPONIBLES=COUNT(B.POSICION)
					FROM (
					SELECT T.POSICION_ID POSICION
						FROM POSICION T INNER JOIN NIVEL_NAVE N
						ON T.NIVEL_ID = N.NIVEL_ID
					WHERE N.CALLE_ID=@CALLE_ID 
					EXCEPT	
	                
					SELECT  A.POSICION FROM
						(
					 SELECT RL.POSICION_ACTUAL  POSICION
					 FROM RL_DET_DOC_TRANS_POSICION RL
					 INTERSECT
					 SELECT P.POSICION_ID POSICION
						 FROM POSICION P INNER JOIN NIVEL_NAVE NN
							ON P.NIVEL_ID = NN.NIVEL_ID
					  WHERE NN.CALLE_ID=@CALLE_ID 
						)A    
					 )B
				SET @NIVEL_DE_BUSQUEDA=3
			END

		IF (@CANTIDAD_POS_OCUPADAS = 0 AND @CANTIDAD_POS_DISPONIBLES=0) OR (@CANTIDAD_POS_DISPONIBLES=0)
			BEGIN -- No tiene ocupaciones en ninguna columna, se busca por nave en otra calle
				SELECT @CANTIDAD_POS_OCUPADAS= COUNT(A.POSICION) FROM
						(
					 SELECT RL.POSICION_ACTUAL  POSICION
					 FROM RL_DET_DOC_TRANS_POSICION RL
					 INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 
					 ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID
					 INNER JOIN DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID
					 AND DD.PRODUCTO_ID=@Producto_Id
					 INTERSECT
					 SELECT P.POSICION_ID POSICION
						 FROM POSICION P INNER JOIN NIVEL_NAVE NN
							ON P.NIVEL_ID = NN.NIVEL_ID
					  WHERE NN.NAVE_ID=@NAVE_ID 
						)A
						
					SELECT @CANTIDAD_POS_DISPONIBLES=COUNT(B.POSICION)
					FROM (
					SELECT T.POSICION_ID POSICION
						FROM POSICION T INNER JOIN NIVEL_NAVE N
						ON T.NIVEL_ID = N.NIVEL_ID
					WHERE N.NAVE_ID=@NAVE_ID 
					EXCEPT	
	                
					SELECT  A.POSICION FROM
						(
					 SELECT RL.POSICION_ACTUAL  POSICION
					 FROM RL_DET_DOC_TRANS_POSICION RL
					 INTERSECT
					 SELECT P.POSICION_ID POSICION
						 FROM POSICION P INNER JOIN NIVEL_NAVE NN
							ON P.NIVEL_ID = NN.NIVEL_ID
					  WHERE NN.NAVE_ID=@NAVE_ID 
						)A    
					 )B
				SET @NIVEL_DE_BUSQUEDA=4
			END
			
			

	-- Return the result of the function
	IF @FiltroResultado='OCUPADAS'
		BEGIN
		SET @CANTIDAD_POSICIONES=  @CANTIDAD_POS_OCUPADAS
		END
	IF @FiltroResultado='LIBRES'		
		BEGIN
		SET @CANTIDAD_POSICIONES= @CANTIDAD_POS_DISPONIBLES
		END
	IF @FiltroResultado='DETALLE'		
		BEGIN
		SET @CANTIDAD_POSICIONES= @NIVEL_DE_BUSQUEDA
		END
	RETURN @CANTIDAD_POSICIONES

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

-- =============================================
-- Author:		Catalina Castillo Puentes
-- Create date: 04/04/2012
-- Description:	Función que devuelve la posición en donde debe ubicarse
-- =============================================
ALTER FUNCTION [dbo].[Fx_Get_Posicion] 
(
	-- Add the parameters for the function here
	@CLIENTE_ID VARCHAR(15), @PRODUCTO_ID VARCHAR(30),@VCANT AS NUMERIC(20)
)

RETURNS NUMERIC(20,0)
AS
BEGIN
	-- Declare the return variable here
	DECLARE @NRO AS INT
	DECLARE @POSICION_ACTUAL NUMERIC(20,0)
	DECLARE @POSICION_COD AS VARCHAR(45)
	DECLARE @COLUMNA_ID AS NUMERIC(20,0)
	DECLARE @CALLE_ID AS NUMERIC(20,0)
	DECLARE @NAVE_ID AS NUMERIC(20,0)
	DECLARE @NIVEL_ID AS NUMERIC(20,0)
	DECLARE @HIJA_DE AS NUMERIC(20,0)
	DECLARE @ORDEN_LOCATOR AS NUMERIC(6,0)
	DECLARE @POSICIONES_OCUPADAS AS INT
	DECLARE @POSICIONES_LIBRES AS INT
	DECLARE @DETALLE AS INT
	DECLARE @POSICION_FINAL AS NUMERIC(20,0)
	 
	SET @POSICION_FINAL = 0
	
	-- Add the T-SQL statements to compute the return value here
	SELECT TOP 1 
	@NRO = ROW_NUMBER() OVER(PARTITION BY A.DETALLE ORDER BY A.OCUPADAS DESC) , 
	@POSICION_ACTUAL= A.POSICION_ACTUAL,
	@POSICION_COD=A.POSICION_COD,
	@COLUMNA_ID=A.COLUMNA_ID,
	@CALLE_ID=A.CALLE_ID,
	@NAVE_ID=A.NAVE_ID,
	@NIVEL_ID=A.NIVEL_ID,
	@HIJA_DE=A.HIJA_DE,
	@ORDEN_LOCATOR=A.ORDEN_LOCATOR,
	@POSICIONES_OCUPADAS=A.OCUPADAS,
	@POSICIONES_LIBRES=A.LIBRES,
	@DETALLE=A.DETALLE 
	FROM (
		SELECT       rl.posicion_actual 
              ,p.POSICION_COD
              ,p.COLUMNA_ID
              ,p.CALLE_ID
              ,p.NAVE_ID
              ,p.NIVEL_ID
              ,isnull(p.HIJA_DE ,0) AS HIJA_DE
              ,MIN(ISNULL(p.ORDEN_LOCATOR,0)) OVER (PARTITION BY rl.posicion_actual) ORDEN_LOCATOR 
              ,dbo.Fx_Algoritmo_Posiciones(rl.posicion_actual,isnull(p.HIJA_DE ,0),@PRODUCTO_ID,'OCUPADAS') OCUPADAS
              ,dbo.Fx_Algoritmo_Posiciones(rl.posicion_actual,isnull(p.HIJA_DE ,0),@PRODUCTO_ID,'LIBRES') LIBRES
              ,dbo.Fx_Algoritmo_Posiciones(rl.posicion_actual,isnull(p.HIJA_DE ,0),@PRODUCTO_ID,'DETALLE') DETALLE
             
        FROM  rl_det_doc_trans_posicion rl (NoLock)
              LEFT OUTER JOIN nave n (NoLock)            on rl.nave_actual = n.nave_id 
                                                         and n.nave_cod <> 'PRE-INGRESO'
              LEFT OUTER JOIN posicion p  (NoLock)       on rl.posicion_actual = p.posicion_id 
              LEFT OUTER JOIN nave n2   (NoLock)         on p.nave_id = n2.nave_id 
                                                         and n2.nave_cod <> 'PRE-INGRESO'
              ,det_documento_transaccion ddt (NoLock)
              ,det_documento dd (NoLock) inner join documento d (NoLock) on(dd.documento_id=d.documento_id) left join sucursal s on(s.sucursal_id=d.sucursal_origen and s.cliente_id=d.cliente_id)
              ,documento_transaccion dt (NoLock)
        WHERE rl.doc_trans_id = ddt.doc_trans_id 
              AND rl.nro_linea_trans = ddt.nro_linea_trans 
              and ddt.documento_id = dd.documento_id 
              and ddt.doc_trans_id = dt.doc_trans_id 
              AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
              AND ISNULL(p.pos_lockeada,'0')='0'
              AND ISNULL(n.deposito_id,n2.deposito_Id)='DEFAULT'
              AND 0 =(SELECT (CASE WHEN (Count (posicion_id))> 0 THEN 1 ELSE 0 end) AS valor
                      From rl_posicion_prohibida_cliente (NoLock)
                      Where Posicion_ID = isnull(p.nivel_id,0)
                            AND cliente_id= dd.cliente_id)
              and 1 = (SELECT (CASE WHEN (Count (cliente_id))> 0 THEN 1 ELSE 0 end) AS valor 
                       FROM   rl_sys_cliente_usuario (NoLock)
                       WHERE  cliente_id = dd.cliente_id)
              AND ISNULL(rl.est_merc_id,'DISPONIBLE')= 'DISPONIBLE' 
              AND ISNULL(rl.cat_log_id,'DISPONIBLE') IN ('DISPONIBLE', 'TRAN_ING') 
              AND dd.PRODUCTO_ID = @PRODUCTO_ID AND dd.CLIENTE_ID=@CLIENTE_ID
              AND dbo.Fx_Algoritmo_Posiciones(rl.posicion_actual,isnull(p.HIJA_DE ,0),@PRODUCTO_ID,'LIBRES')>=1
              GROUP BY 
                 rl.posicion_actual 
                 ,p.POSICION_COD
                 ,p.COLUMNA_ID
				 ,p.CALLE_ID
				 ,p.NAVE_ID
				 ,p.NIVEL_ID
                 ,p.HIJA_DE 
                 ,p.ORDEN_LOCATOR
			
               )A
             ORDER BY A.ORDEN_LOCATOR,A.OCUPADAS DESC
        
               
     IF @VCANT=0
		BEGIN       
		  IF @HIJA_DE <>0
			BEGIN
				
				SELECT TOP 1 @POSICION_FINAL =P.POSICION_ID
				FROM POSICION P INNER JOIN NIVEL_NAVE NN
				ON P.NIVEL_ID=NN.NIVEL_ID
				LEFT JOIN RL_DET_DOC_TRANS_POSICION RL
				ON P.POSICION_ID = RL.POSICION_ACTUAL
				WHERE NN.HIJA_DE=@HIJA_DE AND RL.POSICION_ACTUAL IS NULL
				ORDER BY P.ORDEN_LOCATOR ASC
				
			END
			
		   IF (@COLUMNA_ID IS NOT NULL) AND (@POSICION_FINAL =0) 
			BEGIN
			
				SELECT TOP 1 @POSICION_FINAL =P.POSICION_ID
				FROM POSICION P INNER JOIN NIVEL_NAVE NN
				ON P.NIVEL_ID=NN.NIVEL_ID
				LEFT JOIN RL_DET_DOC_TRANS_POSICION RL
				ON P.POSICION_ID = RL.POSICION_ACTUAL
				WHERE NN.COLUMNA_ID=@COLUMNA_ID AND RL.POSICION_ACTUAL IS NULL
				ORDER BY P.ORDEN_LOCATOR ASC
			
			END
			
		   IF (@COLUMNA_ID IS NOT NULL AND @CALLE_ID IS NOT NULL) AND (@POSICION_FINAL  =0) 
			BEGIN
				SELECT TOP 1 @POSICION_FINAL =P.POSICION_ID
				FROM POSICION P INNER JOIN NIVEL_NAVE NN
				ON P.NIVEL_ID=NN.NIVEL_ID
				LEFT JOIN RL_DET_DOC_TRANS_POSICION RL
				ON P.POSICION_ID = RL.POSICION_ACTUAL
				WHERE NN.CALLE_ID=@CALLE_ID AND RL.POSICION_ACTUAL IS NULL
				ORDER BY P.ORDEN_LOCATOR ASC
			
			END
		END
		ELSE
			BEGIN
				IF @HIJA_DE <>0
			BEGIN
				
				SELECT TOP 1 @POSICION_FINAL =P.POSICION_ID
				FROM POSICION P INNER JOIN NIVEL_NAVE NN
				ON P.NIVEL_ID=NN.NIVEL_ID
				INNER JOIN RL_PRODUCTO_POSICION_PERMITIDA RP
				ON P.POSICION_ID = RP.POSICION_ID
				LEFT JOIN RL_DET_DOC_TRANS_POSICION RL
				ON P.POSICION_ID = RL.POSICION_ACTUAL
				WHERE NN.HIJA_DE=@HIJA_DE AND RL.POSICION_ACTUAL IS NULL
				AND RP.PRODUCTO_ID=@PRODUCTO_ID AND RP.CLIENTE_ID=@CLIENTE_ID
				ORDER BY P.ORDEN_LOCATOR ASC
				
			END
			
		   IF (@COLUMNA_ID IS NOT NULL) AND (@POSICION_FINAL =0) 
			BEGIN
				SELECT TOP 1 @POSICION_FINAL =P.POSICION_ID
				FROM POSICION P INNER JOIN NIVEL_NAVE NN
				ON P.NIVEL_ID=NN.NIVEL_ID
				INNER JOIN RL_PRODUCTO_POSICION_PERMITIDA RP
				ON P.POSICION_ID = RP.POSICION_ID
				LEFT JOIN RL_DET_DOC_TRANS_POSICION RL
				ON P.POSICION_ID = RL.POSICION_ACTUAL
				WHERE NN.COLUMNA_ID=@COLUMNA_ID AND RL.POSICION_ACTUAL IS NULL
				AND RP.PRODUCTO_ID=@PRODUCTO_ID AND RP.CLIENTE_ID=@CLIENTE_ID
				ORDER BY P.ORDEN_LOCATOR ASC
			
			END
			 IF (@COLUMNA_ID IS NOT NULL AND @CALLE_ID IS NOT NULL) AND (@POSICION_FINAL  =0) 
			BEGIN
				SELECT TOP 1 @POSICION_FINAL =P.POSICION_ID
				FROM POSICION P INNER JOIN NIVEL_NAVE NN
				ON P.NIVEL_ID=NN.NIVEL_ID
				INNER JOIN RL_PRODUCTO_POSICION_PERMITIDA RP
				ON P.POSICION_ID = RP.POSICION_ID
				LEFT JOIN RL_DET_DOC_TRANS_POSICION RL
				ON P.POSICION_ID = RL.POSICION_ACTUAL
				WHERE NN.CALLE_ID=@CALLE_ID AND RL.POSICION_ACTUAL IS NULL
				AND RP.PRODUCTO_ID=@PRODUCTO_ID AND RP.CLIENTE_ID=@CLIENTE_ID
				ORDER BY P.ORDEN_LOCATOR ASC
			
			END
			
			
			END

	-- Return the result of the function
	RETURN @POSICION_FINAL

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

-- =======================================================================================================================================
-- Author:		<LRojas>
-- Create date: <Hoy>
-- Description:	<Indica si existen Productos en la Nave/Posicion, y/o Productos asociados a la Nave/Posicion.>
-- =======================================================================================================================================
ALTER FUNCTION [dbo].[FX_VALIDA_STOCK_RELACION_POSIC]
  (
	@PRM_COD_NAVE AS VARCHAR(15),	-- Codigo de Nave
	@PRM_COD_CALLE AS VARCHAR(10),	-- Codigo Calle
	@PRM_COD_COLUMN AS VARCHAR(10),	-- Codigo Columna
	@PRM_COD_NIVEL AS VARCHAR(10),	-- Codigo Nivel
	@PRM_PROFUNDIDAD AS VARCHAR(10)	-- Codigo de Nivel que hace referencia a un nivel que es hijo de otro nivel.
  )
RETURNS varchar(MAX)
BEGIN
	DECLARE @CountPC as integer -- 	Determina el codigo que voy a usar (incluir o no profundidad)
	DECLARE @CodPosicion as varchar(MAX) -- Codigo de Posicion
	DECLARE @CountStock as int	-- 	Para existencias en Stock.
	DECLARE @CountProd as int	-- 	Para relaciones con productos.
	DECLARE @Result as varchar(MAX)	-- 	Para el retorno.
	
	Select @CountPC = Count(P.Posicion_Cod)
	From Nave N Inner Join Calle_Nave CaN On (N.Nave_ID = CaN.Nave_ID)
	Inner Join Columna_Nave ColN On (ColN.Nave_ID = N.Nave_ID And ColN.Calle_ID = CaN.Calle_ID)
	Inner Join Nivel_Nave NN On (NN.Nave_ID = N.Nave_ID And NN.Calle_ID = CaN.Calle_ID And NN.Columna_ID = ColN.Columna_ID)
	Inner Join Posicion P On (P.Nave_ID = N.Nave_ID And P.Calle_ID = P.Calle_ID And P.Columna_ID = ColN.Columna_ID And P.Nivel_ID = NN.Nivel_ID)
	Where N.Nave_Cod = @PRM_COD_NAVE
	And CaN.Calle_Cod = @PRM_COD_CALLE
	And ColN.Columna_Cod = @PRM_COD_COLUMN
	And NN.Nivel_Cod = @PRM_COD_NIVEL + '-' + @PRM_PROFUNDIDAD
	
	IF @CountPC > 0 -- Con profundidad
		SET @CodPosicion = @PRM_COD_NAVE + '-' + @PRM_COD_CALLE + '-' + @PRM_COD_COLUMN + '-' + @PRM_COD_NIVEL + '-' + @PRM_PROFUNDIDAD
	ELSE
		BEGIN
			Select @CountPC = Count(P.Posicion_Cod)
			From Nave N Inner Join Calle_Nave CaN On (N.Nave_ID = CaN.Nave_ID)
			Inner Join Columna_Nave ColN On (ColN.Nave_ID = N.Nave_ID And ColN.Calle_ID = CaN.Calle_ID)
			Inner Join Nivel_Nave NN On (NN.Nave_ID = N.Nave_ID And NN.Calle_ID = CaN.Calle_ID And NN.Columna_ID = ColN.Columna_ID)
			Inner Join Posicion P On (P.Nave_ID = N.Nave_ID And P.Calle_ID = P.Calle_ID And P.Columna_ID = ColN.Columna_ID And P.Nivel_ID = NN.Nivel_ID)
			Where N.Nave_Cod = @PRM_COD_NAVE
			And CaN.Calle_Cod = @PRM_COD_CALLE
			And ColN.Columna_Cod = @PRM_COD_COLUMN
			And NN.Nivel_Cod = @PRM_COD_NIVEL
			
			IF @CountPC > 0 -- Sin profundidad
				SET @CodPosicion = @PRM_COD_NAVE + '-' + @PRM_COD_CALLE + '-' + @PRM_COD_COLUMN + '-' + @PRM_COD_NIVEL
		END
	
	SELECT @CountStock = COUNT(*)
	FROM RL_DET_DOC_TRANS_POSICION 
	WHERE POSICION_ACTUAL = (SELECT POSICION_ID FROM POSICION WHERE POSICION_COD = @CodPosicion) 
	
	SELECT @CountProd = COUNT(*)
	FROM RL_PRODUCTO_POSICION_PERMITIDA
	WHERE POSICION_ID = (SELECT POSICION_ID FROM POSICION WHERE POSICION_COD = @CodPosicion)
	
	SET @Result = ''
	
	IF @CountStock > 0
		SELECT @Result = @Result + 'No se puede modificar la posicion ' + @CodPosicion + ' porque hay existencias en STOCK.' + CHAR(124)
	
	IF @CountProd > 0
		SELECT @Result = @Result + 'No se puede modificar la posicion ' + @CodPosicion + ' porque existen productos asociados a ella.' + CHAR(124)
	
  RETURN @Result
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

-- =======================================================================================================================================
-- Author:		<LRojas>
-- Create date: <Hoy>
-- Description:	<Indica si existen Productos en la Nave/Posicion, y/o Productos asociados a la Nave/Posicion.>
-- =======================================================================================================================================
ALTER FUNCTION [dbo].[FX_VALIDA_STOCK_RELACION_PROD] 
(
	@COD_NAVE AS VARCHAR(15)  -- Codigo de Nave
)
RETURNS varchar(MAX)
AS
BEGIN
	DECLARE @CountStock as int	-- 	Para existencias en Stock.
	DECLARE @CountProd as int	-- 	Para relaciones con productos.
	DECLARE @Result as varchar(MAX)	-- 	Para el retorno.

	SELECT @CountStock = COUNT(*)
	FROM RL_DET_DOC_TRANS_POSICION 
	WHERE NAVE_ACTUAL = (
							Select NAVE_ID From NAVE 
							Where NAVE_COD = @COD_NAVE
						) 
	OR POSICION_ACTUAL IN ( Select POSICION_ID From POSICION 
							Where NAVE_ID = ( 
											select NAVE_ID
											from NAVE 
											where NAVE_COD = @COD_NAVE
											)
							)
	
	SELECT @CountProd = COUNT(*)
	FROM RL_PRODUCTO_POSICION_PERMITIDA
	WHERE NAVE_ID = ( 
						Select NAVE_ID 
						From NAVE 
						Where NAVE_COD = @COD_NAVE
					) 
	OR POSICION_ID IN ( 
						Select POSICION_ID
						From POSICION 
						Where NAVE_ID = ( 
											select NAVE_ID
											from NAVE 
											where NAVE_COD = @COD_NAVE
											)
						)
	
	SET @Result = ''
	
	IF @CountStock > 0
		SELECT @Result = @Result + 'No se puede modificar la nave ' + @COD_NAVE + ' porque hay existencias en STOCK.' + CHAR(124)
	
	IF @CountProd > 0
		SELECT @Result = @Result + 'No se puede modificar la nave ' + @COD_NAVE + ' porque existen productos asociados a ella.' + CHAR(124)
	
	RETURN @Result

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
SET QUOTED_IDENTIFIER ON
GO

-- Batch submitted through debugger: SQLQuery3.sql|7|0|C:\Users\Administrador\AppData\Local\Temp\2\~vs6AD1.sql
ALTER FUNCTION [dbo].[MOB_BUSCAR_POSICION_EXISTENTE]
(@Cliente_ID varchar(15), @Producto_id varchar(30) = null)
RETURNS numeric(20, 0)
AS
BEGIN
Declare @Return as Int
	
	--1. Busca posicion que existe en el inventario y con menor orden_picking.
	BEGIN
    SELECT TOP 1 @Return = A.POSICION_ACTUAL
    FROM 
    (
    SELECT t2.ClienteID 
         ,t2.ProductoID 
         ,sum(ISNULL(t2.cantidad,0)) Cantidad 
         ,T2.POSICION_ACTUAL
         ,sum(ISNULL(t1.cantidad,0)) reservados 
         ,convert(datetime,t2.fecha_cpte,103)as fecha_cpte   
         ,t2.ORDEN_LOCATOR
    FROM 
          CLIENTE C (NoLock)
         ,PRODUCTO P (NoLock)
         ,(SELECT T2.CLIENTEID 
                  ,t2.ProductoID 
                 ,sum(t2.cantidad) AS cantidad 
                 ,t2.documento_id
      FROM (SELECT dd.cliente_id ClienteID 
                       ,dd.producto_id ProductoID 
                       ,sum(ISNULL(dd.cantidad,0)) AS cantidad 
                       ,dd.documento_id
                FROM   documento d (NoLock)
                       ,det_documento dd (NoLock)
                WHERE  d.documento_id = dd.documento_id 
                       AND d.status = 'D20'
                       AND d.tipo_operacion_id = 'EGR'
                       AND ISNULL(dd.est_merc_id,'DISPONIBLE')= 'DISPONIBLE'      
                       AND ISNULL(dd.cat_log_id_final,'DISPONIBLE')= 'DISPONIBLE' 
                GROUP BY dd.cliente_id 
                         ,dd.producto_id 
                         ,dd.documento_id
                UNION ALL 
                SELECT  dd.cliente_id ClienteID 
                          ,dd.producto_id ProductoID 
                          ,sum(ISNULL(dd.cantidad,0)) AS cantidad 
                          ,dd.documento_id
                  FROM    det_documento dd (NoLock)
                          ,det_documento_transaccion ddt (NoLock)
                          ,documento_transaccion dt (NoLock)
                   WHERE  ddt.documento_id = dd.documento_id 
                          AND ddt.nro_linea_doc = dd.nro_linea 
                          AND dt.doc_trans_id = ddt.doc_trans_id 
                          AND dt.status = 'T10'
                          AND dt.tipo_operacion_id = 'EGR'
                          AND not EXISTS  (SELECT rl_id 
                                           FROM rl_det_doc_trans_posicion rl (NoLock)
                                           WHERE rl.doc_trans_id_egr = ddt.doc_trans_id 
                                                 AND rl.nro_linea_trans_egr = ddt.nro_linea_trans )
                          AND ISNULL(dd.est_merc_id,'DISPONIBLE')= 'DISPONIBLE'              
                          AND ISNULL(dd.cat_log_id_final,'DISPONIBLE')= 'DISPONIBLE' 
    GROUP BY dd.cliente_id 
                           ,dd.producto_id 
                           ,dd.documento_id
                    ) t2  
          WHERE t2.ClienteID = @Cliente_ID
     GROUP BY  
                   t2.ClienteID 
                   ,t2.ProductoID 
                   ,t2.documento_id
           ) T1 RIGHT OUTER JOIN 
    (SELECT    dd.cliente_id AS ClienteID 
              ,dd.producto_id AS ProductoID 
              ,sum(ISNULL(rl.cantidad,0)) AS Cantidad 
              ,rl.posicion_actual 
              ,d.Fecha_Cpte 
              ,MIN(ISNULL(p.ORDEN_LOCATOR,0)) OVER (PARTITION BY rl.posicion_actual) ORDEN_LOCATOR 
        FROM  rl_det_doc_trans_posicion rl (NoLock)
              LEFT OUTER JOIN nave n (NoLock)            on rl.nave_actual = n.nave_id 
                                                         and n.nave_cod <> 'PRE-INGRESO'
              LEFT OUTER JOIN posicion p  (NoLock)       on rl.posicion_actual = p.posicion_id 
              LEFT OUTER JOIN nave n2   (NoLock)         on p.nave_id = n2.nave_id 
                                                         and n2.nave_cod <> 'PRE-INGRESO'
              ,det_documento_transaccion ddt (NoLock)
              ,det_documento dd (NoLock) inner join documento d (NoLock) on(dd.documento_id=d.documento_id) left join sucursal s on(s.sucursal_id=d.sucursal_origen and s.cliente_id=d.cliente_id)
              ,documento_transaccion dt (NoLock)
        WHERE rl.doc_trans_id = ddt.doc_trans_id 
              AND rl.nro_linea_trans = ddt.nro_linea_trans 
              and ddt.documento_id = dd.documento_id 
              and ddt.doc_trans_id = dt.doc_trans_id 
              AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
              --AND RL.DISPONIBLE= '1'
              AND ISNULL(p.pos_lockeada,'0')='0'
              AND ISNULL(n.deposito_id,n2.deposito_Id)='DEFAULT'
              AND 0 =(SELECT (CASE WHEN (Count (posicion_id))> 0 THEN 1 ELSE 0 end) AS valor
                      From rl_posicion_prohibida_cliente (NoLock)
                      Where Posicion_ID = isnull(p.nivel_id,0)
                            AND cliente_id= dd.cliente_id)
              and 1 = (SELECT (CASE WHEN (Count (cliente_id))> 0 THEN 1 ELSE 0 end) AS valor 
                       FROM   rl_sys_cliente_usuario (NoLock)
                       WHERE  cliente_id = dd.cliente_id)
              AND ISNULL(rl.est_merc_id,'DISPONIBLE')= 'DISPONIBLE' 
              AND ISNULL(rl.cat_log_id,'DISPONIBLE') IN ('DISPONIBLE', 'TRAN_ING') 
              AND dd.PRODUCTO_ID = @Producto_id
              GROUP BY 
                  dd.cliente_id 
                 ,dd.producto_id
                 ,rl.posicion_actual 
                 ,d.fecha_cpte 
                 ,p.ORDEN_LOCATOR
                 
     ) T2 ON (isnull(T2.CLIENTEID,0) = isnull(T1.CLIENTEID,0)
                    AND isnull(T2.PRODUCTOID,0) = isnull(T1.PRODUCTOID,0) 
                  ) 
         WHERE T2.CLIENTEID = C.CLIENTE_ID 
              AND T2.CLIENTEID = P.CLIENTE_ID 
              AND T2.PRODUCTOID = P.PRODUCTO_ID 
     AND t2.ClienteID = @Cliente_ID
    GROUP BY t2.ClienteID
             ,t2.ProductoID 
             ,t2.POSICION_ACTUAL
             ,t2.fecha_cpte 
             ,T2.ORDEN_LOCATOR
    ) A
    WHERE A.CLIENTEID = @Cliente_ID
    AND A.PRODUCTOID = @Producto_id
    AND A.ORDEN_LOCATOR = (SELECT 
                          MIN(ISNULL(A.ORDEN_LOCATOR,0))
                          FROM (
                              SELECT dd.cliente_id AS ClienteID 
                                    ,dd.producto_id AS ProductoID 
                                    ,d.Fecha_Cpte 
                                    ,ISNULL(p.ORDEN_LOCATOR,0) AS ORDEN_LOCATOR
                              FROM  rl_det_doc_trans_posicion rl (NoLock)
                                    LEFT OUTER JOIN posicion p  (NoLock)       on rl.posicion_actual = p.posicion_id 
                                    ,det_documento_transaccion ddt (NoLock)
                                    ,det_documento dd (NoLock) inner join documento d (NoLock) on(dd.documento_id=d.documento_id) left join sucursal s on(s.sucursal_id=d.sucursal_origen and s.cliente_id=d.cliente_id)
                                    ,documento_transaccion dt (NoLock)
                              WHERE rl.doc_trans_id = ddt.doc_trans_id 
                                    AND rl.nro_linea_trans = ddt.nro_linea_trans 
                                    and ddt.documento_id = dd.documento_id 
                                    and ddt.doc_trans_id = dt.doc_trans_id 
                                    AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
                                    AND RL.DISPONIBLE= '1'
                                    AND ISNULL(p.pos_lockeada,'0')='0'
                                    AND 0 =(SELECT (CASE WHEN (Count (posicion_id))> 0 THEN 1 ELSE 0 end) AS valor
                                            From rl_posicion_prohibida_cliente (NoLock)
                                            Where Posicion_ID = isnull(p.nivel_id,0)
                                                  AND cliente_id= dd.cliente_id)
                                    and 1 = (SELECT (CASE WHEN (Count (cliente_id))> 0 THEN 1 ELSE 0 end) AS valor 
                                             FROM   rl_sys_cliente_usuario (NoLock)
                                             WHERE  cliente_id = dd.cliente_id)
                                    AND ISNULL(rl.est_merc_id,'DISPONIBLE')= 'DISPONIBLE' 
                                    AND ISNULL(rl.cat_log_id,'DISPONIBLE') IN ('DISPONIBLE', 'TRAN_ING') 
                                    AND dd.cliente_id = @Cliente_ID
                                    AND dd.PRODUCTO_ID = @Producto_id
                                   -- AND P.ORDEN_LOCATOR IS NOT NULL
                                    GROUP BY dd.cliente_id 
                                       ,dd.producto_id 
                                       ,d.fecha_cpte
                                       ,p.ORDEN_LOCATOR
                          ) A
                          ) 
    GROUP BY A.POSICION_ACTUAL
	RETURN @RETURN
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