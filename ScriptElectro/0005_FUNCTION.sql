USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 03:05 p.m.
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

CREATE FUNCTION dbo.fn_diagramobjects() 
	RETURNS int
	WITH EXECUTE AS N'dbo'
	AS
	BEGIN
		declare @id_upgraddiagrams		int
		declare @id_sysdiagrams			int
		declare @id_helpdiagrams		int
		declare @id_helpdiagramdefinition	int
		declare @id_creatediagram	int
		declare @id_renamediagram	int
		declare @id_alterdiagram 	int 
		declare @id_dropdiagram		int
		declare @InstalledObjects	int

		select @InstalledObjects = 0

		select 	@id_upgraddiagrams = object_id(N'dbo.sp_upgraddiagrams'),
			@id_sysdiagrams = object_id(N'dbo.sysdiagrams'),
			@id_helpdiagrams = object_id(N'dbo.sp_helpdiagrams'),
			@id_helpdiagramdefinition = object_id(N'dbo.sp_helpdiagramdefinition'),
			@id_creatediagram = object_id(N'dbo.sp_creatediagram'),
			@id_renamediagram = object_id(N'dbo.sp_renamediagram'),
			@id_alterdiagram = object_id(N'dbo.sp_alterdiagram'), 
			@id_dropdiagram = object_id(N'dbo.sp_dropdiagram')

		if @id_upgraddiagrams is not null
			select @InstalledObjects = @InstalledObjects + 1
		if @id_sysdiagrams is not null
			select @InstalledObjects = @InstalledObjects + 2
		if @id_helpdiagrams is not null
			select @InstalledObjects = @InstalledObjects + 4
		if @id_helpdiagramdefinition is not null
			select @InstalledObjects = @InstalledObjects + 8
		if @id_creatediagram is not null
			select @InstalledObjects = @InstalledObjects + 16
		if @id_renamediagram is not null
			select @InstalledObjects = @InstalledObjects + 32
		if @id_alterdiagram  is not null
			select @InstalledObjects = @InstalledObjects + 64
		if @id_dropdiagram is not null
			select @InstalledObjects = @InstalledObjects + 128
		
		return @InstalledObjects 
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

-- =============================================
-- Author:		Catalina Castillo Puentes
-- Create date: 03/04/2012
-- Description:	Función para retornar cantidades libres o cantidades ocupadas
-- =============================================
CREATE FUNCTION [dbo].[Fx_Algoritmo_Posiciones]
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

-- =============================================
-- Author:		Catalina Castillo Puentes
-- Create date: 24/04/2012
-- Description:	Función, para devolver la cantidad
-- =============================================
CREATE FUNCTION [dbo].[Fx_Get_Cantidad_Cumplimiento]
(
	-- Add the parameters for the function here
	@VIAJE_ID VARCHAR(100)
)
RETURNS NUMERIC(20,5)
AS
BEGIN
	-- Declare the return variable here
	DECLARE @Cantidad AS NUMERIC(20,5)
	DECLARE @NroLinea AS NUMERIC(10,0)
	DECLARE @TotalCantidad AS NUMERIC(20,5)

	-- Add the T-SQL statements to compute the return value here
	DECLARE cantidad_Cursor CURSOR FOR
	SELECT DISTINCT CANTIDAD,NRO_LINEA FROM PICKING
		WHERE VIAJE_ID=@VIAJE_ID
	SET @TotalCantidad=0
	open cantidad_Cursor
	fetch next from cantidad_Cursor INTO @Cantidad,@NroLinea
	
	WHILE @@FETCH_STATUS = 0
	BEGIN	
		SELECT @TotalCantidad = @TotalCantidad +@Cantidad

	fetch next from cantidad_Cursor INTO @Cantidad,@NroLinea
	END	
	--COMMIT TRANSACTION
	CLOSE cantidad_Cursor
	DEALLOCATE cantidad_Cursor
	-- Return the result of the function
	RETURN @TotalCantidad

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

-- =============================================
-- Author:		Catalina Castillo Puentes
-- Create date: 04/04/2012
-- Description:	Función que devuelve la posición en donde debe ubicarse
-- =============================================
CREATE FUNCTION [dbo].[Fx_Get_Posicion] 
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

-- =======================================================================================================================================
-- Author:		<LRojas>
-- Create date: <Hoy>
-- Description:	<Indica si existen Productos en la Nave/Posicion, y/o Productos asociados a la Nave/Posicion.>
-- =======================================================================================================================================
CREATE FUNCTION [dbo].[FX_VALIDA_STOCK_RELACION_POSIC]
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
CREATE FUNCTION [dbo].[FX_VALIDA_STOCK_RELACION_PROD] 
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

-- Batch submitted through debugger: SQLQuery3.sql|7|0|C:\Users\Administrador\AppData\Local\Temp\2\~vs6AD1.sql
CREATE FUNCTION [dbo].[MOB_BUSCAR_POSICION_EXISTENTE]
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
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[MOB_BUSCAR_POSICION_GUARDADO]
(@Cliente_ID varchar(15), @Producto_id varchar(30) = null)
RETURNS VARCHAR(45)
WITH EXEC AS CALLER
AS
BEGIN
Declare @Return as VARCHAR(45)
	--1. Busca posicion de guardado para un producto en inventario 
	BEGIN
    -- ALGORITMO DE GUARDADO
    SELECT TOP 1 @Return = POS.POSICION_COD
    FROM POSICION POS
    WHERE POS.POSICION_ID IN
    (
    SELECT B.POSICION_PROPUESTA
    FROM
    (
      SELECT CASE WHEN ISNULL(POS.HIJA_DE, 0) = 0 
      THEN -- NO TIENE PROFUNDIDAD, DIFERENTE NIVEL
      ( SELECT A.NIVEL_ID
        FROM 
        (
          SELECT ROW_NUMBER() OVER( ORDER BY NIVEL_ID ASC) AS NRO
          ,B.NIVEL_ID
          FROM 
          (
          SELECT NN.NIVEL_ID
          FROM NIVEL_NAVE NN 
          WHERE NN.NAVE_ID = POS.NAVE_ID
          AND NN.CALLE_ID = POS.CALLE_ID
          AND NN.COLUMNA_ID = POS.COLUMNA_ID
          EXCEPT
          -- MENOS LAS POSICIONES OCUPADAS
          SELECT A.POSICION
          FROM 
          (
          SELECT RDT.POSICION_ACTUAL POSICION
          FROM RL_DET_DOC_TRANS_POSICION RDT
          INTERSECT
          SELECT POS.POSICION_ID POSICION
          FROM POSICION POS
          ) A
          ) B
         ) A
        WHERE A.NRO = 1 )
      ELSE -- TIENE PROFUNDIDAD, LO UBICA EN UNA PROFUNDIDAD DIFERENTE Y LA MENOR
      (
      -- PROFUNDIDAD ESTA OCUPADA, POS_VACIA
      SELECT C.NIVEL_ID
      FROM 
      (
        SELECT ROW_NUMBER() OVER( ORDER BY NIVEL_ID ASC) AS NRO
        ,B.NIVEL_ID
        FROM 
        (
          SELECT NN.NIVEL_ID
          FROM NIVEL_NAVE NN 
          WHERE NN.NAVE_ID = POS.NAVE_ID
          AND NN.CALLE_ID = POS.CALLE_ID
          AND NN.COLUMNA_ID = POS.COLUMNA_ID
          AND NN.HIJA_DE = POS.HIJA_DE
          EXCEPT
          -- MENOS LAS POSICIONES OCUPADAS
          SELECT A.POSICION
          FROM 
          (
          SELECT RDT.POSICION_ACTUAL POSICION
          FROM RL_DET_DOC_TRANS_POSICION RDT
          INTERSECT
          SELECT POS.POSICION_ID POSICION
          FROM POSICION POS
          ) A
         ) B
      ) C 
      WHERE C.NRO = 1) 
      END POSICION_PROPUESTA
      FROM POSICION POS
      WHERE ISNULL(POS.POSICION_ID,0) = (SELECT DBO.MOB_BUSCAR_POSICION_EXISTENTE(@Cliente_ID, @Producto_id)) 
    ) B
  )   
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
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[STATUS_PICKING_PEDIDO]
( @PICKING_ID AS NUMERIC(20,0) ) RETURNS FLOAT(2)
AS
BEGIN
	DECLARE @TOTAL 		AS NUMERIC(20,0)
	DECLARE @TOTAL_FIN 	AS NUMERIC(20,0)
	DECLARE @RETORNO 	AS NUMERIC(20,0)


	SELECT 	@TOTAL_FIN=COUNT(PICKING_ID)
	FROM 	PICKING
	WHERE	PICKING_ID=LTRIM(RTRIM(UPPER(@PICKING_ID)))
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
	IF @TOTAL_FIN>0
		BEGIN
    		SET @RETORNO=2
		END
	ELSE
		BEGIN
			SET @RETORNO=1
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

CREATE FUNCTION dbo.trunc (@input datetime)RETURNS datetime
AS
BEGIN

	DECLARE @fecha datetime,
	@fechastring varchar(10)

	SET @fechastring = CONVERT(varchar(10),@input, 103)

	SET @fecha = CONVERT(datetime, @fechastring, 103)

	RETURN @fecha

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

GRANT EXECUTE
ON OBJECT::[dbo].[fn_diagramobjects]
TO [public]
GO

DENY EXECUTE
ON OBJECT::[dbo].[fn_diagramobjects]
TO [guest]
GO

EXECUTE [sp_addextendedproperty]
	@name = N'microsoft_database_tools_support',
	@value = 1,
	@level0type = 'SCHEMA',
	@level0name = N'dbo',
	@level1type = 'FUNCTION',
	@level1name = N'fn_diagramobjects'
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