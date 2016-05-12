
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 03:54 p.m.
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

-- =============================================
-- Author:		LRojas
-- Create date: 18/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
ALTER PROCEDURE [dbo].[cerrar_tmp_producto_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(30) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
	DECLARE @PRODUCTO_ID as varchar(30),
            @NRO_LINEA as numeric(10)
    
    DECLARE cur_productos CURSOR FOR
    SELECT DISTINCT PRODUCTO_ID, NRO_LINEA FROM TMP_EMPAQUE_CONTENEDORA WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
    
    OPEN cur_productos
    FETCH cur_productos INTO @PRODUCTO_ID, @NRO_LINEA
    
    WHILE @@FETCH_STATUS = 0
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM TMP_EMPAQUE_CONTENEDORA 
                            WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
                            AND NRO_LINEA = @NRO_LINEA
                            AND PRODUCTO_ID = @PRODUCTO_ID AND PALLET_CONTROLADO = '0')
                BEGIN
                    UPDATE PICKING
                    SET CANTIDAD = TMP.CANTIDAD,
                        CANT_CONFIRMADA = TMP.CANT_CONFIRMADA,
                        PALLET_PICKING = TMP.PALLET_PICKING,
                        PALLET_CONTROLADO = TMP.PALLET_CONTROLADO
                    FROM TMP_EMPAQUE_CONTENEDORA TMP 
                    WHERE TMP.CLIENTE_ID = @CLIENTE_ID AND TMP.NRO_REMITO = @PEDIDO_ID
                    AND TMP.PRODUCTO_ID = @PRODUCTO_ID AND TMP.PALLET_PICKING = @NRO_CONTENEDORA
                    AND TMP.NRO_LINEA = @NRO_LINEA
                    AND PICKING.PICKING_ID = TMP.PICKING_ID
                   -- AND PICKING.PALLET_CONTROLADO = '0'
                   
                   DELETE FROM PICKING WHERE PICKING_ID IN (SELECT P.PICKING_ID FROM PICKING P 
                    INNER JOIN TMP_EMPAQUE_CONTENEDORA T
					   ON P.PRODUCTO_ID = T.PRODUCTO_ID AND P.DOCUMENTO_ID = T.DOCUMENTO_ID
					  AND P.PALLET_CONTROLADO = '0' AND P.PRODUCTO_ID = @PRODUCTO_ID AND P.CLIENTE_ID = @CLIENTE_ID
					  AND T.NRO_REMITO = @PEDIDO_ID AND P.NRO_LINEA = @NRO_LINEA)
                   
                END
            ELSE
                BEGIN
                    UPDATE PICKING
                    SET CANTIDAD = TMP.CANTIDAD,
                        CANT_CONFIRMADA = TMP.CANT_CONFIRMADA
                    FROM TMP_EMPAQUE_CONTENEDORA TMP 
                    WHERE TMP.CLIENTE_ID = @CLIENTE_ID AND TMP.NRO_REMITO = @PEDIDO_ID
                    AND TMP.PRODUCTO_ID = @PRODUCTO_ID AND TMP.PALLET_CONTROLADO = '0'
                    AND TMP.NRO_LINEA = @NRO_LINEA
                    AND PICKING.PICKING_ID = TMP.PICKING_ID
                    
                    IF NOT EXISTS(SELECT 1 
                                    FROM DOCUMENTO D INNER JOIN PICKING P ON(D.DOCUMENTO_ID = P.DOCUMENTO_ID) 
                                    WHERE D.CLIENTE_ID = @CLIENTE_ID AND D.NRO_REMITO = @PEDIDO_ID 
                                    AND PRODUCTO_ID = @PRODUCTO_ID AND PALLET_PICKING = @NRO_CONTENEDORA
                                    AND NRO_LINEA=@NRO_LINEA)
                        INSERT INTO PICKING(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, 
                        POSICION_COD, RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
                        USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
                        TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, USUARIO_CONTROL_FAC, 
                        TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, PALLET_CERRADO, USUARIO_PF, TERMINAL_PF, 
                        REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS, BULTOS_NO_CONTROLADOS, FLG_PALLET_HOMBRE, TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE)
                        SELECT DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, 
                        RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, USUARIO_CONTROL_PICK, 
                        ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, 
                        USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, 
                        HIJO, QTY_CONTROLADO, PALLET_FINAL, PALLET_CERRADO, USUARIO_PF, TERMINAL_PF, REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS, 
                        BULTOS_NO_CONTROLADOS, FLG_PALLET_HOMBRE, TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE
                        FROM TMP_EMPAQUE_CONTENEDORA
                        WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
                        AND PRODUCTO_ID = @PRODUCTO_ID AND PALLET_PICKING = @NRO_CONTENEDORA
                        AND NRO_LINEA = @NRO_LINEA
                    ELSE
                        UPDATE PICKING
                        SET CANTIDAD = TMP.CANTIDAD,
                            CANT_CONFIRMADA = TMP.CANT_CONFIRMADA
                        FROM TMP_EMPAQUE_CONTENEDORA TMP 
                        WHERE TMP.CLIENTE_ID = @CLIENTE_ID AND TMP.NRO_REMITO = @PEDIDO_ID
                        AND TMP.PRODUCTO_ID = @PRODUCTO_ID AND TMP.PALLET_PICKING = @NRO_CONTENEDORA
                        AND TMP.NRO_LINEA = @NRO_LINEA
                        AND PICKING.PICKING_ID = TMP.PICKING_ID
                        AND PICKING.PALLET_PICKING = TMP.PALLET_PICKING
                        AND PICKING.PALLET_CONTROLADO <> '0'
                END
            FETCH cur_productos INTO @PRODUCTO_ID, @NRO_LINEA
        END
    CLOSE cur_productos
    DEALLOCATE cur_productos
    
    DELETE TMP_EMPAQUE_CONTENEDORA WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID
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
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[CerrarProductoEnContenedora]
	-- Add the parameters for the stored procedure here
	@cliente_id		varchar(15) OUTPUT,
	@nro_remito		varchar(30) OUTPUT,
	@producto_id	varchar(30) OUTPUT,
	@cant_elegida	numeric(20,5) OUTPUT,
	@contenedora	numeric(20,0) OUTPUT
AS
BEGIN

	DECLARE @cursorProducto		cursor
	DECLARE @picking_id			numeric(20,0)
	DECLARE @cant_confirmada	numeric(20,5)

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--AGARRO LOS PRODUCTOS LIBERADOS(SIN CONTENEDORA) DEL PEDIDO
	SET @cursorProducto = cursor FOR
	SELECT		p.picking_id,
				p.cant_confirmada
	FROM		picking p
				INNER JOIN	documento d
					on ((d.cliente_id = p.cliente_id) AND (d.documento_id = p.documento_id))
	WHERE		d.cliente_id = @cliente_id
				AND d.nro_remito = @nro_remito
				AND p.producto_id = @producto_id
				AND p.facturado = '0'
				AND p.st_camion = '0'
				AND p.pallet_controlado = '0'
				AND p.cant_confirmada is not null
				AND d.tipo_operacion_id = 'EGR'
	ORDER BY	p.cant_confirmada


	OPEN @cursorProducto
	FETCH NEXT FROM @cursorProducto INTO @picking_id, @cant_confirmada
		
	WHILE ((@@FETCH_STATUS = 0) AND (@cant_elegida - @cant_confirmada) >= 0)
	BEGIN
		
		SET @cant_elegida = @cant_elegida - @cant_confirmada
		
		-- CIERRO LA CANTIDAD DEL PRODUCTO SELECCIONADO
		UPDATE	picking
		SET		pallet_picking = @contenedora,
				pallet_controlado = '1'
		WHERE	picking_id = @picking_id
		
		FETCH NEXT FROM @cursorProducto INTO @picking_id, @cant_confirmada
	END


	--en este punto si @cant_elegida_AUX < 0 entonces tenemos seleccionado el producto que hay que "PRORRATEAR"
	IF ((@cant_elegida - @cant_confirmada < 0) AND (@cant_elegida > 0) AND (@@fetch_status=0))
	BEGIN
		insert into picking 
		(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, PALLET_CERRADO, USUARIO_PF, TERMINAL_PF, REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS, BULTOS_NO_CONTROLADOS, FLG_PALLET_HOMBRE, TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE ) 
		select	DOCUMENTO_ID,
				NRO_LINEA,
				CLIENTE_ID,
				PRODUCTO_ID,
				VIAJE_ID,
				TIPO_CAJA,
				DESCRIPCION,
				@cant_elegida,
				NAVE_COD,
				POSICION_COD,
				RUTA,
				PROP1,
				FECHA_INICIO,
				FECHA_FIN,
				USUARIO,
				@cant_elegida, --CANTIDAD RESTANTE ELEJIDA (cant_confirmada)
				@contenedora, --CONTENEDORA GENERADA (pallet_picking)
				SALTO_PICKING,
				'1', --PALLET_CONTROLADO
				USUARIO_CONTROL_PICK,
				ST_ETIQUETAS,
				ST_CAMION,
				FACTURADO,
				FIN_PICKING,
				ST_CONTROL_EXP,
				FECHA_CONTROL_PALLET,
				TERMINAL_CONTROL_PALLET,
				FECHA_CONTROL_EXP,
				USUARIO_CONTROL_EXP,
				TERMINAL_CONTROL_EXP,
				FECHA_CONTROL_FAC,
				USUARIO_CONTROL_FAC,
				TERMINAL_CONTROL_FAC,
				VEHICULO_ID,
				PALLET_COMPLETO,
				HIJO,
				QTY_CONTROLADO,
				@contenedora, --PALLET_FINAL
				PALLET_CERRADO,
				USUARIO_PF,
				TERMINAL_PF,
				REMITO_IMPRESO,
				NRO_REMITO_PF,
				PICKING_ID_REF,
				BULTOS_CONTROLADOS,
				BULTOS_NO_CONTROLADOS,
				FLG_PALLET_HOMBRE,
				TRANSF_TERMINADA,
				NRO_LOTE,
				NRO_PARTIDA,
				NRO_SERIE
		from picking where picking_id = @picking_id

		UPDATE PICKING SET	CANTIDAD = CANTIDAD - @CANT_ELEGIDA,
							CANT_CONFIRMADA = CANT_CONFIRMADA - @CANT_ELEGIDA
		WHERE PICKING_ID = @PICKING_ID
	END
	------------------------------------------------------------------------------------------------------

	CLOSE @cursorProducto
	DEALLOCATE @cursorProducto

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

ALTER  proc [dbo].[chObjOwner]( @usrName varchar(20), @newUsrName varchar(50))
as
-- @usrName is the current user
-- @newUsrName is the new user

set nocount on
declare @uid int                   -- UID of the user
declare @objName varchar(50)       -- Object name owned by user
declare @currObjName varchar(50)   -- Checks for existing object owned by new user 
declare @outStr varchar(256)       -- SQL command with 'sp_changeobjectowner'
set @uid = user_id(@usrName)

declare chObjOwnerCur cursor static
for
select name from sysobjects where uid = @uid

open chObjOwnerCur
if @@cursor_rows = 0
begin
  print 'Error: No objects owned by ' + @usrName
  close chObjOwnerCur
  deallocate chObjOwnerCur
  return 1
end

fetch next from chObjOwnerCur into @objName

while @@fetch_status = 0
begin
  set @currObjName = @newUsrName + "." + @objName
  if (object_id(@currObjName) > 0)
    print 'WARNING *** ' + @currObjName + ' already exists ***'
  set @outStr = "sp_changeobjectowner '" + @usrName + "." + @objName + "','" + @newUsrName + "'"
  print @outStr
  print 'go'
  fetch next from chObjOwnerCur into @objName
end

close chObjOwnerCur
deallocate chObjOwnerCur
set nocount off
return 0
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

ALTER PROCEDURE [dbo].[CLR_CONSUMO_LOCATOR_EGR]
@FECHA	AS VARCHAR(10)	OUTPUT
AS
BEGIN

	DECLARE @MyFECHA AS DATETIME	
	
	IF @FECHA IS NOT NULL
	BEGIN	
		SET @MyFECHA = CONVERT(VARCHAR,@FECHA, 103)		

		DELETE
		FROM	CONSUMO_LOCATOR_EGR	
		WHERE 	PROCESADO='S' 
				AND CONVERT(VARCHAR,FECHA,103) <= @MyFECHA
				AND DOCUMENTO_ID IN(SELECT 	DOCUMENTO_ID
									FROM	DOCUMENTO	
									WHERE	CONSUMO_LOCATOR_EGR.DOCUMENTO_ID=DOCUMENTO.DOCUMENTO_ID
											AND DOCUMENTO.STATUS IN ('D30','D40'))
	END
	
	IF @FECHA IS NULL
	BEGIN
	
		DELETE 
		FROM 	CONSUMO_LOCATOR_EGR	
		WHERE	PROCESADO='S'
				AND DOCUMENTO_ID IN(SELECT 	DOCUMENTO_ID
									FROM	DOCUMENTO	
									WHERE	CONSUMO_LOCATOR_EGR.DOCUMENTO_ID=DOCUMENTO.DOCUMENTO_ID
											AND DOCUMENTO.STATUS IN ('D30','D40'))

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

ALTER PROCEDURE [dbo].[CLR_DET_DOCUMENTO_AUX]
AS
BEGIN

	
		DELETE
		FROM 	DET_DOCUMENTO_AUX	
		WHERE	DOCUMENTO_ID IN(SELECT 	DOCUMENTO_ID
								FROM	DOCUMENTO	
								WHERE	DET_DOCUMENTO_AUX.DOCUMENTO_ID=DOCUMENTO.DOCUMENTO_ID
										AND DOCUMENTO.STATUS IN ('D30','D40'))

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

ALTER Procedure [dbo].[ConfEtibyProd]
@Cliente_Id		varchar(20),
@Producto_ID	varchar(20),
@Msg			Varchar(max) Output
As
Begin
	Declare @Flg	Char(1)
	Declare @Qty	Numeric(20,0)
	Declare @Count	SmallInt

	Select	@Count=Count(*)
	from	producto
	where	Cliente_id=@Cliente_id
			and Producto_id=@Producto_ID

	if @Count=0
	Begin
		raiserror('No se encontro el producto %s para el cliente %s',16,1,@producto_id,@cliente_id)
		return
	End
	Else
	Begin
		Select	@Flg=flg_bulto, @qty=qty_bulto
		from	producto
		where	Cliente_id=@Cliente_id
				and Producto_id=@Producto_ID

		if (@Flg is null) or (@Flg='0')
		Begin
			Set @Msg='No se generaran etiquetas para este producto.'
			return
		end
		else
		begin
			Set @Msg='Se generaran etiquetas para este producto.'
			return
		end
	end
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

ALTER   PROCEDURE [dbo].[CONTROL_PICKING_STATUS]
	@PALLET 	AS NUMERIC(20,0),
	@USUARIO 	AS VARCHAR(30),
	@STATUS		AS CHAR(1)
AS
BEGIN

	UPDATE 	PICKING 
	SET 	PALLET_CONTROLADO=@STATUS,
			USUARIO_CONTROL_PICK=LTRIM(RTRIM(UPPER(@USUARIO))),
			FECHA_CONTROL_PALLET=GETDATE(),
			TERMINAL_CONTROL_PALLET=HOST_NAME()
	WHERE 	PALLET_PICKING=@PALLET


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

ALTER        PROCEDURE [dbo].[CONTROL_PICKING_PALLET]
	@PALLET_PIC AS NUMERIC(20,0),
	@USUARIO AS VARCHAR(30)
AS
BEGIN
	SELECT 	
			PRODUCTO_ID AS PRODUCTO_ID,DESCRIPCION AS DESCRIPCION, cast(SUM(CANT_CONFIRMADA) as int) AS CANTIDAD
	FROM 	PICKING
	WHERE	PALLET_PICKING=@PALLET_PIC AND FECHA_INICIO IS NOT NULL AND
			FECHA_FIN IS NOT NULL AND USUARIO IS NOT NULL AND
			PALLET_PICKING IS NOT NULL AND CANT_CONFIRMADA>0
	GROUP BY PRODUCTO_ID, DESCRIPCION
	
	--NO COMENTAR SE LEVANTA EN OTRO TABLE PARA SU USO POSTERIOR.
	SELECT 	CAST(SUM(CANT_CONFIRMADA) AS INT)
	FROM 	PICKING 
	WHERE 	PALLET_PICKING=@PALLET_PIC

	EXEC DBO.CONTROL_PICKING_STATUS @PALLET=@PALLET_PIC,@USUARIO=@USUARIO,@STATUS='1'
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

/*
CREATE TABLE DBO.CONTROL_APF(
	EAN				VARCHAR(50),
	SUCURSAL		VARCHAR(20),
	CLIENTE_ID		VARCHAR(15),
	PRODUCTO_ID		VARCHAR(30),
	PALLET_INF		VARCHAR(20),
	OBS				VARCHAR(100)
)
*/
ALTER PROCEDURE [dbo].[CONTROL_PROC_APF]
		@EAN		VARCHAR(50),
		@SUCURSAL	VARCHAR(20),
		@PALLET_INF	VARCHAR(20)
AS
BEGIN
	DECLARE @PRODUCTO_ID	VARCHAR(30)
	DECLARE @CLIENTE_ID		VARCHAR(15)

	SET @CLIENTE_ID='LEADER PRICE'
	
	SELECT	@PRODUCTO_ID=PRODUCTO_ID
	FROM	RL_PRODUCTO_CODIGOS
	WHERE	CLIENTE_ID=@CLIENTE_ID
			AND CODIGO=LTRIM(RTRIM(UPPER(@EAN)))

	IF (@PRODUCTO_ID IS NULL)
	BEGIN
		INSERT INTO CONTROL_APF VALUES(@EAN, @SUCURSAL, @CLIENTE_ID, @PRODUCTO_ID, @PALLET_INF,'NO SE ENCONTRO EL PRODUCTO PARA EL EAN INFORMADO.')
	END

	IF (@PRODUCTO_ID IS NOT NULL)
	BEGIN
		INSERT INTO CONTROL_APF VALUES(@EAN, @SUCURSAL, @CLIENTE_ID, @PRODUCTO_ID, @PALLET_INF,NULL)
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

ALTER  PROCEDURE [dbo].[CONTROL_VIAJE]
	@VIAJE_ID 	AS VARCHAR(50),
	@VALUE		AS CHAR(1)
AS
BEGIN
	DECLARE @VAR VARCHAR(1)
	SET @VAR='A'
--	UPDATE PICKING SET ST_CAMION=@VALUE
--	WHERE	VIAJE_ID=LTRIM(RTRIM(UPPER(@VIAJE_ID)))

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