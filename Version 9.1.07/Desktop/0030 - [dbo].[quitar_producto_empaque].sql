
/****** Object:  StoredProcedure [dbo].[quitar_producto_empaque]    Script Date: 04/11/2014 14:32:00 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[quitar_producto_empaque]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[quitar_producto_empaque]
GO

/****** Object:  StoredProcedure [dbo].[quitar_producto_empaque]    Script Date: 04/11/2014 14:32:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		LRojas
-- Create date: 19/04/2012
-- Description:	Procedimiento para quitar unidades empaquetadas
-- =============================================
CREATE PROCEDURE [dbo].[quitar_producto_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(100) OUTPUT,
	@NRO_LOTE			AS VARCHAR(100) OUTPUT,
	@NRO_PARTIDA		AS VARCHAR(100) OUTPUT,
	@NRO_SERIE			AS VARCHAR(50) OUTPUT,
	@PRODUCTO_ID        as varchar(30) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT,
    @CANT_CONTROLADA    as numeric(20,5) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    DECLARE @DOCUMENTO AS NUMERIC
    
    DECLARE @P_PICKING_ID AS NUMERIC
    DECLARE @CANT_AUX AS NUMERIC
            
	SELECT	@DOCUMENTO=DOCUMENTO_ID
	FROM	DOCUMENTO
	WHERE	NRO_REMITO=@PEDIDO_ID
			AND CLIENTE_ID=@CLIENTE_ID;
	
	--VERIFICO SI HAY UN REGISTRO EN PICKING CON EL TOTAL PARA SACAR DE LA CONTENEDORA
	
	SELECT TOP 1 @P_PICKING_ID = PICKING_ID
	FROM	PICKING
	WHERE	DOCUMENTO_ID = @DOCUMENTO AND
			PRODUCTO_ID = @PRODUCTO_ID AND 
			PALLET_PICKING = @NRO_CONTENEDORA AND
			CANT_CONFIRMADA = @CANT_CONTROLADA AND
			PALLET_CONTROLADO = '1' AND
			((@NRO_LOTE IS NULL OR @NRO_LOTE='') OR (NRO_LOTE = @NRO_LOTE))AND
			((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA='') OR (NRO_PARTIDA = @NRO_PARTIDA))AND
			((@NRO_SERIE IS NULL OR @NRO_SERIE='') OR (NRO_SERIE = @NRO_SERIE));
	
	IF (@P_PICKING_ID IS NULL)
		BEGIN
		--TENGO QUE VERIFICAR SI LA CANTIDAD TOTAL A DESEMPAQUETAR ES MAYOR A LA DE UN REGISTRO 
		SELECT TOP 1 @CANT_AUX = CANT_CONFIRMADA
		FROM	PICKING
		WHERE	DOCUMENTO_ID = @DOCUMENTO AND
				PRODUCTO_ID = @PRODUCTO_ID AND 
				PALLET_PICKING = @NRO_CONTENEDORA AND
				PALLET_CONTROLADO = '1' AND
				((@NRO_LOTE IS NULL OR @NRO_LOTE='') OR (NRO_LOTE = @NRO_LOTE))AND
				((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA='') OR (NRO_PARTIDA = @NRO_PARTIDA))AND
				((@NRO_SERIE IS NULL OR @NRO_SERIE='') OR (NRO_SERIE = @NRO_SERIE))
		ORDER BY CANT_CONFIRMADA DESC;
		
		--SI ES MAYOR AL MAXIMO
		IF (@CANT_CONTROLADA > @CANT_AUX)
			BEGIN
			--TENGO QUE ACTUALIZAR DE MENOR A MAYOR Y CON LO RESTANTE SPLITEAR SI NO LLEGA A COMPLETAR
			--DECLARO UN CURSOR PARA LOOPEAR POR LOS PICKINGS HASTA QUE SE DESEMPAQUE EL TOTAL
			DECLARE @CUR			CURSOR
			DECLARE @PICKID			NUMERIC(20,5)
			DECLARE @CANT			NUMERIC(20,5)
			DECLARE @CANT_DESEMP	NUMERIC
			
			SET @CANT_DESEMP = 0
			
			SET @CUR= CURSOR FOR
			--TRAIGO LAS CANTIDADES DE MENOR A MAYOR CANTIDAD
			SELECT 	PICKING_ID,CANT_CONFIRMADA
			FROM	PICKING
			WHERE	DOCUMENTO_ID = @DOCUMENTO AND
					PRODUCTO_ID = @PRODUCTO_ID AND 
					PALLET_PICKING = @NRO_CONTENEDORA AND
					PALLET_CONTROLADO = '1' AND
					((@NRO_LOTE IS NULL OR @NRO_LOTE='') OR (NRO_LOTE = @NRO_LOTE))AND
					((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA='') OR (NRO_PARTIDA = @NRO_PARTIDA))AND
					((@NRO_SERIE IS NULL OR @NRO_SERIE='') OR (NRO_SERIE = @NRO_SERIE))
			ORDER BY CANT_CONFIRMADA ASC;
			OPEN @CUR
			FETCH NEXT FROM @CUR INTO @PICKID,@CANT
				WHILE @@FETCH_STATUS=0 AND (@CANT_CONTROLADA > @CANT_DESEMP)
				BEGIN
				
				IF (@CANT > (@CANT_CONTROLADA - @CANT_DESEMP))
					BEGIN
					SET @P_PICKING_ID = @PICKID
					SET @CANT_CONTROLADA = (@CANT_CONTROLADA - @CANT_DESEMP)
					GOTO SPLIT;
					END
				ELSE
					BEGIN
				
					UPDATE PICKING SET PALLET_CONTROLADO = '0' WHERE PICKING_ID=@PICKID
					
					SET @CANT_DESEMP = @CANT_DESEMP + @CANT
					END
					
				FETCH NEXT FROM @CUR INTO @PICKID,@CANT
				END
			CLOSE @CUR
			DEALLOCATE @CUR
			END
		ELSE
			BEGIN		
			--TENGO QUE SPLITEAR LAS LINEAS DE PICKING
			SELECT TOP 1 @P_PICKING_ID = PICKING_ID
			FROM	PICKING
			WHERE	DOCUMENTO_ID = @DOCUMENTO AND
					PRODUCTO_ID = @PRODUCTO_ID AND 
					PALLET_PICKING = @NRO_CONTENEDORA AND
					PALLET_CONTROLADO = '1' AND
					((@NRO_LOTE IS NULL OR @NRO_LOTE='') OR (NRO_LOTE = @NRO_LOTE))AND
					((@NRO_PARTIDA IS NULL OR @NRO_PARTIDA='') OR (NRO_PARTIDA = @NRO_PARTIDA))AND
					((@NRO_SERIE IS NULL OR @NRO_SERIE='') OR (NRO_SERIE = @NRO_SERIE))
			ORDER BY CANT_CONFIRMADA DESC;
			SPLIT:
			
			--UPDATEO PICKING
			UPDATE PICKING SET CANT_CONFIRMADA= CANT_CONFIRMADA - @CANT_CONTROLADA,
			PALLET_PICKING=@NRO_CONTENEDORA WHERE PICKING_ID=@P_PICKING_ID
			--INSERTO NUEVO REGISTRO CON LA CANTIDAD SACADA
			INSERT INTO PICKING		
				SELECT 	 DOCUMENTO_ID			,NRO_LINEA			,CLIENTE_ID			,PRODUCTO_ID
						,VIAJE_ID				,TIPO_CAJA			,DESCRIPCION		,CANTIDAD
						,NAVE_COD				,POSICION_COD		,RUTA				,PROP1
						,FECHA_INICIO			,FECHA_FIN			,USUARIO			,@CANT_CONTROLADA		
						,PALLET_PICKING			,0					,'0'				,NULL		
						,ST_ETIQUETAS			,ST_CAMION			,FACTURADO			,FIN_PICKING
						,ST_CONTROL_EXP			,NULL				,NULL				,NULL
						,NULL					,NULL				,NULL				,NULL
						,NULL					,NULL				,NULL				,HIJO
						,NULL					,NULL				,NULL				,NULL
						,NULL					,REMITO_IMPRESO		,NRO_REMITO_PF		,ISNULL(PICKING_ID_REF,PICKING_ID)
						,NULL					,BULTOS_NO_CONTROLADOS					,FLG_PALLET_HOMBRE
						,TRANSF_TERMINADA		,NRO_LOTE			,NRO_PARTIDA		,NULL
						,NULL AS ESTADO
						,NULL AS NRO_UCDESCONSOLIDACION
						,NULL AS FECHA_DESCONSOLIDACION
						,NULL AS USUARIO_DESCONSOLIDACION
						,NULL AS TERMINAL_DESCONSOLIDACION
						,NULL AS NRO_UEMPAQUETADO
						,NULL AS UCEMPAQUETADO_MEDIDAS
						,NULL AS FECHA_UCEMPAQUETADO
						,NULL AS UCEMPAQUETADO_PESO
				FROM	PICKING
				WHERE	PICKING_ID=@P_PICKING_ID
			END
		END
	ELSE
		BEGIN
		UPDATE PICKING SET PALLET_CONTROLADO = '0' WHERE PICKING_ID = @P_PICKING_ID;
		END
END


GO

