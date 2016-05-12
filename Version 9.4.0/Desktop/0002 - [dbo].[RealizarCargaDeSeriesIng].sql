/****** Object:  StoredProcedure [dbo].[RealizarCargaDeSeriesIng]    Script Date: 11/18/2014 09:34:34 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RealizarCargaDeSeriesIng]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[RealizarCargaDeSeriesIng]
GO

CREATE PROCEDURE [dbo].[RealizarCargaDeSeriesIng]
@IDPROCESO NUMERIC (20, 0) OUTPUT
AS
BEGIN
    DECLARE @err AS VARCHAR (100);
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF OBJECT_ID('tempdb..#BULTOS') IS NOT NULL
        DROP TABLE #BULTOS;
    IF OBJECT_ID('tempdb..#PRODUCTOS') IS NOT NULL
        DROP TABLE #PRODUCTOS;
    IF OBJECT_ID('tempdb..#SERIES') IS NOT NULL
        DROP TABLE #SERIES;
        
    DECLARE @CLIENTE_ID				AS VARCHAR (15);
    DECLARE @NRO_BULTO				AS VARCHAR (100);
    DECLARE @PRODUCTO_ID			AS VARCHAR (30);
    DECLARE @SERIE					AS VARCHAR (100);
    DECLARE @CANTIDAD				AS NUMERIC (20, 5);
    DECLARE @CURRL					AS CURSOR;
    DECLARE @CURDATOS				AS CURSOR;
    DECLARE @CURCANT				AS CURSOR;
    DECLARE @CURSERIES				AS CURSOR;
    DECLARE @DOCUMENTO_ID			AS NUMERIC (20, 0);
    DECLARE @NRO_LINEA				AS NUMERIC (20, 0);
    DECLARE @NUMSERIE				AS VARCHAR (100);
    DECLARE @NRO_LINEA_NEW			AS NUMERIC (20, 0);
    DECLARE @NRO_LINEA_NEW_TRANS	AS NUMERIC (20, 0);
    DECLARE @RL_ID					AS NUMERIC (20, 0);
    DECLARE @CANTIDADRL				AS NUMERIC (20, 5);
    DECLARE @CANTSTOCK				AS NUMERIC (20, 5);
    DECLARE @SALGOCURRL				AS VARCHAR (1);
    
	----------------------------------------------------------
	-- Pregunto si los cursores existen, si estan los elimino
	----------------------------------------------------------
	IF (SELECT CURSOR_STATUS('global','@CURRL')) >= -1
	BEGIN
		DEALLOCATE @CURRL
	END	
	
	IF (SELECT CURSOR_STATUS('global','@CURDATOS')) >= -1
	BEGIN
		DEALLOCATE @CURDATOS
	END	
	
	IF (SELECT CURSOR_STATUS('global','@CURCANT')) >= -1
	BEGIN
		DEALLOCATE @CURCANT
	END	
	
	IF (SELECT CURSOR_STATUS('global','@CURSERIES')) >= -1
	BEGIN
		DEALLOCATE @CURSERIES
	END			

    UPDATE  CargaSeriesLog
        SET VALIDA = '1'
    WHERE   IDPROCESO = @IDPROCESO;
    DELETE ResultadosCargaSeriesLog
    WHERE  IDPROCESO = @IDPROCESO;
    --VALIDO QUE NO HAYAN SERIES DUPLICADAS.
    IF EXISTS (SELECT   IDPROCESO,
                        CLIENTE_ID,
                        NRO_BULTO,
                        PRODUCTO_ID,
                        SERIE
               FROM     CargaSeriesLog
               WHERE    IDPROCESO = @IDPROCESO
               GROUP BY IDPROCESO, CLIENTE_ID, NRO_BULTO, PRODUCTO_ID, SERIE
               HAVING   COUNT(*) > 1)
        INSERT  INTO ResultadosCargaSeriesLog
        VALUES (@IDPROCESO, 'Existen series duplicadas en el archivo cargado.', 0);
    --controlo si surgió algun error, si es así salgo
    IF EXISTS (SELECT 1
               FROM   ResultadosCargaSeriesLog
               WHERE  IDPROCESO = @IDPROCESO)
        BEGIN
            UPDATE  CargaSeriesLog
                SET VALIDA = '0'
            WHERE   IDPROCESO = @IDPROCESO;
            RETURN;
        END
    --VALIDO EL CLIENTE_ID
    INSERT INTO ResultadosCargaSeriesLog
    SELECT DISTINCT CS.IDPROCESO,
                    'El Cliente ' + CS.CLIENTE_ID + ' no existe.',
                    1
    FROM   CargaSeriesLog AS CS
    WHERE  CS.IDPROCESO = @IDPROCESO
           AND NOT EXISTS (SELECT 1
                           FROM   CLIENTE
                           WHERE  CLIENTE_ID = CS.CLIENTE_ID);
    UPDATE  CargaSeriesLog
        SET VALIDA = '0'
    WHERE   IDPROCESO = @IDPROCESO
            AND CLIENTE_ID NOT IN (SELECT CLIENTE_ID
                                   FROM   CLIENTE);
    --VALIDO QUE EXISTA LA CONETENEDORA EN STOCK
    SELECT DISTINCT CS.NRO_BULTO
    INTO   #BULTOS
    FROM   CargaSeriesLog AS CS
    WHERE  CS.IDPROCESO = @IDPROCESO
           AND NOT EXISTS (SELECT 1
                           FROM   DOCUMENTO AS D
                                  INNER JOIN
                                  DET_DOCUMENTO AS DD
                                  ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
                                  INNER JOIN
                                  DET_DOCUMENTO_TRANSACCION AS DDT
                                  ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID
                                      AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC)
                                  INNER JOIN
                                  RL_DET_DOC_TRANS_POSICION AS RL
                                  ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID
                                      AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
                           WHERE  D.STATUS = 'D30'
                                  AND DD.NRO_BULTO IS NOT NULL
                                  AND DD.NRO_BULTO = CS.NRO_BULTO);
    INSERT INTO ResultadosCargaSeriesLog
    SELECT @IDPROCESO,
           'La contenedora ' + NRO_BULTO + ' no existe en stock.',
           2
    FROM   #BULTOS;
    UPDATE  CargaSeriesLog
        SET VALIDA = '0'
    WHERE   IDPROCESO = @IDPROCESO
            AND NRO_BULTO IN (SELECT NRO_BULTO
                              FROM   #BULTOS);
    --VALIDO QUE LOS PRODUCTOS TENGAN SERIE AL INGRESO
    SELECT DISTINCT CS.CLIENTE_ID,
                    CS.PRODUCTO_ID
    INTO   #PRODUCTOSFLAG
    FROM   CargaSeriesLog AS CS
    WHERE  CS.IDPROCESO = @IDPROCESO
           AND EXISTS (SELECT 1
                       FROM   PRODUCTO AS P
                       WHERE  CLIENTE_ID = CS.CLIENTE_ID
                              AND PRODUCTO_ID = CS.PRODUCTO_ID
                              AND ISNULL(SERIE_ING, '0') = '0');
    INSERT INTO ResultadosCargaSeriesLog
    SELECT @IDPROCESO,
           'El producto ' + PRODUCTO_ID + ' no tiene habilitada la carga de Series.',
           3
    FROM   #PRODUCTOSFLAG;
    UPDATE  CargaSeriesLog
        SET VALIDA = '0'
    FROM    CargaSeriesLog AS a
            INNER JOIN
            #PRODUCTOSFLAG AS b
            ON a.CLIENTE_ID = b.CLIENTE_ID
               AND a.PRODUCTO_ID = b.PRODUCTO_ID
    WHERE   a.IDPROCESO = @IDPROCESO;
    --VALIDO QUE EXISTA PRODUCTO/CONETENEDORA EN STOCK
    SELECT DISTINCT CS.CLIENTE_ID,
                    CS.NRO_BULTO,
                    CS.PRODUCTO_ID
    INTO   #PRODUCTOS
    FROM   CargaSeriesLog AS CS
    WHERE  CS.IDPROCESO = @IDPROCESO
           AND NOT EXISTS (SELECT 1
                           FROM   DOCUMENTO AS D
                                  INNER JOIN
                                  DET_DOCUMENTO AS DD
                                  ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
                                  INNER JOIN
                                  DET_DOCUMENTO_TRANSACCION AS DDT
                                  ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID
                                      AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC)
                                  INNER JOIN
                                  RL_DET_DOC_TRANS_POSICION AS RL
                                  ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID
                                      AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
                           WHERE  D.STATUS = 'D30'
                                  AND DD.NRO_BULTO IS NOT NULL
                                  AND DD.CLIENTE_ID = CS.CLIENTE_ID
                                  AND DD.NRO_BULTO = CS.NRO_BULTO
                                  AND DD.PRODUCTO_ID = CS.PRODUCTO_ID);
    INSERT INTO ResultadosCargaSeriesLog
    SELECT @IDPROCESO,
           'El producto ' + PRODUCTO_ID + ' no existe dentro de la contenedora ' + NRO_BULTO + '.',
           4
    FROM   #PRODUCTOS;
    UPDATE  CargaSeriesLog
        SET VALIDA = '0'
    FROM    CargaSeriesLog AS a
            INNER JOIN
            #PRODUCTOS AS b
            ON a.CLIENTE_ID = b.CLIENTE_ID
               AND a.NRO_BULTO = b.NRO_BULTO
               AND a.PRODUCTO_ID = b.PRODUCTO_ID
    WHERE   a.IDPROCESO = @IDPROCESO;
    --CONTROLO QUE LA SERIE NO EXISTA
    SELECT CS.CLIENTE_ID,
           CS.PRODUCTO_ID,
           CS.SERIE
    INTO   #SERIES
    FROM   CargaSeriesLog AS CS
    WHERE  CS.IDPROCESO = @IDPROCESO
           AND EXISTS (SELECT 1
                       FROM   DOCUMENTO AS D
                              INNER JOIN
                              DET_DOCUMENTO AS DD
                              ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
                              INNER JOIN
                              DET_DOCUMENTO_TRANSACCION AS DDT
                              ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID
                                  AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC)
                              INNER JOIN
                              RL_DET_DOC_TRANS_POSICION AS RL
                              ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID
                                  AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
                       WHERE  DD.CLIENTE_ID = CS.CLIENTE_ID
                              AND DD.PRODUCTO_ID = CS.PRODUCTO_ID
                              AND DD.NRO_SERIE = CS.SERIE);
    INSERT INTO ResultadosCargaSeriesLog
    SELECT @IDPROCESO,
           'El número de Serie ' + SERIE + ' ya existe.',
           5
    FROM   #SERIES;
    UPDATE  CargaSeriesLog
        SET VALIDA = '0'
    WHERE   IDPROCESO = @IDPROCESO
            AND SERIE IN (SELECT SERIE
                          FROM   #SERIES);
    --CONTROLO CANTIDADES DE PRODUCTOS EN LA CONTENEDORA VERSUS LO QUE HAY EN STOCK
    --ESTE CONTROL ES CON LOS VALIDADOS = '1'
    SET @CURCANT = CURSOR
        FOR SELECT   DISTINCT CLIENTE_ID,
                              NRO_BULTO,
                              PRODUCTO_ID,
                              COUNT(*)
            FROM     CargaSeriesLog
            WHERE    IDPROCESO = @IDPROCESO
                     AND VALIDA = '1'
            GROUP BY CLIENTE_ID, NRO_BULTO, PRODUCTO_ID;
    OPEN @CURCANT;
    FETCH NEXT FROM @CURCANT INTO @CLIENTE_ID, @NRO_BULTO, @PRODUCTO_ID, @CANTIDAD;
    WHILE @@FETCH_STATUS = 0
        BEGIN
            SELECT @CANTSTOCK = SUM(RL.CANTIDAD)
            FROM   DOCUMENTO AS D
                   INNER JOIN
                   DET_DOCUMENTO AS DD
                   ON (D.DOCUMENTO_ID = DD.DOCUMENTO_ID)
                   INNER JOIN
                   DET_DOCUMENTO_TRANSACCION AS DDT
                   ON (DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID
                       AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC)
                   INNER JOIN
                   RL_DET_DOC_TRANS_POSICION AS RL
                   ON (DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID
                       AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS)
            WHERE  D.STATUS = 'D30'
                   AND D.CLIENTE_ID = @CLIENTE_ID
                   AND DD.NRO_BULTO IS NOT NULL
                   AND DD.NRO_BULTO = @NRO_BULTO
                   AND DD.PRODUCTO_ID = @PRODUCTO_ID;
            IF NOT (@CANTSTOCK = @CANTIDAD)
                BEGIN
                    INSERT  INTO ResultadosCargaSeriesLog
                    VALUES (@IDPROCESO, 'La cantidad de series del producto ' + @PRODUCTO_ID + ' en la contenedora ' + @NRO_BULTO + ' ingresadas es distinta a la cantidad del producto en stock.', 6);
                    UPDATE  CargaSeriesLog
                        SET VALIDA = '0'
                    WHERE   CLIENTE_ID = @CLIENTE_ID
                            AND NRO_BULTO = @NRO_BULTO
                            AND PRODUCTO_ID = @PRODUCTO_ID;
                END
            FETCH NEXT FROM @CURCANT INTO @CLIENTE_ID, @NRO_BULTO, @PRODUCTO_ID, @CANTIDAD;
        END
    CLOSE @CURCANT;
    DEALLOCATE @CURCANT;
    --controlo si surgió algun error, si es así salgo
    IF EXISTS (SELECT 1
               FROM   ResultadosCargaSeriesLog
               WHERE  IDPROCESO = @IDPROCESO)
        BEGIN
            UPDATE  CargaSeriesLog
                SET VALIDA = '0'
            WHERE   IDPROCESO = @IDPROCESO;
            RETURN;
        END
    --AHORA TENGO QUE INSERTAR LAS SERIES.

        --PRIMERO CONSIGO DATOS GRALES, CLIENTE_ID, NRO_BULTO, PRODUCTO_ID, SERIE
        --LUEGO BUSCO LAS RL CON ESOS DATOS.
        --POR CADA RL BUSCO TODAS LAS SERIES VALIDADAS Y NO CARGADAS
        --POR CADA UNA HAGO EL INSERT/UPDATE
        SET @CURDATOS = CURSOR
            FOR SELECT CLIENTE_ID,
                       NRO_BULTO,
                       PRODUCTO_ID,
                       SERIE
                FROM   CargaSeriesLog
                WHERE  ISNULL(VALIDA, '0') = '1'
                       AND ISNULL(CARGADA, '0') = '0'
                       AND IDPROCESO = @IDPROCESO;
        OPEN @CURDATOS;
        FETCH NEXT FROM @CURDATOS INTO @CLIENTE_ID, @NRO_BULTO, @PRODUCTO_ID, @SERIE;
        WHILE @@FETCH_STATUS = 0
            BEGIN
                --POR CADA DATO GRAL BUSCO LAS RL
                SET @CURRL = CURSOR
                    FOR SELECT   RL.RL_ID,
                                 RL.CANTIDAD,
                                 DD.DOCUMENTO_ID,
                                 DD.NRO_LINEA
                        FROM     RL_DET_DOC_TRANS_POSICION AS RL
                                 INNER JOIN
                                 DET_DOCUMENTO_TRANSACCION AS DDT
                                 ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID
                                     AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
                                 INNER JOIN
                                 DET_DOCUMENTO AS DD
                                 ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID
                                     AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
                        WHERE    DD.CLIENTE_ID = @CLIENTE_ID
                                 AND DD.NRO_BULTO IS NOT NULL
                                 AND DD.NRO_BULTO = @NRO_BULTO
                                 AND DD.PRODUCTO_ID = @PRODUCTO_ID
                        ORDER BY RL.CANTIDAD;
                OPEN @CURRL;
                FETCH NEXT FROM @CURRL INTO @RL_ID, @CANTIDADRL, @DOCUMENTO_ID, @NRO_LINEA;
                WHILE @@FETCH_STATUS = 0
                    BEGIN
                        SET @CURSERIES = CURSOR
                            FOR SELECT SERIE
                                FROM   CargaSeriesLog
                                WHERE  ISNULL(VALIDA, '0') = '1'
                                       AND ISNULL(CARGADA, '0') = '0'
                                       AND IDPROCESO = @IDPROCESO
                                       AND CLIENTE_ID = @CLIENTE_ID
                                       AND NRO_BULTO = @NRO_BULTO
                                       AND PRODUCTO_ID = @PRODUCTO_ID;
                        OPEN @CURSERIES;
                        FETCH NEXT FROM @CURSERIES INTO @SERIE;
                        SET @SALGOCURRL = '0';
                        WHILE @@FETCH_STATUS = 0
                              AND @SALGOCURRL = '0'
                            BEGIN
                                IF (@CANTIDADRL = 1)
                                    BEGIN
                                        UPDATE  DET_DOCUMENTO
                                            SET NRO_SERIE = @SERIE
                                        WHERE   DOCUMENTO_ID = @DOCUMENTO_ID
                                                AND NRO_LINEA = @NRO_LINEA;
                                        UPDATE  CargaSeriesLog
                                            SET CARGADA = '1'
                                        WHERE   IDPROCESO = @IDPROCESO
                                                AND NRO_BULTO = @NRO_BULTO
                                                AND PRODUCTO_ID = @PRODUCTO_ID
                                                AND SERIE = @SERIE;
                                        SET @SALGOCURRL = '1';
                                    END
                                ELSE
                                    BEGIN
                                        --LA CANTIDAD EN RL ES MAYOR A 1
                                        SELECT @NRO_LINEA_NEW = MAX(NRO_LINEA) + 1
                                        FROM   DET_DOCUMENTO
                                        WHERE  DOCUMENTO_ID = @DOCUMENTO_ID;
                                        INSERT INTO DET_DOCUMENTO
                                        SELECT DOCUMENTO_ID,
                                               @NRO_LINEA_NEW,
                                               CLIENTE_ID,
                                               PRODUCTO_ID,
                                               1,
                                               @SERIE,
                                               NRO_SERIE_PADRE,
                                               EST_MERC_ID,
                                               CAT_LOG_ID,
                                               NRO_BULTO,
                                               DESCRIPCION,
                                               NRO_LOTE,
                                               FECHA_VENCIMIENTO,
                                               NRO_DESPACHO,
                                               NRO_PARTIDA,
                                               UNIDAD_ID,
                                               PESO,
                                               UNIDAD_PESO,
                                               VOLUMEN,
                                               UNIDAD_VOLUMEN,
                                               BUSC_INDIVIDUAL,
                                               TIE_IN,
                                               NRO_TIE_IN_PADRE,
                                               NRO_TIE_IN,
                                               ITEM_OK,
                                               CAT_LOG_ID_FINAL,
                                               MONEDA_ID,
                                               COSTO,
                                               PROP1,
                                               PROP2,
                                               PROP3,
                                               LARGO,
                                               ALTO,
                                               ANCHO,
                                               VOLUMEN_UNITARIO,
                                               PESO_UNITARIO,
                                               CANT_SOLICITADA,
                                               TRACE_BACK_ORDER
                                        FROM   DET_DOCUMENTO
                                        WHERE  DOCUMENTO_ID = @DOCUMENTO_ID
                                               AND NRO_LINEA = @NRO_LINEA;
                                        SELECT @NRO_LINEA_NEW_TRANS = MAX(NRO_LINEA_TRANS) + 1
                                        FROM   DET_DOCUMENTO_TRANSACCION
                                        WHERE  DOCUMENTO_ID = @DOCUMENTO_ID;
                                        INSERT INTO DET_DOCUMENTO_TRANSACCION
                                        SELECT DOC_TRANS_ID,
                                               @NRO_LINEA_NEW_TRANS,
                                               DOCUMENTO_ID,
                                               @NRO_LINEA_NEW,
                                               MOTIVO_ID,
                                               EST_MERC_ID,
                                               CLIENTE_ID,
                                               CAT_LOG_ID,
                                               ITEM_OK,
                                               MOVIMIENTO_PENDIENTE,
                                               DOC_TRANS_ID_REF,
                                               NRO_LINEA_TRANS_REF
                                        FROM   DET_DOCUMENTO_TRANSACCION
                                        WHERE  DOCUMENTO_ID = @DOCUMENTO_ID
                                               AND NRO_LINEA_DOC = @NRO_LINEA;
                                        INSERT INTO RL_DET_DOC_TRANS_POSICION
                                        SELECT RL.DOC_TRANS_ID,
                                               @NRO_LINEA_NEW_TRANS,
                                               RL.POSICION_ANTERIOR,
                                               RL.POSICION_ACTUAL,
                                               1,
                                               RL.TIPO_MOVIMIENTO_ID,
                                               RL.ULTIMA_ESTACION,
                                               RL.ULTIMA_SECUENCIA,
                                               RL.NAVE_ANTERIOR,
                                               RL.NAVE_ACTUAL,
                                               RL.DOCUMENTO_ID,
                                               RL.NRO_LINEA,
                                               RL.DISPONIBLE,
                                               RL.DOC_TRANS_ID_EGR,
                                               RL.NRO_LINEA_TRANS_EGR,
                                               RL.DOC_TRANS_ID_TR,
                                               RL.NRO_LINEA_TRANS_TR,
                                               RL.CLIENTE_ID,
                                               RL.CAT_LOG_ID,
                                               RL.CAT_LOG_ID_FINAL,
                                               RL.EST_MERC_ID
                                        FROM   RL_DET_DOC_TRANS_POSICION AS RL
                                        WHERE  RL.RL_ID = @RL_ID;
                                        UPDATE  DET_DOCUMENTO
                                            SET CANTIDAD = CANTIDAD - 1
                                        WHERE   DOCUMENTO_ID = @DOCUMENTO_ID
                                                AND NRO_LINEA = @NRO_LINEA;
                                        UPDATE  RL_DET_DOC_TRANS_POSICION
                                            SET CANTIDAD = CANTIDAD - 1
                                        WHERE   RL_ID = @RL_ID;
                                        SET @CANTIDADRL = @CANTIDADRL - 1;
                                        UPDATE  CargaSeriesLog
                                            SET CARGADA = '1'
                                        WHERE   IDPROCESO = @IDPROCESO
                                                AND CLIENTE_ID = @CLIENTE_ID
                                                AND NRO_BULTO = @NRO_BULTO
                                                AND PRODUCTO_ID = @PRODUCTO_ID
                                                AND SERIE = @SERIE;
                                    END
                                FETCH NEXT FROM @CURSERIES INTO @SERIE;
                            END
                        CLOSE @CURSERIES;
                        DEALLOCATE @CURSERIES;
                        FETCH NEXT FROM @CURRL INTO @RL_ID, @CANTIDADRL, @DOCUMENTO_ID, @NRO_LINEA;
                    END
                CLOSE @CURRL;
                DEALLOCATE @CURRL;
                FETCH NEXT FROM @CURDATOS INTO @CLIENTE_ID, @NRO_BULTO, @PRODUCTO_ID, @SERIE;
            END
        CLOSE @CURDATOS;
        DEALLOCATE @CURDATOS;
        SELECT @err = @@error;
        
END
GO


