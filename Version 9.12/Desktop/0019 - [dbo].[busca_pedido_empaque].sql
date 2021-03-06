IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[busca_pedido_empaque]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[busca_pedido_empaque]
GO

-- =============================================
-- Author:		LRojas
-- Create date: 16/04/2012
-- Description:	Procedimiento para buscar pedidos para empaquetar
-- =============================================
create PROCEDURE [dbo].[busca_pedido_empaque] 
    @PEDIDO_ID as varchar(100) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
    DECLARE @CountPed           as Integer,
            @DOCUMENTO_ID       as Numeric(20),
            @CountProd          as Integer,
            @TIPO_OPERACION_ID  as Varchar(5), 
            @STATUS             as Varchar(3), 
            @NRO_REMITO         as Varchar(30), 
            @FACTURADO          as Char(1), 
            @ST_CAMION          as Char(1),
            @MSJ_ERR            as Varchar(Max)
    
    SELECT @CountPed = Count(*), @DOCUMENTO_ID = D.DOCUMENTO_ID
      FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
     INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA) 
     INNER JOIN SUCURSAL S ON(S.SUCURSAL_ID = D.SUCURSAL_DESTINO AND S.CLIENTE_ID = P.CLIENTE_ID) 
     INNER JOIN SYS_INT_DOCUMENTO ID ON (ID.CLIENTE_ID = P.CLIENTE_ID AND ID.DOC_EXT = D.NRO_REMITO) -- 
     WHERE D.TIPO_OPERACION_ID = 'EGR' 
       AND D.STATUS = 'D30' 
       AND D.NRO_REMITO IS NOT NULL 
       AND P.FACTURADO = '0' 
       AND P.ST_CAMION = '0' 
       AND D.NRO_REMITO = @PEDIDO_ID 
     GROUP BY D.DOCUMENTO_ID
    
    IF @CountPed > 0 
        BEGIN
            SELECT @CountProd = COUNT(*)
            FROM PICKING WHERE DOCUMENTO_ID = @DOCUMENTO_ID
            AND USUARIO IS NOT NULL
            AND FECHA_INICIO IS NOT NULL
            AND FECHA_FIN IS NOT NULL
            AND CANT_CONFIRMADA IS NOT NULL
            
            IF @CountPed = @CountProd
                BEGIN
                    SELECT DISTINCT 
                           P.CLIENTE_ID, D.NRO_REMITO AS [NRO PEDIDO], 
                           LTRIM(RTRIM(ISNULL(D.CPTE_PREFIJO, '') + ' ' + ISNULL(D.CPTE_NUMERO, ''))) AS [NRO REMITO], 
                           S.NOMBRE AS [SUCURSAL DESTINO] 
                      FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
                     INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA) 
                     INNER JOIN SUCURSAL S ON(S.SUCURSAL_ID = D.SUCURSAL_DESTINO AND S.CLIENTE_ID = P.CLIENTE_ID) 
                     INNER JOIN SYS_INT_DOCUMENTO ID ON (ID.CLIENTE_ID = P.CLIENTE_ID AND ID.DOC_EXT = D.NRO_REMITO) -- 
                     WHERE D.TIPO_OPERACION_ID = 'EGR' 
                       AND D.STATUS = 'D30' 
                       AND D.NRO_REMITO IS NOT NULL 
                       AND P.FACTURADO = '0' 
                       AND P.ST_CAMION = '0' 
                       AND D.NRO_REMITO = @PEDIDO_ID 
                
                    DELETE TMP_EMPAQUE_CONTENEDORA WHERE NRO_REMITO = @PEDIDO_ID 
                END
            ELSE
                RAISERROR('Todos los productos deben estar Pickeados.', 16, 1)
        END
    ELSE
        BEGIN
            IF NOT EXISTS(SELECT 1 FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
                          INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
                          WHERE D.NRO_REMITO = @PEDIDO_ID)
                BEGIN
                    RAISERROR
                        (N'El pedido %s no existe.',
                        16, -- Severity.
                        1, -- State.
                        @PEDIDO_ID, -- First substitution argument.
                        @PEDIDO_ID); -- Second substitution argument.
                END
            ELSE
                BEGIN
                    SELECT @TIPO_OPERACION_ID = D.TIPO_OPERACION_ID, 
                           @STATUS = D.STATUS, 
                           @FACTURADO = P.FACTURADO, 
                           @ST_CAMION = P.ST_CAMION
                      FROM DOCUMENTO D INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID = DD.DOCUMENTO_ID) 
                     INNER JOIN PICKING P ON(DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA) 
                     INNER JOIN SUCURSAL S ON(S.SUCURSAL_ID = D.SUCURSAL_DESTINO) 
                     INNER JOIN SYS_INT_DOCUMENTO ID ON (ID.CLIENTE_ID = P.CLIENTE_ID AND ID.DOC_EXT = D.NRO_REMITO) -- 
                     WHERE D.NRO_REMITO = @PEDIDO_ID 
                    
                    SET @MSJ_ERR = ''
                    
                    IF @TIPO_OPERACION_ID <> 'EGR'
                        SET @MSJ_ERR = @MSJ_ERR + 'Documento no es Egreso. '
                    
                    IF @STATUS <> 'D30'
                        SET @MSJ_ERR = @MSJ_ERR + 'Estado no es D30. '
                    
                    IF @FACTURADO <> '0'
                        SET @MSJ_ERR = @MSJ_ERR + 'Pedido ya facturado. '
                    
                    IF @ST_CAMION <> '0'
                        SET @MSJ_ERR = @MSJ_ERR + 'Ya se encuentra en el vehiculo. '
                        
                    SET @MSJ_ERR = @PEDIDO_ID + ': ' + @MSJ_ERR
                    
                    RAISERROR
                        (N'Error pedido %s',
                        16, -- Severity.
                        1, -- State.
                        @MSJ_ERR, -- First substitution argument.
                        @MSJ_ERR); -- Second substitution argument.
                END
        END
END
