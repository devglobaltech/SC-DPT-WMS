/****** Object:  StoredProcedure [dbo].[eliminar_caja_contenedora_empaque]    Script Date: 10/16/2013 16:23:45 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[eliminar_caja_contenedora_empaque]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[eliminar_caja_contenedora_empaque]
GO

create PROCEDURE [dbo].[eliminar_caja_contenedora_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(100) OUTPUT,
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
    
    delete from UC_EMPAQUE where UC_EMPAQUE=@NRO_CONTENEDORA;
    
    update PICKING set NRO_UCEMPAQUETADO=null where NRO_UCEMPAQUETADO=@NRO_CONTENEDORA
    
END

GO


