
/****** Object:  StoredProcedure [dbo].[cerrar_tmp_producto_empaque]    Script Date: 07/01/2014 15:54:19 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cerrar_tmp_producto_empaque]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[cerrar_tmp_producto_empaque]
GO

CREATE PROCEDURE [dbo].[cerrar_tmp_producto_empaque]
	@CLIENTE_ID         as varchar(15) OUTPUT,
	@PEDIDO_ID          as varchar(100) OUTPUT,
    @NRO_CONTENEDORA    as numeric(20) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
    
	DECLARE @GUIA			as varchar(100),
			@Control		as numeric,
			@DOCUMENTO		AS NUMERIC
			
	SELECT	@DOCUMENTO=DOCUMENTO_ID
	FROM	DOCUMENTO
	WHERE	NRO_REMITO=@PEDIDO_ID
			AND CLIENTE_ID=@CLIENTE_ID
			AND TIPO_OPERACION_ID = 'EGR';
			
	DELETE PICKING WHERE DOCUMENTO_ID = @DOCUMENTO
			AND PRODUCTO_ID IN (
			SELECT DISTINCT PRODUCTO_ID FROM TMP_EMPAQUE_CONTENEDORA
			WHERE DOCUMENTO_ID = @DOCUMENTO);
	
	INSERT INTO PICKING
	SELECT	DOCUMENTO_ID,NRO_LINEA,CLIENTE_ID,PRODUCTO_ID,VIAJE_ID,TIPO_CAJA,DESCRIPCION,CANTIDAD,NAVE_COD,POSICION_COD,
			RUTA,PROP1,FECHA_INICIO,FECHA_FIN,USUARIO,CANT_CONFIRMADA,PALLET_PICKING,SALTO_PICKING,PALLET_CONTROLADO,
			USUARIO_CONTROL_PICK,ST_ETIQUETAS,ST_CAMION,FACTURADO,FIN_PICKING,ST_CONTROL_EXP,FECHA_CONTROL_PALLET,
			TERMINAL_CONTROL_PALLET,FECHA_CONTROL_EXP,USUARIO_CONTROL_EXP,TERMINAL_CONTROL_EXP,FECHA_CONTROL_FAC,
			USUARIO_CONTROL_FAC,TERMINAL_CONTROL_FAC,VEHICULO_ID,PALLET_COMPLETO,HIJO,QTY_CONTROLADO,PALLET_FINAL,
			PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO,NRO_REMITO_PF,PICKING_ID_REF,BULTOS_CONTROLADOS,
			BULTOS_NO_CONTROLADOS,FLG_PALLET_HOMBRE,TRANSF_TERMINADA,NRO_LOTE,NRO_PARTIDA,NRO_SERIE,ESTADO,
			NRO_UCDESCONSOLIDACION,FECHA_DESCONSOLIDACION,USUARIO_DESCONSOLIDACION,TERMINAL_DESCONSOLIDACION,
			NRO_UCEMPAQUETADO,UCEMPAQUETADO_MEDIDAS,FECHA_UCEMPAQUETADO,UCEMPAQUETADO_PESO
    FROM TMP_EMPAQUE_CONTENEDORA WHERE DOCUMENTO_ID = @DOCUMENTO
    
    --1. Aca completo la unidad contenedora en el campo nro_contenedora.
    update	PICKING set NRO_UCEMPAQUETADO=PALLET_PICKING
    from	PICKING p inner join DOCUMENTO d on(p.DOCUMENTO_ID=d.DOCUMENTO_ID)
    where	d.CLIENTE_ID=@CLIENTE_ID
			and d.NRO_REMITO=@PEDIDO_ID
			AND PICKING_ID IN
	(SELECT		PICKING_ID 
	--FROM		TMP_EMPAQUE_CONTENEDORA
	--ESTABA MAL QUE LO SAQUE DE TMP_EMPAQUE_CONTENEDORA PORQUE TRAIA LOS PICKING ID ANTERIORES
	FROM		PICKING P INNER JOIN DOCUMENTO D ON(P.DOCUMENTO_ID=D.DOCUMENTO_ID)
	WHERE		D.CLIENTE_ID = @CLIENTE_ID 
				AND D.NRO_REMITO = @PEDIDO_ID
				AND P.PALLET_PICKING = @NRO_CONTENEDORA);
    --2. Ademas hago que se cree la UC contenedora.
    INSERT INTO UC_EMPAQUE (UC_EMPAQUE,ALTO,ANCHO,LARGO,VOLUMEN)
    SELECT	DISTINCT
			P.NRO_UCEMPAQUETADO,0,0,0,0
    FROM	PICKING P INNER JOIN DOCUMENTO D ON(P.DOCUMENTO_ID=D.DOCUMENTO_ID)
    WHERE	PICKING_ID IN
    (SELECT PICKING_ID 
    --LO MISMO PARA ESTE CASO
	--FROM TMP_EMPAQUE_CONTENEDORA 
	FROM PICKING P INNER JOIN DOCUMENTO D ON(P.DOCUMENTO_ID=D.DOCUMENTO_ID)
		WHERE D.CLIENTE_ID = @CLIENTE_ID 
		AND D.NRO_REMITO = @PEDIDO_ID
		AND P.PALLET_PICKING = @NRO_CONTENEDORA)
		AND NOT EXISTS (SELECT	1 
							FROM	UC_EMPAQUE U2
							WHERE	P.NRO_UCEMPAQUETADO=U2.UC_EMPAQUE);
				
    --3. Si tiene expedicion obligatoria genero la guia.
	SELECT	@control=COUNT(P.PICKING_ID)
	FROM	PICKING P INNER JOIN DOCUMENTO D ON(P.DOCUMENTO_ID=D.DOCUMENTO_ID)
	WHERE	d.CLIENTE_ID=@CLIENTE_ID
			AND D.NRO_REMITO=@PEDIDO_ID
			AND P.NRO_UCEMPAQUETADO IS NULL
			
	if @control=0 begin	    
		EXEC [dbo].[GENERAR_GUIA] @PEDIDO_ID, @GUIA output
	end
	
    DELETE TMP_EMPAQUE_CONTENEDORA WHERE CLIENTE_ID = @CLIENTE_ID AND NRO_REMITO = @PEDIDO_ID

END



GO


