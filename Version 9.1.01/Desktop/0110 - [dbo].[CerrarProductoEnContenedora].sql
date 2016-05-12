/****** Object:  StoredProcedure [dbo].[CerrarProductoEnContenedora]    Script Date: 10/11/2013 16:26:06 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CerrarProductoEnContenedora]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[CerrarProductoEnContenedora]
GO


CREATE PROCEDURE [dbo].[CerrarProductoEnContenedora]
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
	DECLARE @UC_CONT			numeric(20,0)

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
		
		select @UC_CONT=COUNT(*) from UC_EMPAQUE where UC_EMPAQUE=@contenedora;
		
		if @UC_CONT=0 Begin
			insert into UC_EMPAQUE (UC_EMPAQUE,ALTO,ANCHO,LARGO,VOLUMEN)values(@contenedora,0,0,0,0);
		end
		
		SET @cant_elegida = @cant_elegida - @cant_confirmada
		
		-- CIERRO LA CANTIDAD DEL PRODUCTO SELECCIONADO
		UPDATE	picking
		SET		pallet_picking = @contenedora,
				NRO_UCEMPAQUETADO= @contenedora,
				pallet_controlado = '1',
				FECHA_UCEMPAQUETADO=GETDATE(),
				UCEMPAQUETADO_PESO=0
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


