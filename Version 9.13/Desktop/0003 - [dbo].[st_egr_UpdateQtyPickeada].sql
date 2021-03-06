
ALTER       PROCEDURE [dbo].[st_egr_UpdateQtyPickeada]
@pPicking_id     varchar(100) output,
@pCantidad	 Numeric(20,5) output
AS

BEGIN
	Declare @xUSER 		as varchar (20)
	Declare @NEW_PALLET 	as NUMERIC(38) 
	Declare @ViajeId	as varchar(100)
	Declare @Cantidad	as Float
	Declare @Dif		as Float
	declare @vDocumento as numeric(20)

	Select @xUSER = usuario_id from #temp_usuario_loggin
	--SET @xUSER ='ADMIN';
	
	exec dbo.get_value_for_sequence 'PALLET_PICKING', @NEW_PALLET output

	UPDATE	picking 
	SET 	cant_confirmada = @pCantidad,
			usuario = @xUSER,
			fecha_inicio = getdate(),
			fecha_fin = getdate(),
			pallet_picking = @NEW_PALLET		
	WHERE 	picking_id = @pPicking_id

	Select	Distinct
			@ViajeId=Viaje_id
	From	Picking
	Where	Picking_Id=@pPicking_id


	SELECT 	@CANTIDAD=COUNT(PICKING_ID)
	FROM	PICKING
	WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(UPPER(RTRIM(@VIAJEID)))
	
	
	SELECT 	@DIF=COUNT(PICKING_ID)
	FROM 	PICKING 
	WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(UPPER(RTRIM(@VIAJEID)))
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL
	
	
	IF @CANTIDAD=@DIF
	BEGIN
		UPDATE PICKING SET FIN_PICKING='2' WHERE LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
	END
	ELSE
	BEGIN
		UPDATE PICKING SET FIN_PICKING='1' WHERE LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
	END
	--------------------------------------------------------------------------------------------------------------
	-- CORRECCION TRACKER ID: 7415, COMPARTIDO CON FABIAN OHET.
	--------------------------------------------------------------------------------------------------------------
	
	Select	Distinct
			@vDocumento=DOCUMENTO_ID
	From	Picking
	Where	Picking_Id=@pPicking_id

	SELECT 	@CANTIDAD=COUNT(PICKING_ID)
	FROM	PICKING
	WHERE 	DOCUMENTO_ID=@vDocumento 
	
	SELECT 	@DIF=COUNT(PICKING_ID)
	FROM 	PICKING 
	WHERE 	DOCUMENTO_ID=@vDocumento
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL	
			
	IF @CANTIDAD=@DIF
	BEGIN
		UPDATE PICKING SET FIN_PICKING='2' WHERE LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
	END
	ELSE
	BEGIN
		UPDATE PICKING SET FIN_PICKING='1' WHERE LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
	END			
			
END
