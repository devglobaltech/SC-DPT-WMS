CREATE PROCEDURE DBO.MOB_RG_CONTENIDO_PALLET
	@PALLET		AS VARCHAR(100)
AS
BEGIN

	SELECT	PRODUCTO_ID		AS [COD.PRODUCTO],
			CANTIDAD		AS [CANTIDAD],
			NRO_LOTE		AS [NRO.LOTE],
			NRO_PARTIDA		AS [NRO.PARTIDA],
			F_VENCIMIENTO	AS [F.VTO],
			RCP_ID			AS [ID]
	FROM	RECEPCION_GUARDADO
	WHERE	NRO_PALLET=@PALLET 
		
END