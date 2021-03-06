
CREATE PROCEDURE [dbo].[getFlagVtoProducto]
	@CLIENTE_ID		VARCHAR(15),
	@PRODUCTO_ID	VARCHAR(30),
	@OUTFVTO		VARCHAR(1) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;

	SELECT @OUTFVTO = ISNULL(COUNT(*),'0') FROM MANDATORIO_PRODUCTO 
		WHERE CLIENTE_ID = @CLIENTE_ID
		AND PRODUCTO_ID = @PRODUCTO_ID
		AND CAMPO = 'FECHA_VENCIMIENTO'

	SET @OUTFVTO = ISNULL(@OUTFVTO,'0')
	
END
