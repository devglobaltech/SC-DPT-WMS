IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GenerarEtiquetaEmpaque]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GenerarEtiquetaEmpaque]
GO

create PROCEDURE [dbo].[GenerarEtiquetaEmpaque]
	-- Add the parameters for the stored procedure here
	@cliente_id		varchar(15)		OUTPUT,
	@pedido			varchar(100)		OUTPUT,
	@contenedora	numeric(20,0)	OUTPUT,
	@reporte		varchar(100) = null output

AS

BEGIN

	SET NOCOUNT ON;
	

	SELECT  --DISTINCT 
			max(isnull(S.SUCURSAL_ID,'') + ' - ' + isnull(S.NOMBRE,'')) AS[SUCURSAL],
			max(ISNULL(Prov.Descripcion,'') + ', ' + ISNULL(S.LOCALIDAD,'') + ', ' + ISNULL(s.calle,'') + ', ' + ISNULL(s.numero,'')) as [DIRE],
			max(P.viaje_id) as [VIAJE_ID],
			@pedido as [PEDIDO],
			@contenedora as	[CONTENEDORA]
			,(Select Imagen from Imagenes_Reporte Where Cliente = @cliente_id and Reporte = @Reporte) ImagenLogo
	FROM    PICKING P
			INNER JOIN DOCUMENTO D
				ON(P.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S
				ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_DESTINO)
			LEFT JOIN PROVINCIA Prov
				ON (Prov.provincia_id = s.provincia_id)

	WHERE   D.NRO_REMITO=@pedido
			AND D.CLIENTE_ID=@cliente_id
   
END





