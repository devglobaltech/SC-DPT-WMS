/****** Object:  StoredProcedure [dbo].[GenerarEtiquetaEmpaque]    Script Date: 10/13/2015 10:56:12 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GenerarEtiquetaImpresionBulto]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GenerarEtiquetaImpresionBulto]
GO


create PROCEDURE [dbo].[GenerarEtiquetaImpresionBulto]
	@contenedora	numeric(20,0)	OUTPUT
AS

BEGIN

	SET NOCOUNT ON;
	
	SELECT  max(isnull(S.SUCURSAL_ID,'') + ' - ' + isnull(S.NOMBRE,'')) AS[SUCURSAL],
			max(ISNULL(Prov.Descripcion,'') + ', ' + ISNULL(S.LOCALIDAD,'') + ', ' + ISNULL(s.calle,'') + ', ' + ISNULL(s.numero,'')) as [DIRE],
			NULL			as [VIAJE_ID],
			NULL			as [PEDIDO],
			dd.nro_bulto	as [CONTENEDORA],
			(select Imagen AS ImagenLogo from Imagenes_Reporte where CLIENTE=DD.CLIENTE_ID and Reporte='Ar_EtiquetaEmpaque')
	FROM    DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
			ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
			INNER JOIN SUCURSAL S
			ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_ORIGEN)
			LEFT JOIN PROVINCIA Prov
			ON (Prov.provincia_id = s.provincia_id)
	WHERE   DD.NRO_BULTO=@contenedora
			AND D.TIPO_OPERACION_ID='ING'
	GROUP BY 
			DD.NRO_BULTO,DD.CLIENTE_ID
   
END
