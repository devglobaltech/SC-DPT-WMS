/****** Object:  StoredProcedure [dbo].[DocumentosIngresosExpo]    Script Date: 05/20/2014 15:46:27 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DocumentosIngresosExpo]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[DocumentosIngresosExpo]
GO

CREATE Procedure [dbo].[DocumentosIngresosExpo]
	@pFechaDesde	datetime Output,
	@pFechaHasta	datetime Output,
	@pDocumento_id	numeric  Output,
	@Tipo			varchar	 Output
As
Begin

	if @Tipo='D'
	begin
		SELECT	D.NRO_DESPACHO_IMPORTACION [Purchasedocumentnumber], DD.PRODUCTO_ID [Materialnumber],'R501' AS [PlantCode],
				'0001' [Storagelocation], Null [StockType],DD.CANTIDAD [Quantity], dd.NRO_SERIE [Serialnumber], 
				p.PAIS_ID [Countryoforigin], D.CPTE_PREFIJO +'-'+ d.CPTE_NUMERO  [Notafiscalnumber]
		FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
				ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN PRODUCTO P
				ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				LEFT JOIN SYS_INT_DET_DOCUMENTO SDD
				ON(DD.DOCUMENTO_ID=SDD.DOCUMENTO_ID AND DD.PRODUCTO_ID=SDD.PRODUCTO_ID)
		WHERE	D.TIPO_OPERACION_ID='ING'
				AND D.STATUS='D40'
				AND D.DOCUMENTO_ID=@pDocumento_id
		ORDER BY
				DD.DOCUMENTO_ID,DD.NRO_LINEA			
	end
	if (@Tipo='C')
	begin
		SELECT	DISTINCT DD.DOCUMENTO_ID
		FROM	DOCUMENTO D INNER JOIN DET_DOCUMENTO DD
				ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)
				INNER JOIN PRODUCTO P
				ON(DD.CLIENTE_ID=P.CLIENTE_ID AND DD.PRODUCTO_ID=P.PRODUCTO_ID)
				LEFT JOIN SYS_INT_DET_DOCUMENTO SDD
				ON(DD.DOCUMENTO_ID=SDD.DOCUMENTO_ID AND DD.PRODUCTO_ID=SDD.PRODUCTO_ID)
				LEFT JOIN SYS_INT_DOCUMENTO SD
				ON(SDD.CLIENTE_ID=SD.CLIENTE_ID AND SDD.DOC_EXT=SD.DOC_EXT)
		WHERE	D.TIPO_OPERACION_ID='ING'
				AND D.STATUS='D40'
				AND ((@pDocumento_id IS NULL)OR(D.DOCUMENTO_ID=@pDocumento_id))
				AND ((@pFechaDesde IS NULL)OR(D.FECHA_ALTA_GTW BETWEEN @pFechaDesde AND dateadd(d,1,@pFechaHasta)))
		ORDER BY
				DD.DOCUMENTO_ID		
	end		
End--Fin Procedure.
GO


