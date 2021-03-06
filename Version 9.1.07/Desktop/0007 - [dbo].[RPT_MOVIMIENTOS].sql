
/****** Object:  StoredProcedure [dbo].[RPT_MOVIMIENTOS]    Script Date: 04/14/2014 15:56:11 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RPT_MOVIMIENTOS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[RPT_MOVIMIENTOS]
GO

create             PROCEDURE [dbo].[RPT_MOVIMIENTOS]
	@CLIENTE_ID		VARCHAR(15)		OUTPUT,
	@PRODUCTO_ID	VARCHAR(30)		OUTPUT,
	@NRO_PALLET		VARCHAR(100)	OUTPUT,
	@NRO_PARTIDA	VARCHAR(50)		OUTPUT,
	@FECHA_VTO		VARCHAR(8)		OUTPUT,	--ANSI
	@F_DESDE		VARCHAR(8)		OUTPUT,	--ANSI
	@F_HASTA		VARCHAR(8)		OUTPUT,	--ANSI
	@NRO_LOTE		VARCHAR(50)		OUTPUT,
	@PROP2			VARCHAR(100)	OUTPUT,
	@PROP3			VARCHAR(100)	OUTPUT,
	@USUARIO		VARCHAR(20) 	OUTPUT,
	@COD_PEDIDO	VARCHAR(30)			OUTPUT,
	@COD_VIAJE	VARCHAR(100)		OUTPUT,
	@QSERIE			VARCHAR(50)		OUTPUT,
	@NRO_DESPACHO	VARCHAR(50)		OUTPUT	
AS
BEGIN
	/*
	----------------------------------------------------------------------
	CREATE TABLE #TEMP_CRITERIOS_RPT(
		TIPO_AUDITORIA_ID NUMERIC(20,0)
	)
	INSERT INTO #TEMP_CRITERIOS_RPT
	SELECT 	TIPO_AUDITORIA_ID
	FROM	PARAMETROS_AUDITORIA
	*/
	----------------------------------------------------------------------
	--					DECLARACION DE VARIABLES.
	----------------------------------------------------------------------
	DECLARE @SALDO_INICIAL	FLOAT
	DECLARE @TERMINAL_RTP	VARCHAR(100)
	DECLARE @USUARIO_RTP	VARCHAR(20)
	DECLARE @P_FECHA_I		VARCHAR(8)
	DECLARE @CONT			FLOAT
	
	DECLARE @PICK			NUMERIC(20,0)
	DECLARE @SERIE			VARCHAR(100)
	DECLARE @PICK_ANT			NUMERIC(20,0)
	DECLARE @SERIE_ACUM			VARCHAR(3000)
	----------------------------------------------------------------------
	SET 	@TERMINAL_RTP	=HOST_NAME()
	SELECT	@USUARIO_RTP	=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN
	--SET	@USUARIO_RTP	='USER'
	
	SELECT 	@CONT=COUNT(*)
	FROM	#TEMP_CRITERIOS_RPT
	WHERE	TIPO_AUDITORIA_ID IN(4,15,16)

	IF @CONT=3
	BEGIN
		SET @P_FECHA_I=@F_DESDE
	END
	IF (@NRO_PALLET IS NOT NULL) OR(@NRO_PARTIDA IS NOT NULL)OR (@FECHA_VTO IS NOT NULL)
	    OR(@NRO_LOTE IS NOT NULL) OR (@PROP2 IS NOT NULL) OR(@PROP3 IS NOT NULL)OR(@USUARIO IS NOT NULL)
	    OR (@NRO_DESPACHO IS NOT NULL) OR (@QSERIE IS NOT NULL)
	BEGIN
	       SET @P_FECHA_I=NULL
	END
	
	SELECT 	 DBO.GET_SALDO_INICIAL(AH.CLIENTE_ID,AH.PRODUCTO_ID,@P_FECHA_I,@F_HASTA)AS [SD_INICIAL]
			,AH.CLIENTE_ID + ' - ' +C.RAZON_SOCIAL			AS [CLIENTE_RZ]
			,AH.PRODUCTO_ID
			,PA.DESCRIPCION									AS [DESC_OP]
			,'Usuario: ' + @USUARIO_RTP						AS [USOINTERNOUSUARIO]
			,'Terminal: ' + @TERMINAL_RTP					AS [USOINTERNOTERMINAL]
			,AH.FECHA_AUDITORIA
			,CONVERT(VARCHAR,AH.FECHA_AUDITORIA,103) 		AS [FECHA]
			,TC.DESCRIPCION	+ CASE WHEN D.NRO_DESPACHO_IMPORTACION IS NULL THEN 
			'' ELSE ' - ' + D.NRO_DESPACHO_IMPORTACION END AS [DESC_TIPO_COMPROBANTE]
			,AH.USUARIO_ID + (CASE WHEN SU.NOMBRE IS NULL THEN '' ELSE ' - ' + SU.NOMBRE END) AS [USUARIO_OPERACION]
			,AH.TERMINAL									AS [TERMINAL_OPERACION]
			,CAST(CASE WHEN P.POSICION_COD IS NULL THEN ISNULL(N2.NAVE_COD,ISNULL(N.NAVE_COD,'PREING')) ELSE P.POSICION_COD END AS VARCHAR)AS [UBICACION]
			,AH.CANTIDAD
			,AH.DOC_EXT										AS [DOC_EXT]
			,AH.CAT_LOG_ID
			,DD.PRODUCTO_ID + ' - ' + DD.DESCRIPCION		AS [PROD_DESC]
			,D.NRO_REMITO
			,D.CPTE_PREFIJO
		    ,D.CPTE_NUMERO
		    ,D.DOCUMENTO_ID
		    ,D.TIPO_COMPROBANTE_ID
			,AH.CLIENTE_ID
			,CASE 
			  	WHEN AH.NRO_SERIE IS NULL THEN '' 
			  	Else 'Nro.Serie: ' + CAST(AH.NRO_SERIE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_BULTO IS NULL THEN '' 
			    Else 'Nro.Bulto: ' + CAST(AH.NRO_BULTO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_LOTE IS NULL THEN '' 
			    Else 'Nro.Lote: ' + CAST(AH.NRO_LOTE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_DESPACHO IS NULL THEN '' 
			    Else 'Nro.Despacho: ' + CAST(AH.NRO_DESPACHO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.NRO_PARTIDA IS NULL THEN '' 
			    Else 'Nro.Partida: ' + CAST(AH.NRO_PARTIDA AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.PROP1 IS NULL THEN '' 
			    Else 'Nro.Pallet: ' + CAST(AH.PROP1 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.PROP2 IS NULL THEN '' 
			    Else 'Lote Prov.: ' + CAST(AH.PROP2 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.PROP3 IS NULL THEN '' 
			    Else 'Property 3: ' + CAST(AH.PROP3 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN AH.FECHA_VENCIMIENTO IS NULL THEN '' 
			    Else 'Fecha Vencimiento: ' + CONVERT(VARCHAR,AH.FECHA_VENCIMIENTO,103) + ', '
			 END +
			 CASE
				WHEN D.SUCURSAL_ORIGEN IS NULL THEN ''
				ELSE 'Proveedor: ' +CAST(D.SUCURSAL_ORIGEN AS VARCHAR) + ' - ' +CAST(S.NOMBRE AS VARCHAR)+', '
			 END +
			 CASE
				WHEN AH.EST_MERC_ID IS NULL THEN ''
				ELSE 'Est.Merc. : ' + CAST(AH.EST_MERC_ID AS VARCHAR)
			 END +
			 CASE
				WHEN D.NRO_REMITO IS NULL THEN ''
				ELSE 'Nro. Remito : ' + CAST(D.NRO_REMITO AS VARCHAR)
			 END AS [PROPIEDADES]
			,AH.NRO_SERIE
			,AH.NRO_BULTO
			,AH.NRO_LOTE
			,AH.NRO_DESPACHO
			,AH.NRO_PARTIDA				
			,AH.PROP1						[NRO_PALLET]
			,AH.PROP2						[LOTE_PROVEEDOR]
			,AH.FECHA_VENCIMIENTO			[FECHA_VENCIMIENTO]
			,D.SUCURSAL_ORIGEN				[ORIGEN_DESTINO]
			,S.NOMBRE		
			,AH.EST_MERC_ID				
			,AH.AUDITORIA_ID
	FROM	AUDITORIA_HISTORICOS AH (NOLOCK)
			INNER JOIN vDOCUMENTO D	(NOLOCK)						ON(AH.DOCUMENTO_ID=D.DOCUMENTO_ID)
			LEFT JOIN TIPO_COMPROBANTE TC (NOLOCK)					ON(D.TIPO_COMPROBANTE_ID=TC.TIPO_COMPROBANTE_ID)
			LEFT JOIN POSICION P (NOLOCK)							ON(AH.POSICION_ID_FINAL=P.POSICION_ID)
			LEFT JOIN NAVE	N 	(NOLOCK)							ON(AH.NAVE_ID_FINAL=N.NAVE_ID)
			LEFT JOIN NAVE  N2	(NOLOCK)							ON(P.POSICION_ID=N2.NAVE_ID)
			INNER JOIN PARAMETROS_AUDITORIA PA (NOLOCK) 			ON(AH.TIPO_AUDITORIA_ID=PA.TIPO_AUDITORIA_ID)
			INNER JOIN #TEMP_CRITERIOS_RPT TMPC (NOLOCK)			ON(AH.TIPO_AUDITORIA_ID=TMPC.TIPO_AUDITORIA_ID)
			INNER JOIN CLIENTE C (NOLOCK)							ON(AH.CLIENTE_ID=C.CLIENTE_ID)
			INNER JOIN vDET_DOCUMENTO DD(NOLOCK)					ON(AH.DOCUMENTO_ID=DD.DOCUMENTO_ID AND AH.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN SYS_USUARIO SU (NOLOCK)						ON(AH.USUARIO_ID=SU.USUARIO_ID)
			LEFT JOIN SUCURSAL S(NOLOCK)							ON(D.CLIENTE_ID=S.CLIENTE_ID AND D.SUCURSAL_ORIGEN=S.SUCURSAL_ID)
	WHERE	((@NRO_PALLET IS NULL)OR(AH.PROP1=@NRO_PALLET))
			AND((@F_DESDE IS NULL)OR(AH.FECHA_AUDITORIA BETWEEN @F_DESDE AND DATEADD(DD,1,@F_HASTA)))
			AND((@CLIENTE_ID IS NULL) OR (AH.CLIENTE_ID=@CLIENTE_ID))
			AND((@PRODUCTO_ID IS NULL) OR(AH.PRODUCTO_ID=@PRODUCTO_ID))
			AND((@NRO_PARTIDA IS NULL) OR (AH.NRO_PARTIDA=@NRO_PARTIDA))
			AND((@FECHA_VTO IS NULL) OR(AH.FECHA_VENCIMIENTO=@FECHA_VTO))
			AND((@USUARIO IS NULL) OR (AH.USUARIO_ID=@USUARIO))
			AND((@NRO_LOTE IS NULL) OR (AH.NRO_LOTE=@NRO_LOTE))
			AND((@PROP2 IS NULL) OR (AH.PROP2=@PROP2))
			AND((@PROP3 IS NULL) OR (AH.PROP3=@PROP3))
			AND ((@QSERIE IS NULL) OR (AH.NRO_SERIE = @QSERIE))
			AND ((@NRO_DESPACHO IS NULL) OR (AH.NRO_DESPACHO = @NRO_DESPACHO)) 
			
	UNION	
	--Egresos
	SELECT 
			 DBO.GET_SALDO_INICIAL(P.CLIENTE_ID,P.PRODUCTO_ID,@P_FECHA_I,@F_HASTA)
			,P.CLIENTE_ID + ' - ' +C.RAZON_SOCIAL			AS [CLIENTE_RZ]
			,P.PRODUCTO_ID
			,'Egresos'
			,'Usuario: ' + @USUARIO_RTP						AS [USOINTERNOUSUARIO]
			,'Terminal:' + @TERMINAL_RTP					AS [USOINTERNOTERMINAL]
			,P.FECHA_CONTROL_FAC								
			,CONVERT(VARCHAR,P.FECHA_CONTROL_FAC,103) 		AS [FECHA]
			,CASE 
			  	WHEN D.NRO_REMITO IS NULL THEN TC.DESCRIPCION	+ ' - ' + CAST(DD.DOCUMENTO_ID AS VARCHAR)
			  	Else TC.DESCRIPCION	+ ' - ' + D.NRO_REMITO
			 END AS [DESC_TIPO_COMPROBANTE]
			--,TC.DESCRIPCION	+ ' - ' + D.NRO_REMITO			AS [DESC_TIPO_COMPROBANTE]
			,P.USUARIO_CONTROL_FAC + ' - ' +SU.NOMBRE
			,P.TERMINAL_CONTROL_FAC							AS [TERMINAL_OPERACION]
			,P.POSICION_COD									AS [UBICACION]
			,ISNULL((P.CANT_CONFIRMADA *(-1)),0)
			,D.NRO_REMITO
			,DD.CAT_LOG_ID_FINAL
			,P.PRODUCTO_ID + ' - ' + P.DESCRIPCION		AS [PROD_DESC]
			,D.NRO_REMITO
            ,D.CPTE_PREFIJO
            ,D.CPTE_NUMERO
            ,D.DOCUMENTO_ID
            ,D.TIPO_COMPROBANTE_ID
			,P.CLIENTE_ID
			,CASE 
			  	WHEN P.NRO_SERIE IS NULL THEN '' 
			  	Else 'Nro.Serie: ' + CAST(P.NRO_SERIE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_BULTO IS NULL THEN '' 
			    Else 'Nro.Bulto: ' + CAST(DD.NRO_BULTO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_LOTE IS NULL THEN '' 
			    Else 'Nro.Lote: ' + CAST(DD.NRO_LOTE AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_DESPACHO IS NULL THEN '' 
			    Else 'Nro.Despacho: ' + CAST(DD.NRO_DESPACHO AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.NRO_PARTIDA IS NULL THEN '' 
			    Else 'Nro.Partida: ' + CAST(DD.NRO_PARTIDA AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.PROP1 IS NULL THEN '' 
			    Else 'Nro.Pallet: ' + CAST(DD.PROP1 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.PROP2 IS NULL THEN '' 
			    Else 'Lote Prov.: ' + CAST(DD.PROP2 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.PROP3 IS NULL THEN '' 
			    Else 'PROPERTY 3: ' + CAST(DD.PROP3 AS VARCHAR) + ', '
			 END + 
			 CASE 
			    WHEN DD.FECHA_VENCIMIENTO IS NULL THEN '' 
			    Else 'Fecha Vencimiento: ' + CONVERT(VARCHAR,DD.FECHA_VENCIMIENTO,103) + ' '
			 END +
			 CASE
				WHEN D.SUCURSAL_DESTINO IS NULL THEN ''
				ELSE 'Destino: ' + CAST(D.SUCURSAL_DESTINO AS VARCHAR) + ' - ' + CAST(S.NOMBRE AS VARCHAR)+', '
			 END +
			 CASE
				WHEN DD.EST_MERC_ID IS NULL THEN ''
				ELSE 'Est.Merc. : ' + CAST(DD.EST_MERC_ID AS VARCHAR) + ', '
			 END +
			 CASE
				WHEN D.NRO_REMITO IS NULL THEN ''
				ELSE 'Nro. Remito : ' + CAST(D.NRO_REMITO AS VARCHAR)
			 END +
			 CASE 
				WHEN D.TIPO_COMPROBANTE_ID IN('E02','E01','E03','E04','AJE')
				THEN ISNULL(DBO.GET_USUARIO_PEDIDO(P.CLIENTE_ID,D.NRO_DESPACHO_IMPORTACION),'')
			 END +
			 CASE 
				WHEN NOT SP.PICKING_ID IS NULL THEN ', Num. Series : ' + CAST(SP.SERIES AS VARCHAR)
				ELSE ''
			END
			 AS [PROPIEDADES]
			,DD.NRO_SERIE
			,DD.NRO_BULTO
			,DD.NRO_LOTE
			,DD.NRO_DESPACHO
			,DD.NRO_PARTIDA				
			,DD.PROP1						[NRO_PALLET]
			,DD.PROP2						[LOTE_PROVEEDOR]
			,DD.FECHA_VENCIMIENTO			[FECHA_VENCIMIENTO]
			,D.SUCURSAL_DESTINO			,S.NOMBRE	
			,DD.EST_MERC_ID					
			,P.PICKING_ID
	FROM	vPICKING P (NOLOCK) INNER JOIN vDET_DOCUMENTO DD	(NOLOCK) ON(P.DOCUMENTO_ID=DD.DOCUMENTO_ID AND P.NRO_LINEA=DD.NRO_LINEA)
			INNER JOIN vDOCUMENTO D 				(NOLOCK) ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID)
			INNER JOIN TIPO_COMPROBANTE TC 			(NOLOCK) ON(D.TIPO_COMPROBANTE_ID=TC.TIPO_COMPROBANTE_ID)
			INNER JOIN CLIENTE C					(NOLOCK) ON(C.CLIENTE_ID=P.CLIENTE_ID)
			LEFT JOIN SYS_USUARIO SU				(NOLOCK) ON(P.USUARIO_CONTROL_FAC=SU.USUARIO_ID)
			LEFT JOIN SUCURSAL	S					(NOLOCK) ON(S.CLIENTE_ID=D.CLIENTE_ID AND S.SUCURSAL_ID=D.SUCURSAL_DESTINO)
		    LEFT JOIN tmpSeriesPicking SP         (NOLOCK) ON(SP.PICKING_ID = P.PICKING_ID)
	WHERE	((@NRO_PALLET IS NULL)OR(P.PROP1=@NRO_PALLET))
			AND((@F_DESDE IS NULL)OR(P.FECHA_CONTROL_FAC BETWEEN @F_DESDE AND DATEADD(DD,1,@F_HASTA)))
			AND ((P.FACTURADO='1') OR((P.FACTURADO='0') AND (TC.TIPO_COMPROBANTE_ID = 'AJE')))
			AND 16 In (Select Tipo_Auditoria_Id from #TEMP_CRITERIOS_RPT)
			AND((@CLIENTE_ID IS NULL) OR (P.CLIENTE_ID=@CLIENTE_ID))
			AND((@PRODUCTO_ID IS NULL) OR (P.PRODUCTO_ID=@PRODUCTO_ID))
			AND((@NRO_PARTIDA IS NULL) OR (DD.NRO_PARTIDA=@NRO_PARTIDA))
			AND((@FECHA_VTO IS NULL) OR (DD.FECHA_VENCIMIENTO=@FECHA_VTO))
			AND((@USUARIO IS NULL) OR (P.USUARIO_CONTROL_FAC=@USUARIO))
			AND((@NRO_LOTE IS NULL) OR (DD.NRO_LOTE=@NRO_LOTE))
			AND((@PROP2 IS NULL) OR (DD.PROP2=@PROP2))
			AND((@PROP3 IS NULL) OR (DD.PROP3=@PROP3))
			AND((@COD_PEDIDO IS NULL) OR(D.NRO_REMITO LIKE  '%'+ @COD_PEDIDO +  '%'))
			AND((@COD_VIAJE IS NULL) OR(D.NRO_DESPACHO_IMPORTACION LIKE '%' + @COD_VIAJE + '%'))
			AND ((@QSERIE IS NULL) OR (P.NRO_SERIE = @QSERIE))						
			AND ((@NRO_DESPACHO IS NULL) OR (DD.NRO_DESPACHO = @NRO_DESPACHO)) 			
	ORDER BY 3,7,30
	--DROP TABLE #tmpSeriesPicking
END





GO


