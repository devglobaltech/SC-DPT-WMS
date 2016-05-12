/****** Object:  View [dbo].[vStock]    Script Date: 10/10/2013 17:03:46 ******/
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vStock]'))
DROP VIEW [dbo].[vStock]
GO


CREATE  VIEW [dbo].[vStock]
AS
SELECT 	 T2.CLIENTEID								[COD. CLIENTE]
		,C.RAZON_SOCIAL								[RAZON SOCIAL]
		,T2.PRODUCTOID								[COD. PRODUCTO]
		,P.DESCRIPCION								[DESCRIPCION]
		,T2.UNIDAD_ID								[UNIDAD]
		,SUM(ISNULL(T2.CANTIDAD,0))					[CANTIDAD]
		,T2.STORAGE									[NAVE]
		,T2.CALLECOD								[CALLE]
		,T2.COLUMNACOD								[COLUMNA]
		,T2.NIVELCOD								[NIVEL]
		,T2.EST_MERC_ID								[EST. MERC.]
		,T2.CATEGLOGID								[CAT. LOG.]
		,T2.NRO_SERIE								[NRO. SERIE]
		,T2.NRO_BULTO								[NRO. BULTO]
		,T2.NRO_LOTE								[NRO. LOTE]
		,T2.NRO_DESPACHO							[NRO. DESPACHO]
		,T2.NRO_PARTIDA								[NRO. PARTIDA]
		,T2.PROP1									[NRO. PALLET]
		,T2.PROP2									[LOTE.PROVEEDOR]
		,T2.PROP3 									[PROP3]
		,CONVERT(VARCHAR,T2.FECHA_VENCIMIENTO,103)	[FECHA VENCIMIENTO]
		,T2.PESO									[PESO]
	    ,T2.UNIDAD_PESO								[UNIDAD PESO]
		,T2.VOLUMEN									[VOLUMEN]
		,T2.UNIDAD_VOLUMEN							[UNIDAD VOLUMEN]
		,T2.MONEDA_ID								[MONEDA]
		,T2.COSTO									[COSTO]
		,T2.NOMBRE									[PROVEEDOR]
		,T2.FAMILIA_ID								[FAMILIA]
		,T2.SUB_FAMILIA_ID							[SUB-FAMILIA]
		,CONVERT(VARCHAR,T2.FECHA_CPTE,103)			[FECHA CPTE.]
FROM    CLIENTE C (NOLOCK),PRODUCTO P (NOLOCK)
		,(	SELECT	 T2.CLIENTEID 			,T2.PRODUCTOID 			,SUM(T2.CANTIDAD) AS CANTIDAD 
					,T2.NRO_SERIE 			,T2.NRO_LOTE 			,T2.NRO_PARTIDA 
					,T2.FECHA_VENCIMIENTO 	,T2.NRO_DESPACHO		,T2.NRO_BULTO 
					,T2.PESO 				,T2.VOLUMEN 			,T2.TIE_IN 
					,T2.EST_MERC_ID 		,T2.UNIDAD_PESO 		,T2.UNIDAD_VOLUMEN 
					,T2.PROP1 				,T2.PROP2 				,T2.PROP3 
					,T2.UNIDAD_ID 			,T2.MONEDA_ID 			,T2.COSTO 
					,T2.CAT_LOG_ID_FINAL	,T2.DESCRIPCION 
			FROM	(SELECT	 DD.CLIENTE_ID CLIENTEID			,DD.PRODUCTO_ID PRODUCTOID 
							,SUM(ISNULL(DD.CANTIDAD,0))AS CANTIDAD 
							,DD.NRO_SERIE 						,DD.NRO_LOTE 
							,DD.NRO_PARTIDA 					,DD.FECHA_VENCIMIENTO 
							,DD.NRO_DESPACHO					,DD.NRO_BULTO 
							,DD.PESO 							,DD.UNIDAD_PESO 
							,DD.VOLUMEN 						,DD.UNIDAD_VOLUMEN 
							,DD.TIE_IN							,DD.EST_MERC_ID 
							,DD.PROP1 							,DD.PROP2 
							,DD.PROP3 							,DD.UNIDAD_ID 
							,DD.MONEDA_ID						,DD.COSTO 
							,DD.CAT_LOG_ID_FINAL 				,DD.DESCRIPCION 
					FROM	DOCUMENTO D (NOLOCK),DET_DOCUMENTO DD (NOLOCK)
					WHERE	D.DOCUMENTO_ID = DD.DOCUMENTO_ID 
							AND D.STATUS = 'D20'
							AND D.TIPO_OPERACION_ID = 'EGR'
					GROUP BY 
							 DD.CLIENTE_ID 		,DD.PRODUCTO_ID 		,DD.NRO_SERIE 
							,DD.NRO_LOTE 		,DD.NRO_PARTIDA 		,DD.FECHA_VENCIMIENTO 
							,DD.NRO_DESPACHO 	,DD.NRO_BULTO 			,DD.PESO 
							,DD.UNIDAD_PESO 	,DD.VOLUMEN 			,DD.UNIDAD_VOLUMEN 
							,DD.TIE_IN 			,DD.EST_MERC_ID 		,DD.PROP1 
							,DD.PROP2			,DD.PROP3 				,DD.UNIDAD_ID 
							,DD.MONEDA_ID 		,DD.COSTO 				,DD.CAT_LOG_ID_FINAL 
							,DD.DESCRIPCION 
					UNION ALL 
                    SELECT   DD.CLIENTE_ID CLIENTEID ,DD.PRODUCTO_ID PRODUCTOID 
							,SUM(ISNULL(DD.CANTIDAD,0)) AS CANTIDAD 
							,DD.NRO_SERIE 
							,DD.NRO_LOTE 			,DD.NRO_PARTIDA 
							,DD.FECHA_VENCIMIENTO 	,DD.NRO_DESPACHO 
							,DD.NRO_BULTO 			,DD.PESO 
							,DD.UNIDAD_PESO 		,DD.VOLUMEN 
							,DD.UNIDAD_VOLUMEN		,DD.TIE_IN 
							,DD.EST_MERC_ID 		,DD.PROP1 
							,DD.PROP2 				,DD.PROP3 
							,DD.UNIDAD_ID 			,DD.MONEDA_ID  
							,DD.COSTO				,DD.CAT_LOG_ID_FINAL 
							,DD.DESCRIPCION 
					FROM    DET_DOCUMENTO DD (NOLOCK),DET_DOCUMENTO_TRANSACCION DDT (NOLOCK)
							,DOCUMENTO_TRANSACCION DT (NOLOCK)
					WHERE	1<>0 
							AND DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID 
							AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
							AND DT.DOC_TRANS_ID = DDT.DOC_TRANS_ID 
							AND DT.STATUS = 'T10'
							AND DT.TIPO_OPERACION_ID = 'EGR'
							AND NOT EXISTS  (	SELECT	RL_ID 
												FROM	RL_DET_DOC_TRANS_POSICION RL (NOLOCK)
												WHERE	RL.DOC_TRANS_ID_EGR = DDT.DOC_TRANS_ID 
														AND RL.NRO_LINEA_TRANS_EGR = DDT.NRO_LINEA_TRANS )
					GROUP BY 
							 DD.CLIENTE_ID	,DD.PRODUCTO_ID ,DD.NRO_SERIE 
							,DD.NRO_LOTE 	,DD.NRO_PARTIDA ,DD.FECHA_VENCIMIENTO 
							,DD.NRO_DESPACHO,DD.NRO_BULTO 	,DD.PESO 
							,DD.UNIDAD_PESO ,DD.VOLUMEN 	,DD.UNIDAD_VOLUMEN 
							,DD.TIE_IN 		,DD.EST_MERC_ID ,DD.PROP1 
							,DD.PROP2 		,DD.PROP3 		,DD.UNIDAD_ID 
							,DD.MONEDA_ID 	,DD.COSTO 		,DD.CAT_LOG_ID_FINAL 
							,DD.DESCRIPCION 
                ) T2  
		WHERE 1<>0  
		GROUP BY  
               T2.CLIENTEID 
               ,T2.PRODUCTOID 
               ,T2.NRO_SERIE  
               ,T2.NRO_LOTE 
               ,T2.NRO_PARTIDA 
               ,T2.FECHA_VENCIMIENTO 
               ,T2.NRO_DESPACHO 
               ,T2.NRO_BULTO 
               ,T2.PESO 
               ,T2.UNIDAD_PESO 
               ,T2.VOLUMEN 
               ,T2.UNIDAD_VOLUMEN 
               ,T2.TIE_IN 
               ,T2.EST_MERC_ID 
               ,T2.PROP1 
               ,T2.PROP2 
               ,T2.PROP3 
               ,T2.UNIDAD_ID 
               ,T2.MONEDA_ID 
               ,T2.COSTO 
               ,T2.CAT_LOG_ID_FINAL 
               ,T2.DESCRIPCION 
       ) T1 RIGHT OUTER JOIN 
              (SELECT RL.CAT_LOG_ID AS CATEGLOGID 
          ,DD.CLIENTE_ID AS CLIENTEID 
          ,DD.PRODUCTO_ID AS PRODUCTOID 
          ,SUM(ISNULL(RL.CANTIDAD,0)) AS CANTIDAD 
          ,DD.NRO_SERIE 
          ,DD.NRO_LOTE 
          ,DD.FECHA_VENCIMIENTO 
          ,DD.NRO_DESPACHO 
          ,DD.NRO_BULTO 
          ,DD.NRO_PARTIDA 
          ,DD.PESO AS PESO 
          ,DD.UNIDAD_PESO 
          ,DD.VOLUMEN AS VOLUMEN 
          ,DD.UNIDAD_VOLUMEN  
          ,PROD.KIT AS KIT  
          ,DD.TIE_IN AS TIE_IN 
          ,DD.NRO_TIE_IN_PADRE AS TIE_IN_PADRE 
          ,DD.NRO_TIE_IN AS NRO_TIE_IN 
          ,RL.EST_MERC_ID 
          ,ISNULL(N.NAVE_COD,N2.NAVE_COD) AS STORAGE 
          ,ISNULL(RL.NAVE_ACTUAL,P.NAVE_ID) AS NAVEID 
          ,CALN.CALLE_COD AS CALLECOD 
          ,CALN.CALLE_ID AS CALLEID 
          ,COLN.COLUMNA_COD AS COLUMNACOD 
          ,COLN.COLUMNA_ID AS COLUMNAID 
          ,NN.NIVEL_COD AS NIVELCOD 
          ,NN.NIVEL_ID AS NIVELID 
          ,DD.PROP1 
          ,DD.PROP2 
          ,DD.PROP3 
          ,DD.UNIDAD_ID 
          ,DD.MONEDA_ID 
          ,DD.COSTO 
          ,S.NOMBRE 
          ,DD.DESCRIPCION 
          ,PROD.FAMILIA_ID  
          ,PROD.SUB_FAMILIA_ID 
          ,D.FECHA_CPTE 
    FROM  RL_DET_DOC_TRANS_POSICION RL (NOLOCK)
          LEFT OUTER JOIN NAVE N (NOLOCK)            ON RL.NAVE_ACTUAL = N.NAVE_ID 
          LEFT OUTER JOIN POSICION P  (NOLOCK)       ON RL.POSICION_ACTUAL = P.POSICION_ID 
          LEFT OUTER JOIN NAVE N2   (NOLOCK)         ON P.NAVE_ID = N2.NAVE_ID 
          LEFT OUTER JOIN CALLE_NAVE CALN (NOLOCK)   ON P.CALLE_ID = CALN.CALLE_ID 
          LEFT OUTER JOIN COLUMNA_NAVE COLN (NOLOCK) ON P.COLUMNA_ID = COLN.COLUMNA_ID
          LEFT OUTER JOIN NIVEL_NAVE NN  (NOLOCK)    ON P.NIVEL_ID = NN.NIVEL_ID
          ,DET_DOCUMENTO_TRANSACCION DDT (NOLOCK)
          ,DET_DOCUMENTO DD (NOLOCK) INNER JOIN DOCUMENTO D (NOLOCK) ON(DD.DOCUMENTO_ID=D.DOCUMENTO_ID) LEFT JOIN SUCURSAL S 
		  ON(S.SUCURSAL_ID=D.SUCURSAL_ORIGEN AND S.CLIENTE_ID=D.CLIENTE_ID)
          ,CLIENTE C (NOLOCK)
          ,PRODUCTO PROD (NOLOCK)
          ,CATEGORIA_LOGICA CL (NOLOCK)
          ,DOCUMENTO_TRANSACCION DT (NOLOCK)
    WHERE 1<>0 
          AND RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID 
          AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS 
          AND DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID 
          AND DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID 
                     AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
          AND DD.CLIENTE_ID = C.CLIENTE_ID 
          AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID 
          AND DD.CLIENTE_ID = PROD.CLIENTE_ID 
          AND RL.CAT_LOG_ID = CL.CAT_LOG_ID 
          AND RL.CLIENTE_ID = CL.CLIENTE_ID 
          AND RL.DISPONIBLE= '1'
          AND ISNULL(P.POS_LOCKEADA,'0')='0'
          AND ISNULL(N.DEPOSITO_ID,N2.DEPOSITO_ID)='DEFAULT'
          AND 0 =(SELECT (CASE WHEN (COUNT (POSICION_ID))> 0 THEN 1 ELSE 0 END) AS VALOR
                  FROM RL_POSICION_PROHIBIDA_CLIENTE (NOLOCK)
                  WHERE POSICION_ID = ISNULL(P.NIVEL_ID,0)
                        AND CLIENTE_ID= DD.CLIENTE_ID)
     GROUP BY RL.CAT_LOG_ID 
             ,DD.CLIENTE_ID 
             ,DD.PRODUCTO_ID 
             ,DD.NRO_SERIE 
             ,DD.NRO_LOTE 
             ,DD.FECHA_VENCIMIENTO 
             ,DD.NRO_DESPACHO 
             ,DD.NRO_BULTO 
             ,DD.NRO_PARTIDA 
             ,DD.PESO 
             ,DD.UNIDAD_PESO 
             ,DD.VOLUMEN 
             ,DD.UNIDAD_VOLUMEN 
             ,RL.NAVE_ACTUAL 
             ,P.NAVE_ID 
             ,N.NAVE_COD  
             ,N2.NAVE_COD 
             ,CALN.CALLE_COD 
             ,CALN.CALLE_ID 
             ,COLN.COLUMNA_COD 
             ,COLN.COLUMNA_ID 
             ,NN.NIVEL_COD 
             ,NN.NIVEL_ID 
             ,PROD.KIT 
             ,DD.TIE_IN 
             ,DD.NRO_TIE_IN_PADRE 
             ,DD.NRO_TIE_IN 
             ,RL.EST_MERC_ID 
             ,DD.PROP1 
             ,DD.PROP2 
             ,DD.PROP3 
             ,DD.UNIDAD_ID 
             ,DD.MONEDA_ID 
             ,DD.COSTO 
             ,S.NOMBRE 
             ,DD.DESCRIPCION 
             ,PROD.FAMILIA_ID 
             ,PROD.SUB_FAMILIA_ID 
             ,D.FECHA_CPTE 
             ) T2 ON (	ISNULL(T2.CLIENTEID,0) = ISNULL(T1.CLIENTEID,0)
						AND ISNULL(T2.PRODUCTOID,0) = ISNULL(T1.PRODUCTOID,0) 
						AND ISNULL(T2.NRO_SERIE,0) = ISNULL(T1.NRO_SERIE,0)
						AND ISNULL(T2.NRO_LOTE,0) = ISNULL(T1.NRO_LOTE,0)
						AND ISNULL(T2.NRO_DESPACHO,0) = ISNULL(T1.NRO_DESPACHO,0)
						AND ISNULL(T2.NRO_BULTO,0) = ISNULL(T1.NRO_BULTO,0)
						AND ISNULL(T2.NRO_PARTIDA,0) = ISNULL(T1.NRO_PARTIDA,0)
						AND ISNULL(T2.PROP1,0) = ISNULL(T1.PROP1,0)
						AND ISNULL(T2.PROP2,0) = ISNULL(T1.PROP2,0)
						AND ISNULL(T2.PROP3,0) = ISNULL(T1.PROP3,0)
						AND ISNULL(T2.UNIDAD_ID,0) = ISNULL(T1.UNIDAD_ID,0)
						AND ISNULL(T2.FECHA_VENCIMIENTO,'01/01/1900') = ISNULL(T1.FECHA_VENCIMIENTO,'01/01/1900')
						AND ISNULL(T2.EST_MERC_ID,0) = ISNULL(T1.EST_MERC_ID,0) 
						AND ISNULL(T2.CATEGLOGID,0) = ISNULL(T1.CAT_LOG_ID_FINAL,0)
              ) 
			LEFT OUTER JOIN ESTADO_MERCADERIA_RL EMRL (NOLOCK)
                ON (T2.CLIENTEID = EMRL.CLIENTE_ID 
                    AND T2.EST_MERC_ID = EMRL.EST_MERC_ID) 
     WHERE 1<>0 
          AND T2.CLIENTEID = C.CLIENTE_ID 
          AND T2.CLIENTEID = P.CLIENTE_ID 
          AND T2.PRODUCTOID = P.PRODUCTO_ID 

GROUP BY T2.CLIENTEID
         ,T2.PRODUCTOID 
         ,T2.STORAGE 
         ,T2.NAVEID 
         ,T2.CALLECOD 
         ,T2.CALLEID 
         ,T2.COLUMNACOD 
         ,T2.COLUMNAID 
         ,T2.NIVELCOD 
         ,T2.NIVELID 
         ,T2.EST_MERC_ID 
         ,T2.CATEGLOGID 
         ,T2.NRO_SERIE 
         ,T2.NRO_BULTO 
         ,T2.NRO_LOTE 
         ,T2.NRO_DESPACHO 
         ,T2.NRO_PARTIDA 
         ,T2.PROP1 
         ,T2.PROP2 
         ,T2.PROP3 
         ,T2.FECHA_VENCIMIENTO 
         ,T2.PESO 
         ,T2.UNIDAD_PESO 
         ,T2.VOLUMEN 
         ,T2.UNIDAD_VOLUMEN 
         ,T2.KIT 
         ,T2.TIE_IN 
         ,T2.TIE_IN_PADRE 
         ,T2.NRO_TIE_IN 
         ,C.RAZON_SOCIAL 
         ,P.DESCRIPCION 
         ,T2.UNIDAD_ID 
         ,T2.MONEDA_ID 
         ,T2.COSTO 
         ,T2.NOMBRE 
         ,T2.DESCRIPCION 
         ,T2.FAMILIA_ID 
         ,T2.SUB_FAMILIA_ID 
         ,T2.FECHA_CPTE

GO

EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPane1', @value=N'[0E232FF0-B466-11cf-A24F-00AA00A3EFFF, 1.00]
Begin DesignProperties = 
   Begin PaneConfigurations = 
      Begin PaneConfiguration = 0
         NumPanes = 4
         Configuration = "(H (1[40] 4[20] 2[20] 3) )"
      End
      Begin PaneConfiguration = 1
         NumPanes = 3
         Configuration = "(H (1 [50] 4 [25] 3))"
      End
      Begin PaneConfiguration = 2
         NumPanes = 3
         Configuration = "(H (1[50] 2[25] 3) )"
      End
      Begin PaneConfiguration = 3
         NumPanes = 3
         Configuration = "(H (4 [30] 2 [40] 3))"
      End
      Begin PaneConfiguration = 4
         NumPanes = 2
         Configuration = "(H (1[56] 3) )"
      End
      Begin PaneConfiguration = 5
         NumPanes = 2
         Configuration = "(H (2 [66] 3))"
      End
      Begin PaneConfiguration = 6
         NumPanes = 2
         Configuration = "(H (4 [50] 3))"
      End
      Begin PaneConfiguration = 7
         NumPanes = 1
         Configuration = "(V (3))"
      End
      Begin PaneConfiguration = 8
         NumPanes = 3
         Configuration = "(H (1[56] 4[18] 2) )"
      End
      Begin PaneConfiguration = 9
         NumPanes = 2
         Configuration = "(H (1 [75] 4))"
      End
      Begin PaneConfiguration = 10
         NumPanes = 2
         Configuration = "(H (1[66] 2) )"
      End
      Begin PaneConfiguration = 11
         NumPanes = 2
         Configuration = "(H (4 [60] 2))"
      End
      Begin PaneConfiguration = 12
         NumPanes = 1
         Configuration = "(H (1) )"
      End
      Begin PaneConfiguration = 13
         NumPanes = 1
         Configuration = "(V (4))"
      End
      Begin PaneConfiguration = 14
         NumPanes = 1
         Configuration = "(V (2))"
      End
      ActivePaneConfig = 12
   End
   Begin DiagramPane = 
      Begin Origin = 
         Top = 0
         Left = 0
      End
      Begin Tables = 
         Begin Table = "T1"
            Begin Extent = 
               Top = 79
               Left = 53
               Bottom = 491
               Right = 257
            End
            DisplayFlags = 280
            TopColumn = 0
         End
         Begin Table = "T2_1"
            Begin Extent = 
               Top = 27
               Left = 353
               Bottom = 698
               Right = 659
            End
            DisplayFlags = 280
            TopColumn = 0
         End
         Begin Table = "P"
            Begin Extent = 
               Top = 215
               Left = 855
               Bottom = 358
               Right = 1060
            End
            DisplayFlags = 280
            TopColumn = 0
         End
      End
   End
   Begin SQLPane = 
      PaneHidden = 
   End
   Begin DataPane = 
      PaneHidden = 
      Begin ParameterDefaults = ""
      End
   End
   Begin CriteriaPane = 
      PaneHidden = 
      Begin ColumnWidths = 12
         Column = 1440
         Alias = 900
         Table = 1170
         Output = 720
         Append = 1400
         NewValue = 1170
         SortType = 1350
         SortOrder = 1410
         GroupBy = 1350
         Filter = 1350
         Or = 1350
         Or = 1350
         Or = 1350
      End
   End
End
' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'vStock'
GO

EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPaneCount', @value=1 , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'vStock'
GO


