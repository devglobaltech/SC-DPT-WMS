/****** Object:  StoredProcedure [dbo].[MOB_CONSULTASTOCK2]    Script Date: 10/07/2014 11:04:42 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MOB_CONSULTASTOCK2]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[MOB_CONSULTASTOCK2]
GO

CREATE              PROCEDURE [dbo].[MOB_CONSULTASTOCK2]
@Codigo as nvarchar(100) OUT,
@TipoOperacion as integer--,
--@Cliente as varchar(15)
as
DECLARE @EXISTE AS INTEGER

IF @TipoOperacion=1
BEGIN

	SET 		@EXISTE = (SELECT count(prop1) as EXISTE
	FROM	DET_DOCUMENTO
	WHERE 	prop1 = UPPER(LTRIM(RTRIM(@Codigo))))

	IF @EXISTE =0
	BEGIN
		RAISERROR ('El pallet ingresado no existe.', 16, 1)
		RETURN
	END

SELECT 	C.Razon_Social
		,X.ProductoID 
		,X.DESCRIPCION	
		,x.Unidad_id
		,cast(sum(X.cantidad)as int) AS Cantidad 
		,isnull(x.Posicion_Cod,X.Storage) as POSICION
		,ISNULL(X.EST_MERC_ID,'') AS EST_MERC_ID
		,ISNULL(X.CategLogID,'') AS CategLogID
		,ISNULL(X.Nro_Lote,'') AS Nro_Lote
		,ISNULL(X.prop1,'') AS Property_1
		,ISNULL(CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103),'') as Fecha_Vencimiento
		,PR.DESCRIPCION AS PRODUCTO1
FROM 	CLIENTE C, PRODUCTO PR 
		,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID
				,prod.DESCRIPCION AS DESCRIPCION
				,cast(sum(rl.cantidad)as int) AS Cantidad 
				,dd.unidad_id ,dd.moneda_id ,dd.costo 
				,dd.nro_serie AS Nro_Serie 
				,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
				,dd.Nro_Partida 
				,dd.Nro_Despacho, dd.Nro_Bulto 
				,dd.Prop1, dd.Prop2, dd.Prop3 
				,dd.Peso ,dd.Unidad_Peso 
				,dd.Volumen ,dd.Unidad_Volumen 
				,prod.kit AS Kit 
				,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
				,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
				,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
				,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
				,ISNULL(caln.calle_cod,Null) AS CalleCod 
				,ISNULL(caln.calle_id,Null) AS CalleID 
				,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
				,ISNULL(coln.columna_id,Null) AS ColumnaID
				,ISNULL(nn.nivel_cod,Null) AS NivelCod 
				,ISNULL(nn.nivel_id,Null) AS NivelID 
				,rl.cat_log_id as CategLogID
				,p.posicion_cod as posicion_cod
		FROM 
				 rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
				 ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
				 left join nave n  ON rl.nave_actual=n.nave_id 
				 left join posicion p  ON rl.posicion_actual=p.posicion_id 
				 left join nave n2 ON p.nave_id=n2.nave_id 
				 left join calle_nave caln ON  p.calle_id=caln.calle_id 
				 left join columna_nave coln ON p.columna_id=coln.columna_id 
				 left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
				 inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
				 inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
				 inner join cliente c ON dd.cliente_id=c.cliente_id 
				 inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
				 inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
		 WHERE 	1<>0  
				--AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
				AND dd.prop1 = UPPER(LTRIM(RTRIM(@Codigo)))
		GROUP BY dd.cliente_id ,dd.producto_id,PROD.DESCRIPCION
				,dd.unidad_id, dd.moneda_id, dd.costo 
				,dd.Nro_Serie 
				,dd.Nro_lote, dd.Fecha_vencimiento 
				,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
				,dd.Prop1, dd.Prop2, dd.Prop3 
				,dd.Peso ,dd.unidad_peso 
				,dd.Volumen ,dd.unidad_volumen 
				,rl.nave_actual,p.nave_id,n.nave_cod 
				,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
				,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
				,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
				,dd.nro_tie_in , RL.est_merc_id 
				,rl.cat_log_id 
				,p.posicion_cod
		UNION ALL  
		SELECT 	dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID  
				,prod.DESCRIPCION AS DESCRIPCION
				,cast(sum(rl.cantidad)as int) AS Cantidad  
				,dd.unidad_id ,dd.moneda_id ,dd.costo  
				,dd.nro_serie AS Nro_Serie  
				,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23), dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
				,dd.Nro_Partida  
				,dd.Nro_Despacho, dd.Nro_Bulto  
				,dd.Prop1, dd.Prop2, dd.Prop3  
				,cast(dd.Peso as float) AS Peso ,dd.Unidad_Peso  
				,cast(dd.Volumen as float) AS Volumen,dd.Unidad_Volumen  
				,prod.kit AS Kit  
				,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
				,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
				,n.nave_cod AS Storage  
				,rl.nave_actual as NaveID  
				,null AS CalleCod  
				,null AS CalleID  
				,null AS ColumnaCod  
				,null AS ColumnaID  
				,null AS NivelCod  
				,null AS NivelID 
				,rl.cat_log_id as CategLogID  
				,n.nave_cod as Posicion_Cod
		FROM  
				rl_det_doc_trans_posicion rl inner join det_documento dd  
				ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
				left join nave n  ON rl.nave_actual=n.nave_id  
				inner join cliente c  ON dd.cliente_id=c.cliente_id  
				inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
				inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
		WHERE 1<>0  
				--AND dd.Cliente_ID = @Cliente
				AND dd.prop1 = @Codigo
		GROUP BY dd.cliente_id ,dd.producto_id,PROD.DESCRIPCION
				,dd.unidad_id, dd.moneda_id, dd.costo 
				,dd.Nro_Serie 
				,dd.Nro_lote ,dd.Fecha_vencimiento 
				,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
				,dd.Prop1, dd.Prop2, dd.Prop3
				,dd.Peso ,dd.unidad_peso 
				,dd.Volumen ,dd.unidad_volumen 
				,rl.nave_actual,n.nave_cod 
				,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
				,dd.nro_tie_in , RL.est_merc_id 
				,rl.cat_log_id 
				,n.nave_cod
				) x 
WHERE 	C.CLIENTE_ID = X.CLIENTEID 
		AND PR.CLIENTE_ID = X.CLIENTEID 
		AND PR.PRODUCTO_ID = X.PRODUCTOID 
		AND ((X.CategLogID<>'TRAN_ING') OR (X.CategLogID<>'TRAN_EGR'))
group by X.ClienteID, X.ProductoID,X.DESCRIPCION,x.Unidad_id
		 ,X.Storage 
		 ,X.NaveID 
		 ,X.CalleCod 
		 ,X.CalleID 
		 ,X.ColumnaCod 
		 ,X.ColumnaID 
		 ,X.NivelCod 
		 ,X.NivelID 
		 ,X.EST_MERC_ID 
		 ,X.CategLogID 
		 ,X.Nro_Serie 
		 ,X.Nro_Bulto 
		 ,X.Nro_Lote 
		 ,X.Nro_Despacho 
		 ,X.Nro_Partida 
		 ,X.prop1 
		 ,X.prop2 
		 ,X.prop3 
		 ,X.Fecha_Vencimiento 
		 ,X.Peso 
		 ,X.Unidad_Peso 
		 ,X.Volumen 
		 ,X.Unidad_Volumen 
		 ,X.Kit 
		 ,X.TIE_IN  ,X.TIE_IN_PADRE 
		 ,X.NRO_TIE_IN 
		 ,C.RAZON_SOCIAL 
		 ,PR.DESCRIPCION 
		 ,X.unidad_id 
		 ,X.moneda_id 
		 ,x.costo 
		 ,x.posicion_cod
END
ELSE
	BEGIN
	IF  @TipoOperacion=2
		BEGIN
		SET @EXISTE = (SELECT COUNT(X.TIPO) AS EXISTE
			FROM
			(
				SELECT     POSICION_ID,'POS' AS TIPO
				FROM       POSICION
				WHERE     (POSICION_COD = UPPER(LTRIM(RTRIM(@Codigo))))
				UNION ALL
				SELECT     NAVE_ID, 'NAVE' AS TIPO
				FROM       NAVE
				WHERE     (NAVE_COD = UPPER(LTRIM(RTRIM(@Codigo))))
			) AS X)

		IF @EXISTE =0
		BEGIN
		    RAISERROR ('El ubicación no existe.', 16, 1)
		END

		--CONSULTA UBICACION
			SELECT X.*
			FROM
				(
					SELECT DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID, cast(SUM(RL.CANTIDAD)as int) AS CANTIDAD_TOTAL, ISNULL(CONVERT(VARCHAR(23), DD.FECHA_VENCIMIENTO, 103),'') as FECHA_VENCIMIENTO, ISNULL(DD.NRO_LOTE,'') AS NRO_LOTE, ISNULL(DD.PROP1,'') AS PROP1
					FROM RL_DET_DOC_TRANS_POSICION RL INNER JOIN
				        POSICION P ON RL.POSICION_ACTUAL = P.POSICION_ID INNER JOIN
				        DET_DOCUMENTO_TRANSACCION DDT ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND 
				        RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS INNER JOIN
				        DOCUMENTO_TRANSACCION DT ON DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID INNER JOIN
				        DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA
						INNER JOIN PRODUCTO PROD ON DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID
					WHERE     --(RL.POSICION_ACTUAL = @Codigo) SGG
						  P.POSICION_COD=UPPER(LTRIM(RTRIM(@Codigo)))
					GROUP BY DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID, DD.FECHA_VENCIMIENTO, DD.NRO_LOTE, DD.PROP1
			
			
					UNION ALL
			
					SELECT 	DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID,cast(SUM(RL.CANTIDAD)as int) AS CANTIDAD_TOTAL, ISNULL(CONVERT(VARCHAR(23), DD.FECHA_VENCIMIENTO, 103),'') as FECHA_VENCIMIENTO, ISNULL(DD.NRO_LOTE,'') AS NRO_LOTE, ISNULL(DD.PROP1,'') AS PROP1
					FROM 	RL_DET_DOC_TRANS_POSICION RL 
							INNER JOIN NAVE N
							ON(RL.NAVE_ACTUAL=N.NAVE_ID)
							INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
							ON(RL.DOC_TRANS_ID=DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS =DDT.NRO_LINEA_TRANS)
							INNER JOIN DET_DOCUMENTO DD 
							ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
							INNER JOIN PRODUCTO PROD ON DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID
					WHERE     --(RL.POSICION_ACTUAL = @Codigo) SGG
						  	N.NAVE_COD=UPPER(LTRIM(RTRIM(@Codigo)))
					GROUP BY DD.CLIENTE_ID, DD.PRODUCTO_ID,PROD.DESCRIPCION,DD.UNIDAD_ID, DD.FECHA_VENCIMIENTO, DD.NRO_LOTE, DD.PROP1
				)AS X
		END
	ELSE
BEGIN
--------------------------------------
--		CONSULTA PRODUCTO		    --
--------------------------------------
--	SELECT     PRODUCTO_ID, CLIENTE_ID
--	FROM         PRODUCTO
SET @EXISTE = (	SELECT	count(Producto_ID) as EXISTE
				FROM	PRODUCTO
				WHERE	Producto_ID = UPPER(LTRIM(RTRIM(@Codigo))))
IF @EXISTE =0
BEGIN
	SET @EXISTE=NULL

	SET @EXISTE = (	SELECT	count(Producto_ID) as EXISTE
					FROM	RL_PRODUCTO_CODIGOS
					WHERE	CODIGO = UPPER(LTRIM(RTRIM(@Codigo))))
	IF @EXISTE=0 BEGIN
		RAISERROR ('El producto ingresado no existe.', 16, 1)
	END
	ELSE BEGIN
		SELECT	@Codigo=Producto_ID
		FROM	RL_PRODUCTO_CODIGOS 
		WHERE	CODIGO = UPPER(LTRIM(RTRIM(@Codigo)))
    END
END



SELECT	C.razon_social
		,X.ProductoID 
	 ,PR.DESCRIPCION
	 ,X.UNIDAD_ID
     ,cast(sum(X.cantidad)as int) AS Cantidad 
     ,ISNULL(X.EST_MERC_ID,'') AS EST_MERC_ID
	 ,isnull(X.POSICION_COD,X.STORAGE) AS POSICION
	 ,ISNULL(X.Storage,'') AS STORAGE
     ,ISNULL(X.CategLogID,'') AS CategLogID
     ,ISNULL(X.Nro_Lote,'') AS Nro_Lote
     ,ISNULL(X.prop1,'') AS Property_1
     ,ISNULL(CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103),'') as Fecha_Vencimiento
     ,PR.DESCRIPCION AS PRODUCTO1
FROM CLIENTE C, PRODUCTO PR 
     ,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID
             ,cast(sum(rl.cantidad)as int) AS Cantidad 
             ,dd.unidad_id ,dd.moneda_id ,dd.costo 
             ,dd.nro_serie AS Nro_Serie 
             ,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
             ,dd.Nro_Partida 
             ,dd.Nro_Despacho, dd.Nro_Bulto 
             ,dd.Prop1, dd.Prop2, dd.Prop3 
             ,cast(dd.Peso as float) as Peso,dd.Unidad_Peso 
             ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen 
             ,prod.kit AS Kit 
             ,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
             ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
             ,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
             ,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
             ,ISNULL(caln.calle_cod,Null) AS CalleCod 
             ,ISNULL(caln.calle_id,Null) AS CalleID 
             ,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
             ,ISNULL(coln.columna_id,Null) AS ColumnaID
             ,ISNULL(nn.nivel_cod,Null) AS NivelCod 
             ,ISNULL(nn.nivel_id,Null) AS NivelID 
             ,rl.cat_log_id as CategLogID 
			 ,P.POSICION_COD AS POSICION_COD
     FROM 
         rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
         ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
         left join nave n  ON rl.nave_actual=n.nave_id 
         left join posicion p  ON rl.posicion_actual=p.posicion_id 
         left join nave n2 ON p.nave_id=n2.nave_id 
         left join calle_nave caln ON  p.calle_id=caln.calle_id 
         left join columna_nave coln ON p.columna_id=coln.columna_id 
         left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
         inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
         inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
         inner join cliente c ON dd.cliente_id=c.cliente_id 
         inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
         inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
     WHERE 1<>0  
   --AND dd.Cliente_ID = @Cliente
   	AND dd.Producto_ID = @Codigo
	--AND n.Pre_ingreso <>'1'
	--AND N.PRE_EGRESO <>'1'	

GROUP BY dd.cliente_id ,dd.producto_id
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote, dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3 
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,p.nave_id,n.nave_cod 
     ,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
     ,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
     ,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
	 ,P.POSICION_COD
UNION ALL  
     SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID
           ,cast(sum(rl.cantidad)as int) AS Cantidad  
           ,dd.unidad_id ,dd.moneda_id ,dd.costo  
           ,dd.nro_serie AS Nro_Serie  
           ,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23),dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
           ,dd.Nro_Partida  
           ,dd.Nro_Despacho, dd.Nro_Bulto  
           ,dd.Prop1, dd.Prop2, dd.Prop3  
           ,cast(dd.Peso as float) as Peso ,dd.Unidad_Peso  
           ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen  
           ,prod.kit AS Kit  
           ,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
           ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
           ,n.nave_cod AS Storage  
           ,rl.nave_actual as NaveID  
           ,null AS CalleCod  
           ,null AS CalleID  
           ,null AS ColumnaCod  
           ,null AS ColumnaID  
           ,null AS NivelCod  
           ,null AS NivelID 
           ,rl.cat_log_id as CategLogID  
		   ,N.NAVE_COD AS POSICION_COD
     FROM  
           rl_det_doc_trans_posicion rl inner join det_documento dd  
           ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
           left join nave n  ON rl.nave_actual=n.nave_id  
           inner join cliente c  ON dd.cliente_id=c.cliente_id  
           inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
           inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
     WHERE 1<>0    
 --AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
 AND dd.Producto_ID = UPPER(LTRIM(RTRIM(@Codigo)))
	--AND n.Pre_ingreso <>'1'
	--AND N.PRE_EGRESO <>'1'
GROUP BY dd.cliente_id ,dd.producto_id
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote ,dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,n.nave_cod 
     ,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
	 ,N.NAVE_COD
     ) x 
WHERE C.CLIENTE_ID = X.CLIENTEID 
     AND PR.CLIENTE_ID = X.CLIENTEID 
     AND PR.PRODUCTO_ID = X.PRODUCTOID 
     group by X.ClienteID, X.ProductoID
     ,X.Storage 
     ,X.NaveID 
     ,X.CalleCod 
     ,X.CalleID 
     ,X.ColumnaCod 
     ,X.ColumnaID 
     ,X.NivelCod 
     ,X.NivelID 
     ,X.EST_MERC_ID 
     ,X.CategLogID 
     ,X.Nro_Serie 
     ,X.Nro_Bulto 
     ,X.Nro_Lote 
     ,X.Nro_Despacho 
     ,X.Nro_Partida 
     ,X.prop1 
     ,X.prop2 
     ,X.prop3 
     ,X.Fecha_Vencimiento 
     ,X.Peso 
     ,X.Unidad_Peso 
     ,X.Volumen 
     ,X.Unidad_Volumen 
     ,X.Kit 
     ,X.TIE_IN  ,X.TIE_IN_PADRE 
     ,X.NRO_TIE_IN 
     ,C.RAZON_SOCIAL 
     ,PR.DESCRIPCION 
     ,X.unidad_id 
     ,X.moneda_id 
     ,x.costo 
	 ,X.POSICION_COD
	
END

END

GO


