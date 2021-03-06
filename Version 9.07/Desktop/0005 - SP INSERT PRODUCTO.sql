ALTER PROCEDURE [dbo].[INSERT_PRODUCTO]
	 @CLIENTE_ID             VARCHAR (15)	OUTPUT	
	,@PRODUCTO_ID            VARCHAR (30)	OUTPUT	
	,@VALIDACION_VENCIMIENTO VARCHAR (50)	OUTPUT
	,@DESCRIPCION            VARCHAR (200)	OUTPUT	
	,@FRACCIONABLE           VARCHAR (1)	OUTPUT	
	,@UNIDAD_FRACCION        VARCHAR (5)	OUTPUT
	,@UNIDAD_ID              VARCHAR (5)	OUTPUT	
	,@TIPO_PRODUCTO_ID       VARCHAR (5)	OUTPUT	
	,@PAIS_ID                VARCHAR (5)	OUTPUT
	,@FAMILIA_ID             VARCHAR (30)	OUTPUT	
	,@OBSERVACIONES          VARCHAR (400)	OUTPUT	
	,@POSICIONES_PURAS       VARCHAR (1)	OUTPUT
	,@LOTE_AUTOMATICO        VARCHAR (1)	OUTPUT	
	,@PALLET_AUTOMATICO      VARCHAR (1)	OUTPUT	
	,@ING_CAT_LOG_ID         VARCHAR (50)	OUTPUT
	,@SUB_FAMILIA_ID         VARCHAR (30)	OUTPUT	
	,@TIPO_CONTENEDORA       VARCHAR (100)	OUTPUT	
	,@GRUPO_PRODUCTO         VARCHAR (5)	OUTPUT
	,@ENVASE                 VARCHAR (1)	OUTPUT	
	,@INGPARTIDA             VARCHAR (1)	OUTPUT	
	,@SERIE_ING              VARCHAR (1)	OUTPUT
	,@SERIE_EGR              VARCHAR (1)	OUTPUT	
	,@VAL_COD_ING            VARCHAR (1)	OUTPUT	
	,@VAL_COD_EGR            VARCHAR (1)	OUTPUT
	,@LARGO                  VARCHAR (13)	OUTPUT	
	,@ALTO                   VARCHAR (13)	OUTPUT	
	,@ANCHO                  VARCHAR (13)	OUTPUT
	,@PESO                   VARCHAR (13)	OUTPUT	
	,@BACK_ORDER             VARCHAR (1)	OUTPUT	
	,@TOLERANCIA_MIN         VARCHAR (8)	OUTPUT
	,@TOLERANCIA_MAX         VARCHAR (8)	OUTPUT	
	,@CLASIFICACION_COT      VARCHAR (100)	OUTPUT	
	,@NO_AGRUPA_ITEMS		 VARCHAR (1)	OUTPUT
    ,@ROTACION_ID			 VARCHAR (20)	OUTPUT	
    ,@Costo					 VARCHAR (13)	OUTPUT	
    ,@MONEDA_ID				 VARCHAR (20)	OUTPUT
    ,@INGLOTEPROVEEDOR		 VARCHAR (1)	OUTPUT	
    ,@STATUS_LN				 VARCHAR (10)	OUTPUT
AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT OFF
	DECLARE @COUNT SMALLINT
	
	SET @STATUS_LN='0';
	
	--VALIDACION DEL CLIENTE
	SELECT @COUNT=COUNT(*) FROM CLIENTE WHERE CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTE_ID)))
	IF @COUNT=0
	BEGIN
		SET @STATUS_LN='1';
		RETURN
	END
	SET @COUNT=NULL

	--VALIDACION DEL PRODUCTO
	IF LTRIM(RTRIM(@PRODUCTO_ID))=''
	BEGIN
		SET @STATUS_LN='2';
		RETURN	
	END
	ELSE
	BEGIN
		
		SELECT @COUNT=COUNT(PRODUCTO_ID)FROM PRODUCTO WHERE CLIENTE_ID=@CLIENTE_ID AND PRODUCTO_ID=@PRODUCTO_ID
		IF @COUNT>0
		BEGIN
			SET @STATUS_LN='12';
			RETURN			
		END
	END
	
	--VALIDACION DE LA DESCRIPCION
	IF LTRIM(RTRIM(@DESCRIPCION))=''
	BEGIN
		SET @STATUS_LN='3';
		RETURN	
	END
	--SI FRACCIONABLE ES '' ENTONCES LO MANDO COMO UN 0
	IF (@FRACCIONABLE<>'1' AND @FRACCIONABLE<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@FRACCIONABLE)))=''
		BEGIN
			SET @FRACCIONABLE='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END
	--VALIDACION DE LA UNIDAD DE MEDIDA
	SELECT @COUNT=COUNT(*) FROM UNIDAD_MEDIDA WHERE UNIDAD_ID=LTRIM(RTRIM(UPPER(@UNIDAD_ID)))
	IF @COUNT=0
	BEGIN
		SET @STATUS_LN='6';
		RETURN		
	END
	--VALIDACION DE LA UNIDAD DE MEDIDA FRACCIONAMIENTO.
	IF LTRIM(RTRIM((@UNIDAD_FRACCION))) <> ''
	BEGIN
		SELECT @COUNT=COUNT(*) FROM UNIDAD_MEDIDA WHERE UNIDAD_ID=LTRIM(RTRIM(UPPER(@UNIDAD_FRACCION )))
		IF @COUNT=0
		BEGIN
			SET @STATUS_LN='6';
			RETURN		
		END
	END 
	SET @COUNT=NULL
	--VALIDACION DEL PAIS
	SELECT @COUNT=COUNT(*) FROM PAIS WHERE PAIS_ID=@PAIS_ID
	IF @COUNT=0
	BEGIN
		SET @STATUS_LN='7';
		RETURN		
	END
	SET @COUNT=NULL
	--VALIDACION DE LA FAMILIA DEL PRODUCTO
	SELECT @COUNT=COUNT(*) FROM FAMILIA_PRODUCTO WHERE FAMILIA_ID=LTRIM(RTRIM(UPPER(@FAMILIA_ID)))
	IF @COUNT=0
	BEGIN
		SET @STATUS_LN='5';
		RETURN		
	END
	SET @COUNT=NULL
	--VALIDO SI VA CON POSICIONES PURAS.
	IF (@POSICIONES_PURAS<>'1' AND @POSICIONES_PURAS<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@POSICIONES_PURAS)))=''
		BEGIN
			SET @POSICIONES_PURAS='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END
	--VALIDO SI VA CON POSICIONES PURAS.
	IF (@INGLOTEPROVEEDOR<>'1' AND @INGLOTEPROVEEDOR<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@INGLOTEPROVEEDOR)))=''
		BEGIN
			SET @INGLOTEPROVEEDOR='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END	
	--INGRESA PARTIDA.
	IF (@INGPARTIDA<>'1' AND @INGPARTIDA<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@INGPARTIDA)))=''
		BEGIN
			SET @INGPARTIDA='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END
	--SERIE INGRESO
	IF (@SERIE_ING<>'1' AND @SERIE_ING<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@SERIE_ING)))=''
		BEGIN
			SET @SERIE_ING='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END	
	--SERIE EGRESO
	IF (@SERIE_EGR<>'1' AND @SERIE_EGR<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@SERIE_EGR)))=''
		BEGIN
			SET @SERIE_EGR='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END		
	--VALIDO LA MONEDA
	IF LTRIM(RTRIM(UPPER(@MONEDA_ID)))<>''
	BEGIN
		SELECT @COUNT=COUNT(*) FROM MONEDAS WHERE MONEDA_ID=LTRIM(RTRIM(UPPER(@MONEDA_ID)))
		IF @COUNT=0
		BEGIN
			SET @STATUS_LN='8';
			RETURN		
		END
		SET @COUNT=NULL
	END
	ELSE
	BEGIN
		SET @MONEDA_ID=NULL 	
	END
	
	--SI VALE NADA LO MANDO COMO 0
	IF (@NO_AGRUPA_ITEMS<>'1' AND @NO_AGRUPA_ITEMS<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@NO_AGRUPA_ITEMS)))=''
		BEGIN
			SET @NO_AGRUPA_ITEMS='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END
	--POR EXCEL SI TIENE UNA , LA CAMBIO POR UN PUNTO.
	SET @LARGO	=REPLACE(@LARGO,',','.')
	SET @ALTO	=REPLACE(@ALTO,',','.')
	SET @ANCHO	=REPLACE(@ANCHO,',','.')
	SET @PESO	=REPLACE(@PESO,',','.')
	SET @Costo  =REPLACE(@COSTO,',','.')
	
	IF (@LOTE_AUTOMATICO<>'1' AND @LOTE_AUTOMATICO<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@LOTE_AUTOMATICO)))=''
		BEGIN
			SET @LOTE_AUTOMATICO='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END
	
	IF (@PALLET_AUTOMATICO<>'1' AND @PALLET_AUTOMATICO<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@PALLET_AUTOMATICO)))=''
		BEGIN
			SET @PALLET_AUTOMATICO='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END
	--POR SI VIENE CON UNA COMA.
	SET @TOLERANCIA_MIN=REPLACE(@TOLERANCIA_MIN,',','.')
	SET @TOLERANCIA_MAX=REPLACE(@TOLERANCIA_MAX,',','.')

	IF (@BACK_ORDER<>'1' AND @BACK_ORDER<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@BACK_ORDER)))=''
		BEGIN
			SET @BACK_ORDER='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END

	SELECT @COUNT=COUNT(*) FROM CATEGORIA_LOGICA WHERE CLIENTE_ID=LTRIM(RTRIM(UPPER(@CLIENTE_ID))) AND CAT_LOG_ID=LTRIM(RTRIM(UPPER(@ING_CAT_LOG_ID)))
	IF @COUNT=0
	BEGIN
		SET @STATUS_LN='9';
		RETURN		
	END
	SET @COUNT=NULL	

	SELECT @COUNT=COUNT(*) FROM SUB_FAMILIA WHERE SUB_FAMILIA_ID=LTRIM(RTRIM(UPPER(@SUB_FAMILIA_ID)))
	IF @COUNT=0
	BEGIN
		SET @STATUS_LN='5';
		RETURN		
	END
	SET @COUNT=NULL	
	
	IF (@GRUPO_PRODUCTO IS NOT NULL)AND(LTRIM(RTRIM(@GRUPO_PRODUCTO))<>'')
	BEGIN
		SELECT @COUNT=COUNT(*) FROM TIPO_PRODUCTO WHERE TIPO_PRODUCTO_ID=LTRIM(RTRIM(UPPER(@GRUPO_PRODUCTO)))
		IF @COUNT=0
		BEGIN
			SET @STATUS_LN='10';
			RETURN		
		END
		SET @COUNT=NULL		
	END 
	
	IF (@ENVASE<>'1' AND @ENVASE<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@ENVASE)))=''
		BEGIN
			SET @ENVASE='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END

	IF (@VAL_COD_ING<>'1' AND @VAL_COD_ING<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@VAL_COD_ING)))=''
		BEGIN
			SET @VAL_COD_ING='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END

	IF (@VAL_COD_EGR<>'1' AND @VAL_COD_EGR<>'0')
	BEGIN
		IF LTRIM(RTRIM(UPPER(@VAL_COD_EGR)))=''
		BEGIN
			SET @VAL_COD_EGR='0'
		END
		ELSE
		BEGIN
			SET @STATUS_LN='4';
			RETURN
		END
	END

	IF(LTRIM(RTRIM(@ROTACION_ID))<>'')
	BEGIN
		SELECT @COUNT=COUNT(*) FROM NIVEL_ROTACION WHERE ROTACION_ID=LTRIM(RTRIM(UPPER(@ROTACION_ID)))
		IF @COUNT=0
		BEGIN
			SET @STATUS_LN='11';
			RETURN		
		END
		SET @COUNT=NULL	
	END
	ELSE
	BEGIN
		SET @ROTACION_ID =NULL
	END
	
	IF(LTRIM(RTRIM(@TIPO_PRODUCTO_ID))='')
	BEGIN
		SET @TIPO_PRODUCTO_ID=NULL 
	END 
	
	--SI LLEGUE HASTA ACA ES PORQUE TODO ESTA OK.
	INSERT INTO PRODUCTO (	 CLIENTE_ID			,PRODUCTO_ID		,DESCRIPCION		,FRACCIONABLE
							,UNIDAD_ID			,PAIS_ID			,FAMILIA_ID			,OBSERVACIONES
							,POSICIONES_PURAS	,MONEDA_ID			,NO_AGRUPA_ITEMS	,LARGO			
							,ALTO				,ANCHO				,UNIDAD_VOLUMEN		,VOLUMEN_UNITARIO	
							,PESO				,UNIDAD_PESO		,PESO_UNITARIO		,LOTE_AUTOMATICO	
							,PALLET_AUTOMATICO	,INGRESO			,EGRESO				,TOLERANCIA_MIN		
							,TOLERANCIA_MAX		,BACK_ORDER			,CLASIFICACION_COT	,ING_CAT_LOG_ID		
							,SUB_FAMILIA_ID		,GRUPO_PRODUCTO		,ENVASE				,VAL_COD_ING		
							,VAL_COD_EGR		,ROTACION_ID		,SUBCODIGO_1		,UNIDAD_FRACCION
							,TIPO_PRODUCTO_ID	,TIPO_CONTENEDORA	,ingPartida			,SERIE_ING 
							,SERIE_EGR			,COSTO   
	)
	VALUES(
		 LTRIM(RTRIM(UPPER(@CLIENTE_ID)))				,LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))		
		,LTRIM(RTRIM(UPPER(@DESCRIPCION)))				,LTRIM(RTRIM(UPPER(@FRACCIONABLE)))
		,LTRIM(RTRIM(UPPER(@UNIDAD_ID)))				,LTRIM(RTRIM(UPPER(@PAIS_ID)))
		,LTRIM(RTRIM(UPPER(@FAMILIA_ID)))				,LTRIM(RTRIM(UPPER(@OBSERVACIONES)))
		,LTRIM(RTRIM(UPPER(@POSICIONES_PURAS)))			,LTRIM(RTRIM(UPPER(@MONEDA_ID)))
		,LTRIM(RTRIM(UPPER(@NO_AGRUPA_ITEMS)))			,CAST(@LARGO AS FLOAT)
		,CAST(@ALTO AS FLOAT)							,CAST(@ANCHO AS FLOAT)
		,'M3'											,'1'
		,CAST(@PESO AS FLOAT)							,'KG'
		,'1'											,LTRIM(RTRIM(UPPER(@LOTE_AUTOMATICO)))
		,LTRIM(RTRIM(UPPER(@PALLET_AUTOMATICO)))		,'ING_ABAST_F'
		,'PICK_ABAST'									,CAST(@TOLERANCIA_MIN AS FLOAT)
		,CAST(@TOLERANCIA_MAX AS FLOAT)					,LTRIM(RTRIM(UPPER(@BACK_ORDER)))
		,LTRIM(RTRIM(UPPER(@CLASIFICACION_COT)))		,LTRIM(RTRIM(UPPER(@ING_CAT_LOG_ID)))
		,LTRIM(RTRIM(UPPER(@SUB_FAMILIA_ID)))			,LTRIM(RTRIM(UPPER(@GRUPO_PRODUCTO)))
		,LTRIM(RTRIM(UPPER(@ENVASE)))					,LTRIM(RTRIM(UPPER(@VAL_COD_ING)))
		,LTRIM(RTRIM(UPPER(@VAL_COD_EGR)))				,LTRIM(RTRIM(UPPER(@ROTACION_ID)))
		,LTRIM(RTRIM(UPPER(@VALIDACION_VENCIMIENTO)))	,LTRIM(RTRIM(UPPER(@UNIDAD_FRACCION)))
		,LTRIM(RTRIM(UPPER(@TIPO_PRODUCTO_ID)))			,LTRIM(RTRIM(UPPER(@TIPO_CONTENEDORA)))
		,LTRIM(RTRIM(UPPER(@INGPARTIDA)))				,LTRIM(RTRIM(UPPER(@SERIE_ING)))
		,LTRIM(RTRIM(UPPER(@SERIE_EGR)))				,CAST(@Costo AS FLOAT)
	)

	INSERT INTO MANDATORIO_PRODUCTO VALUES(
		 LTRIM(RTRIM(UPPER(@CLIENTE_ID)))		
		,LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
		,'ING'
		,'CANTIDAD')

	INSERT INTO MANDATORIO_PRODUCTO VALUES(
		 LTRIM(RTRIM(UPPER(@CLIENTE_ID)))		
		,LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
		,'EGR'
		,'CANTIDAD')

END--FIN PROCEDURE
