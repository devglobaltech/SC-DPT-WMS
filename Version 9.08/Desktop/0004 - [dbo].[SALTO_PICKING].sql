

ALTER                     PROCEDURE [dbo].[SALTO_PICKING]
	@USUARIO 			AS VARCHAR(30),
	@VIAJEID 			AS VARCHAR(100),
	@PRODUCTO_ID		AS VARCHAR(50),
	@POSICION_COD		AS VARCHAR(45),
	@PALLET				AS VARCHAR(100),
	@RUTA				AS VARCHAR(50)
AS
BEGIN

	DECLARE @MAX		AS INT 
	DECLARE @SALTO		AS INT
	DECLARE @XSQL		AS VARCHAR(100)
	DECLARE @TRUTA		AS VARCHAR(2)
	DECLARE @CLIENTE	AS VARCHAR(50)
	DECLARE @MSG		AS VARCHAR(4000)
	
	IF (RTRIM(LTRIM(@PALLET))='')
	BEGIN
		SET @PALLET=NULL;
	END
	/*
	SET @MSG='USUARIO: ' + @USUARIO;
	RAISERROR(@MSG,16,1);
	SET @MSG='@VIAJEID: ' + @VIAJEID;
	RAISERROR(@MSG,16,1);
	SET @MSG='@PRODUCTO_ID: ' + @PRODUCTO_ID;
	RAISERROR(@MSG,16,1);
	SET @MSG='@POSICION_COD: ' + @POSICION_COD;
	RAISERROR(@MSG,16,1);
	SET @MSG='@PALLET: ' + isnull(@PALLET,'NULL');
	RAISERROR(@MSG,16,1);	
	SET @MSG='@RUTA: ' + @RUTA;
	RAISERROR(@MSG,16,1);	
	*/	
	
	SELECT DISTINCT @CLIENTE=P.CLIENTE_ID FROM PICKING P WHERE LTRIM(RTRIM(UPPER(P.VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID))) 
	SELECT @TRUTA=ISNULL(FLG_TOMAR_RUTA,'0') FROM CLIENTE_PARAMETROS WHERE CLIENTE_ID=@CLIENTE

	SELECT 	@SALTO=COUNT(PICKING_ID)
	FROM	PICKING P
			INNER JOIN RL_VIAJE_USUARIO rl on(rl.viaje_id = p.viaje_id)
	WHERE	P.FECHA_INICIO IS NULL AND
			P.FECHA_FIN IS NULL AND
			P.CANT_CONFIRMADA IS NULL AND
			RL.USUARIO_id=LTRIM(RTRIM(UPPER(@USUARIO))) 
			and LTRIM(RTRIM(UPPER(P.VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID))) 
			AND ((@TRUTA='1')OR(LTRIM(RTRIM(UPPER(P.RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))))


	IF @SALTO=0
		BEGIN
			RAISERROR('Este es el ultimo item de la ruta. No es posible realizar el salto.',16,1)
			RETURN
		END

	SELECT 	@MAX=MAX(SALTO_PICKING) +1
	FROM 	PICKING
	WHERE	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))

	UPDATE 	PICKING SET FECHA_INICIO=NULL, PALLET_PICKING=NULL, SALTO_PICKING=@MAX, USUARIO=NULL
	WHERE 	USUARIO=LTRIM(RTRIM(UPPER(@USUARIO)))
			AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
			AND PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
			AND POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD)))
			AND ((PROP1 IS NULL)OR(PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
			AND ((@TRUTA='1')OR(LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))))
			AND PICKING_ID NOT IN (	SELECT 	PICKING_ID 
									FROM 	PICKING P2
									WHERE	USUARIO=LTRIM(RTRIM(UPPER(@USUARIO)))
											AND LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(@VIAJEID)))
											AND PRODUCTO_ID=LTRIM(RTRIM(UPPER(@PRODUCTO_ID)))
											AND POSICION_COD=LTRIM(RTRIM(UPPER(@POSICION_COD)))
											AND ((PROP1 IS NULL) OR(PROP1=LTRIM(RTRIM(UPPER(@PALLET)))))
											AND ((@TRUTA='1')OR(LTRIM(RTRIM(UPPER(RUTA)))=LTRIM(RTRIM(UPPER(@RUTA)))))
											AND FECHA_INICIO IS NOT NULL AND FECHA_FIN IS NOT NULL AND PALLET_PICKING IS NOT NULL
											AND (PICKING.PICKING_ID=P2.PICKING_ID)
			)

	IF @@ERROR <>0
		BEGIN
			PRINT 'OCURRIO UN ERROR AL SALTAR EL PICKING'
			RETURN (99)
		END
END




























