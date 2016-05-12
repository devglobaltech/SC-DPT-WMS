
/****** Object:  StoredProcedure [dbo].[SP_CARGA_POSICIONES]    Script Date: 01/09/2015 16:25:28 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SP_CARGA_POSICIONES]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SP_CARGA_POSICIONES]
GO
-- ===================================================================================================================================================
-- Author:		<GLOBALTECH S.A., Lenin Bueno Escolar>
-- Create date: <Viernes, 16 de Septiembre de 2011,>
-- Description:	<SP util para validar datos desde un archivo csv, da la opción de insertar regsitros en las tablas de Nave, Calle, Nivel y Posición>
-- ===================================================================================================================================================
CREATE PROCEDURE [dbo].[SP_CARGA_POSICIONES]
@PRM_COD_EMP AS VARCHAR (15),    --Codigo de Emplazamiento
@PRM_COD_DEPOSITO AS VARCHAR(15), --Codigo de Deposito
@PRM_COD_NAVE AS VARCHAR(15),  --Codigo de Nave
@PRM_COD_CALLE AS VARCHAR(10), --Codigo Calle
@PRM_COD_COLUMN AS VARCHAR(10), --Codigo Columna
@PRM_COD_NIVEL AS VARCHAR(10), -- Codigo Nivel
@PRM_PROFUNDIDAD AS VARCHAR(10),  --Codigo de Nivel que hace referencia a un nivel que es hijo de otro nivel.
@PRM_PESO AS NUMERIC(25,5), --Peso
@PRM_LARGO AS NUMERIC(10,3), --Largo
@PRM_ALTO AS NUMERIC (10,3), --Alto
@PRM_ANCHO AS NUMERIC(10,3), --Ancho
@PRM_PICKING AS VARCHAR(10), --Posición de Picking
@PRM_ORDENPICKING AS NUMERIC(6,0), --Orden der Picking
@PRM_ORDENING AS NUMERIC(6,0), --Orden de Ingreso u orden locator
@PRM_INTERMEDIA AS VARCHAR(10), --Intermedia
@PRM_POS_ABASTECIBLE AS VARCHAR(10), --Posición Abastecible
@PRM_POS_BESTFIT AS VARCHAR(10), -- Posición BestFit
@PRM_ORD_BESTFIT AS NUMERIC(20,0), -- Orden BestFit
@PRM_PCJ_OCUPACION AS NUMERIC(20,2)
AS
BEGIN

  SET NOCOUNT ON;
  DECLARE @ID_NAV AS INT, @ID_CALL AS INT, @ID_COL AS INT, @ID_NIV AS INT
  DECLARE @ERR AS VARCHAR(MAX)

  DECLARE @HIJA_DE AS NUMERIC(20,0)
  DECLARE @ORDEN AS INT
  DECLARE @DEPOSITO AS VARCHAR(15)

  -- Tabla Calle_Nave
  DECLARE @ID_CALL_MAX AS INT
  DECLARE @POS_Y AS INT
  
  -- Tabla Columna_Nave
  DECLARE @ID_COL_MAX AS INT
  
  -- Tabla Nivel_Nave
  DECLARE @ID_NIV_MAX AS INT
  
  -- Tabla Posiciones
  DECLARE @ID_POS_MAX AS INT
  
  DECLARE @CountNivel as integer
  
  DECLARE @PosicionGuion AS INT
  
  SET @ERR  = ''
  IF @PRM_PROFUNDIDAD IS NULL OR RTRIM(LTRIM(@PRM_PROFUNDIDAD))='' SET @PRM_PROFUNDIDAD = NULL
	-- Se valida si el código de emplazamiento existe.
	IF NOT EXISTS(SELECT * FROM EMPLAZAMIENTO WHERE emplazamiento_id = @PRM_COD_EMP) BEGIN
		SET @ERR = @ERR + 'El código de emplazamiento (' + @PRM_COD_EMP + ') no está registrado en la Base de Datos. ' 
	END 
	-- Se valida si el código del depóstio existe.
	IF NOT EXISTS(SELECT * FROM DEPOSITO_EXTERNO WHERE DEPOSITO_EXTERNO_ID = @PRM_COD_DEPOSITO) BEGIN
		SET @ERR = @ERR + 'El código de depósito (' + @PRM_COD_DEPOSITO + ') no está registrado en la Base de Datos. ' 
	END 
	--Se valida si el Código de la Nave está en la Tabla de Nave, sino está se envía un mensaje de error.
	IF NOT EXISTS(SELECT * FROM NAVE WHERE NAVE_COD = @PRM_COD_NAVE) BEGIN
		SET @ERR = @ERR + 'El código de la Nave (' + @PRM_COD_NAVE + ') no está registrado en la Base de Datos. ' 
	END 

	--Se valida si la Nave tiene layout (1), sino tiene se envía un mensaje de error.
	IF EXISTS(SELECT * FROM NAVE WHERE NAVE_COD = @PRM_COD_NAVE AND NAVE_TIENE_LAYOUT=0) BEGIN
		SET @ERR = @ERR + 'La Nave (' + @PRM_COD_NAVE + ') trata de una ubicación de piso sin Layout de posiciones. ' 
	END 

	IF UPPER(@PRM_PICKING) IN ('VERDADERO','SI','TRUE','S','V')  SET @PRM_PICKING = '1'
	IF UPPER(@PRM_PICKING) IN ('FALSO','NO','FALSE','N','F')  SET @PRM_PICKING = '0'
	IF UPPER(@PRM_PICKING) NOT IN ('VERDADERO','SI','TRUE','S','V','1','FALSO','NO','FALSE','N','F','0') SET @ERR= @ERR + 'La columna [Nave de Picking] solo admite valores lógicos, 1 o 0, Falso 0 Verdadero, Si o No, etc. '

	IF UPPER(@PRM_INTERMEDIA) IN ('VERDADERO','SI','TRUE','S','V')  SET @PRM_INTERMEDIA = '1'
	IF UPPER(@PRM_INTERMEDIA) IN ('FALSO','NO','FALSE','N','F')  SET @PRM_INTERMEDIA = '0'
	IF UPPER(@PRM_INTERMEDIA) NOT IN ('VERDADERO','SI','TRUE','S','V','1','FALSO','NO','FALSE','N','F','0') SET @ERR= @ERR + 'La colmna [Nave Intermedia] solo admite valores lógicos, 1 o 0, Falso 0 Verdadero, Si o No, etc. '

	IF UPPER(@PRM_POS_ABASTECIBLE) IN ('VERDADERO','SI','TRUE','S','V')  SET @PRM_POS_ABASTECIBLE = '1'
	IF UPPER(@PRM_POS_ABASTECIBLE) IN ('FALSO','NO','FALSE','N','F')  SET @PRM_POS_ABASTECIBLE = '0'
	IF UPPER(@PRM_POS_ABASTECIBLE) NOT IN ('VERDADERO','SI','TRUE','S','V','1','FALSO','NO','FALSE','N','F','0') SET @ERR= @ERR + 'La columna [Posición Abastecible] solo admite valores lógicos, 1 o 0, Falso 0 Verdadero, Si o No, etc. '

	IF UPPER(@PRM_POS_BESTFIT) IN ('VERDADERO','SI','TRUE','S','V')  SET @PRM_POS_BESTFIT = '1'
	IF UPPER(@PRM_POS_BESTFIT) IN ('FALSO','NO','FALSE','N','F')  SET @PRM_POS_BESTFIT = '0'
	IF UPPER(@PRM_POS_BESTFIT) NOT IN ('VERDADERO','SI','TRUE','S','V','1','FALSO','NO','FALSE','N','F','0') SET @ERR= @ERR + 'La columna [Posición BestFit] solo admite valores lógicos, 1 o 0, Falso 0 Verdadero, Si o No, etc. '

	IF ISNUMERIC(@PRM_PESO)<> 1 OR @PRM_PESO IS NULL SET @ERR = @ERR + 'La columna [Peso] solo admite valores numéricos. '
	IF ISNUMERIC(@PRM_LARGO)<> 1 OR @PRM_LARGO IS NULL SET @ERR = @ERR + 'La columna [Largo] solo admite valores numéricos. '
	IF ISNUMERIC(@PRM_ALTO)<> 1 OR @PRM_ALTO IS NULL SET @ERR = @ERR + 'La columna [Alto] solo admite valores numéricos. '
	IF ISNUMERIC(@PRM_ANCHO)<> 1 OR @PRM_ANCHO IS NULL SET @ERR = @ERR + 'La columna [Ancho] solo admite valores numéricos. '

	IF @ERR = '' 
		BEGIN
    
			-- Se valida si el Código de la Calle está en la Tabla de Calle_Nave, sino está se inserta el registro en dicha tabla
			SELECT @ID_NAV = nave_id from NAVE where NAVE_COD=@PRM_COD_NAVE
      
			
            IF (@PRM_COD_CALLE IS NOT NULL)
                BEGIN -- CALLE_NAVE
                    IF NOT EXISTS(SELECT * FROM calle_nave WHERE calle_cod = @PRM_COD_CALLE AND nave_id = @ID_NAV) 
                        BEGIN
                            SELECT @ID_CALL_MAX = MAX(calle_id)+1 FROM calle_nave
                            IF @ID_CALL_MAX IS NULL
                              BEGIN
                              SET @ID_CALL_MAX=1
                              END
                              
                            SELECT @POS_Y = ISNULL(MAX(pos_y)+500,1000) FROM calle_nave WHERE nave_id = @ID_NAV 
                            INSERT INTO calle_nave(calle_id,
                                                    nave_id,
                                                    calle_cod,
                                                    descripcion,
                                                    pos_horizontal,
                                                    pos_x,
                                                    pos_y,
                                                    modif_layout)
                                            VALUES (@ID_CALL_MAX,
                                                    @ID_NAV,
                                                    @PRM_COD_CALLE,
                                                    'CALLE ' + @PRM_COD_CALLE,
                                                    'H',
                                                    330,
                                                    @POS_Y,
                                                    0)
                        END  
                    -- Se valida si el Código de la Columna está en la Tabla de COLUMNA_NAVE, sino está se inserta el registro
                    SELECT @ID_CALL = calle_id from calle_nave where calle_cod=@PRM_COD_CALLE AND nave_id=@ID_NAV
                    
                    IF (@PRM_COD_COLUMN IS NOT NULL) AND (@PRM_COD_COLUMN <> '')
                        BEGIN -- COLUMNA_NAVE
                            IF NOT EXISTS(SELECT * FROM COLUMNA_NAVE WHERE COLUMNA_COD = @PRM_COD_COLUMN AND NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL)  
                                BEGIN
                                    SELECT @ID_COL_MAX = MAX(COLUMNA_ID) + 1 FROM COLUMNA_NAVE
                                    IF @ID_COL_MAX IS NULL
                                    BEGIN
                                    SET @ID_COL_MAX=1
                                    END
                                    INSERT INTO COLUMNA_NAVE(COLUMNA_ID,
                                                            NAVE_ID,
                                                            CALLE_ID,
                                                            COLUMNA_COD,
                                                            DESCRIPCION,
                                                            MODIF_LAYOUT)
                                                    VALUES (@ID_COL_MAX,
                                                            @ID_NAV,
                                                            @ID_CALL,
                                                            @PRM_COD_COLUMN,	
                                                            'COLUMNA ' + @PRM_COD_COLUMN,
                                                            0)
                                END 
                            -- Se valida si el Código del Nivel está en la Tabla de NIVEL_NAVE, sino está se inserta el registro
                            SELECT @ID_COL = COLUMNA_ID from COLUMNA_NAVE where COLUMNA_COD = @PRM_COD_COLUMN AND NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL 
                            SELECT @PosicionGuion =  CASE WHEN CHARINDEX('-', NIVEL_COD)>0 THEN CHARINDEX('-', NIVEL_COD)-1 ELSE LEN(NIVEL_COD) END  
								FROM NIVEL_NAVE WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL  AND ORDEN = 1 AND COLUMNA_ID = @ID_COL
                                                                                                                                              
                            SET @HIJA_DE = (CASE 
                                                WHEN NOT @PRM_PROFUNDIDAD IS NULL 
                                                THEN (SELECT NIVEL_ID 
                                                      FROM NIVEL_NAVE 
                                                      WHERE NIVEL_COD = (SELECT NIVEL_COD
                                                                        FROM NIVEL_NAVE
                                                                        WHERE NAVE_ID = @ID_NAV
                                                                        AND CALLE_ID = @ID_CALL
                                                                        AND COLUMNA_ID = @ID_COL
                                                                        --AND SUBSTRING(NIVEL_COD, 1, (LEN(NIVEL_COD) - (CASE WHEN CHARINDEX('-', NIVEL_COD)>0 THEN CHARINDEX('-', NIVEL_COD)+1 ELSE CHARINDEX('-', NIVEL_COD)END)))
                                                                        --AND SUBSTRING(NIVEL_COD, 1, CHARINDEX('-', NIVEL_COD)-1)= @PRM_COD_NIVEL
                                                                        AND SUBSTRING(NIVEL_COD,1,@PosicionGuion) = @PRM_COD_NIVEL
                                                                        AND ORDEN = 1)
                                                      AND NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL 
                                                      AND COLUMNA_ID = @ID_COL) 
                                                ELSE NULL 
                                            END )
                            
                            IF (@PRM_COD_NIVEL IS NOT NULL) AND (@PRM_COD_NIVEL <> '')                                
                                BEGIN -- NIVEL_NAVE
                                    SELECT @ID_NIV_MAX = MAX(NIVEL_ID)+1 FROM NIVEL_NAVE
                                    IF @ID_NIV_MAX IS NULL
                                    BEGIN
                                    SET @ID_NIV_MAX=1
                                    END
                                    IF NOT EXISTS (
                                                    SELECT * FROM NIVEL_NAVE 
                                                    WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL 
                                                    AND NIVEL_COD LIKE @PRM_COD_NIVEL + '%'
                                                    -- AND HIJA_DE IS NULL
                                                    ) 
                                        BEGIN
                                            -- No existe Nivel
                                            BEGIN TRY 
                                                SELECT @ORDEN=COUNT(*)+1 FROM NIVEL_NAVE WHERE HIJA_DE = @HIJA_DE AND NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL
                                                
                                                INSERT INTO NIVEL_NAVE (NIVEL_ID,
                                                                        NAVE_ID,
                                                                        CALLE_ID,
                                                                        COLUMNA_ID,
                                                                        NIVEL_COD,
                                                                        DESCRIPCION,
                                                                        ORDEN,
                                                                        MODIF_LAYOUT,
                                                                        HIJA_DE)
                                                                VALUES (@ID_NIV_MAX,
                                                                        @ID_NAV,
                                                                        @ID_CALL,
                                                                        @ID_COL,
                                                                        (CASE WHEN @PRM_PROFUNDIDAD IS NULL THEN @PRM_COD_NIVEL ELSE @PRM_COD_NIVEL +'-'+@PRM_PROFUNDIDAD END ),
                                                                        'NIVEL ' + @PRM_COD_NIVEL,
                                                                        (CASE WHEN @HIJA_DE IS NULL THEN @ORDEN ELSE 0 END ),
                                                                        0,
                                                                        (CASE 
                                                                            WHEN @PRM_PROFUNDIDAD IS NULL 
                                                                                THEN NULL 
                                                                            ELSE (CASE 
                                                                                    WHEN @HIJA_DE IS NULL 
                                                                                        THEN @ID_NIV_MAX 
                                                                                    ELSE @HIJA_DE 
                                                                                    END 
                                                                                    ) 
                                                                            END 
                                                                        )
                                                                        )
                        
                                            END TRY
                                            BEGIN CATCH
                                                PRINT ''
                                            END CATCH
                                        END
                                    ELSE
                                        BEGIN
                                            SELECT @ERR = dbo.FX_VALIDA_STOCK_RELACION_POSIC(@PRM_COD_NAVE, @PRM_COD_CALLE, @PRM_COD_COLUMN, @PRM_COD_NIVEL, @PRM_PROFUNDIDAD)
                                            IF @ERR = ''
                                                BEGIN
                                                    IF EXISTS(SELECT * FROM NIVEL_NAVE 
                                                              WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL 
                                                              AND NIVEL_COD = @PRM_COD_NIVEL
                                                              AND HIJA_DE IS NULL
                                                              ) 
                                                    AND @PRM_PROFUNDIDAD = '1'
                                                        BEGIN
                                                            -- Existe Nivel sin Profundidad
                                                            UPDATE NIVEL_NAVE
                                                            SET NIVEL_COD = CASE WHEN @PRM_PROFUNDIDAD IS NULL 
                                                                                THEN @PRM_COD_NIVEL
                                                                                ELSE @PRM_COD_NIVEL + '-' + @PRM_PROFUNDIDAD
                                                                            END ,
                                                                HIJA_DE = @HIJA_DE 
                                                            WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL 
                                                            AND (NIVEL_COD = @PRM_COD_NIVEL OR NIVEL_COD = @PRM_COD_NIVEL + '-' + @PRM_PROFUNDIDAD) 
                                                            AND HIJA_DE IS NULL
                                                        END
                                                    ELSE
                                                        BEGIN
                                                            IF @PRM_PROFUNDIDAD IS NOT NULL
                                                                BEGIN
                                                                    SELECT @CountNivel = COUNT(*) FROM NIVEL_NAVE 
                                                                    WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL 
                                                                    AND NIVEL_COD = @PRM_COD_NIVEL + '-' + @PRM_PROFUNDIDAD
                                                                END
                                                            ELSE
                                                                BEGIN
                                                                    SELECT @CountNivel = COUNT(*) FROM NIVEL_NAVE 
                                                                    WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL 
                                                                    AND SUBSTRING(NIVEL_COD, 1, (LEN(NIVEL_COD) - CHARINDEX('-', NIVEL_COD))) = @PRM_COD_NIVEL
                                                                END
                                                            
                                                            IF @CountNivel = 0
                                                                BEGIN
                                                                    INSERT INTO NIVEL_NAVE (NIVEL_ID,
                                                                                            NAVE_ID,
                                                                                            CALLE_ID,
                                                                                            COLUMNA_ID,
                                                                                            NIVEL_COD,
                                                                                            DESCRIPCION,
                                                                                            ORDEN,
                                                                                            MODIF_LAYOUT,
                                                                                            HIJA_DE)
                                                                                    VALUES (@ID_NIV_MAX,
                                                                                            @ID_NAV,
                                                                                            @ID_CALL,
                                                                                            @ID_COL,
                                                                                            (CASE WHEN @PRM_PROFUNDIDAD IS NULL THEN @PRM_COD_NIVEL ELSE @PRM_COD_NIVEL +'-'+@PRM_PROFUNDIDAD END ),
                                                                                            'NIVEL ' + @PRM_COD_NIVEL,
                                                                                            (CASE WHEN @HIJA_DE IS NULL THEN @ORDEN ELSE 0 END ),
                                                                                            0,
                                                                                            (CASE 
                                                                                                WHEN @PRM_PROFUNDIDAD IS NULL 
                                                                                                    THEN NULL 
                                                                                                ELSE (CASE 
                                                                                                        WHEN @HIJA_DE IS NULL 
                                                                                                            THEN @ID_NIV_MAX 
                                                                                                        ELSE @HIJA_DE 
                                                                                                        END 
                                                                                                        ) 
                                                                                                END 
                                                                                            )
                                                                                            )
                                                                END
                                                            IF @CountNivel > 1
                                                                BEGIN
                                                                    SET @ERR = 'Este nivel tiene profundidades y no se pueden eliminar de manera automÃƒÆ’Ã‚Â¡tica'
                                                                    SELECT @ERR ERR
                                                                END
                                                        END
                                                END
                                            ELSE
                                                BEGIN
                                                    SELECT @ERR ERR
                                                END
                                        END
                                    -- Paso final para el cargue de la información a la tabla de Posiciones  
                                    IF NOT @PRM_PROFUNDIDAD IS NULL 
                                        BEGIN
                                            SET @PRM_COD_NIVEL = @PRM_COD_NIVEL + '-' + @PRM_PROFUNDIDAD
                                        END
                                    
                                    SELECT DISTINCT @ID_NIV = NIVEL_ID, @HIJA_DE = HIJA_DE, @ORDEN = ORDEN FROM NIVEL_NAVE 
                                    WHERE NIVEL_COD = @PRM_COD_NIVEL AND NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL
                                    
                                    IF NOT EXISTS(SELECT * FROM POSICION WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL AND NIVEL_ID = @ID_NIV) 
                                      AND @ERR = ''
                                        BEGIN 
                                           SELECT @ID_POS_MAX = MAX(POSICION_ID)+1 FROM POSICION   
                                           IF @ID_POS_MAX IS NULL
                                           BEGIN
                                           SET @ID_POS_MAX=1
                                           END
                                           INSERT INTO POSICION (POSICION_ID,
                                                                 NAVE_ID,
                                                                 CALLE_ID,
                                                                 COLUMNA_ID,
                                                                 NIVEL_ID,
                                                                 POSICION_COD,
                                                                 UNIDAD_VOLUMEN,
                                                                 PESO,
                                                                 UNIDAD_PESO,
                                                                 POS_VACIA,
                                                                 POS_LOCKEADA,
                                                                 POS_COMPLETA,
                                                                 MODIF_LAYOUT,
                                                                 LCK_TIPO_OPERACION,
                                                                 LARGO,
                                                                 ALTO,
                                                                 ANCHO,
                                                                 PICKING,
                                                                 PENETRABLE,
                                                                 HIJA_DE,
                                                                 ORDEN,
                                                                 ORDEN_PICKING,
                                                                 VOLUMEN,
                                                                 ORDEN_LOCATOR,
                                                                 intermedia,
                                                                 ABASTECIBLE,
                                                                 BESTFIT,
                                                                 Orden_BestFit,
                                                                 PCJ_OCUPACION)																 
                                                         VALUES (@ID_POS_MAX,
                                                                 @ID_NAV,
                                                                 @ID_CALL,
                                                                 @ID_COL,
                                                                 @ID_NIV,	
                                                                 @PRM_COD_NAVE + '-' + @PRM_COD_CALLE + '-' + @PRM_COD_COLUMN + '-' + @PRM_COD_NIVEL,
                                                                 'M3',
                                                                 @PRM_PESO,
                                                                 'KG',
                                                                 '1',
                                                                 '0',
                                                                 '0',
                                                                 '0',	
                                                                 '0',							 
                                                                 @PRM_LARGO,
                                                                 @PRM_ALTO,
                                                                 @PRM_ANCHO,
                                                                 @PRM_PICKING,
                                                                 (CASE WHEN @PRM_PROFUNDIDAD IS NULL 
                                                                    THEN '0' 
                                                                    ELSE '1' 
                                                                    END 
                                                                 ),
                                                                 (CASE WHEN @PRM_PROFUNDIDAD IS NULL THEN NULL ELSE ISNULL(@HIJA_DE, @ID_POS_MAX) END ),
                                                                 (CASE WHEN @PRM_PROFUNDIDAD IS NULL THEN NULL ELSE @PRM_PROFUNDIDAD END ),
                                                                 @PRM_ORDENPICKING,
                                                                 @PRM_LARGO * @PRM_ALTO * @PRM_ANCHO,
                                                                 @PRM_ORDENING,
                                                                 @PRM_INTERMEDIA,
                                                                 @PRM_POS_ABASTECIBLE,
                                                                 @PRM_POS_BESTFIT,
                                                                 @PRM_ORD_BESTFIT,
                                                                 @PRM_PCJ_OCUPACION)
                                        END 
                                    ELSE 
                                        BEGIN
                                            -- SET @ERR='En este Registro los datos de Nave, Calle, Columna y Nivel ya están ingresados en la Base de Datos, No se pueden duplicar.'
                                            -- SELECT ERR = @ERR 
                                            IF @ERR = ''
                                                BEGIN
                                                    UPDATE POSICION 
                                                    SET POSICION_COD = @PRM_COD_NAVE + '-' + @PRM_COD_CALLE + '-' + @PRM_COD_COLUMN + '-' + @PRM_COD_NIVEL,
                                                        PENETRABLE = (CASE WHEN @PRM_PROFUNDIDAD IS NULL 
                                                                        THEN '0' 
                                                                        ELSE '1' 
                                                                        END 
                                                                      ),
                                                        HIJA_DE = CASE WHEN @PRM_PROFUNDIDAD IS NULL 
                                                                    THEN NULL 
                                                                    ELSE ISNULL(@HIJA_DE, @ID_POS_MAX)
                                                                  END ,
                                                        ORDEN = (CASE WHEN @PRM_PROFUNDIDAD IS NULL THEN NULL ELSE @PRM_PROFUNDIDAD END )
                                                    WHERE NAVE_ID = @ID_NAV AND CALLE_ID = @ID_CALL AND COLUMNA_ID = @ID_COL AND NIVEL_ID = @ID_NIV
                                                END
                                        END
                                END -- NIVEL_NAVE
                        END -- COLUMNA_NAVE
                END -- CALLE_NAVE
		END 
	ELSE 
		BEGIN
			SELECT ERR = @ERR  
		END
END

GO


