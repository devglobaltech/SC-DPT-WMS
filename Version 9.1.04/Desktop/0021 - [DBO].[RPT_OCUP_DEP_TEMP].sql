

/****** Object:  StoredProcedure [dbo].[RPT_OCUP_DEP_TEMP]    Script Date: 02/07/2014 13:16:02 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RPT_OCUP_DEP_TEMP]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[RPT_OCUP_DEP_TEMP]
GO


/****** Object:  StoredProcedure [dbo].[RPT_OCUP_DEP_TEMP]    Script Date: 02/07/2014 13:16:02 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [DBO].[RPT_OCUP_DEP_TEMP]
	@FDESDE			VARCHAR(8)		OUTPUT,	--ANSI
	@FHASTA			VARCHAR(8)		OUTPUT,	--ANSI
	@NAVE_ID		VARCHAR(15)		OUTPUT	
AS
BEGIN
	DECLARE @CONT			FLOAT
	DECLARE @CONT2			FLOAT
	DECLARE @FECHA_COMP		DATETIME
	DECLARE @NAVE_ID_01		VARCHAR(15)
	
	
	--/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	--////////////////////////////		POSICIONES LIBRES Y VOLUMEN DE DICHAS POSICIONES 	///////////////////////////////////////
	--/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	--CREATE TABLE TEMP_RPT_OCUP_DEP(FECHA_SNAP DATETIME, NAVE_ID VARCHAR(15) ,POSICIONES_LIBRES	INT, VOLUMEN_LIBRE	FLOAT )
	TRUNCATE TABLE TEMP_RPT_OCUP_DEP
	
	INSERT INTO TEMP_RPT_OCUP_DEP	
	SELECT	DBO.TRUNC(F_SNAP)					AS FECHA,
			POS.NAVE_ID							AS NAVE,
			COUNT(DISTINCT(POSICION_ACTUAL))	AS POS_OCUPADAS,
			0									AS VOLUMEN_LIBRE
	FROM	SNAP_EXISTENCIAS SE
			INNER JOIN POSICION AS POS ON (SE.POSICION_ACTUAL = POS.POSICION_ID)
			INNER JOIN NAVE AS N ON (POS.NAVE_ID = N.NAVE_ID)
	WHERE	SE.DISPONIBLE = '1'		
			AND POSICION_ACTUAL IS NOT NULL			
			AND ((@NAVE_ID IS NULL)OR(POS.NAVE_ID=@NAVE_ID))		
			AND ((@FDESDE IS NULL)OR (DBO.TRUNC(SE.F_SNAP) BETWEEN @FDESDE AND @FHASTA))			
	GROUP BY SE.F_SNAP, POS.NAVE_ID
	ORDER BY SE.F_SNAP, POS.NAVE_ID

	DECLARE	PCUR CURSOR FOR
		SELECT T.FECHA_SNAP, T.NAVE_ID FROM TEMP_RPT_OCUP_DEP AS T ORDER BY T.FECHA_SNAP, T.NAVE_ID

	OPEN PCUR
	FETCH NEXT FROM PCUR INTO @FECHA_COMP,@NAVE_ID_01
	WHILE @@FETCH_STATUS = 0
		BEGIN
	
			-- TOTAL DE POSICIONES ACTUALES
			SELECT	@CONT = COUNT(*) 
			FROM	POSICION AS POS												
			WHERE	(POS.NAVE_ID=@NAVE_ID_01)	

			-- ACTUALIZO LA TEMPORAL CON LAS POSICIONES LIBRES
			UPDATE	TEMP_RPT_OCUP_DEP 
			SET		POSICIONES_LIBRES = @CONT - POSICIONES_LIBRES				
			WHERE	DBO.TRUNC(FECHA_SNAP) = DBO.TRUNC(@FECHA_COMP)
					AND NAVE_ID = @NAVE_ID_01

			-- ACTUALIZO LA TEMPORAL CON EL VOLUMEN DE LAS POS. LIBRES
			SELECT	@CONT2 = SUM((ISNULL(POS.LARGO,0) * ISNULL(POS.ALTO,0) * ISNULL(POS.ANCHO,0))/1000000)
			FROM	POSICION AS POS
			WHERE	POSICION_ID NOT IN (
					SELECT	DISTINCT (SE.POSICION_ACTUAL)
					FROM	SNAP_EXISTENCIAS SE
							INNER JOIN POSICION AS POS ON (SE.POSICION_ACTUAL = POS.POSICION_ID)							
					WHERE	SE.DISPONIBLE = '1'		
							AND POSICION_ACTUAL IS NOT NULL							
							AND (POS.NAVE_ID=@NAVE_ID_01)		
							AND DBO.TRUNC (SE.F_SNAP) = DBO.TRUNC(@FECHA_COMP)	
					)
					AND POS.NAVE_ID = @NAVE_ID_01

			UPDATE	TEMP_RPT_OCUP_DEP 
			SET		VOLUMEN_LIBRE = @CONT2 
			WHERE	(NAVE_ID=@NAVE_ID_01)
					AND DBO.TRUNC(FECHA_SNAP) = DBO.TRUNC(@FECHA_COMP)			

			FETCH NEXT FROM PCUR INTO @FECHA_COMP,@NAVE_ID_01
		END

	CLOSE PCUR
	DEALLOCATE PCUR

END;


GO