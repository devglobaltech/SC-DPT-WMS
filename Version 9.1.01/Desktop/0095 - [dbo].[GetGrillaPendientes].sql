/****** Object:  StoredProcedure [dbo].[GetGrillaPendientes]    Script Date: 10/07/2013 16:44:08 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetGrillaPendientes]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetGrillaPendientes]
GO

CREATE PROCEDURE [dbo].[GetGrillaPendientes]  
as
	select   distinct
			U.NRO_GUIA,            
			(SELECT COUNT(UC_EMPAQUE) FROM UC_EMPAQUE WHERE NRO_GUIA = U.NRO_GUIA)  AS [CANTIDAD DE CAJAS],            
			T.TRANSPORTE_ID AS [COD. TRANSPORTE],            
			T.NOMBRE AS [DESCRIPCION TRANSPORTE],            
			S.NOMBRE AS [CLIENTE DESTINATARIO]            
	FROM	UC_EMPAQUE U INNER JOIN PICKING P	ON P.NRO_UCEMPAQUETADO = U.UC_EMPAQUE            
			INNER JOIN DOCUMENTO D				ON D.DOCUMENTO_ID = P.DOCUMENTO_ID AND D.CLIENTE_ID = P.CLIENTE_ID           
			left JOIN TRANSPORTE T				ON U.TRANSPORTE_ID = T.TRANSPORTE_ID            
			left JOIN SUCURSAL S				ON S.SUCURSAL_ID = D.SUCURSAL_DESTINO  AND S.CLIENTE_ID = D.CLIENTE_ID           
	where	NRO_GUIA IS NOT NULL   
			AND DOCK_ID IS NULL  
	GROUP BY U.NRO_GUIA,T.TRANSPORTE_ID,T.NOMBRE,S.NOMBRE ,P.NRO_UCEMPAQUETADO

GO


