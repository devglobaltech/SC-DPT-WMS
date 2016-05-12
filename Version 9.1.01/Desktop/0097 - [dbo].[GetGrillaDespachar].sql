/****** Object:  StoredProcedure [dbo].[GetGrillaDespachar]    Script Date: 10/07/2013 17:04:42 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetGrillaDespachar]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetGrillaDespachar]
GO

CREATE procedure [dbo].[GetGrillaDespachar]                 
@DOCK VARCHAR(50) output             
AS              
              
SELECT  distinct  
	'0' AS [CHECK],            
	U.NRO_GUIA,              
	(select count(uc_empaque) from UC_EMPAQUE WHERE NRO_GUIA = U.NRO_GUIA) AS [CANTIDAD DE BULTOS],              
	T.TRANSPORTE_ID AS [COD. TRANSPORTE],              
	T.NOMBRE AS [DESCRIPCION TRANSPORTE],              
	S.NOMBRE AS [CLIENTE DESTINATARIO],  
	isnull( cast(U.NRO_HOJACARGA as varchar) ,'') as [HOJA DE CARGA]  
FROM UC_EMPAQUE U
INNER JOIN PICKING P ON P.NRO_UCEMPAQUETADO = U.UC_EMPAQUE              
INNER JOIN DOCUMENTO D ON D.DOCUMENTO_ID = P.DOCUMENTO_ID              
left JOIN TRANSPORTE T ON U.TRANSPORTE_ID = T.TRANSPORTE_ID              
left JOIN SUCURSAL S ON S.CLIENTE_ID = D.CLIENTE_ID AND S.SUCURSAL_ID = D.SUCURSAL_DESTINO              
WHERE              
	U.DOCK_ID = (SELECT DOCK_ID FROM DOCKS WHERE DOCK_COD = @dock)
	and P.ST_CONTROL_EXP <> '1'
GROUP BY U.NRO_GUIA,T.TRANSPORTE_ID,T.NOMBRE,S.NOMBRE,NRO_HOJACARGA




GO


