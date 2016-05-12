
/****** Object:  StoredProcedure [dbo].[GetContenedorasAsociadas]    Script Date: 09/18/2013 10:46:53 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetContenedorasAsociadas]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetContenedorasAsociadas]
GO
 
CREATE PROCEDURE [dbo].[GetContenedorasAsociadas]                
	@viaje_id varchar(50) OUTPUT                   
as
Begin
	select  '0' as checkbox,  
			pallet_picking AS nro_carro,                    
			dbo.date_picking(P.viaje_id,'1') as fecha_inicio,                    
			dbo.date_picking(P.viaje_id,'2') as fecha_fin,            
			dbo.GetPickerMans(P.VIAJE_ID) AS pickeadores,                    
			sum(p.cant_confirmada) as bultos             
	from	picking p left JOIN SYS_INT_DET_DOCUMENTO DD 
			ON (P.DOCUMENTO_ID = DD.DOCUMENTO_ID AND P.NRO_LINEA = DD.NRO_LINEA AND P.CLIENTE_ID = DD.CLIENTE_ID)
	where   p.viaje_id = @viaje_id
	GROUP BY 
			viaje_id, p.pallet_picking

End
