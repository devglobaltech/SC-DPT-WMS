
/****** Object:  StoredProcedure [dbo].[ExisteCarro]    Script Date: 09/18/2013 17:02:53 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ExisteCarro]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ExisteCarro]
GO

CREATE procedure [dbo].[ExisteCarro]    
 @nroCarro as varchar(50) output    
as    
begin    
	select	CASE WHEN count(picking_id) > 0 THEN 1 ELSE 0 END as cantidad    
	from	picking p     
			inner join documento d   
			on d.documento_id = p.documento_id  
			inner join documento_x_contenedoradesconsolidacion dxuc     
			on d.nro_remito = dxuc.documento_id    
	where	isnull(p.estado,'0') <> '3'
			and p.CANT_CONFIRMADA > 0
			and nro_ucempaquetado is null    
			and dxuc.NroUCDesconsolidacion = @nroCarro   
end

GO


