IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CarroenViaje]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[CarroenViaje]
GO

CREATE procedure [dbo].[CarroenViaje]
@viaje_id as varchar(50),
@nrocarro as varchar(50),
@VALUE AS NUMERIC(1) OUTPUT  

as

begin
SET @VALUE = (    
	select CASE WHEN count(picking_id) > 0 THEN 1 ELSE 0 END   
	from picking
	where viaje_id = @viaje_id
	and pallet_picking = @nrocarro
	and isnull(estado,'0') = '0'
)
end





GO


