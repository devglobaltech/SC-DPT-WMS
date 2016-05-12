/****** Object:  StoredProcedure [dbo].[Frontera_PickearDesdePosicion]    Script Date: 06/06/2014 16:04:11 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_PickearDesdePosicion]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_PickearDesdePosicion]
GO

CREATE  PROCEDURE [dbo].[Frontera_PickearDesdePosicion]
	@pViaje_id		varchar(50) output,
	@picking_id		numeric(20,0) output
AS
BEGIN
	update picking set TRANSF_TERMINADA='1' 
	where viaje_id= @pViaje_id 
	and picking_id= @picking_id

END --PROCEDURE

GO


