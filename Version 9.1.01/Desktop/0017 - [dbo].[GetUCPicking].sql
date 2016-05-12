/****** Object:  StoredProcedure [dbo].[GetUCPicking]    Script Date: 09/18/2013 10:56:07 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetUCPicking]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetUCPicking]
GO

create procedure [dbo].[GetUCPicking]
	@viaje as varchar(50) output
as
Begin
	Select	distinct pallet_picking
	from	picking
	where	fin_picking = 2
			and facturado = 0
			and viaje_id = @viaje
End



