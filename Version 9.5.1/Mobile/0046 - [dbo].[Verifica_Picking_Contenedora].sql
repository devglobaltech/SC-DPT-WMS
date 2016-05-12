/****** Object:  StoredProcedure [dbo].[Verifica_Picking_Contenedora]    Script Date: 09/30/2015 13:13:52 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Verifica_Picking_Contenedora]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Verifica_Picking_Contenedora]
GO

Create  PROCEDURE [dbo].[Verifica_Picking_Contenedora]
@CONTENEDORA	varchar(100),
@Movimiento		char(1) Output
As
Begin
	Declare @Control 	as Int

	SELECT 	@Control=Count(*)
	FROM 	PICKING p inner join DET_DOCUMENTO dd
			on(p.DOCUMENTO_ID=dd.DOCUMENTO_ID and p.NRO_LINEA=dd.NRO_LINEA)
	WHERE 	dd.NRO_BULTO=@CONTENEDORA
			AND CANT_CONFIRMADA	IS NULL

	If @Control>0 
	Begin
		set @Movimiento=1
		return
	End 
	Else
	Begin
		set @Movimiento=0
	End
End

GO

