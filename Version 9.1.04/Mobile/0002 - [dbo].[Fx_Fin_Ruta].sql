IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Fx_Fin_Ruta]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[Fx_Fin_Ruta]
GO

CREATE     function [dbo].[Fx_Fin_Ruta](
	@Viaje_id 	as Varchar(100),
	@Ruta		as Varchar(100)
)Returns int
As
Begin
	Declare @vCant 	as int --Saco el total.
	Declare @vTotal as int --Para el control.
	Declare @Return as int --Para el retorno.

	Select 	@vCant=Count(picking_id)
	From	Picking
	where	ltrim(rtrim(upper(Viaje_Id)))=Ltrim(Rtrim(Upper(@Viaje_id)))
			and
			((@ruta='9999') or(ltrim(rtrim(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))))

	Select 	@vTotal=Count(picking_id)
	From	Picking
	Where	fecha_inicio is not null
			and 
			fecha_Fin is not null
			and
			Cant_confirmada is not null
			and 
			Usuario is not null
			and
			((@ruta='9999') or(ltrim(rtrim(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))))
			and
			ltrim(rtrim(upper(viaje_id)))=ltrim(rtrim(upper(@Viaje_id)))

	If @vCant=@vTotal
		Begin
			Set @Return=1
		End
	Else
		Begin
			Set @Return=0
		End
	Return @Return
End

GO


