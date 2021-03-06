
ALTER        function [dbo].[Fx_Fin_Ruta_Usuario](
	@Viaje_id 	as Varchar(100),
	@Ruta		as Varchar(100),
	@Usuario	as varchar(30)
)Returns int
As
Begin
	Declare @vCant 	as int --Saco el total.
	Declare @vTotal as int --Para el control.
	Declare @Return as int --Para el retorno.

	Select 	@vCant=Count(picking_id)
	From	Picking
	where	ltrim(rtrim(upper(Viaje_Id)))=Ltrim(Rtrim(Upper(@Viaje_id)))
			and	ltrim(rtrim(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))
			and usuario=ltrim(rtrim(upper(@Usuario)))
	
	Select 	@vTotal=Count(picking_id)
	From	Picking
	Where	fecha_inicio is not null
			and 
			fecha_Fin is not null
			and
			Cant_confirmada is not null
			and	ltrim(upper(upper(Ruta)))=Ltrim(Rtrim(Upper(@Ruta)))
			and	ltrim(rtrim(upper(viaje_id)))=ltrim(rtrim(upper(@Viaje_id)))
			and usuario=ltrim(rtrim(upper(@Usuario)))

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
