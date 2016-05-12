Create Function [dbo].[Remover_CaracteresNoAlfaNumericos](@Str VarChar(1000))
Returns VarChar(1000)
AS
Begin

    Declare @ValOK  as varchar(50)
	Declare @ChrRplc as varchar(100)
    
    Set @ValOK = '%[^a-z^0-9^#^.^+^=^*^/^?^!^¿^¡^(^)^&^%^# ]%'
	Set @ChrRplc= '#'
    
    While PatIndex(@ValOK, @Str) > 0
    Begin
        Set @Str = Stuff(@Str, PatIndex(@ValOK, @Str), 1, @ChrRplc)
	End
	
    Return @Str

End --Fin Fx.