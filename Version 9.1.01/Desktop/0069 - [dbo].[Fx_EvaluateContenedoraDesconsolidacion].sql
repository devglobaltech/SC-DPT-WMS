/****** Object:  UserDefinedFunction [dbo].[Fx_EvaluateContenedoraDesconsolidacion]    Script Date: 10/03/2013 12:17:10 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Fx_EvaluateContenedoraDesconsolidacion]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[Fx_EvaluateContenedoraDesconsolidacion]
GO

CREATE Function [dbo].[Fx_EvaluateContenedoraDesconsolidacion](
@UC					varchar(100),
@documento_id		numeric(20,0)) 
returns varchar(1)
begin
	declare @Total		float
	declare @Cons		float
	declare @Retorno	Varchar(1)
	
	select	@Total=count(*)
	from	picking 
	where	nro_ucdesconsolidacion=@UC
			and DOCUMENTO_ID=@documento_id
			and nro_ucempaquetado is null
			AND CANT_CONFIRMADA>0
			
	if @total=0
	begin
		set @Retorno='1'
	end
	else
	begin
		set @Retorno='0'
	end
	return @Retorno
end

GO


