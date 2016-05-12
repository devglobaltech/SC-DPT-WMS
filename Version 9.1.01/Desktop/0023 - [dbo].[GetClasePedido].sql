/****** Object:  UserDefinedFunction [dbo].[GetClasePedido]    Script Date: 09/18/2013 11:17:16 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetClasePedido]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[GetClasePedido]
GO

CREATE Function [dbo].[GetClasePedido]
(@Doc_ext	varchar(100))
Returns Varchar(100)
As
Begin
	Declare @Retorno	Varchar(100)
	
	Select 	Distinct @Retorno=RTRIM(d.CLASE_PEDIDO)
	From	SYS_INT_DOCUMENTO d
	Where	d.DOC_EXT=@Doc_ext

	Return @Retorno

End



