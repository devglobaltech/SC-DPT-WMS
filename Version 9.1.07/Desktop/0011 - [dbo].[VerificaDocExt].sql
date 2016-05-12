
/****** Object:  UserDefinedFunction [dbo].[VerificaDocExt]    Script Date: 04/09/2014 11:43:53 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[VerificaDocExt]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[VerificaDocExt]
GO

CREATE  Function [dbo].[VerificaDocExt](
@Cliente_id 	as varchar(15),
@Doc_Ext	as varchar(100)
)Returns SmallInt
Begin
	Declare @Ret	as smallint
	Declare @Total	as int
	Declare @Proc	as Int

	SELECT 	@Total=COUNT(DOC_EXT)
	FROM	SYS_INT_DET_DOCUMENTO
	WHERE	Cliente_ID=@Cliente_ID
			And DOC_EXT=@Doc_Ext
			
	SELECT	@Proc=COUNT(DOC_EXT)
	FROM	SYS_INT_DET_DOCUMENTO SIDD
			LEFT JOIN DOCUMENTO D ON (D.DOCUMENTO_ID = SIDD.DOCUMENTO_ID) 
	WHERE	SIDD.Cliente_Id=@Cliente_id
			And SIDD.DOC_EXT=@Doc_Ext
			AND SIDD.ESTADO_GT IS NOT NULL	
			AND (D.STATUS IS NULL OR D.STATUS = 'D40')
			
	If (@Total=@Proc)
	Begin
		Set @Ret=1
	End
	Else
	Begin
		Set @Ret=0
	End	
	Return @Ret		
End

GO


