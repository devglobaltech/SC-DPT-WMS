
/****** Object:  StoredProcedure [dbo].[DeleteNroLinea]    Script Date: 10/14/2014 14:46:31 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DeleteNroLinea]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[DeleteNroLinea]
GO

CREATE Procedure [dbo].[DeleteNroLinea]
@Documento_Id	Numeric(20,0)Output,
@Nro_linea		Numeric(10,0)Output
As
Begin

	DECLARE @RL_ID AS NUMERIC(20,0)
	
	SELECT @RL_ID = RL_ID FROM Consumo_Locator_Egr WHERE Documento_id=@Documento_id and Nro_Linea=@Nro_Linea
	DELETE #TMP_CONSUMO_LOCATOR_EGR WHERE RL_ID = @RL_ID
	
	Delete from Det_Documento_Aux 	where Documento_id=@Documento_id and Nro_Linea=@Nro_Linea
	Delete from Consumo_Locator_Egr where Documento_id=@Documento_id and Nro_Linea=@Nro_Linea

End

GO


