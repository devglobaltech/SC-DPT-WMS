/****** Object:  StoredProcedure [dbo].[Val_Prod]    Script Date: 07/16/2013 13:07:25 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Val_Prod]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Val_Prod]
GO
CREATE procedure [dbo].[Val_Prod]    
@CLIENTE_ID varchar(20)=null,    
@CODIGO  varchar(50)=null,    
@PRODUCTO_ID varchar(30) Output    
As    
Begin    
	Declare @Count  SmallInt 
	DECLARE @PROD_ENC_POR_EAN	VARCHAR(30)   
	DECLARE @RAZ_SOCIAL AS VARCHAR(60)
	--Set @Cliente='1'    

	SELECT	@PROD_ENC_POR_EAN = PR.PRODUCTO_ID
	FROM	PRODUCTO Pr
			INNER JOIN RL_PRODUCTO_CODIGOS RPC on (Pr.CLIENTE_ID = RPC.CLIENTE_ID AND Pr.PRODUCTO_ID = RPC.PRODUCTO_ID) 
	WHERE	RPC.CLIENTE_ID = @CLIENTE_ID
			AND RPC.CODIGO = @CODIGO
	
	IF @PROD_ENC_POR_EAN IS NOT NULL
	begin
		SET @PRODUCTO_ID = @PROD_ENC_POR_EAN	
		return
	end
		
	Select	@Count=Count(*)    
	from	Producto    
	Where	Producto_Id = @Codigo AND CLIENTE_ID = @CLIENTE_ID

	IF (@Count=1) 
		Set @Producto_ID=@Codigo

	---  BUSCO POR EAN/DUN  
	IF @PRODUCTO_ID IS NULL
		BEGIN
		SELECT @RAZ_SOCIAL = RAZON_SOCIAL FROM CLIENTE WHERE CLIENTE_ID = @CLIENTE_ID
		raiserror('No Existe el producto para el codigo %s, cliente %s',16,1,@codigo,@RAZ_SOCIAL)
		END
End