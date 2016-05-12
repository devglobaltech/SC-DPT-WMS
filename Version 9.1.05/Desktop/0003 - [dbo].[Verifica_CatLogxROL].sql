
/****** Object:  StoredProcedure [dbo].[Verifica_CatLogxROL]    Script Date: 03/12/2014 10:49:10 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Verifica_CatLogxROL]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Verifica_CatLogxROL]
GO

/****** Object:  StoredProcedure [dbo].[Verifica_CatLogxROL]    Script Date: 03/12/2014 10:49:10 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[Verifica_CatLogxROL]
@CLIENTE_ID	as varchar(15),
@CAT_LOG_ID as varchar(50),
@CONTROL	as numeric(1,0) Output
as
begin
DECLARE @USUARIO	VARCHAR(50)

	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	SELECT @CONTROL = COUNT(*)
	FROM CATEGORIA_LOGICA 
	WHERE CLIENTE_ID= @CLIENTE_ID  
	AND CAT_LOG_ID = @CAT_LOG_ID  
	--AND DISP_TRANSF = 1  
	AND CAT_LOG_ID IN (	SELECT	CAT_LOG_ID
						FROM	RL_ROL_CATEGORIA_LOGICA RCL
						INNER JOIN SYS_USUARIO U ON(U.ROL_ID = RCL.ROL_ID)
						WHERE	USUARIO_ID = @USUARIO
								AND CLIENTE_ID = @CLIENTE_ID
								AND CAT_LOG_ID = @CAT_LOG_ID)
end


GO


