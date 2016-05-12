/****** Object:  StoredProcedure [dbo].[insertarSerieLog]    Script Date: 04/23/2014 16:07:06 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[insertarSerieLog]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[insertarSerieLog]
GO

CREATE PROCEDURE [dbo].[insertarSerieLog]
  (
      @IDPROCESO         NUMERIC(20,0) OUTPUT,
      @CLIENTE_ID        VARCHAR(15) OUTPUT,
      @NRO_BULTO         VARCHAR(100) OUTPUT,
      @PRODUCTO_ID       VARCHAR(30) OUTPUT,
      @SERIE             VARCHAR(50) OUTPUT,
      @TERMINAL          VARCHAR(100) OUTPUT,
      @USUARIO           VARCHAR(100) OUTPUT,
      @ARCHIVO           VARCHAR(100) OUTPUT
  )
  AS
  BEGIN

	IF @PRODUCTO_ID IS NULL BEGIN
		SELECT	@PRODUCTO_ID=DD.PRODUCTO_ID 
		FROM	DET_DOCUMENTO DD
		WHERE	DD.CLIENTE_ID=@CLIENTE_ID 
				AND DD.NRO_BULTO =@NRO_BULTO;	
	END
    
    IF (ISNULL(@IDPROCESO,0)<>0 AND ISNULL(@CLIENTE_ID,'')<>'' AND ISNULL(@NRO_BULTO,'')<>'' AND ISNULL(@PRODUCTO_ID,'')<>'' AND ISNULL(@SERIE,'')<>'' AND ISNULL(@TERMINAL,'')<>'' AND ISNULL(@USUARIO,'')<>'' AND ISNULL(@ARCHIVO,'')<>'')
    INSERT INTO CargaSeriesLog VALUES
    (@IDPROCESO,@CLIENTE_ID,@NRO_BULTO,@PRODUCTO_ID,@SERIE,GETDATE(),@TERMINAL,@USUARIO,@ARCHIVO,'0','0')
  
  END

GO

