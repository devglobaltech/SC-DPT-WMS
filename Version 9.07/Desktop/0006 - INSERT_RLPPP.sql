
ALTER PROCEDURE [dbo].[INSERT_RLPPP]
@CLIENTE_ID		VARCHAR(15)	OUTPUT,
@PRODUCTO_ID	VARCHAR(30)	OUTPUT,
@NAVE_COD		VARCHAR(15)	OUTPUT,
@POSICION_COD	VARCHAR(45)	OUTPUT,
@STATUS_LN		VARCHAR(10)	OUTPUT
As
Begin

	Declare @Nave_ID 		as Numeric(20,0)
	Declare @Posicion_Id	as Numeric(20,0)
	Declare @Control		as float(1)
	Declare @Msg			as varchar(4000)
	Declare @error_var		as int
	
	SET @STATUS_LN='0'
	--Obtengo la Posicion Id en caso de que no sea null
	If @POSICION_COD is not null
	Begin
		Set @Posicion_Id=Dbo.Get_Posicion_id(@Posicion_Cod)
	End
	Else
	Begin
		Set @Posicion_Id=Null
	End	

	--Obtengo la Posicion Id en caso de que no sea null
	If @NAVE_COD is not null
	Begin
		Select 	@Nave_ID=Nave_Id
		From 	Nave
		Where	nave_cod=ltrim(rtrim(upper(@NAVE_COD)))
	End
	Else
	Begin
		Set @Nave_ID=Null
	End	

	If (@Producto_id Is null) Or (ltrim(rtrim(upper(@Producto_Id)))='')
	Begin
		SET @STATUS_LN='1';
		Set @Msg='El campo producto no puede estar vacio.'		
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	--Controlo el producto
	Select 	@Control=Count(*)
	from 	Producto
	Where	Cliente_id=ltrim(rtrim(Upper(@Cliente_id)))
			and Producto_id=ltrim(rtrim(Upper(@Producto_id)))

	If @Control=0
	Begin
		SET @STATUS_LN='2';
		Set @Msg='Producto inexistente, por favor verifique estos valores.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	-- Controlo que no se ingrese basura a la tabla, al menos uno deberia tener valores.
	If (@Nave_id is null) and (@posicion_id is null)
	Begin
		SET @STATUS_LN='3';
		Set @Msg='La nave o la posicion no existen, por favor verifique estos valores.'
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg);
		Return
	End

	--Inserto en la tabla
	INSERT INTO RL_PRODUCTO_POSICION_PERMITIDA (CLIENTE_ID, PRODUCTO_ID, NAVE_ID, POSICION_ID) 
	VALUES(@Cliente_id, @Producto_Id, @Nave_Id, @Posicion_id)


	--Controlo la condicion de error.

	SELECT @error_var = @@ERROR
	If @error_var<> 0 
	Begin
		Set @Msg='Ocurrio un error inesperado al insertar en la tabla Rl_Producto_Posicion_Permitida. - COD. ERROR: ' + CAST(@error_var AS VARCHAR(10))
		Insert into AUDITORIA_RLPPP (CLIENTE_ID, PRODUCTO_ID, NAVE_COD, POSICION_COD, OBSERVACIONES)
		Values (@Cliente_id, @Producto_id, @Nave_Cod, @Posicion_Cod, @Msg );
		Return
	End

End
