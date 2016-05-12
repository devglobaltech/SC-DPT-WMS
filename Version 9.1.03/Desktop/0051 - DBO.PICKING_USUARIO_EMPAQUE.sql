create trigger DBO.PICKING_USUARIO_EMPAQUE
on PICKING
for update
as
Begin
	declare @usr		varchar(100)
	declare @fecha		datetime
	declare @terminal	varchar(100)
	declare @nro_uc		varchar(100)
	declare @cont		numeric(20)
	 
	select	@nro_uc=nro_ucempaquetado
	from	inserted
	where	nro_ucempaquetado is not null
	
	if @nro_uc is not null begin

		select	@cont=count(*)
		from	usuario_empaque
		where	nro_ucempaquetado=@nro_uc
		
		if @cont=0 begin
			set @fecha		=getdate()
			set @terminal	=host_name()
			select @usr=usuario_id from #temp_usuario_loggin
			
			INSERT INTO USUARIO_EMPAQUE VALUES(@nro_uc,@fecha, @usr, @terminal)
		end
	end
end