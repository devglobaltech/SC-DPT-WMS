begin
	declare @cont	numeric(10,0)
	
	select	@cont=count(*)
	from	secuencia
	where	nombre='NRO_OC'	
	
	if @cont=0 begin
		insert into secuencia values('NRO_OC',0);
	end
end