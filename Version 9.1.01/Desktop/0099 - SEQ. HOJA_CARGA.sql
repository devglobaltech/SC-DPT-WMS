begin
	declare @cont as numeric(20,0);
	
	select	@cont=COUNT(*)
	from	SECUENCIA
	where	NOMBRE='HOJA_CARGA'
	
	if @cont=0 begin
		insert into SECUENCIA (NOMBRE,VALOR)values('HOJA_CARGA',0);
	end
end