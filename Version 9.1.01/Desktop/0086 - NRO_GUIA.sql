begin
	declare @cont numeric;
	
	select	@cont=COUNT(valor)
	from	SECUENCIA
	where	NOMBRE='NRO_GUIA'

	if(@cont=0) begin
		insert into SECUENCIA(NOMBRE,VALOR)VALUES('NRO_GUIA',0);
	end
end