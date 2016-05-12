update	SYS_VERSION 
set		VER_ACTUAL_WARP='9.12',
		VER_MIN_COMPATIBLE='9.12',
		FECHA_INSTALACION=GETDATE(),
		OBS_ACTUALIZACION='Debe actualizar la versión actual a la version 9.12. Por favor, comuniquese con el departamento de sistemas.'
where	NOM_DLL='WARP.EXE'

update	SYS_VERSION 
set		VER_ACTUAL_WARP='9.12',
		VER_MIN_COMPATIBLE='9.12',
		FECHA_INSTALACION=GETDATE(),
		OBS_ACTUALIZACION='Version de DLL'
where	NOM_DLL<>'WARP.EXE'