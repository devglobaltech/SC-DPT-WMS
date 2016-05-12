update	SYS_VERSION 
set		VER_ACTUAL_WARP='9.13',
		VER_MIN_COMPATIBLE='9.13',
		FECHA_INSTALACION=GETDATE(),
		OBS_ACTUALIZACION='Debe actualizar la versión actual a la version 9.11. Por favor, comuniquese con el departamento de sistemas.'
WHERE	NOM_DLL='WARP.EXE'

update	SYS_VERSION 
set		VER_ACTUAL_WARP='9.13',
		VER_MIN_COMPATIBLE='9.13',
		FECHA_INSTALACION=GETDATE(),
		OBS_ACTUALIZACION='Version de DLL'
WHERE	NOM_DLL<>'WARP.EXE'