UPDATE SYS_VERSION SET	VER_ACTUAL_WARP='9.41',
						VER_MIN_COMPATIBLE='9.41',
						FECHA_INSTALACION=GETDATE()
WHERE NOM_DLL<>'WARP.EXE'						

UPDATE SYS_VERSION SET	VER_ACTUAL_WARP='9.41',
						VER_MIN_COMPATIBLE='9.41',
						FECHA_INSTALACION=GETDATE(),
						OBS_ACTUALIZACION='Debe actualizar la versión actual a la version 9.4.0. Por favor, comuniquese con el departamento de sistemas.'
WHERE NOM_DLL='WARP.EXE'
