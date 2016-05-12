UPDATE SYS_VERSION SET	VER_ACTUAL_WARP='9.010',
						VER_MIN_COMPATIBLE='9.010',
						FECHA_INSTALACION=GETDATE(),
						OBS_ACTUALIZACION='Debe actualizar la versión actual a la version 9.010. Por favor, comuniquese con el departamento de sistemas.'
WHERE	NOM_DLL='WARP.EXE'						
						
UPDATE SYS_VERSION SET	VER_ACTUAL_WARP='9.010',
						VER_MIN_COMPATIBLE='9.010',
						FECHA_INSTALACION=GETDATE()