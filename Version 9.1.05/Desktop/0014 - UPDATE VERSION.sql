
UPDATE SYS_VERSION SET VER_ACTUAL_WARP = '9.15',VER_MIN_COMPATIBLE = '9.15', FECHA_INSTALACION = GETDATE();
UPDATE SYS_VERSION SET OBS_ACTUALIZACION = 'Debe actualizar la versión actual a la version 9.1.5. 
Por favor, comuniquese con el departamento de sistemas.' WHERE NOM_DLL = 'WARP.EXE';