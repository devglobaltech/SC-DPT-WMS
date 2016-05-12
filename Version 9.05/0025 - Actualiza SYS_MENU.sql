/*Realizar Insert de Controles faltantes en aplicaci�n DEPOT.*/
insert into sys_menu values ('ABSTATUS','ABSTATUS');
insert into sys_menu values ('MBASE','Procesamiento Electr�nico');
insert into sys_menu values ('MDIEXTEND1','MDI Extend.');
insert into sys_menu values ('MNCONFINTERFAZ','Configuraci�n de Interfaz');
insert into sys_menu values ('MNU_PROC_MENSAJES','Procesar Mensajes');
insert into sys_menu values ('MNU33','Personal');
insert into sys_menu values ('MNU38','Transporte');
insert into sys_menu values ('MNU45','Interfaz Transport');
insert into sys_menu values ('MNUARMADOPALLETFINAL','Armado Pallet Final');
insert into sys_menu values ('MNUCARGDOCMAN','Carga de Documento Manual');
insert into sys_menu values ('MNUCNFCLIENTE','Conf. Cliente');
insert into sys_menu values ('MNUCNFIMPRESORAS','Impresoras');
insert into sys_menu values ('MNUCONVMEDIDAS','Medidas');
insert into sys_menu values ('MNUCONVMONEDAS','Monedas');
insert into sys_menu values ('MNUDEPEXTERNO','Dep�sito Externo');
insert into sys_menu values ('MNUDIVISAS','Divisas');
insert into sys_menu values ('MNUEMPAQUETADO','Empaquetado');
insert into sys_menu values ('MNUEMPAQUETADOITSA','Empaquetado ITSA');
insert into sys_menu values ('MNUESTACIONES','Estaciones');
insert into sys_menu values ('MNUIMPETIPICK',' Impresi�n de Etiquetas Picking ');
insert into sys_menu values ('MNUIMPRESIONDEREMITOS','Impresi�n de Remitos');
insert into sys_menu values ('MNUIMPRESIONDEREMITOSTRANSF','Impresi�n de Remitos - Transferencia');
insert into sys_menu values ('MNUIMPRETIPROD',' Impresi�n Etiqueta de Producto ');
insert into sys_menu values ('MNULOCATOREGRESO',' Locator Egreso ');
insert into sys_menu values ('MNUMEDIDAS','Medidas');
insert into sys_menu values ('MNUMOTIVOLOCKEOPOSICION','Motivo del Lockeo de la Posici�n');
insert into sys_menu values ('MNUMP','Maestro de Productos');
insert into sys_menu values ('MNUREIMPRESION','Re-Impresi�n');
insert into sys_menu values ('MNURI_INF','MNURI_INF');
insert into sys_menu values ('MNUSALIR','Salir');
insert into sys_menu values ('MNUSEPARADORESTACIONES','Separador Estaciones');
insert into sys_menu values ('MNUSEPARATOR','Separador Estaciones');
insert into sys_menu values ('MNUVEHICULOS','Veh�culos');
insert into sys_menu values ('MUNNAVPOS','MUNNAVPOS');
insert into sys_menu values ('SERIESEXCEL','Series Excel');
insert into sys_menu values ('SISTEMA','Sistema');
insert into sys_menu values ('TABLAS','Tablas');
insert into sys_menu values ('TRANSFERENCIAS','Transferencias');
insert into sys_menu values ('XENCRYPT','Encriptaci�n');
insert into sys_menu values ('MNU_PROC_MENSAJES','Listas de Correos para Mensajes de Procesos');

/*Eliminar registros de RL_SYS_ROL_MENU (donde se incorporan opciones del men� cuando son desactivadas*/
delete from rl_sys_rol_menu  

/*Eliminar registros de SYS_MENU que no representan Men�s o Controles*/
delete from sys_menu where menu_id = 'ABSTATUS'
delete from sys_menu where menu_id = 'GUION_01'
delete from sys_menu where menu_id = 'GUION_02'
delete from sys_menu where menu_id = 'GUION_03'
delete from sys_menu where menu_id = 'GUION2'
delete from sys_menu where menu_id = 'GUION3'
delete from sys_menu where menu_id = 'GUION4'
delete from sys_menu where menu_id = 'MDIEXTEND1'
delete from sys_menu where menu_id = 'MNUGUION'
delete from sys_menu where menu_id = 'MNUSEPARATOR'


