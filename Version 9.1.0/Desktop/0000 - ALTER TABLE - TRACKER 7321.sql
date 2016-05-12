Alter table sys_dev_documento add NRO_GUIA numeric(20,0) null;
go
Alter table sys_dev_documento add IMPORTE_FLETE numeric(15,3) null;
go
Alter table sys_dev_documento add TRANSPORTE_ID varchar(20) null;
go
Alter table sys_dev_det_documento add NRO_CMR varchar(100) NULL;
go
Alter table sys_dev_det_documento add NRO_INTERNO bigint IDENTITY(1,1) NOT NULL;
go
Alter table sys_dev_documento_historico add NRO_GUIA numeric(20,0) null;
go
Alter table sys_dev_documento_historico add IMPORTE_FLETE numeric(15,3) null;
go
Alter table sys_dev_documento_historico add TRANSPORTE_ID varchar(20) null;
go
Alter table sys_dev_det_documento_historico add NRO_CMR varchar(100) NULL;
go
Alter table sys_dev_det_documento_historico add NRO_INTERNO bigint;