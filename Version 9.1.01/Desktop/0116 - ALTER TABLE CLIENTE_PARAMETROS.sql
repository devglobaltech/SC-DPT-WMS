
ALTER TABLE CLIENTE_PARAMETROS
ADD TIPO_EMPAQUETADO_ID NUMERIC(10,0)

GO
ALTER TABLE CLIENTE_PARAMETROS
ADD CONSTRAINT FK_TIPO_EMPAQUETADO FOREIGN KEY (TIPO_EMPAQUETADO_ID)
REFERENCES TIPO_EMPAQUETADO(TIPO_EMPAQUETADO_ID)

GO

INSERT INTO sys_tabColumns VALUES('CLIENTE_PARAMETROS','TIPO_EMPAQUETADO_ID',21,'NUMBER','Y',10,8,0,0,NULL)
INSERT INTO SYS_DET_TABLA VALUES('CLIENTE_PARAMETROS','TIPO_EMPAQUETADO_ID','Tipo Empaquetado','S',10,1);
