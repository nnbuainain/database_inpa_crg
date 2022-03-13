CREATE DATABASE db_inpa_crg
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Portuguese_Brazil.1252'
    LC_CTYPE = 'Portuguese_Brazil.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

CREATE TABLE ORDEM (
    id_ordem INT PRIMARY KEY,
    nome_ordem VARCHAR(100)
);

CREATE TABLE FAMILIA (
    id_familia INT PRIMARY KEY,
    nome_familia VARCHAR(100),
    FK_id_ordem INT
);
 
ALTER TABLE FAMILIA ADD CONSTRAINT FK_FAMILIA_2
    FOREIGN KEY (FK_id_ordem)
    REFERENCES ORDEM (id_ordem);

CREATE TABLE GENERO (
    id_genero INT PRIMARY KEY,
    nome_genero VARCHAR(100),
    FK_id_familia INT
);
 
ALTER TABLE GENERO ADD CONSTRAINT FK_GENERO_2
    FOREIGN KEY (FK_id_familia)
    REFERENCES FAMILIA (id_familia);

CREATE TABLE ESPECIE (
    id_especie INT NOT NULL PRIMARY KEY,
    nome_especie VARCHAR(100),
    FK_id_genero INT
);
 
ALTER TABLE ESPECIE ADD CONSTRAINT FK_ESPECIE_2
    FOREIGN KEY (FK_id_genero)
    REFERENCES GENERO (id_genero);

CREATE TABLE PAIS (
    id_pais INT PRIMARY KEY,
    nome_pais VARCHAR(50)
);

CREATE TABLE ESTADO (
    id_estado INT PRIMARY KEY,
    nome_estado VARCHAR(50),
    FK_id_pais INT
);
 
ALTER TABLE ESTADO ADD CONSTRAINT FK_ESTADO_2
    FOREIGN KEY (FK_id_pais)
    REFERENCES PAIS (id_pais);
	

CREATE TABLE LOCALIDADE (
    id_localidade INT NOT NULL PRIMARY KEY,
    nome_localidade VARCHAR(255),
    latitude DECIMAL(5,3),
    longitude DECIMAL(5,3),
    coordenadas_obs VARCHAR(100),
    FK_id_estado INT
);
 
ALTER TABLE LOCALIDADE ADD CONSTRAINT FK_LOCALIDADE_1
    FOREIGN KEY (FK_id_estado)
    REFERENCES ESTADO (id_estado);

CREATE TABLE LOCALIDADE_PEIXE (
    id_local_peixe INT PRIMARY KEY,
    drenagem VARCHAR(100),
    subdrenagem VARCHAR(100),
    local_pesca VARCHAR(255),
    PK_id_localidade INT
);
 
ALTER TABLE LOCALIDADE_PEIXE ADD CONSTRAINT FK_LOCALIDADE_PEIXE_2
    FOREIGN KEY (PK_id_localidade)
    REFERENCES LOCALIDADE (id_localidade);

CREATE TABLE AMOSTRA (
    num_amostra VARCHAR(20) PRIMARY KEY,
    num_campo VARCHAR(50),
    num_voucher VARCHAR(50),
	municipio VARCHAR(255),
	obs VARCHAR(300),
	identificacao_especie_obs VARCHAR(100),
	colecao VARCHAR(50) CHECK (colecao in ('aves','herpeto','peixes')),
    FK_id_ordem INT,
    FK_id_familia INT,
    FK_id_genero INT,
    FK_id_especie INT,
    FK_id_pais INT,
    FK_id_estado INT,
    FK_id_localidade INT
);
 
CREATE TABLE PESQUISADOR (
    id_pesq INT PRIMARY KEY,
    nome_pesquisador VARCHAR(50),
    sobrenome_pesquisador VARCHAR(50),
    email_pesquisador VARCHAR(255),
    instituicao_pesquisador VARCHAR(255)
); 
 

ALTER TABLE AMOSTRA ADD CONSTRAINT FK_AMOSTRA_2
    FOREIGN KEY (FK_id_localidade)
    REFERENCES LOCALIDADE (id_localidade);
 
ALTER TABLE AMOSTRA ADD CONSTRAINT FK_AMOSTRA_3
    FOREIGN KEY (FK_id_especie)
    REFERENCES ESPECIE (id_especie);
 
ALTER TABLE AMOSTRA ADD CONSTRAINT FK_AMOSTRA_4
    FOREIGN KEY (FK_id_genero)
    REFERENCES GENERO (id_genero);
 
ALTER TABLE AMOSTRA ADD CONSTRAINT FK_AMOSTRA_5
    FOREIGN KEY (FK_id_familia)
    REFERENCES FAMILIA (id_familia);
 
ALTER TABLE AMOSTRA ADD CONSTRAINT FK_AMOSTRA_6
    FOREIGN KEY (FK_id_ordem)
    REFERENCES ORDEM (id_ordem);
 
ALTER TABLE AMOSTRA ADD CONSTRAINT FK_AMOSTRA_7
    FOREIGN KEY (FK_id_estado)
    REFERENCES ESTADO (id_estado);
 
ALTER TABLE AMOSTRA ADD CONSTRAINT FK_AMOSTRA_8
    FOREIGN KEY (FK_id_pais)
    REFERENCES PAIS (id_pais);
	
CREATE TABLE PEIXE (
    FK_num_amostra VARCHAR(20) PRIMARY KEY,
    nome_comum VARCHAR(100),
    caixa INT,
    responsavel_projeto VARCHAR(255),
    comprimento_padrao DECIMAL(5,2),
    status VARCHAR(100),
    PK_id_localidade_peixe INT
);
 
ALTER TABLE PEIXE ADD CONSTRAINT FK_PEIXE_2
    FOREIGN KEY (FK_num_amostra)
    REFERENCES AMOSTRA (num_amostra);
 
ALTER TABLE PEIXE ADD CONSTRAINT FK_PEIXE_3
    FOREIGN KEY (PK_id_localidade_peixe)
    REFERENCES LOCALIDADE_PEIXE (id_local_peixe);
	
CREATE TABLE AVE (
    FK_num_amostra VARCHAR(20) PRIMARY KEY,
    sexo CHAR(1) CHECK (sexo in ('M','F','I')),
	sexo_obs VARCHAR(100),
    expedicao VARCHAR(100),
    tempo_ate_conservar VARCHAR(60),
    metodo_coleta VARCHAR(50),
    meio_pres_def VARCHAR(60),
    data_preparacao DATE,
	data_preparacao_obs VARCHAR(100),
	musculo BOOLEAN,
	sangue BOOLEAN,
    figado BOOLEAN,
    coracao BOOLEAN,
    subespecie VARCHAR(50),
    num_preparador VARCHAR(50)
);
 
ALTER TABLE AVE ADD CONSTRAINT FK_AVE_2
    FOREIGN KEY (FK_num_amostra)
    REFERENCES AMOSTRA (num_amostra);
 

CREATE TABLE COLETA_VOUCHER (
	id_pesq_ave INT PRIMARY KEY,
	FK_id_pesq INT,
	FK_num_amostra VARCHAR(20)
);

ALTER TABLE COLETA_VOUCHER ADD CONSTRAINT FK_COLETA_VOUCHER
	FOREIGN KEY (FK_num_amostra)
	REFERENCES AVE (FK_num_amostra);
	
ALTER TABLE COLETA_VOUCHER ADD CONSTRAINT FK_COLETA_VOUCHER_2
	FOREIGN KEY (FK_id_pesq)
	REFERENCES PESQUISADOR (id_pesq);

CREATE TABLE SOLICITA (
    id_solicita INT PRIMARY KEY,
    num_guia INT,
    data_solicitacao DATE,
    data_devolucao DATE,
    instituicao_solicitante VARCHAR(100),
    pais_solicitante VARCHAR(50),
    FK_id_pesq INT,
    FK_num_amostra VARCHAR(20)
);
 
ALTER TABLE SOLICITA ADD CONSTRAINT FK_SOLICITA_2
    FOREIGN KEY (FK_num_amostra)
    REFERENCES AMOSTRA (num_amostra);
 
ALTER TABLE SOLICITA ADD CONSTRAINT FK_SOLICITA_3
    FOREIGN KEY (FK_id_pesq)
    REFERENCES PESQUISADOR (id_pesq);
	
CREATE TABLE COLETA (
    id_coleta INT PRIMARY KEY,
    data_coleta DATE,
    data_coleta_obs VARCHAR(100),
    FK_num_amostra VARCHAR(20),
    FK_id_pesq INT
);
 
ALTER TABLE COLETA ADD CONSTRAINT FK_COLETA_2
    FOREIGN KEY (FK_num_amostra)
    REFERENCES AMOSTRA (num_amostra);
 
ALTER TABLE COLETA ADD CONSTRAINT FK_COLETA_3
    FOREIGN KEY (FK_id_pesq)
    REFERENCES PESQUISADOR (id_pesq);	

	
