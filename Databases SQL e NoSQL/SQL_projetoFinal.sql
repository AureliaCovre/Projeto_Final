create database projeto_final;

use projeto_final;

create table if not exists empreendimentosGD(
	id int auto_increment primary key,
    NomeConjunto varchar(50),
    DataGeracaoConjunto datetime,
    PeriodoReferencia varchar(7),
    CNPJ_Distribuidora varchar(14),
    SigAgente varchar(20),
    NomAgente varchar(200),
    CodClasseConsumo int, 
    ClasseClasseConsumo varchar(20),
    CodigoSubgrupoTarifario int,
    GrupoSubgrupoTarifario varchar(10),
    codUFibge varchar(5),
    SigUF varchar(2),
    codRegiao varchar(8),
    NomRegiao varchar(50),
    CodMunicipioIbge int,
    NomMunicipio  varchar(40),
    CodCEP varchar(12),
    TipoConsumidor varchar(2),
    NumCPFCNPJ varchar(14),
    NomTitularUC varchar(8000),
    CodGD varchar(21),
    DthConexao datetime,
    CodModalidade varchar(1),
    DscModalidade varchar(50),
    QtdUCRecebeCredito int,
    TipoGeracao varchar(10),
    FonteGeracao varchar(50),
    Porte varchar(12),
    PotenciaInstaladaKW float,
    MdaLatitude float,
    MdaLongitude float
);

create table if not exists tarifaMediaFornecimento (
	ideTarifaMediaFornecimento int primary key,
    nomClasseConsumo varchar(50),
    nomRegiao varchar(15),
    vlrConsumoMWh float,
    mesReferencia int,
    anoReferencia int,
    dthProcessamento datetime
);

create table if not exists tarifaResidencial (
	ideTarifaFornecimento int primary key,
    nomConcessao varchar(15),
    SigDistribuidora varchar(25),
    SigRegiao varchar(2),
    VlrTUSDConvencional float,
    VlrTEConvencional float,
    VlrTotaTRFConvencional float,
    VlrTRFBrancaPonta float,
    VlrTRFBrancaIntermediaria float,
    VlrTRFBrancaForaPonta float,
    NumResolucao varchar(10),
    DthInicioVigencia datetime,
    DthProcessamento datetime 
);

create table if not exists geracaoDistribuida(
	ideGeracaoDistribuida int primary key,
    nomGeracaoDistribuida varchar(40),
    sigGeracaoDistribuida varchar(3),
    qtdUsina int, 
    mdaPotenciaInstaladakW float, 
    mesReferencia int, 
    anoReferencia int,
    dthProcessamento datetime
);

create table if not exists logDados(
	usuario_log varchar(45) not null,
	data_log text,
	tipo_registro text
);

delimiter $
create procedure InsereLog (in usuario_log varchar(45), in data_log text, in tipo_registro text)
	begin
		insert into logDados(usuario_log, data_log, tipo_registro) values (usuario_log, data_log, tipo_registro);
	end $
    
delimiter $
create trigger tg_insert_geracaoDistribuida_logDados_AI after insert on geracaoDistribuida
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item inserido na tabela geracaoDistribuida');
		END $

delimiter $
create trigger tg_update_geracaoDistribuida_logDados_AU after update on geracaoDistribuida
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item atualizado na tabela geracaoDistribuida');
		END $

delimiter $
create trigger tg_delete_geracaoDistribuida_logDados_AD after delete on geracaoDistribuida
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item excluído na tabela geracaoDistribuida');
		END $

delimiter $
create trigger tg_insert_tarifaResidencial_logDados_AI after insert on tarifaResidencial
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item inserido na tabela tarifaResidencial');
		END $

delimiter $
create trigger tg_update_tarifaResidencial_logDados_AU after update on tarifaResidencial
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item atualizado na tabela tarifaResidencial');
		END $

delimiter $
create trigger tg_delete_tarifaResidencial_logDados_AD after delete on tarifaResidencial
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item excluído na tabela tarifaResidencial');
		END $

delimiter $
create trigger tg_insert_tarifaMediaFornecimento_logDados_AI after insert on tarifaMediaFornecimento
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item inserido na tabela tarifaMediaFornecimento');
		END $

delimiter $
create trigger tg_update_tarifaMediaFornecimento_logDados_AU after update on tarifaMediaFornecimento
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item atualizado na tabela tarifaMediaFornecimento');
		END $

delimiter $
create trigger tg_delete_tarifaMediaFornecimento_logDados_AD after delete on tarifaMediaFornecimento
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item excluído na tabela tarifaMediaFornecimento');
		END $

delimiter $
create trigger tg_insert_empreendimentosGD_logDados_AI after insert on empreendimentosGD
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item inserido na tabela empreendimentosGD');
		END $

delimiter $
create trigger tg_update_empreendimentosGD_logDados_AU after update on empreendimentosGD
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item atualizado na tabela empreendimentosGD');
		END $

delimiter $
create trigger tg_delete_empreendimentosGD_logDados_AD after delete on empreendimentosGD
	for each row
		begin
			call InsereLog(current_user,current_timestamp,'Item excluído na tabela empreendimentosGD');
		END $

select * from empreendimentosGD;
select * from tarifaMediaFornecimento;
select * from tarifaResidencial;
select * from geracaoDistribuida;
select * from logDados;



-- drop table empreendimentosGD;
-- drop table tarifaMediaFornecimento;
-- drop table tarifaResidencial;
-- drop table geracaoDistribuida;
-- drop table logDados;