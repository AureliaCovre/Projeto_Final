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
    codUFibge varchar(2),
    SigUF varchar(2),
    codRegiao varchar(4),
    NomRegiao varchar(50),
    CodMunicipioIbge int,
    NomMunicipio  varchar(40),
    CodCEP varchar(8),
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


select * from tarifaMediaFornecimento;

drop table tarifaMediaFornecimento;