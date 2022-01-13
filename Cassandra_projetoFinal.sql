CREATE KEYSPACE IF NOT EXISTS projeto_final WITH replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

use projeto_final;

create table if not exists empreendimentosGD(
    id uuid primary key,
    NomeConjunto text,
    DataGeracaoConjunto TIMESTAMP,
    PeriodoReferencia text,
    CNPJ_Distribuidora text,
    SigAgente text,
    NomAgente text,
    CodClasseConsumo int, 
    ClasseClasseConsumo text,
    CodigoSubgrupoTarifario int,
    GrupoSubgrupoTarifario text,
    codUFibge text,
    SigUF text,
    codRegiao text,
    NomRegiao text,
    CodMunicipioIbge int,
    NomMunicipio text,
    CodCEP text,
    TipoConsumidor text,
    NumCPFCNPJ text,
    NomTitularUC text,
    CodGD text,
    DthConexao TIMESTAMP,
    CodModalidade text,
    DscModalidade text,
    QtdUCRecebeCredito int,
    TipoGeracao text,
    FonteGeracao text,
    Porte text,
    PotenciaInstaladaKW float,
    MdaLatitude float,
    MdaLongitude float,
);

create table if not exists tarifaMediaFornecimento(
	ideTarifaMediaFornecimento int primary key,
    nomClasseConsumo text,
    nomRegiao text,
    vlrConsumoMWh float,
    mesReferencia int,
    anoReferencia int,
    dthProcessamento TIMESTAMP,
);

create table if not exists tarifaResidencial(
	ideTarifaFornecimento int primary key,
    nomConcessao text,
    SigDistribuidora text,
    SigRegiao text,
    VlrTUSDConvencional float,
    VlrTEConvencional float,
    VlrTotaTRFConvencional float,
    VlrTRFBrancaPonta float,
    VlrTRFBrancaIntermediaria float,
    VlrTRFBrancaForaPonta float,
    NumResolucao text,
    DthInicioVigencia TIMESTAMP,
    DthProcessamento TIMESTAMP, 
);

create table if not exists geracaoDistribuida(
	ideGeracaoDistribuida int primary key,
    nomGeracaoDistribuida text,
    sigGeracaoDistribuida text,
    qtdUsina int, 
    mdaPotenciaInstaladakW float, 
    mesReferencia int, 
    anoReferencia int,
    dthProcessamento TIMESTAMP,
);
