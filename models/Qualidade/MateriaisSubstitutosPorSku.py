import ConexaoCSW
import pandas as pd
import  BuscasAvancadas
import ConexaoPostgreMPL
import controle


# Passo 1 - Automacao que impulta no WMS os dados referente as OP que ocorreram substituicao nos utimos 100 dias
def SubstitutosSkuOP(rotina, datainicio):
    with ConexaoCSW.Conexao() as conn:

        # Consultando Sql Obter os itens substitutos dos ultimos 100 dias
        consultaSubstitudos = pd.read_sql(BuscasAvancadas.RegistroSubstituto(),conn)
        consultaCor = pd.read_sql(BuscasAvancadas.ConsultaCOr(), conn)
        consultaPad = pd.read_sql(BuscasAvancadas.ComponentesPadraoEng(), conn)

    etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao',datainicio,'etapa busca no CSW: registro de substitutos')


    #Acrescentando o codigoSortimento aos compontentes variaveis
    consultaSortVar = ComponentesPrincipalPorSKU()
    consultaSortVar['tipo'] = 'Variavel'
    consultaSubstitudos = pd.merge(consultaSubstitudos,consultaSortVar,on=['codproduto', 'componente'], how='left')
    consultaSubstitudos['tipo'].fillna('Padrao',inplace=True)#Caso vazio, marcar como componente padrao
    etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao',etapa1,'etapa CSW: Acrescentando o codigoSortimento aos compontentes variaveis')


    #Acrescentando as cores aos componentes variaveis
    consultaCor['tipo'] = 'Variavel'
    consultaCor['codSortimento']=consultaCor['codSortimento'].astype(str)
    consultaSubstitudos = pd.merge(consultaSubstitudos,consultaCor,on=['numeroop', 'codSortimento','tipo'], how='left')
    etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao',etapa2,'etapa CSW: Acrescentando as cores aos componentes variaveis')



    #Levantamento dos compontentes padroes
    consultaPad['tipo'] = 'Padrao'
    consultaSubstitudosPad = consultaSubstitudos
    consultaSubstitudosPad = consultaSubstitudosPad[consultaSubstitudosPad['tipo'] == 'Padrao']
    consultaSubstitudosPad.drop(['codSortimento', 'cor'], axis=1, inplace=True)

    consultaSubstitudosPad = pd.merge(consultaSubstitudosPad,consultaPad,on=['codproduto','componente','tipo'], how='left')
    etapa4 = controle.salvarStatus_Etapa4(rotina, 'automacao',etapa3,'etapa CSW: Levantamento dos compontentes padroes')



    #Acrescentando as cores aos componentes padroes
    consultaCor['tipo'] = 'Padrao'
    consultaSubstitudosPad = pd.merge(consultaSubstitudosPad,consultaCor,on=['numeroop','tipo'], how='left')

    consultaSubstitudos = consultaSubstitudos[consultaSubstitudos['tipo']=='Variavel']
    consulta = pd.concat([consultaSubstitudos, consultaSubstitudosPad], ignore_index=True)

    consulta['aplicacaoPad'].fillna('-',inplace=True)
    consulta['aplicacao'] = consulta.apply(lambda row : row['aplicacaoPad'] if row['aplicacaoPad']!='-' else row['aplicacao'], axis=True )
    # acrescentando as categorias
    consulta['categoria'] = '-'
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('ZIPER', row['nomecompontente'], 'ZIPER', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('ENTRETELA', row['nomecompontente'], 'ENTRETELA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('RIBANA', row['nomecompontente'], 'RIBANA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('ENTRETELA', row['nomecompontente'], 'ENTRETELA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('GOLA', row['nomecompontente'], 'GOLAS', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('KIT GOLA', row['nomecompontente'], 'KIT GOLA/PUNHO', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('MALHA', row['nomecompontente'], 'MALHA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('TECIDO', row['nomecompontente'], 'TECIDO PLANO', row['categoria']), axis=1)

    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('BOTAO DE POLIESTER', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('BOTAO DE MASSA', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)



    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('BOTAO', row['nomecompontente'], 'BOTAO', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('MOLET', row['nomecompontente'], 'MOLETOM', row['categoria']), axis=1)

    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('ETIQUETA DE TAMANHO', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('ETIQUETA TAMANHO', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)



    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('ETIQUETA', row['nomecompontente'], 'ETIQUETAS', row['categoria']), axis=1)




    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('CADAR', row['nomecompontente'], 'CADARCO', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('ELAST', row['nomecompontente'], 'ELASTICOS', row['categoria']), axis=1)

    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('COLARINHO PARA CAMISA', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)


    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('COLARIN', row['nomecompontente'], 'AV CAMISARIAS', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('PUNHO', row['nomecompontente'], 'PUNHO', row['categoria']), axis=1)
    # colunas carragadas: requisicao, numeroop, codproduto, databaixa_req, componente, nomecompontente, subst, nomesub


    consulta['exessao'] = '-'
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101846','250101051',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101918','250101717',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250301842','250301406',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101409','250101836',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250301842','250301309',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101842','250101912',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250100009','250100923',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101859','250100214',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250198000','250101912',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)

    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250198000','250101848',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)

    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101848','250198000',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)

    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101842','250101848',row['coodigoPrincipal'],row['coodigoSubs'], row['exessao']), axis=1)

    ultimobackup = ConsultaSubstitutosFlegadoSim()
    consulta = pd.merge(consulta,ultimobackup, on=['numeroop', 'componente'],how='left')
    consulta['considera'].fillna('-',inplace=True)
    consulta['tipo'].fillna('variavel',inplace=True)
    consulta['cor'].fillna('-',inplace=True)
    consulta['aplicacao'].fillna('-', inplace=True)

    consulta = consulta[consulta['cor'] != '-']
    consulta = consulta[consulta['categoria'] != 'RETIRAR']
    consulta = consulta[consulta['exessao'] != 'acessorios']

    consulta['id']= 'req:'+consulta['requisicao'].astype(str)+'||cor:'+consulta['cor']+'||aplicacao:'+consulta['aplicacao']+'||rdzPrinc:'+consulta['componente']+'||rdzSubst'+consulta['subst']
    consulta = consulta.drop_duplicates()

    #Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_Inserir(consulta,consulta['requisicao'].size,'SubstitutosSkuOP','replace')
    etapa5 = controle.salvarStatus_Etapa5(rotina, 'automacao',etapa4,'Carregando dados no Wms')

def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia and categoria == '-':
        return valorNovo
    else:
        return categoria

def Excessoes(Pai, SubstFilho, codigoPrincipal,codigosubst,exessao):
    if (SubstFilho in codigosubst) and exessao == '-' and (Pai in codigoPrincipal) :
        TerminacaoSubt = codigosubst[9:]
        TerminacaoPAi = codigoPrincipal[9:]

        if  TerminacaoSubt == TerminacaoPAi:
            return 'acessorios'
        else:
            return '-'
    else:
        return exessao

def ConsultaSubstitutosFlegadoSim():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select numeroop, componente, considera from "Reposicao"."SubstitutosSkuOP" '
                           'where considera = %s ',conn, params=('sim',))


    conn.close()

    return consulta


#22- Sql Obter o compontente de cadas sku nas engenharias , relativo as 10Mil primeiras OP : velocidade 36 segundos (lento)
def ComponentesPrincipalPorSKU():
    with ConexaoCSW.Conexao() as conn:

        # Consultando Sql Obter os skus x compontente principal para poder "ligar" ao calculos
        consulta = pd.read_sql(BuscasAvancadas.ComponentesPrincipaisEngenharia(),conn)

    # Dividir os valores da coluna 2 por ";"
    consulta['codSortimento'] = consulta['codSortimento'].str.split(';')

    # Usar explode para expandir os valores em múltiplas linhas
    consulta = consulta.explode('codSortimento')

    return consulta
