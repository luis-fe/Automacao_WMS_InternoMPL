import ConexaoCSW
import pandas as pd
import  BuscasAvancadas
import ConexaoPostgreMPL

# Passo 1 - Automacao que impulta no WMS os dados referente as OP que ocorreram substituicao nos utimos 100 dias
def SubstitutosSkuOP():
    conn = ConexaoCSW.Conexao()

    # Consultando Sql Obter os itens substitutos dos ultimos 100 dias
    consultaSubstitudos = pd.read_sql(BuscasAvancadas.RegistroSubstituto(),conn)

    #Acrescentando o codigoSortimento aos compontentes variaveis
    consultaSortVar = ComponentesPrincipalPorSKU()
    consultaSortVar['tipo'] = 'Variavel'
    consultaSubstitudos = pd.merge(consultaSubstitudos,consultaSortVar,on=['codproduto', 'componente'], how='left')
    consultaSubstitudos['tipo'].fillna('Padrao',inplace=True)#Caso vazio, marcar como componente padrao

    #Acrescentando as cores aos componentes variaveis
    consultaCor = pd.read_sql(BuscasAvancadas.ConsultaCOr(),conn)
    consultaCor['tipo'] = 'Variavel'
    consultaCor['codSortimento']=consultaCor['codSortimento'].astype(str)
    consultaSubstitudos = pd.merge(consultaSubstitudos,consultaCor,on=['numeroop', 'codSortimento','tipo'], how='left')


    #Levantamento dos compontentes padroes
    consultaPad = pd.read_sql(BuscasAvancadas.ComponentesPadraoEng(),conn)
    consultaPad['tipo'] = 'Padrao'
    consultaSubstitudosPad = consultaSubstitudos
    consultaSubstitudosPad = consultaSubstitudosPad[consultaSubstitudosPad['tipo'] == 'Padrao']
    consultaSubstitudosPad.drop(['codSortimento', 'cor'], axis=1, inplace=True)

    consultaSubstitudosPad = pd.merge(consultaSubstitudosPad,consultaPad,on=['codproduto','componente','tipo'], how='left')


    #Acrescentando as cores aos componentes padroes
    consultaCor['tipo'] = 'Padrao'
    consultaSubstitudosPad = pd.merge(consultaSubstitudosPad,consultaCor,on=['numeroop','tipo'], how='left')

    consultaSubstitudos = consultaSubstitudos[consultaSubstitudos['tipo']=='Variavel']
    consulta = pd.concat([consultaSubstitudos, consultaSubstitudosPad], ignore_index=True)

    conn.close()
    consulta['aplicacaoPad'].fillna('-',inplace=True)
    consulta['aplicacao'] = consulta.apply(lambda row : row['aplicacaoPad'] if row['aplicacaoPad']!='-' else row['aplicacao'], axis=True )
    # acrescentando as categorias
    consulta['categoria'] = '-'
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
        lambda row: Categoria('COLARIN', row['nomecompontente'], 'AV CAMISARIAS', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(
        lambda row: Categoria('PUNHO', row['nomecompontente'], 'PUNHO', row['categoria']), axis=1)
    # colunas carragadas: requisicao, numeroop, codproduto, databaixa_req, componente, nomecompontente, subst, nomesub


    consulta['exessao'] = '-'
    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101051', row['coodigoPrincipal'], row['coodigoSubs'], row['exessao'],'250101846'), axis=1)

    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101836', row['coodigoPrincipal'], row['coodigoSubs'], row['exessao'],'250101409'), axis=1)

    consulta['exessao'] = consulta.apply(
        lambda row: Excessoes('250101717', row['coodigoPrincipal'], row['coodigoSubs'], row['exessao'],'250101918'), axis=1)

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
    return consulta
def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia and categoria == '-':
        return valorNovo
    else:
        return categoria

def Excessoes(contem1,valorReferencia1,valorReferencia2,exessao, contem2):
    if contem1 in valorReferencia2 and exessao == '-' and contem2 in valorReferencia1 :
        subst = contem1 + valorReferencia2[9:]
        principal = contem1 +valorReferencia1[9:]

        if  subst == principal:
            return 'acessorios'
        else:
            return '-'
    else:
        return '-'

def ConsultaSubstitutosFlegadoSim():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select numeroop, componente, considera from "Reposicao"."SubstitutosSkuOP" '
                           'where considera = %s ',conn, params=('sim',))


    conn.close()

    return consulta


#22- Sql Obter o compontente de cadas sku nas engenharias , relativo as 10Mil primeiras OP : velocidade 36 segundos (lento)
def ComponentesPrincipalPorSKU():
    conn = ConexaoCSW.Conexao()

    # Consultando Sql Obter os skus x compontente principal para poder "ligar" ao calculos
    consulta = pd.read_sql(BuscasAvancadas.ComponentesPrincipaisEngenharia(),conn)

    conn.close()

    # Dividir os valores da coluna 2 por ";"
    consulta['codSortimento'] = consulta['codSortimento'].str.split(';')

    # Usar explode para expandir os valores em múltiplas linhas
    consulta = consulta.explode('codSortimento')

    return consulta
