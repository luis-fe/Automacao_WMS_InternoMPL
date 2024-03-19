import ConexaoCSW
import pandas as pd
import  BuscasAvancadas
import ConexaoPostgreMPL

# Passo 1 - Automacao que impulta no WMS os dados referente as OP que ocorreram substituicao nos utimos 100 dias
def SubstitutosSkuOP():
    conn = ConexaoCSW.Conexao()

    # Consultando Sql Obter os itens substitutos dos ultimos 100 dias
    consulta = pd.read_sql(BuscasAvancadas.RegistroSubstituto(),conn)
    consulta2 = ComponentesPrincipalPorSKU()

    consulta = pd.merge(consulta,consulta2,on=['codproduto', 'componente'], how='left')

    consultaCor = pd.read_sql(BuscasAvancadas.ConsultaCOr(),conn)
    consultaCor['codSortimento']=consultaCor['codSortimento'].astype(str)
    consulta = pd.merge(consulta,consultaCor,on=['numeroop', 'codSortimento'], how='left')




    conn.close()
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

    # colunas carragadas: requisicao, numeroop, codproduto, databaixa_req, componente, nomecompontente, subst, nomesub

    ultimobackup = ConsultaSubstitutosFlegadoSim()
    consulta = pd.merge(consulta,ultimobackup, on=['numeroop', 'componente'],how='left')
    consulta['considera'].fillna('-',inplace=True)

    #Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_Inserir(consulta,consulta['requisicao'].size,'SubstitutosSkuOP','replace')
    return consulta
def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia and categoria == '-':
        return valorNovo
    else:
        return categoria

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
