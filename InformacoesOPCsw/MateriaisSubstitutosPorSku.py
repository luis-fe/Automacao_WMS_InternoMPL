import ConexaoCSW
import pandas as pd
import  BuscasAvancadas
import ConexaoPostgreMPL

# Passo 1 - Automacao que impulta no WMS os dados referente as OP que ocorreram substituicao nos utimos 100 dias
def SubstitutosSkuOP():
    conn = ConexaoCSW.Conexao()

    # Consultando Sql Obter os itens substitutos dos ultimos 100 dias
    consulta = pd.read_sql(BuscasAvancadas.RegistroSubstituto(),conn)

    #Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_Inserir(consulta,consulta['requisicao'].size,'SubstitutosSkuOP','replace')

    conn.close()

    # colunas carragadas: requisicao, numeroop, codproduto, databaixa_req, componente, nomecompontente, subst, nomesub
    return consulta

