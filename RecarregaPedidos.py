import pandas as pd

import CalculoNecessidadesEndereco
import ConexaoPostgreMPL
import ConexaoCSW
import datetime
from psycopg2 import sql

# Recarga de fila de Pedidos
def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def criar_agrupamentos(grupo):
    return '/'.join(sorted(set(grupo)))
def SeparacoPedidos():
    conn = ConexaoCSW.Conexao()
    SugestoesAbertos = pd.read_sql('SELECT codPedido, dataGeracao,  priorizar, vlrSugestao,situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed  '
                                   'WHERE codEmpresa =1 and situacaoSugestao =2',conn)
    PedidosSituacao = pd.read_sql("select DISTINCT p.codPedido, 'Em Conferencia' as situacaopedido FROM ped.SugestaoPedItem p "
                                  'join ped.SugestaoPed s on s.codEmpresa = p.codEmpresa and s.codPedido = p.codPedido '
                                  'WHERE p.codEmpresa = 1 and p.qtdePecasConf > 0 and s.situacaoSugestao = 2', conn)
    SugestoesAbertos = pd.merge(SugestoesAbertos, PedidosSituacao, on='codPedido', how='left')

    CapaPedido = pd.read_sql('select top 100000 codPedido, codCliente, '
                             '(select c.nome from fat.Cliente c WHERE c.codEmpresa = 1 and p.codCliente = c.codCliente) as desc_cliente, '
                             '(select r.nome from fat.Representante  r WHERE r.codEmpresa = 1 and r.codRepresent = p.codRepresentante) as desc_representante, '
                             '(select c.nomeCidade from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as cidade, '
                             '(select c.nomeEstado from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as estado, '
                             ' codRepresentante , codTipoNota, CondicaoDeVenda as condvenda  from ped.Pedido p  '
                                   ' WHERE p.codEmpresa = 1 order by codPedido desc ',conn)
    SugestoesAbertos = pd.merge(SugestoesAbertos,CapaPedido,on= 'codPedido', how = 'left')
    SugestoesAbertos.rename(columns={'codPedido': 'codigopedido', 'vlrSugestao': 'vlrsugestao'
        , 'dataGeracao': 'datageracao','situacaoSugestao':'situacaosugestao','dataFaturamentoPrevisto':'datafaturamentoprevisto',
        'codCliente':'codcliente', 'codRepresentante':'codrepresentante','codTipoNota':'codtiponota'}, inplace=True)
    tiponota = pd.read_sql('select  p.tipoNota as desc_tiponota  from dw_ped.Pedido p WHERE p.codEmpresa = 1 '
                           'group by tipoNota',conn)
    condicaopgto = pd.read_sql('select  p.condVenda as condicaopgto  from dw_ped.Pedido p WHERE p.codEmpresa = 1 '
                           'group by condVenda ',conn)

    # Extrair caracteres à esquerda do hífen
    tiponota['codtiponota'] = tiponota['desc_tiponota'].str.split(' -').str[0]
    condicaopgto['condvenda'] = '1||'+condicaopgto['condicaopgto'].str.split(' -').str[0]
    SugestoesAbertos = pd.merge(SugestoesAbertos, tiponota, on='codtiponota', how='left')
    SugestoesAbertos = pd.merge(SugestoesAbertos, condicaopgto, on='condvenda', how='left')

    conn2 = ConexaoPostgreMPL.conexao()
    validacao = pd.read_sql('select codigopedido, '+"'ok'"+' as "validador"  from "Reposicao".filaseparacaopedidos f ', conn2)

    SugestoesAbertos = pd.merge(SugestoesAbertos, validacao, on='codigopedido', how='left')
    SugestoesAbertos = SugestoesAbertos.loc[SugestoesAbertos['validador'].isnull()]
    # Excluir a coluna 'B' inplace
    SugestoesAbertos.drop('validador', axis=1, inplace=True)
    tamanho = SugestoesAbertos['codigopedido'].size
    dataHora = obterHoraAtual()
    SugestoesAbertos['datahora'] = dataHora
    # Contar o número de ocorrências de cada valor na coluna 'coluna'
    contagem = SugestoesAbertos['codcliente'].value_counts()

    # Criar uma nova coluna 'contagem' no DataFrame com os valores contados
    SugestoesAbertos['contagem'] = SugestoesAbertos['codcliente'].map(contagem)
    # Aplicar a função de agrupamento usando o método groupby
    SugestoesAbertos['agrupamentopedido'] = SugestoesAbertos.groupby('codcliente')['codigopedido'].transform(criar_agrupamentos)

    try:
        ConexaoPostgreMPL.Funcao_Inserir(SugestoesAbertos,tamanho,'filaseparacaopedidos','append')
    except:
        print('\n4.1.1 Sem dados a Incluir')
    return tamanho, dataHora

def avaliacaoPedidos():
    conn = ConexaoCSW.Conexao()
    SugestoesAbertos = pd.read_sql("SELECT 'estoque' as estoque, codPedido as codigopedido, dataGeracao,  priorizar, vlrSugestao, situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed "
                                   " WHERE codEmpresa =1 and situacaoSugestao =2",conn)
    conn2 = ConexaoPostgreMPL.conexao()

    tagWms = pd.read_sql('select * from "Reposicao".filaseparacaopedidos t ', conn2)
    tagWms = pd.merge(tagWms,SugestoesAbertos, on='codigopedido', how='left')
    tagWms = tagWms[tagWms['estoque']!='estoque']

    tamanho = tagWms['codigopedido'].size

    # Obter os valores para a cláusula WHERE do DataFrame
    lista = tagWms['codigopedido'].tolist()
    # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame

    query = sql.SQL('DELETE FROM "Reposicao"."filaseparacaopedidos" WHERE codigopedido IN ({})').format(
        sql.SQL(',').join(map(sql.Literal, lista))
    )

    if tamanho != 0:
        # Executar a consulta DELETE
        with conn2.cursor() as cursor:
            cursor.execute(query)
            conn2.commit()
    else:
        print('3.1.1 - sem Pedidos para serem eliminados da Fila de Pedidos')

    datahora = obterHoraAtual()
    return tamanho, datahora

def SugestaoSKU():
    conn = ConexaoCSW.Conexao()
    SugestoesAbertos = pd.read_sql(
        'select s.codPedido as codpedido, s.produto, s.qtdeSugerida as qtdesugerida , s.qtdePecasConf as qtdepecasconf  '
        'from ped.SugestaoPedItem s  '
        'left join ped.SugestaoPed p on p.codEmpresa = s.codEmpresa and p.codPedido = s.codPedido  '
        'WHERE s.codEmpresa =1 and p.situacaoSugestao =2'
        ' order by p.dataGeracao, p.codPedido ', conn)

    valorUnitaroio = pd.read_sql('select st.codPedido as codpedido , st.produto , p.valorUnitarioLiquido as valorunitarioliq  FROM ped.SugestaoPedItem  st '
                                 'join dw_ped.PedidoItem p on p.codEmpresa = st.codEmpresa  '
                                 'and p.codPedido = st.codPedido '
                                 'and p.seqItem = st.codItemPedido  '
                                 'WHERE  st.codEmpresa = 1 ',conn)

    SugestoesAbertos = pd.merge(SugestoesAbertos, valorUnitaroio, on=['codpedido','produto'], how='left')

    SugestoesAbertos['necessidade'] = SugestoesAbertos['qtdesugerida'] - SugestoesAbertos['qtdepecasconf']
    tamanho = SugestoesAbertos['codpedido'].size
    dataHora = obterHoraAtual()
    SugestoesAbertos['datahora'] = dataHora

    if not SugestoesAbertos.empty:

        SugestoesAbertos['endereco'] = 'Não Reposto'
        print("inicar insercao de dados no ComunicaoCsw")
        ConexaoPostgreMPL.Funcao_Inserir(SugestoesAbertos, tamanho, 'comunicaoskucsw', 'replace')
        print(f"{tamanho} linhas de dados inseridos com sucesso no Comunicao CsW")


        return SugestoesAbertos
    else:
        return SugestoesAbertos

def IncrementarSku():

    conn2 = ConexaoPostgreMPL.conexao()
    sku_csw = SugestaoSKU()
    sku_anterior = pd.read_sql('select codpedido, '+"'ok'"+' as verifica from "Reposicao".pedidossku ',conn2)
    sku = pd.merge(sku_csw, sku_anterior, on='codpedido', how='left')

    sku = sku.loc[sku['verifica'].isnull()]
    # Excluir a coluna 'B' inplace
    sku.drop('verifica', axis=1, inplace=True)
    # Chamar a função NecessidadesPedidos() para obter os novos valores calculados
    novos_valores = CalculoNecessidadesEndereco.NecessidadesPedidos()
    novos_valoresTamanho = novos_valores['codpedido'].size
    print(f'inserindo {novos_valoresTamanho} linhas novos dados calculados no POSTGRE')
    try:
        ConexaoPostgreMPL.Funcao_Inserir(novos_valores,novos_valoresTamanho,'necessidadeendereco','replace')
    except:
        print('erro ao inserir dados no Postgre')

    if not sku.empty:
        print('2 - iniciando a atualizacao do incremento no pedidossku')
        tamanho = sku['codpedido'].size
        ConexaoPostgreMPL.Funcao_Inserir(sku, tamanho, 'pedidossku', 'append')
       # print(f'incremento realizado{sku["codpedido"][0]}')
    else:
        print('sem dados a incrementar')

    #Atualizar a tabela "Reposicao.pedidossku" com os novos valores em massa
    CalculoNecessidadesEndereco.AtualizarTabelaPedidosSKU(novos_valores)
    conn2.close()
    return 'teste'

def LimpezaPedidosSku():
    conn = ConexaoPostgreMPL.conexao()
    query = 'delete from "Reposicao".pedidossku  p ' \
            'where p.codpedido  in ( ' \
            'select f.codigopedido   from "Reposicao".filaseparacaopedidos f  ' \
            'left join "Reposicao".pedidossku p ' \
            'on p.codpedido = f.codigopedido ' \
            'where p.codpedido is null)'
    cursor = conn.cursor()
    cursor.execute(query,)
    conn.commit()
    cursor.close()
    datahora = obterHoraAtual()
    return datahora

def AtualizarPedidosConferidos():
    conn = ConexaoCSW.Conexao()
    PedidosSituacao = pd.read_sql("select DISTINCT p.codPedido as codigopedido, 'Em Conferencia' as situacaopedido FROM ped.SugestaoPedItem p "
                                  'join ped.SugestaoPed s on s.codEmpresa = p.codEmpresa and s.codPedido = p.codPedido '
                                  'WHERE p.codEmpresa = 1 and p.qtdePecasConf > 0 and s.situacaoSugestao = 2', conn)


    conn2 = ConexaoPostgreMPL.conexao()

    tamanho = PedidosSituacao['codigopedido'].size

    # Obter os valores para a cláusula WHERE do DataFrame
    lista = PedidosSituacao['codigopedido'].tolist()
    # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame

    query = sql.SQL('update "Reposicao"."filaseparacaopedidos" set situacaopedido = '+"'Em Conferencia'  WHERE codigopedido IN ({})").format(
        sql.SQL(',').join(map(sql.Literal, lista))
    )

    if tamanho != 0:
        # Executar a consulta DELETE
        with conn2.cursor() as cursor:
            cursor.execute(query)
            conn2.commit()
    else:
        print('3.1.1 - sem Pedidos para serem eliminados da Fila de Pedidos')

    datahora = obterHoraAtual()
    return tamanho, datahora