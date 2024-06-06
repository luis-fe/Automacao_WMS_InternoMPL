import gc

import pandas as pd
import psutil
import pytz
from colorama import Fore
from sqlalchemy import create_engine

import CalculoNecessidadesEndereco
import ConexaoPostgreMPL
import ConexaoCSW
import datetime
from psycopg2 import sql
import controle
import empresaConfigurada
import os
def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # Retorna o uso de memória em bytes
# Recarga de fila de Pedidos
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def criar_agrupamentos(grupo):
    return '/'.join(sorted(set(grupo)))
def SeparacoPedidos():
    conn = ConexaoCSW.Conexao()
    SugestoesAbertos = pd.read_sql("SELECT codPedido||'-'||codsequencia as codPedido, codPedido as codPedido2, dataGeracao,  priorizar, vlrSugestao,situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed  "
                                   'WHERE codEmpresa =1 and situacaoSugestao =2',conn)


    PedidosSituacao = pd.read_sql("select DISTINCT p.codPedido||'-'||p.codSequencia as codPedido , 'Em Conferencia' as situacaopedido  FROM ped.SugestaoPedItem p "
                                  'join ped.SugestaoPed s on s.codEmpresa = p.codEmpresa and s.codPedido = p.codPedido '
                                  'WHERE p.codEmpresa = 1 and s.situacaoSugestao = 2 and p.qtdePecasConf > 0', conn)

    SugestoesAbertos = pd.merge(SugestoesAbertos, PedidosSituacao, on='codPedido', how='left')

    CapaPedido = pd.read_sql('select top 100000 codPedido as codPedido2, codCliente, '
                             '(select c.nome from fat.Cliente c WHERE c.codEmpresa = 1 and p.codCliente = c.codCliente) as desc_cliente, '
                             '(select r.nome from fat.Representante  r WHERE r.codEmpresa = 1 and r.codRepresent = p.codRepresentante) as desc_representante, '
                             '(select c.nomeCidade from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as cidade, '
                             '(select c.nomeEstado from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as estado, '
                             ' codRepresentante , codTipoNota, CondicaoDeVenda as condvenda  from ped.Pedido p  '
                                   ' WHERE p.codEmpresa = 1 order by codPedido desc ',conn)
    SugestoesAbertos = pd.merge(SugestoesAbertos,CapaPedido,on= 'codPedido2', how = 'left')
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
    SugestoesAbertos.drop('codPedido2', axis=1, inplace=True)


    #try:
    ConexaoPostgreMPL.Funcao_Inserir(SugestoesAbertos,tamanho,'filaseparacaopedidos','append')
    #except:
    print('\n4.1.1 Sem dados a Incluir')
    return tamanho, dataHora

def avaliacaoPedidos(rotina, datahoraInicio):
    emp = empresaConfigurada.EmpresaEscolhida()
    conn = ConexaoCSW.ConexaoInternoMPL()
    SugestoesAbertos = pd.read_sql("SELECT 'estoque' as estoque, codPedido||'-'||codsequencia   as codigopedido, dataGeracao,  priorizar, vlrSugestao, situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed "
                                   " WHERE codEmpresa = "+emp+ "and situacaoSugestao =2",conn)


    conn2 = ConexaoPostgreMPL.conexao()

    tagWms = pd.read_sql('select * from "Reposicao".filaseparacaopedidos t ', conn2)
    tagWms = pd.merge(tagWms,SugestoesAbertos, on='codigopedido', how='left')
    tagWms = tagWms[tagWms['estoque']!='estoque']
    etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao',datahoraInicio,'etapa csw ped.sugestao + wms')


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
    etapa2= controle.salvarStatus_Etapa2(rotina, 'automacao',etapa1,'etapa excluindo pedidos ja faturados')


def SugestaoSKU():
    conn = ConexaoCSW.Conexao()
    SugestoesAbertos = pd.read_sql(
        'select s.codPedido as codpedido, s.codSequencia , s.produto, s.qtdeSugerida as qtdesugerida , s.qtdePecasConf as qtdepecasconf  '
        'from ped.SugestaoPedItem s  '
        'left join ped.SugestaoPed p on p.codEmpresa = s.codEmpresa and p.codPedido = s.codPedido and p.codSequencia = s.codSequencia '
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
    SugestoesAbertos['reservado'] = 'nao'

    SugestoesAbertos['codpedido'] =SugestoesAbertos["codpedido"]+'-'+SugestoesAbertos["codSequencia"]
    SugestoesAbertos.drop('codSequencia', axis=1, inplace=True)
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
    sku['reservado'] = 'nao'

    if not sku.empty:
        print('8.2 - Iniciando a atualizacao do Incremento na Tabela pedidossku ' )
        tamanho = sku['codpedido'].size
        ConexaoPostgreMPL.Funcao_Inserir(sku, tamanho, 'pedidossku', 'append')
        conn2.close()
        datahora = obterHoraAtual()
        return tamanho, datahora
    else:
        print('8.2.1 sem dados a incrementar na tabela pedidossku')
        datahora = obterHoraAtual()
        conn2.close()
        return 0, datahora


def LimpezaPedidosSku(rotina, datainicio):
    memoria_antes = memory_usage()
    print(f'A MEMORIA ANTES DA LIMPEZA DE SKU ERA  {round(memoria_antes/100000)}')
    try:
        # Usar contexto gerenciado para conexão
        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as cursor:
                query = '''
                DELETE FROM "Reposicao".pedidossku p 
                WHERE p.codpedido IN (
                    SELECT p.codpedido
                    FROM "Reposicao".filaseparacaopedidos f 
                    RIGHT JOIN "Reposicao".pedidossku p 
                    ON p.codpedido = f.codigopedido 
                    WHERE f.codigopedido IS NULL
                )
                '''
                cursor.execute(query)
                conn.commit()

        etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao', datainicio,
                                              'deletando pedidos na pedidossku que nao foram encontrados')
        del etapa1
        memoria_antes = memory_usage()
        print(f'A MEMORIA APOS DA LIMPEZA DE SKU ERA  {round(memoria_antes / 100000)}')
    except Exception as e:
        print(f"Erro durante a limpeza de pedidos SKU: {e}")
    finally:
        # Forçar coleta de lixo
        gc.collect()


def AtualizarPedidosConferidos():
    conn = ConexaoCSW.Conexao()
    PedidosSituacao = pd.read_sql("select DISTINCT p.codPedido||'-'||p.codSequencia as codigopedido, 'Em Conferencia' as situacaopedido FROM ped.SugestaoPedItem p "
                                  'join ped.SugestaoPed s on s.codEmpresa = p.codEmpresa and s.codPedido = p.codPedido '
                                  'WHERE p.codEmpresa = 1 and p.qtdePecasConf > 0 ', conn)


    conn.close()

    conn2 = ConexaoPostgreMPL.conexao()

    # Voltando os pedidos:
    query = 'update "Reposicao"."filaseparacaopedidos" set situacaopedido = %s '
    chave = 'No Retorna'
    cursor2 = conn2.cursor()
    cursor2.execute(query,(chave,) )
    conn2.commit()

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


def avaliacaoReposicao(rotina, datainicio):
    emp = empresaConfigurada.EmpresaEscolhida()
    memoria_antes = memory_usage()
    print(Fore.GREEN+f'A MEMORIA ANTES de avaliacaoReposicao  {round(memoria_antes/1000000)}')

    try:
        # Conectar usando SQLAlchemy
        postgre_engine = ConexaoPostgreMPL.conexaoEngine()
        # Conexão CSW via jaydebeapi
        conn_csw = ConexaoCSW.ConexaoInternoMPL()
        cursor_csw = conn_csw.cursor()
        query_csw = (
            "select br.codBarrasTag as codbarrastag, 'estoque' as estoque "
            f"from Tcr.TagBarrasProduto br WHERE br.codEmpresa = {emp} "
            "and br.situacao in (3, 8) and codNaturezaAtual in (5, 7, 54)"
        )
        cursor_csw.execute(query_csw)
        rows = cursor_csw.fetchall()

        SugestoesAbertos = pd.DataFrame(rows, columns=['codbarrastag', 'estoque'])
        cursor_csw.close()
        conn_csw.close()

        etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao', datainicio, 'etapa csw Tcr.TagBarrasProduto br')

        # Obter tags do WMS
        tagWms = pd.read_sql('select * from "Reposicao".tagsreposicao t', postgre_engine)
        tagWms = pd.merge(tagWms, SugestoesAbertos, on='codbarrastag', how='left')
        tagWms = tagWms[tagWms['estoque'] != 'estoque']
        tamanho = tagWms['codbarrastag'].size
        etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao', etapa1, 'comparando csw Tcr.TagBarrasProduto x WMS')

        # Obter os valores para a cláusula WHERE do DataFrame
        lista = tagWms['codbarrastag'].tolist()
        if lista:
            query = sql.SQL('DELETE FROM "Reposicao"."tagsreposicao" WHERE codbarrastag IN ({})').format(
                sql.SQL(',').join(map(sql.Literal, lista))
            )
            # Executar a consulta DELETE
            with ConexaoPostgreMPL.conexao() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()
                cursor.close()
                conn.close()
            etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao', etapa2, f'excluindo tags fora WMS {tamanho} tags')
        else:
            etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao', etapa2, 'excluindo tags fora WMS 0 tags')

        # Remover grandes DataFrames explicitamente para liberar memória
        del SugestoesAbertos
        del tagWms
        del lista
        gc.collect()
        memoria_antes = memory_usage()
        print(Fore.YELLOW + f'A MEMORIA apos de avaliacaoReposicao  {round(memoria_antes / 1000000)}')

    except Exception as e:
        print(f"Erro durante a avaliação de reposição: {e}")
        # Adicionar lógica adicional de tratamento de erros, se necessário

    finally:
        # Forçar coleta de lixo no final para garantir a liberação de memória
        gc.collect()


