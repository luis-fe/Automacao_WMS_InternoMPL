import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW
import datetime

# Recarga de fila de Pedidos
def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def SeparacoPedidos():
    conn = ConexaoCSW.Conexao()
    SugestoesAbertos = pd.read_sql('SELECT codPedido, dataGeracao,  priorizar, vlrSugestao,situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed  '
                                   'WHERE codEmpresa =1 and situacaoSugestao =2',conn)
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

   # try:
    ConexaoPostgreMPL.Funcao_Inserir(SugestoesAbertos,tamanho,'filaseparacaopedidos','append')
     #   hora = obterHoraAtual()
      #  return tamanho, hora

    #except:
     #   print('falha na funçao Inserir Separacao')
      #  hora = obterHoraAtual()
       # conn.close()
        #return tamanho, hora
SeparacoPedidos()