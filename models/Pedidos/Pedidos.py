import gc
import _queue
import pandas as pd
import psutil
import pytz
import datetime
from colorama import Fore
from future.moves import sys
from gevent import os
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import controle
import fastparquet as fp
import jaydebeapi
import funcoesGlobais.listaObjetosMemoria

def listar_objetos_locais():
    try:
        # Obtém o quadro de chamada da função atual
        frame_atual = sys._getframe(1)  # 1 para o quadro do chamador
        objetos_locais = frame_atual.f_locals
        print("Objetos locais na função atual:")
        for nome, obj in objetos_locais.items():
            print(f"{nome}: {obj}")
        return objetos_locais
    except AttributeError:
        print("sys._getframe() não está disponível neste ambiente.")
        return {}
    except ValueError:
        print("sys._getframe() não conseguiu acessar o quadro.")
        return {}
def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # Retorna o uso de memória em bytes
# Recarga de fila de Pedidos
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def IncrementarPedidos(rotina,datainico ):
    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(BuscasAvancadas.IncrementarPediosProdutos())
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            pedidos = pd.DataFrame(rows, columns=colunas)

            # Executa a segunda consulta e armazena os resultados
            cursor_csw.execute(BuscasAvancadas.ValorDosItensPedido())
            colunas2 = [desc[0] for desc in cursor_csw.description]
            rows2 = cursor_csw.fetchall()
            pedidosValores = pd.DataFrame(rows2, columns=colunas2)

            pedidos = pd.merge(pedidos, pedidosValores, on=['codPedido', 'seqCodItem'], how='left')
            del pedidosValores

            # Executa a terceira consulta e armazena os resultados
            cursor_csw.execute(BuscasAvancadas.SugestaoItemAberto())
            colunas3 = [desc[0] for desc in cursor_csw.description]
            rows3 = cursor_csw.fetchall()
            sugestoes = pd.DataFrame(rows3, columns=colunas3)
            pedidos = pd.merge(pedidos, sugestoes, on=['codPedido', 'codProduto'], how='left')
            del sugestoes

            # Executa a quarta consulta e armazena os resultados
            cursor_csw.execute(BuscasAvancadas.CapaPedido2())  # Verifique se a consulta é correta
            colunas4 = [desc[0] for desc in cursor_csw.description]
            rows4 = cursor_csw.fetchall()
            capaPedido = pd.DataFrame(rows4, columns=colunas4)
            pedidos = pd.merge(pedidos, capaPedido, on='codPedido', how='left')
            # Limpeza de memória
            del rows, rows2, rows3, rows4, capaPedido
            gc.collect()

    etapa1 = controle.salvarStatus_Etapa1(rotina,'automacao', datainico,'from ped.pedidositemgrade')


    etapa2 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa1,'realizar o mergem entre pedidos+pedidositemgrade ')


    pedidos['codTipoNota'] = pedidos['codTipoNota'].astype(str)
    pedidos = pedidos[(pedidos['codTipoNota'] != '38') & (pedidos['codTipoNota'] != '239') & (pedidos['codTipoNota'] != '223')]
    etapa3 = controle.salvarStatus_Etapa3(rotina,'automacao', etapa2,'filtrando tipo de notas')

    # Escolha o diretório onde deseja salvar o arquivo Parquet
    fp.write('pedidos.parquet', pedidos)
    etapa4 = controle.salvarStatus_Etapa4(rotina,'automacao', etapa3,'Salvando o DataFrame em formato fast')
    del pedidos, etapa4, etapa1, etapa2, conn , cursor_csw
    gc.collect()
    memoria_antes = memory_usage()
    print(Fore.MAGENTA + f'A MEMORIA apos de IncrementarPedidos  {round(memoria_antes / 1000000)}')

def CadastroSKU(rotina, datainico):
    memoria_antes = memory_usage()
    print(Fore.MAGENTA + f'A MEMORIA ANTES de CadastroSKU  {round(memoria_antes / 1000000)}')
    # Etapa buscando o cadastro geral do sku do CSW
    funcoesGlobais.listaObjetosMemoria.lista()
    print('##################### parte 2')
    conn = None
    cursor_csw = None
    try:
        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(BuscasAvancadas.CadastroSKU())
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                sku = pd.DataFrame(rows, columns=colunas)
                sku.info()
                del rows
                gc.collect()
    except jaydebeapi.Error as e:
        print(Fore.RED + f'Erro de JayDeBeAPI: {e}')
    except Exception as e:
        print(Fore.RED + f'Erro durante a execução da consulta: {e}')

    finally:
        try:
            if cursor_csw:
                cursor_csw.close()
        except jaydebeapi.Error as e:
            print(Fore.RED + f'Erro ao fechar cursor: {e}')

        try:
            if conn:
                conn.close()
        except jaydebeapi.Error as e:
            print(Fore.RED + f'Erro ao fechar conexão: {e}')
    del _queue.SimpleQueue
    memoria_antes = memory_usage()
    print(Fore.MAGENTA + f'A MEMORIA na Etapa 1 de CadastroSKU  {round(memoria_antes / 1000000)}')

    etapa1 = controle.salvarStatus_Etapa1(rotina,'automacao', datainico,'from cgi.item i')

    # Etapa Verificando e excluindo relacionamento na tabela
    ExcluindoRelacoes()
    etapa2 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa1,'Verifica se existe chavePrimaria p/excluir')
    ConexaoPostgreMPL.Funcao_InserirPCP(sku, sku['codSKU'].size, 'SKU', 'replace')

    etapa3 = controle.salvarStatus_Etapa3(rotina,'automacao', etapa2,'inserir no Postgre o cadastroSKU')
    del sku
    gc.collect()
    memoria_antes = memory_usage()
    print(Fore.MAGENTA + f'A MEMORIA na Etapa 3 de CadastroSKU  {round(memoria_antes / 1000000)}')

    ## Criando a chave primaria escolhendo a coluna codSKU
    chave = """ALTER TABLE pcp."SKU" ADD CONSTRAINT sku_pk PRIMARY KEY ("codSKU")"""
    conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre
    cursor = conn2.cursor()# Abrindo o cursor com o Postgre
    cursor.execute(chave)
    conn2.commit() # Atualizando a chave
    cursor.close()# Fechando o cursor com o Postgre
    conn2.close() #Fechando a Conexao com o POSTGRE
    etapa4 = controle.salvarStatus_Etapa4(rotina,'automacao', etapa3,'Criar Chave primaria na tabela codSKU')
    memoria_antes = memory_usage()
    print(Fore.LIGHTCYAN_EX + f'A MEMORIA depois de CadastroSKU  {round(memoria_antes / 1000000)}')
    del etapa4
    del conn2
    del cursor, cursor_csw, conn
    gc.collect()
    print('\n OBJETOS NA MEMORIA')
    funcoesGlobais.listaObjetosMemoria.lista()

    """
    objetos_locais = listar_objetos_locais()
    if objetos_locais:
        # Liste os objetos que referenciam os objetos locais
        print("\nReferenciadores dos objetos locais:")
        for obj in gc.get_referrers(objetos_locais):
            print(obj)
    """


def ExcluindoRelacoes():

    try:
        chave = """ALTER TABLE pcp."pedidosItemgrade" DROP CONSTRAINT pedidositemgrade_fk """

        conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre

        cursor = conn2.cursor()# Abrindo o cursor com o Postgre
        cursor.execute(chave)
        conn2.commit() # Atualizando a chave
        cursor.close()# Fechando o cursor com o Postgre

        conn2.close() #Fechando a Conexao com o POSTGRE

    except:
        print('sem relacao de chave estrangeira')



