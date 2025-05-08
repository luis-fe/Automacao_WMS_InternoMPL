import os
import gc
from routes.Qualidade.MateriaisSubstitutosPorSku import SubstitutosSkuOP
import psutil

from routes.TagsFilaReposicao import TagsReposicao
from routes.Pedidos import Pedidos,PedidosRoute

"""
NESSE DOCUMENTO .mani é realizado o processo de automacao via python da transferencia PLANEJADA de dados do banco de dados Origem "CACHÉ" do ERP CSW , 
PARA O BANCO DE DADOS DA APLICACAO DE WMS E PORTAL DA QUALIDADE
"""

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # Retorna o uso de memória em bytes





# INICIANDO O PROCESSO DE AUTOMACAO
if __name__ == '__main__':
    PedidosRoute.AutomacaoRealizadoFases(1)
    gc.collect()



