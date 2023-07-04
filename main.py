import RecarregaPedidos
import RecarregarBanco
from flask import Flask, render_template, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
import os

app = Flask(__name__)
port = int(os.environ.get('PORT', 8000))
def my_task():
    print('Começando a Automacao Automatica do WMS:')
    print(f'\n 1 - Iniciando a Fila das Tags para Repor:')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho, datahora = RecarregarBanco.FilaTags()
        print(f'1.1 Sucesso - Fila das Tag \n{tamnho} tags foram adicionadas, as {datahora}')
    except:
        print('1.1.1 falha na automacao - Fila Reposicao \n 0 tags foram adicionadas')

    print('\n 2 - Limpando a Fila das Tags com saidas fora do WMS')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamanho, datahora2 = RecarregarBanco.avaliacaoFila()
        print(f'2.1- Sucesso - avaliacao Fila Reposicao \n {tamanho} tags foram eliminadas, as {datahora2}')
    except:
        print('2.1.1 falha na automacao - avaliacao Fila Reposicao')

    print('\n 3 - Limpando os Pedidos Faturados da Fila')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho, datahora3 = RecarregaPedidos.avaliacaoPedidos()
        print(f'3.1 Sucesso - avaliacao Fila Pedidos \n {tamnho} pedidos eliminados, as {datahora3}')
    except:
        print('3.1.1 falha na automacao - avaliacao Fila Pedidos')

    print('\n 4 - Carregando a Fila de Pedidos')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho4, datahora4 = RecarregaPedidos.SeparacoPedidos()
        print(f'4.1 Sucesso - Atualizacao Fila Pedidos \n {tamnho4} Pedidos, as {datahora4}')
    except:
        print(f'4.1.1 falha na automacao - Fila Pedidos')

    print('\n 5 - Limpando PedidoSku')
    try:
        # coloque o código que você deseja executar continuamente aqui
        datahora5 = RecarregaPedidos.LimpezaPedidosSku()
        print(f'5.1 Sucesso - Pedios Faturados Limpados do PedidoSKU, as {datahora5}')
    except:
        print(f'5.1.1 falha na automacao - Limpeza PedidosSku')

    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho, datahora = RecarregaPedidos.LimpezaPedidosSku()
        print('Sucesso - Limpeza PedidosSKU')
    except:
        print('falha na automacao - Limpeza PedidosSKU')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho, datahora = RecarregaPedidos.IncrementarSku()
        print('Sucesso - Incrementacao Sku')
    except:
        print('falha na automacao - Incrementacao SKU')

    print('Fim do Ciclo')

scheduler = BackgroundScheduler()
scheduler.add_job(func=my_task, trigger='interval', seconds=270)
scheduler.start()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)