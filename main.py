import RecarregaPedidos
import RecarregarBanco
from flask import Flask, render_template, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
import os

app = Flask(__name__)
port = int(os.environ.get('PORT', 8000))
def my_task():
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho, datahora = RecarregarBanco.FilaTags()
        print('Sucesso - Fila Tag')
    except:
        print('falha na automacao - Fila Reposicao')

    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho = RecarregarBanco.avaliacaoFila()
        print(f'Sucesso - avaliacao Fila Reposicao {tamnho} tags eliminadas')
    except:
        print('falha na automacao - avaliacao Fila Reposicao')

    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho = RecarregaPedidos.avaliacaoPedidos()
        print(f'Sucesso - avaliacao Fila Pedidos  {tamnho} tags eliminadas')
    except:
        print('falha na automacao - avaliacao Fila Reposicao')

    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho, datahora = RecarregaPedidos.SeparacoPedidos()
        print('Sucesso - Fila Pedidos')
    except:
        print('falha na automacao - Fila Pedidos')

    print('fim do ciclo')

scheduler = BackgroundScheduler()
scheduler.add_job(func=my_task, trigger='interval', seconds=270)
scheduler.start()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)