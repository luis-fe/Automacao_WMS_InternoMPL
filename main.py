import pytz

import CalculoEnderecos
import CalculoNecessidadesEndereco
import ConexaoCSW
import RecarregaPedidos
import RecarregarBanco
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from functools import wraps
from apscheduler.schedulers.background import BackgroundScheduler
import os
from datetime import datetime, time
import Ritmo
import TratamentoErro
import Usuarios
import pandas as pd

app = Flask(__name__)
CORS(app)
port = int(os.environ.get('PORT', 9000))
def my_task():
    print('\n ###Começando a Automacao  do WMS ### ')
    print(f'\n1 - Iniciando a Fila das Tags para Repor:')

    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho1, datahora1 = RecarregarBanco.FilaTags()
        print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')
    except:
        print(' 1.1.1 falha na automacao - Fila Reposicao \n Atenção! 0 tags foram adicionadas')

    print('\n 2 - Limpando a Fila das Tags com saidas fora do WMS')

    try:
        # coloque o código que você deseja executar continuamente aqui
        tamanho2, datahora2 = RecarregarBanco.avaliacaoFila()
        print(f' 2.1- Sucesso - avaliacao Fila Reposicao \n Atencao!{tamanho2} tags foram eliminadas, as {datahora2}')
    except:
        print(' 2.1.1 falha na automacao - avaliacao Fila Reposicao')

    print('\n 3 - Limpando os Pedidos Faturados da Fila')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho3, datahora3 = RecarregaPedidos.avaliacaoPedidos()
        print(f' 3.1 Sucesso - avaliacao Fila Pedidos \n Atenção!  {tamnho3} pedidos eliminados, as {datahora3}')
    except:
        print(' 3.1.1 falha na automacao - avaliacao Fila Pedidos')


    print('\n 6 - Limpando PedidoSku')
    try:
        # coloque o código que você deseja executar continuamente aqui
        datahora6 = RecarregaPedidos.LimpezaPedidosSku()
        print(f'6.1 Sucesso - Pedios Faturados Limpados do PedidoSKU, as {datahora6}')
    except:
        print(f'6.1.1 falha na automacao - Limpeza PedidosSku')

    print('\n 7- Limpando as saidas de Tags Repostas fora do WMS')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho7, datahora7 = RecarregaPedidos.avaliacaoReposicao()
        print(f'7.1 Sucesso - Limpando Saida da Reposicao \nAtenção!  {tamnho7} tags limpadas, as {datahora7}')
    except:
        print(f'7.1.1 falha na automacao - Limpando Saida da Reposicao')

    print('\n 9- TratamentoErrosDuplicacoes')
    try:
        datahora9 = TratamentoErro.RemoveDuplicatasUsuario()
        print(f'9.1 Sucesso - Limpeza de Duplicatas Usuario Atribuido na Reposicao, as {datahora9}')
    except:
        print('9.1.1 Falha na automacao - Tratamento de Erros')



    #print('\n 10- Calculando Necessidade de Enderecos')
    '''''
    try:
        tamanho10 , inseridos10 = CalculoEnderecos.Calculo('5')
        print(f'10.1 Sucesso -Atualizdo novos  {tamanho10} enderecos e inseridos Duplicados {inseridos10}')
    except:
        print('10.1.1 Falha na automacao - Calculo dos Enderecos')
    '''''
    print('Fim do Ciclo')

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
@app.route('/api/UsuarioSenha', methods=['GET'])
@token_required
def check_user_password():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codigo = request.args.get('codigo')
    senha = request.args.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    result = Usuarios.ConsultaUsuarioSenha(codigo, senha)

    # Verifica se o usuário existe
    if result == 1:
        # Consulta no banco de dados para obter informações adicionais do usuário

        nome, funcao, situacao = Usuarios.PesquisarUsuariosCodigo(codigo)

        # Verifica se foram encontradas informações adicionais do usuário
        if nome != 0:
            Usuarios.RegistroLog(codigo)
            # Retorna as informações adicionais do usuário
            return jsonify({
                "status": True,
                "message": "Usuário e senha VALIDADOS!",
                "nome": nome,
                "funcao": funcao,
                "situacao": situacao
            })
        else:
            return jsonify({'message': 'Não foi possível obter informações adicionais do usuário.'}), 500
    else:
        return jsonify({"status": False,
                        "message": 'Usuário ou senha não existe'}), 401



scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Sao_Paulo'))

# Define o horário de início do agendamento (8:00 AM)
start_time = datetime.combine(datetime.today(), time(hour=8, minute=0, second=0))

# Define o horário de término do agendamento (5:30 PM)
end_time = datetime.combine(datetime.today(), time(hour=17, minute=30, second=0))


scheduler.add_job(func=my_task, trigger='interval', seconds=270, start_date=start_time, end_date=end_time)
scheduler.start()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho1, datahora1 = RecarregarBanco.FilaTags()
        print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')
    except:
        print(' 1.1.1 falha na automacao - Fila Reposicao \n Atenção! 0 tags foram adicionadas')
    app.run(host='0.0.0.0', port=port)