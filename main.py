import pytz
import controle
import empresaConfigurada
from InformacoesOPCsw import OP_emAberto, MateriaisSubstitutosPorSku
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
import datetime
import Ritmo
import TagsDisponivelGarantia
import TratamentoErro
import Usuarios
import subprocess
import subprocess

app = Flask(__name__)
CORS(app)
port = int(os.environ.get('PORT', 9000))


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%H')
    return hora_str
def restart_server():
    print("Reiniciando o aplicativo...")
    subprocess.call(["python", "main.py"])

def my_task():
    hora = obterHoraAtual()

    if hora in ['07','08','09','10', '11', '12', '13', '14', '15', '16','17','18']:
        my_task2()
    else:
        print('Aguardando a Data e Hora correto '+hora)


def my_task2():
    #OP_emAberto.IncrementadoDadosPostgre('1')
    print('\n ###Começando a Automacao  do WMS ### ')
    print(f'\n1 - Iniciando a Fila das Tags para Repor:')

    try:
        # coloque o código que você deseja executar continuamente aqui

        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'fila Tags Reposicao')
        limite = 20 * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()
        if tempo > limite and empresa == '1':

            tamnho1, datahora1 = RecarregarBanco.FilaTags()
            controle.salvar('fila Tags Reposicao','automacao',datainicio)

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')

        elif empresa == '4':
            tamnho1, datahora1 = RecarregarBanco.FilaTags()

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')
        else:
            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! ja tinha atualizacao congelada')
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
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'avaliacaoReposicao')
        limite = 5 * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()
        if tempo > limite and empresa == '1':

            tamnho7, datahora7 = RecarregaPedidos.avaliacaoReposicao()
            controle.salvar('avaliacaoReposicao',client_ip,datainicio)

            print(f'7.1 Sucesso - Limpando Saida da Reposicao \nAtenção!  {tamnho7} tags limpadas, as {datahora7}')
        else:
            print(f'7.1 Sucesso - Limpando Saida da Reposicao \nCongelado')

    except:
        print(f'7.1.1 falha na automacao - Limpando Saida da Reposicao')

    print('\n 9- TratamentoErrosDuplicacoes')
    try:
        datahora9 = TratamentoErro.RemoveDuplicatasUsuario()
        print(f'9.1 Sucesso - Limpeza de Duplicatas Usuario Atribuido na Reposicao, as {datahora9}')
    except:
        print('9.1.1 Falha na automacao - Tratamento de Erros')
    print('\n 10 - Importando Tags da Reposicao off')
    try:
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'atualiza tag off')
        limite = 60 * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:

            TagsDisponivelGarantia.SalvarTagsNoBancoPostgre()
            controle.salvar('atualiza tag off',client_ip,datainicio)

        else:
            print('JA EXISTE UMA ATUALIZACAO DA FILA TAGS OFF EM MENOS DE 1 HORA - 60 MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})
    print('\n 11 - Importando as Ordem de Producao')

    try:
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'ordem de producao')
        limite = 10 * 60  # (limite de 10 minutos , convertido para segundos)
        if tempo > limite:
            OP_emAberto.IncrementadoDadosPostgre('1')
            controle.salvar('ordem de producao', client_ip, datainicio)

        else:

            print('JA EXISTE UMA ATUALIZACAO DA FILA TAGS OFF EM MENOS DE 1 HORA - 60 MINUTOS')
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

    print('\n 12 - Salvando as OPs que tiveram substitutos')

    try:
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'SubstitutosSkuOP')
        limite = 60 * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
            MateriaisSubstitutosPorSku.SubstitutosSkuOP()
            controle.salvar('SubstitutosSkuOP', client_ip, datainicio)

        else:

            print('JA EXISTE UMA ATUALIZACAO Dos SubstitutosSkuOP   EM MENOS DE 1 HORA - 60 MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

    print('\n 13 - Salvando as OPs que tiveram substitutos - PARTE 2 ComponentesPrincipalPorSKU')
    try:
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'ComponentesPrincipalPorSKU')
        limite = 90 * 60  # (limite de 90 minutos , convertido para segundos)
        situacaoAutomacao = controle.distinctStatus('ComponentesPrincipalPorSKU')
        if tempo > limite and situacaoAutomacao == 'nao iniciado':
            controle.InserindoStatus('ComponentesPrincipalPorSKU', client_ip, datainicio)
            MateriaisSubstitutosPorSku.ComponentesPrincipalPorSKU()
            controle.salvarStatus('ComponentesPrincipalPorSKU', client_ip, datainicio)

        else:

            print('JA EXISTE UMA ATUALIZACAO Dos ComponentesPrincipalPorSKU   EM MENOS DE 1 HORA - 60 MINUTOS')


    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

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



scheduler.add_job(func=my_task, trigger='interval', seconds=300)
scheduler.start()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    print('INICIANDO A AUTOMACAO DOS DADOS REFERENTE AO WMS')

    try:

        print('\n 1- INCREMENTANDO NO BANCO DE DADOS AS TAGS PRONTAS PARA O PROCESSO DE REPOSICAO')
        # coloque o código que você deseja executar continuamente aqui

        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'fila Tags Reposicao')
        tempoMin = 10
        limite = tempoMin * 60  # (limite de 10 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()

        if tempo > limite and empresa == '1':
            print('\n 1- Inicio do Processo de Capturar e salvar')
            tamnho1, datahora1 = RecarregarBanco.FilaTags()
            controle.salvar('fila Tags Reposicao','automacao',datainicio)

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')

        elif empresa == '4':
            print('\n 1- Inicio do Processo de Capturar e salvar')
            tamnho1, datahora1 = RecarregarBanco.FilaTags()

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')
        else:
            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! a ultima atualizacao ocorreu a {tempo} , com {tempoMin} minutos de antecendencia antes do limite planejado')

        print('\n 12 - Salvando as OPs que tiveram substitutos')

        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'SubstitutosSkuOP')
        limite = 60 * 60  # (limite de 60 minutos , convertido para segundos)

        print('ETAPA 2: SUBSTITUTOS DAS OPS')
        print(f'ultima atualizacao {tempo} e o limite {limite}')
        if tempo > limite:
            print('iniciando o salvamento')
            MateriaisSubstitutosPorSku.SubstitutosSkuOP()
            controle.salvar('SubstitutosSkuOP', client_ip, datainicio)
            print('salvo com sucesso !')

        else:

            print('JA EXISTE UMA ATUALIZACAO Dos SubstitutosSkuOP   EM MENOS DE 1 HORA - 60 MINUTOS')
        print('\n 13 - Salvando as OPs que tiveram substitutos - PARTE 2 ComponentesPrincipalPorSKU')

        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'ComponentesPrincipalPorSKU')
        limite = 90 * 60  # (limite de 90 minutos , convertido para segundos)
        situacaoAutomacao = controle.distinctStatus('ComponentesPrincipalPorSKU')
        if tempo > limite and situacaoAutomacao == 'nao iniciado':
            controle.InserindoStatus('ComponentesPrincipalPorSKU', client_ip, datainicio)
            MateriaisSubstitutosPorSku.ComponentesPrincipalPorSKU()
            controle.salvarStatus('ComponentesPrincipalPorSKU', client_ip, datainicio)



        else:

            print('JA EXISTE UMA ATUALIZACAO Dos ComponentesPrincipalPorSKU   EM MENOS DE 1 HORA - 60 MINUTOS')



    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()

    try:
        my_task()
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()


app.run(host='0.0.0.0', port=port)