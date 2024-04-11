import pytz
import controle
import empresaConfigurada
from InformacoesOPCsw import OP_emAberto, MateriaisSubstitutosPorSku, InformacoesSilkFaccionista, TecidosDefeitosOP, pedios
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
import requests

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


def AtualizarOPSilks():
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'OPsSilksFaccionista')
    limite = 30 * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            InformacoesSilkFaccionista.ObterOpsEstamparia()
            controle.salvar('OPsSilksFaccionista', client_ip, datainicio)

    else:

            print('JA EXISTE UMA ATUALIZACAO Dos OPsSilksFaccionista   EM MENOS DE 1 HORA - 60 MINUTOS')

## Funcao de Automacao: Buscando a atualizacao dos SKUs: Duracao media de 30 Segundos
def AtualizarSKU(IntervaloAutomacao):
    print('\nETAPA 1 - ATUALIZACAO DO AutomacaoCadastroSKU')
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AutomacaoCadastroSKU')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print('ETAPA AutomacaoCadastroSKU- Inicio')
            controle.InserindoStatus('AutomacaoCadastroSKU',client_ip,datainicio)
            pedios.CadastroSKU('AutomacaoCadastroSKU',datainicio)
            controle.salvarStatus('AutomacaoCadastroSKU',client_ip,datainicio)
            print('ETAPA AutomacaoCadastroSKU- Fim')
            restart_server()
    else:
            print(f'JA EXISTE UMA ATUALIZACAO Dos AutomacaoCadastroSKU   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo')

def AtualizarPedidos(IntervaloAutomacao):
    print('\nETAPA 2 - ATUALIZACAO DOS PEDIDOS-item-grade ultimos 1 Milhao de linhas ')
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'pedidosItemgrade')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print('ETAPA AtualizarPedidos- Inicio')

            pedios.IncrementarPedidos()
            controle.salvar('pedidosItemgrade', client_ip, datainicio)
            print('ETAPA AtualizarPedidos- FIM')

    else:
            print(f'JA EXISTE UMA ATUALIZACAO Dos pedidosItemgrade   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo')

def AtualizaApiReservaFaruamento(IntervaloAutomacao):
    print('\nETAPA 4 - Atualiza Api ReservaFaruamento')
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AtualizaApiReservaFaruamento')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
        print('ETAPA AtualizaApiReservaFaruamento- Inicio')
        url = 'http://192.168.0.183:8000/pcp/api/ReservaPreFaturamento'

        token = "a44pcp22"


        # Defina os parâmetros em um dicionário

        # Defina os headers
        headers = {
            'accept': 'application/json',
            'Authorization': f'{token}'
        }


        # Faça a requisição POST com parâmetros e headers usando o método requests.post()
        response = requests.get(url,  headers=headers,  verify=False)
        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            # Converter os dados JSON em um dicionário
            dados_dict = response.json()
            controle.salvar('AtualizaApiReservaFaruamento', client_ip, datainicio)
            print('ETAPA AtualizaApiReservaFaruamento- Fim')
        else:
            print(f'AtualizaApiReservaFaruamento : erro  {response.status_code} ')

def AtualizarOPSDefeitoTecidos():
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'OPSDefeitoTecidos')
    limite = 30 * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            TecidosDefeitosOP.DefeitosTecidos()
            controle.salvar('OPSDefeitoTecidos', client_ip, datainicio)

    else:

            print('JA EXISTE UMA ATUALIZACAO Dos OPSDefeitoTecidos   EM MENOS DE 1 HORA - 60 MINUTOS')

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
        rotina = 'fila Tags Reposicao'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'fila Tags Reposicao')
        limite = 20 * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()
        if tempo > limite and empresa == '1':

            controle.InserindoStatus(rotina,client_ip,datainicio)
            tamnho1, datahora1 = RecarregarBanco.FilaTags(datainicio, rotina)
            controle.salvarStatus('fila Tags Reposicao','automacao',datainicio)

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')

        elif empresa == '4':
            tamnho1, datahora1 = RecarregarBanco.FilaTags(datainicio, rotina)

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
        rotina = 'atualiza tag off'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'atualiza tag off')
        limite = 60 * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
            controle.InserindoStatus(rotina,client_ip,datainicio)
            TagsDisponivelGarantia.SalvarTagsNoBancoPostgre(rotina, client_ip,datainicio)
            controle.salvarStatus('atualiza tag off',client_ip,datainicio)

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
    print('\n 13 - Salvando as OPsSilksFaccionista')


    AtualizarOPSilks()
    AtualizarSKU(30)
    AtualizarPedidos(60)
    AtualizaApiReservaFaruamento(60)

    print('\n 14 - Salvando as OPSDefeitoTecidos')

    #try:
    AtualizarOPSDefeitoTecidos()
'''
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})
'''

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


#Variavel que inicia a aplicacao
if __name__ == '__main__':

    print('################# INICIANDO A AUTOMACAO DOS DADOS ')

    AtualizarOPSDefeitoTecidos()
    AtualizarSKU(30)
    AtualizarPedidos(60)
    AtualizaApiReservaFaruamento(60)


    try:

        print('\n 1- INCREMENTANDO NO BANCO DE DADOS AS TAGS PRONTAS PARA O PROCESSO DE REPOSICAO')
        # coloque o código que você deseja executar continuamente aqui
        rotina = 'fila Tags Reposicao'

        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'fila Tags Reposicao')
        tempoMin = 10
        limite = tempoMin * 60  # (limite de 10 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()

        if tempo > limite and empresa == '1':
            print('\n 1- Inicio do Processo de Capturar e salvar')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            tamnho1, datahora1 = RecarregarBanco.FilaTags(datainicio, rotina)
            controle.salvarStatus('fila Tags Reposicao','automacao',datainicio)

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')

        elif empresa == '4':
            print('\n 1- Inicio do Processo de Capturar e salvar')
            tamnho1, datahora1 = RecarregarBanco.FilaTags(datainicio, rotina)

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')
        else:
            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! a ultima atualizacao ocorreu a {tempo} , com {tempoMin} minutos de antecendencia antes do limite planejado')




    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()

    print('\n 12 - Salvando as OPs que tiveram substitutos')

    #try:
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'SubstitutosSkuOP')
    limite = 60 * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:

            MateriaisSubstitutosPorSku.SubstitutosSkuOP()
            controle.salvar('SubstitutosSkuOP', client_ip, datainicio)

    else:

            print('JA EXISTE UMA ATUALIZACAO Dos SubstitutosSkuOP   EM MENOS DE 1 HORA - 60 MINUTOS')

    #except Exception as e:
     #   print(f"Erro detectado: {str(e)}")
      #  restart_server()

    try:
        my_task()
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()


app.run(host='0.0.0.0', port=port)
