import pandas as pd

import ConexaoCSW
import ConexaoPostgreMPL
import empresaConfigurada


def BuscarTagsGarantia():
    emp = empresaConfigurada.EmpresaEscolhida()
    conn = ConexaoCSW.Conexao()

    consulta = pd.read_sql('SELECT p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia,'
                           '(select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa,'
                           " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"
                           ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop'
                           ' from Tcr.TagBarrasProduto p WHERE p.codEmpresa = '+emp+' and '
                        ' p.numeroOP in ( SELECT numeroOP  FROM tco.OrdemProd o WHERE codEmpresa = '+emp+' and codFaseAtual in (210, 320, 56, 432, 441, 452 ) and situacao = 3) ', conn)
    conn.close()
    return consulta

def SalvarTagsNoBancoPostgre():
    consulta = BuscarTagsGarantia()

    ConexaoPostgreMPL.Funcao_InserirOFF(consulta,consulta.size,'filareposicaoof', 'replace')

    print('Tags filareposicaoOF atualizadas com sucesso')