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
                        ' p.numeroOP in ( SELECT numeroOP  FROM tco.OrdemProd o WHERE codEmpresa = '+emp+' and codFaseAtual in (210, 320, 56, 432, 441, 452, 423, 433, 437, 429, 365 ) and situacao = 3) ', conn)
    conn.close()


    restringe = BuscaResticaoSubstitutos()
    consulta = pd.merge(consulta,restringe,on=['numeroop','cor'],how='left')


    return consulta

def SalvarTagsNoBancoPostgre():
    consulta = BuscarTagsGarantia()

    ConexaoPostgreMPL.Funcao_InserirOFF(consulta,consulta.size,'filareposicaoof', 'replace')

    print('Tags filareposicaoOF atualizadas com sucesso')

def BuscaResticaoSubstitutos():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql("select numeroop , codproduto||'||'||numeroop  as resticao,  "
                            'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                           "sso where sso.considera = 'sim'",conn)

    conn.close()

    return consulta