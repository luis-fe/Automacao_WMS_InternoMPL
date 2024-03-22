import pandas as pd

import ConexaoCSW
import ConexaoPostgreMPL
import controle
import empresaConfigurada


def BuscarTagsGarantia(rotina, ip,datahoraInicio):
    emp = empresaConfigurada.EmpresaEscolhida()
    conn = ConexaoCSW.Conexao()

    consulta = pd.read_sql('SELECT p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia,'
                           '(select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa,'
                           " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"
                           ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop'
                           ' from Tcr.TagBarrasProduto p WHERE p.codEmpresa = '+emp+' and '
                        ' p.numeroOP in ( SELECT numeroOP  FROM tco.OrdemProd o WHERE codEmpresa = '+emp+' and codFaseAtual in (210, 320, 56, 432, 441, 452, 423, 433, 437, 429, 365 ) and situacao = 3) ', conn)
    conn.close()
    etapa1 = controle.salvarStatus_Etapa1(rotina, ip,datahoraInicio,'etapa csw Tcr.TagBarrasProduto p')

    restringe = BuscaResticaoSubstitutos()
    print(consulta)
    print(restringe['numeroop'][0])

    if restringe['numeroop'][0] != 'vazio':
        consulta = pd.merge(consulta,restringe,on=['numeroop','cor'],how='left')
        print('passou aqui')

        consulta['resticao'].fillna('-', inplace=True)
    else:
        consulta['resticao'] = '-'
        consulta['considera'] = '-'


    etapa2 = controle.salvarStatus_Etapa2(rotina, ip, etapa1, 'Adicionando os substitutos selecionados no wms')



    return consulta, etapa2

def SalvarTagsNoBancoPostgre(rotina, ip,datahoraInicio):
    consulta, etapa2 = BuscarTagsGarantia(rotina, ip,datahoraInicio)
    print(consulta)
    ConexaoPostgreMPL.Funcao_InserirOFF(consulta,consulta.size,'filareposicaoof', 'replace')
    etapa3 = controle.salvarStatus_Etapa3(rotina, ip, etapa2, 'Adicionar as tags ao wms')

    print('Tags filareposicaoOF atualizadas com sucesso')

def BuscaResticaoSubstitutos():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql("select numeroop , codproduto||'||'||numeroop  as resticao,  "
                            'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                           "sso where sso.considera = 'sim' ",conn)

    conn.close()

    if consulta.empty:

        return pd.DataFrame([{'numeroop':'vazio'}])

    else:

        return consulta