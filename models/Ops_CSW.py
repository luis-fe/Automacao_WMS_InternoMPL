import gc
import pandas as pd
import ConexaoCSW
import controle


class Ops_CSW():
    '''Classe que busca os dados, via sql,  referente às Ordem Producao no ERP CSW '''
    def __init__(self, dataInicio, rotina=None, empresa=None, diasPesquisa = 100):
        '''Contrutor da classe'''

        self.dataInicio = dataInicio
        self.rotina = rotina
        self.empresa = empresa
        self.diasPesquisa = diasPesquisa

    def registroSubstituto_porReq(self):
        '''Metodo que busca no CSW o registro de Substitutos a nivel de requisicao'''

        # 1 Buscando os substitutos a nivel de requisicao
        sql = f""" 
        SELECT 
            s.codRequisicao as requisicao , 
            r.numOPConfec as numeroop ,
            (SELECT op.codProduto from tco.OrdemProd op WHERE op.codempresa = 1 and op.numeroop = r.numOPConfec ) as codproduto,
            r.dtBaixa as databaixa_req, 
            s.codItemPrincipal as componente, 
            ri.nomeMaterial as nomecompontente, 
            s.codMaterial as subst,
            (select ri2.nomeMaterial from tcq.RequisicaoItem ri2 where s.codEmpresa = ri2.codEmpresa and s.codRequisicao = ri2.codRequisicao  and ri2.codMaterial = s.codMaterial) as nomesub,
            (select ri2.codmaterialedt from tcq.RequisicaoItem ri2 where s.codEmpresa = ri2.codEmpresa and s.codRequisicao = ri2.codRequisicao  and ri2.codMaterial = s.codMaterial) as coodigoSubs, 
            (select ri2.codmaterialedt from tcq.RequisicaoItem ri2 where s.codEmpresa = ri2.codEmpresa and s.codRequisicao = ri2.codRequisicao  and ri2.codMaterial = s.codItemPrincipal) as coodigoPrincipal
        FROM 
            TCQ.Requisicao R
        inner join 
            tcq.RequisicaoItemSubst s 
            on s.codEmpresa = {self.empresa}  
            and s.codRequisicao = r.numero
        left join 
            tcq.RequisicaoItem ri on s.codEmpresa = ri.codEmpresa 
            and s.codRequisicao = ri.codRequisicao  
            and ri.codMaterial = s.codItemPrincipal 
        WHERE 
            R.codEmpresa = {self.empresa} 
            and r.dtBaixa  > DATEADD('day', -{self.diasPesquisa}, CURRENT_TIMESTAMP)"""

        with ConexaoCSW.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                registro = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()


        return registro


    def consulta_OP_porCorSortimentoCSW(self):
        '''Método que busca no ERP CSW numeroOP x CorSortimento, nas primeira 80000 registros de op's '''

        sql = f"""
                    SELECT top 80000
                        op.numeroOP as numeroop , 
                        op.codSortimento, 
                        s.corbase||'-'||s.nomeCorBase as cor  
                    FROM 
                        tco.OrdemProdGrades op
                   inner join 
                        tcp.SortimentosProduto s 
                        on s.codEmpresa = {self.empresa} 
                        and s.codProduto = op.codProduto 
                        and s.codSortimento = op.codSortimento
                   WHERE 
                        op.codEmpresa = {self.empresa}  
                        and op.codproduto like '%-0'"
                   order by 
                        numeroOP desc"""


        with ConexaoCSW.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta

