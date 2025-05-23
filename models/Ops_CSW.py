import gc
import pandas as pd
from connection import ConexaoERPCSW

class Ops_CSW():
    '''Classe que busca os dados, via sql,  referente às Ordem Producao no ERP CSW '''
    def __init__(self, dataInicio, rotina=None, empresa=None, diasPesquisa = 100):
        '''Contrutor da classe'''

        self.dataInicio = dataInicio
        self.rotina = rotina
        self.empresa = str(empresa)
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

        with ConexaoERPCSW.ConexaoInternoMPL() as conn:
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
        '''Método que busca no ERP CSW numeroOP x CorSortimento, nas primeira 80000 registros de op's'''

        sql = f"""
            SELECT TOP 80000
                op.numeroOP AS numeroop, 
                op.codSortimento, 
                s.corbase || '-' || s.nomeCorBase AS cor  
            FROM 
                tco.OrdemProdGrades op
            INNER JOIN 
                tcp.SortimentosProduto s 
                ON s.codEmpresa = {self.empresa} 
                AND s.codProduto = op.codProduto 
                AND s.codSortimento = op.codSortimento
            WHERE 
                op.codEmpresa = {self.empresa}  
                AND CAST(op.codproduto AS VARCHAR) LIKE '%-0'
            ORDER BY 
                numeroOP DESC
        """

        with ConexaoERPCSW.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


