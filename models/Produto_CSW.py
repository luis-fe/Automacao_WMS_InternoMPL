import gc
import pandas as pd
import ConexaoCSW
import controle



class Produto_CSW():
    '''Classe que busca os dados, via sql,  referente aos Produtos no ERP CSW '''


    def __init__(self, dataInicio, rotina=None, empresa=None, diasPesquisa = 100):
        '''Contrutor da classe'''

        self.dataInicio = dataInicio
        self.rotina = rotina
        self.empresa = empresa
        self.diasPesquisa = diasPesquisa


    def componentesPadroes_Subs_Eng(self):
        '''Metodo que busca os compontentes padroes Substitutos vinculados aos Produtos - "Engenharias" no Csw, restrigindo as 10.000 Ops mais recentes'''


        # 1 - Pesquisando a mascara de classificacao de produto por empresa para utilizar no like da consulta sql:
        if self.empresa == 1:
            self.likeProdutoMascara = '01'

        elif self.empresa == 4:
            self.likeProdutoMascara = '03'
        else:
            self.likeProdutoMascara = ''

        # 2 - Consulta Sql

        sql = f"""
            SELECT 
                c.CodComponente as componente, 
                c.codProduto as codproduto, 
                codaplicacao as aplicacaoPad  
            FROM 
                tcp.ComponentesPadroes  c
            WHERE 
                c.codEmpresa = {self.empresa} 
                and c.codProduto in (
                                    SELECT top 10000 
                                        op.codproduto 
                                    from 
                                        tco.OrdemProd op 
                                    WHERE 
                                        op.codempresa =  {self.empresa}
                                    order by 
                                        numeroOP desc
                                    ) 
                and c.CodComponente in (
                                        SELECT 
                                            s.codItemPrincipal 
                                        from 
                                            tcq.RequisicaoItemSubst s 
                                        WHERE 
                                            s.codempresa = {self.empresa}
                                        ) 
                and c.codproduto like '{self.likeProdutoMascara}%' """

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



    def componentesVariaveis_Subs_Eng(self):
        sql = f"""
            SELECT 
                c.CodComponente as componente , 
                c.codSortimento, 
                c.codProduto as codproduto, 
                codAplicacao as aplicacao 
            FROM 
                tcp.ComponentesVariaveis c
            WHERE 
                c.codEmpresa = {self.empresa} 
                and c.codProduto in (
                                        SELECT top 10000 
                                            op.codproduto 
                                        from 
                                            tco.OrdemProd op 
                                        WHERE 
                                            op.codempresa = {self.empresa}
                                        order by 
                                            numeroOP desc
                                    ) 
                and c.CodComponente in (
                                        SELECT 
                                            s.codItemPrincipal 
                                        from 
                                            tcq.RequisicaoItemSubst s 
                                        WHERE 
                                            s.codempresa = {self.empresa}
                                        ) """

        with ConexaoCSW.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()


        # Dividir os valores da coluna 2 por ";"
        consulta['codSortimento'] = consulta['codSortimento'].str.split(';')

        # Usar explode para expandir os valores em múltiplas linhas
        consulta = consulta.explode('codSortimento')


        return consulta