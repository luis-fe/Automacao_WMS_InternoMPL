import ConexaoPostgreMPL
import gc
import ConexaoCSW
import controle
import pandas as pd

class TagsOFF():
    def __init__(self, empresa, rotina, ip, datahoraInicio):
        self.empresa = empresa
        self.rotina = rotina
        self.ip = ip
        self.datahoraInicio = datahoraInicio
    def BuscarTagsGarantia(self):

        query = """
        SELECT 
            p.codBarrasTag as codbarrastag, 
            p.codReduzido as codreduzido, 
            p.codEngenharia as engenharia,
            (SELECT i.nome FROM cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, 
            situacao, 
            codNaturezaAtual as natureza, 
            codEmpresa as codempresa,
            (SELECT s.corbase || '-' || s.nomecorbase FROM tcp.SortimentosProduto s WHERE s.codempresa = 1 AND s.codproduto = p.codEngenharia AND s.codsortimento = p.codSortimento) as cor, 
            (SELECT t.descricao FROM tcp.Tamanhos t WHERE t.codempresa = 1 AND t.sequencia = p.seqTamanho) as tamanho, 
            p.numeroOP as numeroop
        FROM 
            Tcr.TagBarrasProduto p 
        WHERE 
            p.codEmpresa = ? AND 
            p.numeroOP IN (
                SELECT numeroOP  
                FROM tco.OrdemProd o 
                WHERE codEmpresa = ? AND codFaseAtual > 428 AND situacao = 3
            )
        """

        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a consulta e armazena os resultados
                cursor_csw.execute(query, (self.empresa, self.empresa))
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        # Salva o status da etapa 1
        etapa1 = controle.salvarStatus_Etapa1(self.rotina, self.ip, self.datahoraInicio, 'etapa csw Tcr.TagBarrasProduto p')

        # Busca restrições e substitutos
        restringe = self.BuscaResticaoSubstitutos()

        if restringe['numeroop'][0] != 'vazio':
            consulta.fillna('-', inplace=True)
            consulta = pd.merge(consulta, restringe, on=['numeroop', 'cor'], how='left')
            consulta['resticao'].fillna('-', inplace=True)
        else:
            consulta['resticao'] = '-'
            consulta['considera'] = '-'

        # Salva o status da etapa 2
        etapa2 = controle.salvarStatus_Etapa2(self.rotina, self.ip, etapa1, 'Adicionando os substitutos selecionados no wms')

        return consulta, etapa2

    def BuscaResticaoSubstitutos(self):
        conn = ConexaoPostgreMPL.conexaoMatriz()

        consulta = pd.read_sql("select numeroop , codproduto||'||'||cor||'||'||numeroop  as resticao,  "
                                'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                               "sso where sso.considera = 'sim' ",conn)

        conn.close()

        if consulta.empty:

            return pd.DataFrame([{'numeroop':'vazio'}])

        else:

            return consulta

    def SalvarTagsNoBancoPostgre(self):
        consulta, etapa2 = self.BuscarTagsGarantia()
        ConexaoPostgreMPL.Funcao_InserirOFFMatriz(consulta,consulta.size,'filareposicaoof', 'replace')
        etapa3 = controle.salvarStatus_Etapa3(self.rotina, self.ip, etapa2, 'Adicionar as tags ao wms')
        del consulta, etapa2
        gc.collect()