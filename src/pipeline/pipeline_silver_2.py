import duckdb
from src.drivers.filemanager import FileManager

class Pipeline_Silver_2:
    """
    Aplicando regras de negócio e gerando arquivo .csv final
    """

    def __init__(self) -> None:
        self.con = duckdb.connect()
        self.filemanager = FileManager()


    def run(self):
        self.__create_table()
        self.__set_columns()
        self.__filter_table()
        self.__save_in_csv()

    def __create_table(self):
        path_parquet = 'database/silver/parquet/estabelecimentos.parquet'
        self.con.execute(f"""
                    CREATE TABLE final_table as SELECT * FROM read_parquet('{path_parquet}')
                        """)
    def __set_columns(self):
        """
        Mudando nome das colunas
        **Acredito que esse passo seja elimnado ao criar um schema para tabela
        """
        cursor = self.con.execute("PRAGMA table_info(final_table);")
        current_columns = cursor.fetchall()

        new_col_names = ['CNPJ', 'CNPJ_ORDEDM', 'CNPJ_DV', 'MATRIZ_FILIAL', 'NOME',
                'SITUACAO_CADASTRO', 'DATA_SITUACAO_CADASTRAL', 'MOTIVO_SITUACAO_CADASTRAL',
                'NOME_CIDADE_EXTERIOR', 'PAIS', 'DATA_INICIO_ATIVIDADE', 'CNAE_FISCAL_PRINCIPAL',
                'CNAE_FISCAL_SECUNDARIA', 'TIPO_LOGRADOURO', 'LOGRADOURO', 'NUMERO', 'COMPLEMENTO', 'BAIRRO',
                'CEP', 'UF', 'MUNICIPIO', 'DDD_1', 'TELEFONE_1', 'DDD_2', 'TELEFONE_2', 'DDD_FAX', 'FAX', 'CORREIRO_ELETRONICO',
                'SITUACAO_ESPECIAL', 'DATA_SITUACAO_ESPECIAL']
           
        # Verificar e renomear as colunas se necessário
        for i, column_info in enumerate(current_columns):
            current_name = column_info[1]  # Nome atual da coluna
            new_name = new_col_names[i]
            if current_name != new_name:
                query = f"ALTER TABLE final_table RENAME COLUMN {current_name} TO {new_name};"
                self.con.execute(query)
        
        self.columns = new_col_names

    def __filter_table(self):
        """
        Aplica Regra de Negócio
        """
        query = """
                SELECT *
                FROM final_table
                WHERE (CNAE_FISCAL_PRINCIPAL = '4729601' OR CNAE_FISCAL_PRINCIPAL = '4729602' OR CNAE_FISCAL_SECUNDARIA = '4729601' OR CNAE_FISCAL_SECUNDARIA = '4729602')
                AND (TELEFONE_1 != '' OR TELEFONE_2 != '')
                """

        cursor = self.con.execute(query)
        self.results = cursor.fetchall()
        return self.results
    
    def __save_in_csv(self):
        path = 'database/silver/csv'
        self.filemanager.check_path(path)
        print(self.columns)
        self.filemanager.create_and_write_csv(path,
                                              'silver_final',
                                              self.columns,
                                              self.results)
    
    def __show_new_columns(self):
        # Verificar os nomes atualizados das colunas
        updated_columns = self.con.execute("PRAGMA table_info(final_table);").fetchall()
        message = f"""
                    Novas colunas {updated_columns}
                   """
        

        