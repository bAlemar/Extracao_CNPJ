
import os
import duckdb
from src.drivers.filemanager import FileManager

class Pipeline_Silver:
    """
    Juntando os arquivos em um único arquivo parquet
    """
    def __init__(self) -> None:
        self.con = duckdb.connect()
        self.filemanager = FileManager()

    def run(self):
        self.__creating_table()
        self.__show_tables()
        self.__combined_tables()
        self.__show_total_rows_combined_table()
        self.__export_to_parquet()
        self.con.close()
        pass

    def __creating_table(self):
        """
        Criando tabela de cada Estabelecimento
        """
        path_database = 'database/bronze/raw_data_utf8'
        
        for arquivo in os.listdir(path_database):
            try:
                table_number = self.__get_table_number(arquivo)
                table_name = f"ESTABELECIMENTO{table_number}"
                file_data = rf"{path_database}/{arquivo}"
                query = f"""
                         CREATE TABLE IF NOT EXISTS {table_name} AS
                         SELECT * FROM read_csv_auto('{file_data}',delim=';',ignore_errors=True)
                        """
                self.con.execute(query)
            except Exception as error:
                print(error)
    
    def __combined_tables(self):
         # Combinar todas as tabelas em uma única consulta
        combined_query = " UNION ALL ".join([f"SELECT * FROM {table[0]}" for table in self.tables])

        query = f"""
                CREATE TABLE ESTABELECIMENTOSFULL as {combined_query}
                """
        self.con.execute(query)
        return combined_query
    

    def __export_to_parquet(self,file_path='database/silver/parquet/estabelecimentos.parquet'):
        self.filemanager.check_path(file_path)
        #Exportando a tabela combinada para arquivo Parquet
        query = f"COPY ESTABELECIMENTOSFULL TO '{file_path}' (FORMAT PARQUET)"
        self.con.execute(query)
    
    def __show_total_rows_combined_table(self):
        query = "SELECT COUNT(*) FROM ESTABELECIMENTOSFULL"
        combined_count = self.con.execute(query).fetchone()[0]
        message = f"""
                  Número de total de registro combinados:{combined_count}
                  """
        print(message)
    
    def __show_tables(self):
        """
        Mostrar as tabelas
        """
        tables = self.con.execute("SHOW TABLES").fetchall()
        self.tables = tables
        tables_message = [tables[pos][0] for pos in range(len(self.tables))]
        tables_message = '\n'.join(tables)
        message = f"""
        As tabelas criadas foram:
        \n{tables}
        """
        print(message)

    def __get_table_number(arquivo):
        return arquivo.split('.')[1][-1]