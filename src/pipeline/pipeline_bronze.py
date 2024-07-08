from src.drivers.request.driver_requests import Requests
from src.drivers.filemanager import FileManager
import os
import subprocess

class Pipeline_Bronze:
    def __init__(self) -> None:
        self.request = Requests()
        self.file_manager = FileManager()

    def run(self):
        lista_zip_dados = self.__download_dados()
    
        path_database = 'database/bronze/raw_data_latin'
        self.file_manager.check_path(path_database)
        self.__extract_data_from_zip(path_database,lista_zip_dados)
        
        path_database_utf8 = 'database/bronze/raw_data_utf8'
        self.file_manager.check_path(path_database_utf8)
        self.__transform_latin_to_utf8(path_database,
                                       path_database_utf8)

    def __download_dados(self):
        lista_zip_dados = []
        for pos in range(0,10):
            url = f'https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos{pos}.zip'
            dados_estabelecimento =  self.request.get(url)
            lista_zip_dados.append(dados_estabelecimento.content)
        return lista_zip_dados

    def __extract_data_from_zip(self,path_database,lista_zip_dados):
        for dados in lista_zip_dados:
            self.file_manager.extract_zip_from_memory(dados,path_database)


    def __convert_to_utf8(self,input_path, output_path):
        subprocess.run(['iconv', '-f', 'LATIN1', '-t', 'UTF-8', input_path, '-o', output_path], check=True)

    def __transform_latin_to_utf8(self,path_database,path_converted):
        for arquivo in os.listdir(path_database):
            input_path = os.path.join(path_database, arquivo)
            output_path = os.path.join(path_converted, arquivo)
            self.__convert_to_utf8(input_path, output_path)