import os
import csv 
from typing import Any,Dict,List
import zipfile
import io

class FileManager:

    def __init__(self) -> None:
        pass

    @staticmethod
    def extract_zip_from_memory(data, extract_to):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            archive.extractall(extract_to)

    def check_path(self,file_path:str) -> None:
        if not os.path.exists(file_path):
            os.makedirs(file_path)
    
    def create_and_write_csv(self,path,file_name,columns,data):
        self.create_csv(path,file_name,columns)
        self.write_to_csv(path,file_name,columns,data)
        return
    
    def create_csv(self,path:str,file_name:str, columns: List):
        file_path = rf'{path}/{file_name}.csv'
        # Cria o arquivo CSV e escreve o cabeçalho com as colunas
        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
    
    def write_to_csv(self,path:str,file_name:str,data:List):
        file_path = rf'{path}/{file_name}.csv'
        with open(file_path, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data)
    
    def save_file(self,path,endwith,file):
        with open(path + endwith, 'wb') as file_path:
            file_path.write(file)