import pandas as pd
from src.drivers.filemanager import FileManager
from src.drivers.google_sheets import GoogleSheetsManager
from typing import List,AnyStr
class PipelineGold_2:
    """
    Retirando os telefones Duplicados que o cliente já possui:
    Google Maps
    Insta
    E Enviando para Sheets
    """
    def __init__(self) -> None:
        self.df = pd.read_csv('database/gold/csv/gold.csv')
        self.google_sheets = GoogleSheetsManager()
        self.filemanager = FileManager()
        pass

    def run(self,cod):
        self.loading_dataset_into_sheets('ATIVA','ATIVA',cod)
        self.loading_dataset_into_sheets(['BAIXADA','INAPTA','NULA','SUSPENSA'],'INATIVO',cod)

    def loading_dataset_into_sheets(self,tipo,sheet_name,cod):
        try:
            if isinstance(tipo,str):
                df_ativo =  self.df[self.df['SITUACAO_CADASTRO'] == tipo]
            elif isinstance(tipo,list):
                df_ativo = self.df[self.df['SITUACAO_CADASTRO'].isin(tipo)]
            self.google_sheets.create_and_upload_df_to_new_sheet(cod,sheet_name,df_ativo)
        except Exception as error:
            message = f""""
                    Error na criação da Sheets do tipo {tipo}
                    Error:
                    {error}
                    """
            print(message)
    