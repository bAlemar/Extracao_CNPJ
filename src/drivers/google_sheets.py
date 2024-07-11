from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
import time 

class GoogleSheetsManager:
    """
    Para planilhas com mesma estrutura de abas e coluna
    """
    def __init__(self) -> None:
        pass
    def planilha_google_sheets(self,cod):
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            r'keys_from_sheets.json', scope)
        client = gspread.authorize(credentials)
        planilha = client.open_by_key(cod)
        return planilha

    def get_values_sheets(self,planilha,sheet_name):
        planilha_sheet = planilha.worksheet(sheet_name)
        values = planilha_sheet.get_all_values()
        return values

    def get_df_from_sheet(self,planilha_cod,sheet_name):
        planilha = self.planilha_google_sheets(planilha_cod)
        values_sheet = self.get_values_sheets(planilha,sheet_name)
        df = pd.DataFrame(values_sheet[1:],columns=values_sheet[0])
        return df

    def df_from_sheets(self,planilha,sheets_names):
        lista_df = []
        for sheet_name in sheets_names:
            values = self.get_values_sheets(planilha,sheet_name)
            df = pd.DataFrame(values[1:],columns=values[0])
            lista_df.append(df)
        return pd.concat(lista_df)

    def create_and_upload_df_to_new_sheet(self, cod, sheet_name, df):
        planilha = self.planilha_google_sheets(cod)
        new_sheet = planilha.add_worksheet(title=sheet_name, rows=df.shape[0]+1, cols=df.shape[1])
        new_sheet.update([df.columns.values.tolist()] + df.values.tolist())


    def run(self,all_cods):
        """
        Todas as planilhas compartilham do mesmo nome de aba
        """
        self.lista_df_final = []
        self.lista_cods = []
        #Evitar numero de requisições por minuto
        
        for cod in all_cods:
            print(cod)
            planilha = self.planilha_google_sheets(cod)
            sheets_name = [sheet.title for sheet in planilha.worksheets()][1:]
            df_tipo = self.df_from_sheets(planilha,sheets_name)
            self.lista_df_final.append(df_tipo)
            self.lista_cods.append(cod)
            time.sleep(60)
        self.df_final = pd.concat(self.lista_df_final)
        return self.df_final
