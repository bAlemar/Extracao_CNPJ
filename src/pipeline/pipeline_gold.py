import pandas as pd
import numpy as np
from src.drivers.filemanager import FileManager


class PipeLineGold:
    """
    Tratamento com pandas do arquivo csv gerado...
    """
    
    def __init__(self) -> None:
        self.filemanager = FileManager()
        self.df = pd.read_csv('database/silver/csv/silver_final.csv',low_memory=False)
        

    def run(self):
        df = self.formating_df(self.df)
        df = self.creating_anos_empresa(df)
        df = self.selected_columns(df)
        df = self.rename_columns(df)
        df = self.clean_dataframe(df)
        self.save_in_gold(df)
        return df

    def save_in_gold(self,df):
        path = 'database/gold/csv'
        self.filemanager.check_path(path)
        df.to_csv(f'{path}/gold.csv',index=False)

    def formating_df(self,df):
        situacao_cadastral_map =  {1:'NULA',
                            2:'ATIVA',
                            3:'SUSPENSA',
                            4:'INAPTA',
                            8:'BAIXADA'}
        motivo_situacao_cadastral_map = {
            0: 'Ausência de Motivo',
            1: 'Extinção Por Encerramento Liquidação Voluntária',
            2: 'Incorporação',
            3: 'Fusão',
            4: 'Cisão Total',
            5: 'Encerramento Da Falência',
            6: 'Encerramento Da Liquidação',
            7: 'Elevação A Matriz',
            8: 'Transpasse',
            9: 'Não Início De Atividade',
            10: 'Extinção Pelo Encerramento Da Liquidação Judicial',
            11: 'Anulação Por Multiciplidade',
            12: 'Anulação Online De Oficio',
            13: 'Omissa Contumaz',
            14: 'Omissa não Localizada',
            15: 'Inexistente De Fato',
            16: 'Anulação Por Vícios',
            17: 'Baixa Iniciada E Ainda não Deferida',
            18: 'Interrupção Temporária Das Atividades',
            19: 'Omisso De Dirpj Até 5 Exercícios',
            20: 'Em Condição De Inaptidão',
            21: 'Pedido De Baixa Indeferida',
            22: 'Restabelecimento Com Certidão Positiva Com Efeito De Negativa',
            23: 'Com Pendência Fiscal',
            24: 'Por Emissão Certidão Negativa',
            25: 'Certidão Positiva Com Efeito De Negativa',
            26: 'Irregularidade De Pagamento',
            27: 'Irregularidade De Recolhimento E Exigibilidade Suspensa',
            28: 'Transferência Filial Condição Matriz',
            29: 'Aguardando Conf. De Dirpj/Dipj',
            30: 'Anr | Aguardando Conf. De Dirpj/Dipj',
            31: 'Extinção Da Filial',
            32: 'Inexistente De Fato – Ade/Cosar',
            33: 'Transferência Do Órgão Local A condição De Filial Do órgão Regional',
            34: 'Anulação De Inscrição Indevida',
            35: 'Empresa Estrangeira Aguardando Documentação',
            36: 'Prática Irregular De Operação De Comercio Exterior',
            37: 'Baixa De Produtor Rural',
            38: 'Baixa Deferida Pela RFB Aguardando Analise Do Convenente',
            39: 'Baixa Deferida Pela RFB E Indeferida Pelo Convenente',
            40: 'Baixa Indeferida Pela RFB E Aguardando Analise Do Convenente',
            41: 'Baixa Indeferida Pela RFB E Deferida Pelo Convenente',
            42: 'Baixa Deferida Pela RFB E Sefin, Aguardando Analise Sefaz',
            43: 'Baixa Deferida Pela RFB, Aguardando Analise Da Sefaz E Indeferida Pela Sefin',
            44: 'Baixa Deferida Pela RFB E Sefaz, Aguardando Analise Sefin',
            45: 'Baixa Deferida Pela RFB, Aguardando Analise Da Sefin E Indeferida Pela Sefaz',
            46: 'Baixa Deferida Pela RFB E Sefaz E Indeferida Pela Sefin',
            47: 'Baixa Deferida Pela RFB E Sefin E Indeferida Pela Sefaz',
            48: 'Baixa Indeferida Pela RFB, Aguardando Analise Sefaz E Deferida Pela Sefin',
            49: 'Baixa Indeferida Pela RFB, Aguardando Analise Da Sefaz E Indeferida Pela Sefin',
            50: 'Baixa Indeferida Pela RFB, Deferida Pela Sefaz E Aguardando Analise Da Sefin',
            51: 'Baixa Indeferida Pela RFB E Sefaz, Aguardando Analise Da Sefin',
            52: 'Baixa Indeferida Pela RFB, Deferida Pela Sefaz E Indeferida Pela Sefin',
            53: 'Baixa Indeferida Pela RFB E Sefaz E Deferida Pela Sefin',
            54: 'Baixa | Tratamento Diferenciado Dado As ME E EPP (Lei Complementar Numero 123/2006)',
            55: 'Deferido Pelo Convenente, Aguardando Analise Da Rfb',
            60: 'Artigo 30, Vi, Da In 748/2007',
            61: 'Indicio Interpos. Fraudulenta',
            62: 'Falta De Pluralidade De Socios',
            63: 'Omissão De Declarações',
            64: 'Localização Desconhecida',
            66: 'Inaptidão',
            67: 'Registro Cancelado',
            70: 'Anulação Por Não Confirmado Ato De Registro Do Mei Na Junta Comercial',
            71: 'Inaptidão (Lei 11.941/2009 Art.54)',
            72: 'Determinação Judicial',
            73: 'Omissão Contumaz',
            74: 'Inconsistência Cadastral',
            75: 'Óbito do MEI | Titular Falecido',
            80: 'Baixa Registrada Na Junta, Indeferida Na Rfb',
            82: 'Suspenso perante a Comissão de Valores Mobiliários - CVM'
        }

        df['CNPJ_FORMATADO'] = df.apply(self.__format_cnpj, axis=1)
        df['Telefone_1_Formatado'] = df.apply(lambda x: self.__format_phone(x['DDD_1'], x['TELEFONE_1']), axis=1)
        df['SITUACAO_CADASTRO'] = df['SITUACAO_CADASTRO'].map(situacao_cadastral_map)
        df['MOTIVO_SITUACAO_CADASTRAL'] = df['MOTIVO_SITUACAO_CADASTRAL'].map(motivo_situacao_cadastral_map)
        #Tratamento de Datas
        df['DATA_INICIO_ATIVIDADE'] = pd.to_datetime(df['DATA_INICIO_ATIVIDADE'].astype(str), format='%Y%m%d').dt.date
        df['DATA_SITUACAO_CADASTRAL'] = df['DATA_SITUACAO_CADASTRAL'].replace(0,pd.NA)
        df['DATA_SITUACAO_CADASTRAL'] = pd.to_datetime(df['DATA_SITUACAO_CADASTRAL'].astype(str), format='%Y%m%d',errors='coerce').dt.date
        return df
        
    def creating_anos_empresa(self,df):
        data_atual = pd.to_datetime('today')
        df['ANOS_DE_EMPRESA'] = df['DATA_INICIO_ATIVIDADE'].apply(lambda x: data_atual.year - x.year - ((data_atual.month, data_atual.day) < (x.month, x.day)))
        return df
    
    def selected_columns(self,df):
        df = df[['CNPJ_FORMATADO','NOME','Telefone_1_Formatado','ANOS_DE_EMPRESA',
                 'DATA_INICIO_ATIVIDADE','DATA_SITUACAO_CADASTRAL',
                 'SITUACAO_CADASTRO',	'MOTIVO_SITUACAO_CADASTRAL']]
        return df

    def rename_columns(self,df):
        df = df.rename(columns={'Telefone_1_Formatado':'Telefone'})
        return df
    
    def clean_dataframe(self,df):
        df = df.replace([np.inf, -np.inf], np.nan)  # Substituir inf e -inf por NaN
        df = df.fillna('Não encontrado')  # Substituir NaNs por 0 (ou outro valor apropriado)
        return df

    def __format_phone(self,ddd, phone):
        try:
            phone_str = str(int(phone))
        except:
            phone_str = str(int(phone.replace('-','').replace(' ','')))
        
        if not phone_str.startswith('3'):
            # Caso1: 5922024, 4841223, 8751598
            if not phone_str.startswith('9') and len(phone_str) == 7:
                phone_str = '9' + phone_str
                formatted_phone = f"({int(ddd)}) {phone_str[:4]}-{phone_str[4:]}"
                return formatted_phone
            
            #Caso2: 44882334, 88190392
            elif not phone_str.startswith('9') and len(phone_str) == 8:
                phone_str = '9' + phone_str
                formatted_phone = f"({int(ddd)}) {phone_str[:5]}-{phone_str[5:]}"
                return formatted_phone
        
        #Caso3: 32736868, 99299869
        if phone_str.startswith('9') or phone_str.startswith('3') and len(phone_str)==8:
            formatted_phone = f"({int(ddd)}) {phone_str[:4]}-{phone_str[4:]}"
            return formatted_phone

        return f"({int(ddd)}){phone_str}" 
    
    def __format_cnpj(self,row):
        cnpj = f"{row['CNPJ']:08d}"
        ordem = f"{row['CNPJ_ORDEDM']:04d}"
        dv = f"{row['CNPJ_DV']:02d}"
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{ordem}-{dv}"
    