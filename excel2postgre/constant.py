from enum import Enum
import glob

PATH = '../spreadsheets'
AVE = glob.glob(PATH + '/INPA_AVES*.xlsx')[0]

TABLE_DICT = {
    1: ['filter_data_order(sheet)', 'id_ordem, nome_ordem'],
    2: ['filter_data_family(sheet)', 'id_familia, nome_familia, fk_id_ordem'],
    3: ['filter_data_genus(sheet)', 'id_genero, nome_genero, fk_id_familia'],
    4: ['filter_data_species(sheet)', 'id_especie, nome_especie, fk_id_genero'],
    5: ['filter_data_country(sheet)', 'id_pais, nome_pais'],
    6: ['filter_data_state(sheet)', 'id_estado, nome_estado, fk_id_pais'],
    7: ['filter_data_locality(sheet)', 'id_localidade, nome_localidade, latitude, longitude, fk_id_estado'],
    8: ['filter_data_sample(sheet)', 'num_amostra, num_campo, num_voucher, obs, fk_id_localidade, fk_id_especie'],
    9: ['filter_data_researcher(sheet)', 'id_pesq, nome, sobrenome, email, instituicao'],
    10: ['filter_data_ave(sheet)', 'fk_num_amostra, sexo, expedicao, tempo_ate_conservar, metodo_coleta,meio_pres_def,data_preparacao,musculo,sangue,figado,coracao,subespecie']
}

class table(Enum):
    ordem = 1
    familia = 2
    genero = 3
    especie = 4
    pais = 5
    estado = 6
    localidade = 7
    amostra = 8
    pesquisador = 9
    ave = 10