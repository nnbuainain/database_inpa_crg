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
    7: ['filter_data_locality(sheet)', 'id_localidade, nome_localidade, latitude, longitude, coordenadas_obs, fk_id_estado'],
    8: ['filter_data_sample(sheet)', 'num_amostra, num_campo, num_voucher, municipio, obs, identificacao_especie_obs, colecao, fk_id_ordem, fk_id_familia, fk_id_genero, fk_id_especie, fk_id_pais,fk_id_estado, fk_id_localidade'],
    9: ['filter_data_researcher(sheet)', 'id_pesq, nome_pesquisador, sobrenome_pesquisador, email_pesquisador, instituicao_pesquisador'],
    10: ['filter_data_ave(sheet)', 'fk_num_amostra, sexo, sexo_obs, expedicao, tempo_ate_conservar, metodo_coleta, meio_pres_def, data_preparacao, data_preparacao_obs, musculo, sangue, figado, coracao, subespecie, num_preparador'],
    11: ['filter_data_collect_voucher(sheet)', 'id_pesq_ave, fk_id_pesq,fk_num_amostra'],
    12: ['filter_data_collect(sheet)', 'id_coleta, data_coleta, data_coleta_obs, fk_num_amostra, fk_id_pesq']
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
    coleta_voucher = 11
    coleta = 12