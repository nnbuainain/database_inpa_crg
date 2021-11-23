from enum import Enum

FILE_PATH = '../spreadsheets/INPA_AVES Tecidos_Novembro2021.xlsx'

TABLE_DICT = {
    1: ['filter_data_order(sheet)', 'id_ordem, nome_ordem'],
    2: ['filter_data_family(sheet)', 'id_familia, nome_familia, fk_id_ordem'],
    3: ['filter_data_genus(sheet)', 'id_genero, nome_genero, fk_id_familia'],
    4: ['filter_data_species(sheet)', 'id_especie, nome_especie, fk_id_genero'],
    5: ['filter_data_country(sheet)', 'id_pais, nome_pais'],
    6: ['filter_data_state(sheet)', 'id_estado, nome_estado, fk_id_pais'],
    7: ['filter_data_locality(sheet)', 'id_localidade, nome_localidade, latitude, longitude, fk_id_estado']
}

class table(Enum):
	ordem = 1
	familia = 2
	genero = 3
	especie = 4
	pais = 5
	estado = 6
	localidade = 7

