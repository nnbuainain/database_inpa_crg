import time, constant
import utils.util as util
import utils.db as db
from models.data_filter import *

def migrate_data(option: int) -> None:
    table_name = constant.table(option).name.capitalize()

    print(f'\n*********** Migrating table {table_name} ***********\n')

    print('Filtering data ...')

    filtered_data = eval(constant.TABLE_DICT.get(option)[0])
    time.sleep(1)
    print(f'Result filter -> {len(filtered_data)} rows found')

    if filtered_data.empty:
        input('\nIt is not possible to continue migration. Type something to return to menu... ')
    elif input('\nType C to continue migration. Otherwise, type something to return to menu... ').lower() == 'c':
        util.delete_last_line(num_rows=1, wait=1)
        keep = True

        if db.select_register(conn, f'select * from {table_name}') > 0:
            print('Deleting data already inserted...')
            time.sleep(1)

            if db.delete_register(conn, f'delete from {table_name}') < 1:
                input('\nType something to return to menu... ')
                keep = False

        if keep:
            # Converting dataframe to list
            data_list = list(map(tuple, filtered_data.values.tolist()))

            print('Inserting data into PostgreeSQL...')
            table_columns = constant.TABLE_DICT.get(option)[1]
            result = db.insert_register(conn, f'insert into {table_name} ({table_columns}) values ', data_list)
            time.sleep(1)

            if result > 0: print(f'<{result}> rows inserted with success!')

            input('\nType something to return to menu... ')

def menu() -> bool:
    print('\n### MIGRATE EXCEL TO POSTGRE ###\n')
    print(f'Choose one option bellow:')
    print('---------------------------\n'
          '0 - Exit\n'
          '1 - Migrate table ORDER\n'
          '2 - Migrate table FAMILY\n' 
          '3 - Migrate table GENUS\n' 
          '4 - Migrate table SPECIES\n'
          '5 - Migrate table COUNTRY\n'
          '6 - Migrate table STATE\n'
          '7 - Migrate table LOCALITY\n'
          '8 - Migrate table SAMPLE\n'
          '9 - Migrate table RESEARCHER\n'
          '10 - Migrate table AVE\n'
          '11 - Migrate table PESQUISADOR_AVE\n'
          '12 - Migrate table COLETOR\n'
          '13 - Migrate table HERPETO\n'
          '--------------------------')

    while True:
        try:
            option = int(input())
        except ValueError:
            print('Invalid option!\n')
            util.delete_last_line(num_rows=2, wait=2)
        else:
            if (option < 0) or (option > 13):
                print('Menu doe not have this option! Try again...')
                util.delete_last_line(num_rows=2, wait=2)
            else:
                util.cleanup()
                break
    # Sair
    if option == 0:
        return False

    util.cleanup()
    migrate_data(option)

    return True

def main():
    util.cleanup()
    keep = True

    global conn, sheet
    conn = db.create_connection()
    sheet = util.read_file()

    while keep:
        util.cleanup()
        keep = menu()

if __name__ == "__main__":
    main()