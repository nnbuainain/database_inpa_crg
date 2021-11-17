from utils.utils import delete_last_line, cleanup, read_file
from utils.db import create_connection, select_register, delete_register, insert_register
from models.data_filter import filter_data_order, filter_data_family
import time

def migrate_order() -> None:
    print('\n*********** Migrate Order ***********\n')
    print('Reading spreadsheet file...')
    data = read_file()
    time.sleep(1)

    print('Filtering data ...')
    data = filter_data_order(data)
    time.sleep(1)
    print(f'Result filter -> {len(data)} rows found')

    if data.empty:
        input('\nIt is not possible to continue migration. Type something to return to menu... ')
    elif input('\nType C to continue migration. Otherwise, type something to return to menu... ').lower() == 'c':
        delete_last_line(num_rows=1, wait=1)
        keep = True

        if select_register(conn, 'select * from ordem') > 0:
            print('Deleting data already inserted...')
            time.sleep(1)

            if delete_register(conn, 'delete from ordem') < 1:
                input('\nType something to return to menu... ')
                keep = False

        if keep:
            # Converting dataframe to list
            data_list = list(map(tuple, data.values.tolist()))

            print('Inserting data into PostgreeSQL...')
            result = insert_register(conn, 'insert into ordem (id_ordem, nome_ordem) values ', data_list)
            time.sleep(1)

            if result > 0: print(f'<{result}> rows inserted with success!')

            input('\nType something to return to menu... ')

def migrate_family() -> None:
    pass

def menu() -> bool:
    print('\n### MIGRATE EXCEL TO POSTGREE ###\n')
    print(f'Choose one option bellow:')
    print('---------------------------\n'
          '0 - Exit\n'
          '1 - Migrate table ORDER\n'
          '2 - Migrate table FAMILY\n' 
          '--------------------------')

    while True:
        try:
            option = int(input())
        except ValueError:
            print('Invalid option!\n')
            delete_last_line(num_rows=2, wait=2)
        else:
            if (option < 0) or (option > 2):
                print('Menu doe not have this option! Try again...')
                delete_last_line(num_rows=2, wait=2)
            else:
                cleanup()
                break
    #Sair
    if option == 0:
        return False

    # Order ---------------------------------------------------------------------------------
    elif option == 1:
        cleanup()
        migrate_order()

    # Family ---------------------------------------------------------------------------------
    elif option == 2:
        cleanup()
        migrate_family()

    return True

def main():
    cleanup()
    keep = True
    global conn
    conn = create_connection()

    while keep:
        cleanup()
        keep = menu()

if __name__ == "__main__":
    main()