import psycopg2
from utils.utils import config, delete_last_line, cleanup

def create_connection():
    conn = None

    try:
        # read connection parameters
        params = config()

        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        cur = conn.cursor()
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print(db_version)
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def menu() -> bool:
    print('### MIGRATE EXCEL TO POSTGREE ###\n')
    print(f'Choose one option bellow:')
    print('---------------------------\n'
          '0 - Exit\n'
          '1 - Connect database\n'
          '--------------------------')

    while True:
        try:
            option = int(input())
        except ValueError:
            print('Invalid option!\n')
            delete_last_line(num_rows=2, wait=2)
        else:
            if (option < 0) or (option > 1):
                print('Menu doe not have this option! Try again...')
                delete_last_line(num_rows=2, wait=2)
            else:
                cleanup()
                break
    #Sair
    if option == 0:
        return False

    #Create connection ----------------------------------------------------------------------
    elif option == 1:
        cleanup()
        conn = create_connection()
        input('\nType something to return to menu... ')
    return True

def main():
    keep = True

    while keep:
        cleanup()
        keep = menu()

if __name__ == "__main__":
    main()