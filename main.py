from LinkTwo import LinkTwo



user = input('Enter Postgres User name: ')
dbname = input('Enter database name: ')
password = input('Enter Password: ')
port = 5432

LinkTwo(dbname, user, password, port)
