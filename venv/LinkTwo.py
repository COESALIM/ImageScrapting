import psycopg2 as psql
import pandas as pd
import string
import math
import re

class LinkTwo:
    def __init__(self, dbname, user,password, port):
        conn = psql.connect(f"dbname ={dbname} user = {user} password = {password} port = {port}")
        cur = conn.cursor()

        q_sellables = '''
        select sellables.description, sellables.barcode from sellables
        where 
        (sellables.description is not null)
        '''
        # Fetch data from sellables table
        try:
            sellables = cur.execute(q_sellables)
            sellables = cur.fetchall()
            print("Fetch data from sellables table")
        except:
            print("Error, Fetching data from sellables table")

        # read csv file
        try:
            df = pd.read_csv('C:/Users/soook/Downloads/1.csv')
            print("Read csv file")
        except:
            print("error read csv file")

        list_sellables = list(sellables)
        sellables_df = pd.DataFrame(list_sellables, columns =['description', 'barcode'])

        list_csv = df.values.tolist()
        print('Dataframe length:', len(df))


        def find_pack(l, n):
            l = l.split(' ')
            for sku in l:
                try:
                    i = int(sku)
                    if isinstance(i, int) and i > 1:
                        l = i
                        # print('l:', l)

                except:
                    None

            n = n.split(' ')
            length = len(n)
            avr = math.floor(length / 2)
            n = n[avr::]
            for descr in n:
                try:

                    j = int(descr)
                    if isinstance(j, int) and j > 1:
                        n = j
                        #print('n:', n)
                except:
                    None
            if n == l:
                return True
            else:
                return False

        def clean_x_pack(lst):

            for tx in lst:
                try:
                    i = int(tx)
                    if isinstance(i, int) and i == 1:
                        lst.remove(tx)
                except:
                    None

            for x in lst:
                if x == 'x':
                    lst.remove('x')
                elif x == 'each':
                    lst.remove('each')
            return lst

        def clean_descritpion(t):
            t = str(t)
            t = t.translate(str.maketrans('', '', string.punctuation)).lower()
            return t

        total_process = 0
        for SKU in sellables:
            pack_list = []
            sku_description = str(SKU[0])
            sku_barcode = str(SKU[1])
            sku_description_split = (clean_descritpion(sku_description)).split(' ')


            while '' in sku_description_split:
                sku_description_split.remove('')
            sku_description_join = ' '.join(sku_description_split)
            i = 0
            ckeck_length = len(sku_description_split)

            for element in list_csv:

                element_url = element[1]
                element_decription = element[2]
                element_decription_two =clean_descritpion(str(element_decription))

                if ('x' in element_decription_two) and ('x' in sku_description_join) and find_pack(sku_description_join, element_decription_two):
                    check = all(item in element_decription_two for item in sku_description_split)
                    if(check == True):
                        print(sku_description)
                        print(element_url)
                        print(element_decription_two)
                        total_process += 1
                        try:
                            update = f'''
                            insert into image_connect_two (description, barcode, image_link)
                            values ('{sku_description}', '{sku_barcode}', '{element_url}')
                            '''
                            cur.execute(update)
                            conn.commit()
                        except:
                            print('Update, Error')

                    else:
                        None

                elif ('x' not in element_decription_two) and ('1' in sku_description_join):
                    description_no_pack = clean_x_pack(sku_description_split)
                    check2 = all(item in element_decription_two for item in description_no_pack)
                    if (check2 == True):
                        print(sku_description)
                        print(element_url)
                        print(element_decription_two)
                        try:
                            update2 = f'''
                            insert into image_connect_two (description, barcode, image_link)
                            values ('{sku_description}', '{sku_barcode}', '{element_url}')
                            '''
                            cur.execute(update2)
                            conn.commit()
                        except:
                            print('Update, Error')

                        total_process += 1

                    else:
                        None
                else:
                    None

        print('total processes:', total_process)

        exit()

        cur.close()
        conn.close()
