

import argparse


def main(params):

    print(params)
    user = params.user
    host = params.host
    port = params.port
    db = params.db
    table_name  = params.table_name
    url = params.url
    password = params.password  

    csv_name = 'output.csv'  

    #os.system(f'wget {url} -O {csv_name}')
   

if __name__ == '__main__' : 
    parser = argparse.ArgumentParser(description='ingest CSV data to Postgres')

    # user
    # password
    # host
    # port 
    # database name
    # table name
    # url of the csv

    parser.add_argument('user',help='usename for postgres')
    parser.add_argument('password',help='password for postgres')
    parser.add_argument('host',help='host for postgres')
    parser.add_argument('port',help='port for postgres')
    parser.add_argument('db',help='database name for postgres')
    parser.add_argument('table_name',help='Table name for postgres database where we will write the data')
    parser.add_argument('url',help='url of the csv file')


    args = parser.parse_args()

    main(args)



    


