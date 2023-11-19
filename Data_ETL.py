import git
import os
import json
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from config import SQL_USERNAME, SQL_PASSWORD, SQL_HOST, SQL_DATABASE,ssh_url,dataset_path,output_path

def clone():
    try:
        git.Repo.clone_from(ssh_url, dataset_path, branch='master')
    except git.GitCommandError as e:
        print(f"Error cloning repository: {e}")

def agg_trn():
    if "agg_tra_state.csv" not in os.listdir(output_path):
        agg_tra_state = dataset_path + "/data/aggregated/transaction/country/india/state"
        agg_tra_state_df = pd.DataFrame(
            columns=["state", "year", "quarter", "transaction_type", "transaction_count", "transaction_amount"])
        for i in os.listdir(agg_tra_state):
            for j in os.listdir(agg_tra_state + "/" + i):
                for k in os.listdir(agg_tra_state + "/" + i + "/" + j):
                    data = open(agg_tra_state + "/" + i + "/" + j + "/" + k, 'r')
                    ld_data = json.load(data)
                    try:
                        for l in ld_data['data']['transactionData']:
                            agg_tra_state_lst = [i, j, k.split(".")[0], l['name'], l['paymentInstruments'][0]['count'],
                                                 l['paymentInstruments'][0]['amount']]
                            agg_tra_state_df.loc[len(agg_tra_state_df)] = agg_tra_state_lst
                    except:
                        pass
        agg_tra_state_df.to_csv(output_path + "agg_tra_state.csv" , index=False)

def agg_usr():
    if "agg_usr_state.csv" not in os.listdir(output_path):
        agg_usr_state = dataset_path + "/data/aggregated/user/country/india/state"
        agg_usr_state_df = pd.DataFrame(
            columns=["state", "year", "quarter", "user_brand", "user_count", "user_percentage"])
        for i in os.listdir(agg_usr_state):
            for j in os.listdir(agg_usr_state + "/" + i):
                for k in os.listdir(agg_usr_state + "/" + i + "/" + j):
                    data = open(agg_usr_state + "/" + i + "/" + j + "/" + k, 'r')
                    ld_data = json.load(data)
                    try:
                        for l in ld_data['data']['usersByDevice']:
                            agg_usr_state_lst = [i, j, k.split(".")[0], l['brand'], l['count'],
                                                 l['percentage']]
                            agg_usr_state_df.loc[len(agg_usr_state_df)] = agg_usr_state_lst
                    except:
                        pass

        agg_usr_state_df.to_csv(output_path + "agg_usr_state.csv" , index=False)

def map_trn():
    if "map_trn_state.csv" not in os.listdir(output_path):
        map_trn_state = dataset_path + "/data/map/transaction/hover/country/india/state"
        map_trn_state_df = pd.DataFrame(
            columns=["state", "year", "quarter", "district_name", "transaction_count", "transaction_amount"])
        for i in os.listdir(map_trn_state):
            for j in os.listdir(map_trn_state + "/" + i):
                for k in os.listdir(map_trn_state + "/" + i + "/" + j):
                    data = open(map_trn_state + "/" + i + "/" + j + "/" + k, 'r')
                    ld_data = json.load(data)
                    try:
                        for l in ld_data['data']['hoverDataList']:
                            map_trn_state_lst = [i, j, k.split(".")[0], l['name'], l['metric'][0]['count'],
                                                 l['metric'][0]['amount']]
                            map_trn_state_df.loc[len(map_trn_state_df)] = map_trn_state_lst
                    except:
                        pass

        map_trn_state_df.to_csv(output_path + "map_trn_state.csv" , index=False)

def map_usr():
    if "map_usr_state.csv" not in os.listdir(output_path):
        map_usr_state = dataset_path + "/data/map/user/hover/country/india/state"
        map_usr_state_df = pd.DataFrame(
            columns=["state", "year", "quarter", "district_name", "registeredUsers","appOpens"])
        for i in os.listdir(map_usr_state):
            for j in os.listdir(map_usr_state + "/" + i):
                for k in os.listdir(map_usr_state + "/" + i + "/" + j):
                    data = open(map_usr_state + "/" + i + "/" + j + "/" + k, 'r')
                    ld_data = json.load(data)
                    try:
                        for l in ld_data['data']['hoverData'].items():
                            map_trn_state_lst = [i, j, k.split(".")[0], l[0],
                                                 l[1]['registeredUsers'],l[1]['appOpens']]
                            map_usr_state_df.loc[len(map_usr_state_df)] = map_trn_state_lst
                    except:
                        pass

        map_usr_state_df.to_csv(output_path + "map_usr_state.csv" , index=False)

def top_trn():
    if "top_trn_state.csv" not in os.listdir(output_path):
        top_trn_state = dataset_path + "/data/top/transaction/country/india/state"
        top_trn_state_df = pd.DataFrame(
            columns=["state", "year", "quarter", "district_pincode", "transaction_count", "transaction_amount"])
        for i in os.listdir(top_trn_state):
            for j in os.listdir(top_trn_state + "/" + i):
                for k in os.listdir(top_trn_state + "/" + i + "/" + j):
                    data = open(top_trn_state + "/" + i + "/" + j + "/" + k, 'r')
                    ld_data = json.load(data)
                    try:
                        for l in ld_data['data']['pincodes']:
                            top_trn_state_lst = [i, j, k.split(".")[0], l['entityName'], l['metric']['count'],
                                                 l['metric']['amount']]
                            top_trn_state_df.loc[len(top_trn_state_df)] = top_trn_state_lst
                    except:
                        pass
        top_trn_state_df.to_csv(output_path + "top_trn_state.csv" , index=False)

def top_usr():
    if "top_usr_state.csv" not in os.listdir(output_path):
        top_usr_state = dataset_path + "/data/top/user/country/india/state"
        top_usr_state_df = pd.DataFrame(
            columns=["state", "year", "quarter", "district_pincode", "registeredUsers"])
        for i in os.listdir(top_usr_state):
            for j in os.listdir(top_usr_state + "/" + i):
                for k in os.listdir(top_usr_state + "/" + i + "/" + j):
                    data = open(top_usr_state + "/" + i + "/" + j + "/" + k, 'r')
                    ld_data = json.load(data)
                    try:
                        for l in ld_data['data']['pincodes']:
                            top_usr_state_lst = [i, j, k.split(".")[0], l['name'], l['registeredUsers']]
                            top_usr_state_df.loc[len(top_usr_state_df)] = top_usr_state_lst
                    except:
                        pass
        top_usr_state_df.to_csv(output_path + "top_usr_state.csv" , index=False)

def data_migration():
    dir_lst=os.listdir(output_path)
    if "agg_tra_state.csv" in dir_lst and "agg_usr_state.csv" in dir_lst and "map_trn_state.csv" in dir_lst and "map_usr_state.csv" in dir_lst and "top_trn_state.csv" in dir_lst and "top_usr_state.csv" in dir_lst:
        df_agg_tran = pd.read_csv(output_path + "agg_tra_state.csv")
        df_agg_user = pd.read_csv(output_path + "agg_usr_state.csv")
        df_map_tran = pd.read_csv(output_path + "map_trn_state.csv")
        df_map_user = pd.read_csv(output_path + "map_usr_state.csv")
        df_top_tran = pd.read_csv(output_path + "top_trn_state.csv")
        df_top_user = pd.read_csv(output_path + "top_usr_state.csv")

        db = mysql.connector.connect(
            host=SQL_HOST,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            auth_plugin="mysql_native_password"
        )

        cursor = db.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")

        cursor.close()
        db.close()

        engine = create_engine(f'mysql+pymysql://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DATABASE}')

        df_agg_tran.to_sql('aggregated_transaction', engine, if_exists='replace', index=False)

        df_agg_user.to_sql('aggregated_user', engine, if_exists='replace', index=False)

        df_map_tran.to_sql('map_transaction', engine, if_exists='replace', index=False)

        df_map_user.to_sql('map_user', engine, if_exists='replace', index=False)

        df_top_tran.to_sql('top_transaction', engine, if_exists='replace', index=False)

        df_top_user.to_sql('top_user', engine, if_exists='replace', index=False)
    else:
        print("CSV Not found! First fetch data and then migrate")
def main():
    clone()
    agg_trn()
    agg_usr()
    map_trn()
    map_usr()
    top_trn()
    top_usr()
    data_migration()

if __name__ == '__main__':
    main()



