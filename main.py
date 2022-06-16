import pandas as pd
from utils_connect_server_db import UtilsConnexion
from utils_data_accumulator import UtilsDataAccumulator
import cbpro
from datetime import datetime

#Opening connections
ssh_tunnel = UtilsConnexion.get_ssh_tunnel()
connection = UtilsConnexion.get_mysql_connected(ssh_tunnel)

#Get currency pair stored in database
df_currency_pair = pd.read_sql_query('SELECT * FROM currency_pair;', connection).set_index('currency_pair_id', drop=True)

#Init cbpro public client
public_client = cbpro.PublicClient()

#Loop through stored currency pair
for idx, currency_pair in df_currency_pair.iterrows():

    #Init needed variables
    currency_pair_id = idx
    currency_pair_name = currency_pair['name']
    datetime_now = datetime.utcnow()

    #Researchs asks and bids
    dict_20_asks_bids = UtilsDataAccumulator.from_product_get_20_asks_and_bids_dict(currency_pair_name, public_client)

    #Research tick data
    dict_tick_data = UtilsDataAccumulator.from_product_get_tick_data(currency_pair_name, public_client, datetime_now)

    #Insert row into database
    UtilsDataAccumulator.insert_datatick_to_db(currency_pair_id, dict_tick_data, dict_20_asks_bids, connection)


#Closing connections
UtilsConnexion.mysql_disconnect(connection)
UtilsConnexion.close_ssh_tunnel(ssh_tunnel)