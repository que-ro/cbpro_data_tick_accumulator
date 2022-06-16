from datetime import timedelta, timezone

class UtilsDataAccumulator:

    @staticmethod
    def from_product_get_20_asks_and_bids_dict(product_id, public_client):
        """Get dictionnary containing the twenty first asks and bids
        of a currency pair id, at the actual timestamp

        :arg:
            product_id (str): id of the currency pair, ex: 'MANA-USD'
            public_client (PublicClient): Public client from the cbro api

        :return: dictionnary: Example:
            {
                '1_ask_price' : 1.001,
                '1_ask_volume' : 34658.4,
                ...
                '10_ask_price' : 1.028,
                '10_ask_volume' : 456123.1,
                '1_bid_price' : 0.98
                '1_bid_volume' : 250.0
                ...
                '10_bid_price' : 0.89
                '10_bid_volume' : 1654.1
            }
        """

        # Initialization of returned dictionnary
        dict_20_bids_asks = {}

        # Get order book from api
        order_book = public_client.get_product_order_book(product_id, level=2)

        # Get data
        for i in range(20):
            ask_data = order_book['asks'][i]
            bid_data = order_book['bids'][i]
            dict_20_bids_asks[str(i + 1) + '_ask_price'] = ask_data[0]
            dict_20_bids_asks[str(i + 1) + '_ask_volume'] = ask_data[1]
            dict_20_bids_asks[str(i + 1) + '_bid_price'] = bid_data[0]
            dict_20_bids_asks[str(i + 1) + '_bid_volume'] = bid_data[1]

        return dict_20_bids_asks

    @staticmethod
    def from_product_get_tick_data(product_id, public_client, now):
        """Return dictionnary of data tick

        :arg:
            product_id (str): id of the currency pair, ex: 'MANA-USD'
            public_client (PublicClient): Public client from the cbro api
            now (datetime): Datetime with the utc timezone

        :return dictionnary: Example:
            {
                'timestamp': 1652884500,
                'low': 1.101,
                'high': 1.105,
                'open': 1.103,
                'close': 1.103,
                'volume': 12171.09
            }
        """

        # Get start and end date
        end = now
        start = end - timedelta(minutes=5)

        # Get timestamp from api
        timestamp_api = UtilsDataAccumulator.get_api_timestamp(start)

        # Fetch api data
        list_tick_data = public_client.get_product_historic_rates(product_id=product_id, start=start, end=end, granularity=300)

        # Build dictionnary
        if(list_tick_data is None or len(list_tick_data) == 0):
            dict_tick_data = {
                'timestamp': timestamp_api,
                'low': 'NULL',
                'high': 'NULL',
                'open': 'NULL',
                'close': 'NULL',
                'volume': 'NULL'
            }
        else:
            dict_tick_data = {
                'timestamp': timestamp_api,
                'low': list_tick_data[0][1],
                'high': list_tick_data[0][2],
                'open': list_tick_data[0][3],
                'close': list_tick_data[0][4],
                'volume': list_tick_data[0][5]
            }

        return dict_tick_data

    @staticmethod
    def insert_datatick_to_db(currency_pair_id, dict_tick_data, dict_asks_bids, connection):
        #Création de la requête
        query_insert_row_into_datatick = """
        INSERT INTO tick_data (
            currency_pair_id,
            timestamp_as_int,
            low,
            high,
            open,
            close,
            volume,
            1_ask_price,
            1_ask_volume,
            2_ask_price,
            2_ask_volume,
            3_ask_price,
            3_ask_volume,
            4_ask_price,
            4_ask_volume,
            5_ask_price,
            5_ask_volume,
            6_ask_price,
            6_ask_volume,
            7_ask_price,
            7_ask_volume,
            8_ask_price,
            8_ask_volume,
            9_ask_price,
            9_ask_volume,
            10_ask_price,
            10_ask_volume,
            11_ask_price,
            11_ask_volume,
            12_ask_price,
            12_ask_volume,
            13_ask_price,
            13_ask_volume,
            14_ask_price,
            14_ask_volume,
            15_ask_price,
            15_ask_volume,
            16_ask_price,
            16_ask_volume,
            17_ask_price,
            17_ask_volume,
            18_ask_price,
            18_ask_volume,
            19_ask_price,
            19_ask_volume,
            20_ask_price,
            20_ask_volume,
            1_bid_price,
            1_bid_volume,
            2_bid_price,
            2_bid_volume,
            3_bid_price,
            3_bid_volume,
            4_bid_price,
            4_bid_volume,
            5_bid_price,
            5_bid_volume,
            6_bid_price,
            6_bid_volume,
            7_bid_price,
            7_bid_volume,
            8_bid_price,
            8_bid_volume,
            9_bid_price,
            9_bid_volume,
            10_bid_price,
            10_bid_volume,
            11_bid_price,
            11_bid_volume,
            12_bid_price,
            12_bid_volume,
            13_bid_price,
            13_bid_volume,
            14_bid_price,
            14_bid_volume,
            15_bid_price,
            15_bid_volume,
            16_bid_price,
            16_bid_volume,
            17_bid_price,
            17_bid_volume,
            18_bid_price,
            18_bid_volume,
            19_bid_price,
            19_bid_volume,
            20_bid_price,
            20_bid_volume
        )
        VALUES(
            {currency_pair_id},
            {timestamp_as_int},
            {low},
            {high},
            {open},
            {close},
            {volume},
            {v1_ask_price},
            {v1_ask_volume},
            {v2_ask_price},
            {v2_ask_volume},
            {v3_ask_price},
            {v3_ask_volume},
            {v4_ask_price},
            {v4_ask_volume},
            {v5_ask_price},
            {v5_ask_volume},
            {v6_ask_price},
            {v6_ask_volume},
            {v7_ask_price},
            {v7_ask_volume},
            {v8_ask_price},
            {v8_ask_volume},
            {v9_ask_price},
            {v9_ask_volume},
            {v10_ask_price},
            {v10_ask_volume},
            {v11_ask_price},
            {v11_ask_volume},
            {v12_ask_price},
            {v12_ask_volume},
            {v13_ask_price},
            {v13_ask_volume},
            {v14_ask_price},
            {v14_ask_volume},
            {v15_ask_price},
            {v15_ask_volume},
            {v16_ask_price},
            {v16_ask_volume},
            {v17_ask_price},
            {v17_ask_volume},
            {v18_ask_price},
            {v18_ask_volume},
            {v19_ask_price},
            {v19_ask_volume},
            {v20_ask_price},
            {v20_ask_volume},
            {v1_bid_price},
            {v1_bid_volume},
            {v2_bid_price},
            {v2_bid_volume},
            {v3_bid_price},
            {v3_bid_volume},
            {v4_bid_price},
            {v4_bid_volume},
            {v5_bid_price},
            {v5_bid_volume},
            {v6_bid_price},
            {v6_bid_volume},
            {v7_bid_price},
            {v7_bid_volume},
            {v8_bid_price},
            {v8_bid_volume},
            {v9_bid_price},
            {v9_bid_volume},
            {v10_bid_price},
            {v10_bid_volume},
            {v11_bid_price},
            {v11_bid_volume},
            {v12_bid_price},
            {v12_bid_volume},
            {v13_bid_price},
            {v13_bid_volume},
            {v14_bid_price},
            {v14_bid_volume},
            {v15_bid_price},
            {v15_bid_volume},
            {v16_bid_price},
            {v16_bid_volume},
            {v17_bid_price},
            {v17_bid_volume},
            {v18_bid_price},
            {v18_bid_volume},
            {v19_bid_price},
            {v19_bid_volume},
            {v20_bid_price},
            {v20_bid_volume}
        );
        """.format(
            currency_pair_id=currency_pair_id,
            timestamp_as_int=dict_tick_data['timestamp'],
            low=dict_tick_data['low'],
            high=dict_tick_data['high'],
            open=dict_tick_data['open'],
            close=dict_tick_data['close'],
            volume=dict_tick_data['volume'],
            v1_ask_price=dict_asks_bids['1_ask_price'],
            v1_ask_volume=dict_asks_bids['1_ask_volume'],
            v2_ask_price=dict_asks_bids['2_ask_price'],
            v2_ask_volume=dict_asks_bids['2_ask_volume'],
            v3_ask_price=dict_asks_bids['3_ask_price'],
            v3_ask_volume=dict_asks_bids['3_ask_volume'],
            v4_ask_price=dict_asks_bids['4_ask_price'],
            v4_ask_volume=dict_asks_bids['4_ask_volume'],
            v5_ask_price=dict_asks_bids['5_ask_price'],
            v5_ask_volume=dict_asks_bids['5_ask_volume'],
            v6_ask_price=dict_asks_bids['6_ask_price'],
            v6_ask_volume=dict_asks_bids['6_ask_volume'],
            v7_ask_price=dict_asks_bids['7_ask_price'],
            v7_ask_volume=dict_asks_bids['7_ask_volume'],
            v8_ask_price=dict_asks_bids['8_ask_price'],
            v8_ask_volume=dict_asks_bids['8_ask_volume'],
            v9_ask_price=dict_asks_bids['9_ask_price'],
            v9_ask_volume=dict_asks_bids['9_ask_volume'],
            v10_ask_price=dict_asks_bids['10_ask_price'],
            v10_ask_volume=dict_asks_bids['10_ask_volume'],
            v11_ask_price=dict_asks_bids['11_ask_price'],
            v11_ask_volume=dict_asks_bids['11_ask_volume'],
            v12_ask_price=dict_asks_bids['12_ask_price'],
            v12_ask_volume=dict_asks_bids['12_ask_volume'],
            v13_ask_price=dict_asks_bids['13_ask_price'],
            v13_ask_volume=dict_asks_bids['13_ask_volume'],
            v14_ask_price=dict_asks_bids['14_ask_price'],
            v14_ask_volume=dict_asks_bids['14_ask_volume'],
            v15_ask_price=dict_asks_bids['15_ask_price'],
            v15_ask_volume=dict_asks_bids['15_ask_volume'],
            v16_ask_price=dict_asks_bids['16_ask_price'],
            v16_ask_volume=dict_asks_bids['16_ask_volume'],
            v17_ask_price=dict_asks_bids['17_ask_price'],
            v17_ask_volume=dict_asks_bids['17_ask_volume'],
            v18_ask_price=dict_asks_bids['18_ask_price'],
            v18_ask_volume=dict_asks_bids['18_ask_volume'],
            v19_ask_price=dict_asks_bids['19_ask_price'],
            v19_ask_volume=dict_asks_bids['19_ask_volume'],
            v20_ask_price=dict_asks_bids['20_ask_price'],
            v20_ask_volume=dict_asks_bids['20_ask_volume'],
            v1_bid_price=dict_asks_bids['1_bid_price'],
            v1_bid_volume=dict_asks_bids['1_bid_volume'],
            v2_bid_price=dict_asks_bids['2_bid_price'],
            v2_bid_volume=dict_asks_bids['2_bid_volume'],
            v3_bid_price=dict_asks_bids['3_bid_price'],
            v3_bid_volume=dict_asks_bids['3_bid_volume'],
            v4_bid_price=dict_asks_bids['4_bid_price'],
            v4_bid_volume=dict_asks_bids['4_bid_volume'],
            v5_bid_price=dict_asks_bids['5_bid_price'],
            v5_bid_volume=dict_asks_bids['5_bid_volume'],
            v6_bid_price=dict_asks_bids['6_bid_price'],
            v6_bid_volume=dict_asks_bids['6_bid_volume'],
            v7_bid_price=dict_asks_bids['7_bid_price'],
            v7_bid_volume=dict_asks_bids['7_bid_volume'],
            v8_bid_price=dict_asks_bids['8_bid_price'],
            v8_bid_volume=dict_asks_bids['8_bid_volume'],
            v9_bid_price=dict_asks_bids['9_bid_price'],
            v9_bid_volume=dict_asks_bids['9_bid_volume'],
            v10_bid_price=dict_asks_bids['10_bid_price'],
            v10_bid_volume=dict_asks_bids['10_bid_volume'],
            v11_bid_price=dict_asks_bids['11_bid_price'],
            v11_bid_volume=dict_asks_bids['11_bid_volume'],
            v12_bid_price=dict_asks_bids['12_bid_price'],
            v12_bid_volume=dict_asks_bids['12_bid_volume'],
            v13_bid_price=dict_asks_bids['13_bid_price'],
            v13_bid_volume=dict_asks_bids['13_bid_volume'],
            v14_bid_price=dict_asks_bids['14_bid_price'],
            v14_bid_volume=dict_asks_bids['14_bid_volume'],
            v15_bid_price=dict_asks_bids['15_bid_price'],
            v15_bid_volume=dict_asks_bids['15_bid_volume'],
            v16_bid_price=dict_asks_bids['16_bid_price'],
            v16_bid_volume=dict_asks_bids['16_bid_volume'],
            v17_bid_price=dict_asks_bids['17_bid_price'],
            v17_bid_volume=dict_asks_bids['17_bid_volume'],
            v18_bid_price=dict_asks_bids['18_bid_price'],
            v18_bid_volume=dict_asks_bids['18_bid_volume'],
            v19_bid_price=dict_asks_bids['19_bid_price'],
            v19_bid_volume=dict_asks_bids['19_bid_volume'],
            v20_bid_price=dict_asks_bids['20_bid_price'],
            v20_bid_volume=dict_asks_bids['20_bid_volume']
        )

        #Exécution de la requête
        connection.cursor().execute(query_insert_row_into_datatick)
        connection.commit()

    @staticmethod
    def get_api_timestamp(date):
        """Return timestamp of date as the cbpro api would

        :arg date (datetime): datetime of utc zone

        :return timestamp_api: timestamp of date as the api would return
        """

        date = date.replace(second=0, microsecond=0)
        minutes_to_substract_to_5min = date.minute % 5
        minutes_to_add_to_date = 5 - minutes_to_substract_to_5min
        timestamp_api = (date + timedelta(minutes=minutes_to_add_to_date)).replace(
            tzinfo=timezone.utc).timestamp()

        return timestamp_api