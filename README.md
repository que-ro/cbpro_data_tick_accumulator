# cbpro_data_tick_accumulator
Accumulator of sell/price ticks for crypto currency pair along with volumes of the ask and bid.

## Purpose of the project:
During my lame tentative of finding a model that could predict the outcome of some strategies I thought that an important missing feature would be the volume of asks and bids. If for example there is a large volume for asks, the price will struggle to get past the price of the ask. A deep reinforcement learning could decipher good relations from such data.

The script is executed with the main.py. It will connect to the database you informed, retrieve all the cryptocurrencies pair stored in a table and then proceed to check current informations of those currencies pair through the coinbase pro api. Next the data is stored. The script is made to be used by a scheduler that can fire the script each 5 minutes. 

## How to use it:
Create your own mysql database locally or on the cloud (I hosted mine using kamatera cloud service) <br/>
Create a  currency_pair table and a tick_data table (you can check the structure of the latter in utils_data_accumulator.py) <br/>
Change the credentials for connexion in utils_connect_server_db.py <br/>
Create your own scheduler on your local computer or on a cloud services (I use a raspberryPi) <br/>
Execute the script with the scheduler and voilà! Each 5 minutes you database will receive data of the currencies pair you are interested
