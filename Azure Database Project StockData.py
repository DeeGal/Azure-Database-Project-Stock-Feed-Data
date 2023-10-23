import pandas as pd
import json
from yahoo_fin import stock_info as si
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.eventhub.exceptions import EventHubError
import asyncio
from datetime import datetime

connect_str = 'Endpoint=sb://stockdatafeed.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=VpDd0ccnRH2PidOMl/ybcoBPQbMK8sFCa+AEhGWX4hM='
event_hub = 'stockdatafeed'

test = pd.si.get_quote_data('msft')
test_df = pd.DataFrame([test], columns=test.keys())[['RegularMarketTime', 'RegularMarketPrice', 'MarketCap', 'Exchange', 'AverageAnalysisRating']]
datetime.fromtimestamp(test_df['RegularMarketTime'])
test_df['AverageAnalysting'].str.split(' - ', 1, expands = True)
test_df.apply(lambda row: "$" + str(round(row['MarketCap']/1000000000000, 2)) + 'MM', axis = 1)


def get_data_for_stock(stock):
    stock_pull = si.get_quote_data(stock)
    stock_dataframe = pd.DataFrame([stock_pull], columns =stock_pull.keys())[['RegularMarketTime', 'RegularMarketPrice', 'MarketCap', 'Exchange', 'AverageAnalysisRating']]

    stock_dataframe['RegularMarketTime'] = datetime.fromtimestamp(stock_dataframe['RegularMarketTime'])
    stock_dataframe['RegularMarketTime'] = stock_dataframe['RegularMarketTime'].astype(str)


    stock_dataframe[['AnalystRating', 'AnalystBuySell']] = stock_dataframe['AveragrAnalystRating'].str.split(' - ', 1, expands = True)

    stock_dataframe.drop('AveragrAnalystRating', axis = 1, implace = True)

    stock_dataframe['MarketCapInTrill$$'] = stock_dataframe.apply(lambda row: "$" + str(round(row['MarketCap']/1000000000000, 2)) + 'MM', axis = 1)
    return stock_dataframe.to_dict('record')

datetime.now()
get_data_for_stock('msft')

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a credential that has correct role assigned to access
    # event hubs namespace and the event hub name.
    
    while True:
        await asyncio.sleep(5)
        producer = EventHubProducerClient.from_connection_string(conn_str = connect_str, eventhub_name = event_hub)
        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData(json.dumps(get_data_for_stock('msft'))))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            print('Success Sent To Azure Event Hubs.')

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(run())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("ClosingLoopNow")
    loop.close()
    