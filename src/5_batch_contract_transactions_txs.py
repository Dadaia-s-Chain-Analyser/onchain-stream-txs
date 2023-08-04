import time, os, sys, logging
from datetime import datetime
from utils.etherscan_api import get_txlist_url, get_block_by_time_url, req_chain_scan
from utils.utils import get_kafka_producer

logging.basicConfig(level='INFO')


def get_block_interval(start_date, end_date=None): 
    api_key = os.environ['SCAN_API_KEY']
    start_date, end_date = [int(datetime.timestamp(i)) for i in [start_date, end_date]]
    block_bottom = req_chain_scan(api_key, get_block_by_time_url, dict(timestamp=start_date, closest='after'))
    block_top = req_chain_scan(api_key, get_block_by_time_url, dict(timestamp=end_date, closest='before'))
    return int(block_bottom),int(block_top)


def produce_smart_contract_txs(producer, transaction):
    topic = os.environ['TOPIC_BATCH_TX']
    try: producer.send(topic=topic, value=transaction)
    except: logging.error(f'Error sending message through topic {topic}') ; return False
    else: return True  


def get_transactions(address, startblock, endblock):
    api_key = os.environ['SCAN_API_KEY']
    parms_get_txlist_url = dict(
                address=address, 
                startblock=startblock, 
                endblock=endblock, 
                offset=10000)
    list_transactions = req_chain_scan(api_key, get_txlist_url, parms_get_txlist_url)
    return list_transactions


def handle_smart_contract_txs(transactions):
    interest_cols = ['hash', 'blockNumber','timeStamp','from', 'to', 'isError','value','nonce','gasPrice','gasUsed','functionName', 'input']
    renamed_cols = ['tx_id','block_number','timestamp','from','to','is_error','value','nonce','gasPrice','gasUsed','method', 'input']
    tx_filtered = [{col: transaction[col] for col in interest_cols} for transaction in transactions]
    tx_renamed = [{renamed_cols[col]: tx[interest_cols[col]] for col in range(len(renamed_cols))} for tx in tx_filtered]
    my_split = lambda x: x.split("(")[0]
    for i in tx_renamed:
        i['method'] = my_split(i['method'])
    return tx_renamed


def batch_contract_txs(producer, contract_address, block_bottom, block_top):
    list_gross_tx = get_transactions(contract_address, block_bottom, block_top)
    if list_gross_tx == False: return "COMPLETED"
    list_cleaned_tx = handle_smart_contract_txs(list_gross_tx)
    for tx in list_cleaned_tx:
        time.sleep(0.00001)
        produce_smart_contract_txs(producer, tx)
    next_bottom = max([int(i['block_number']) for i in list_cleaned_tx])
    if (block_top - next_bottom > 100) and (len(list_cleaned_tx) > 1):
        logging.info(f"Blocks to be analysed: {block_top - next_bottom}")
        batch_contract_txs(producer, contract_address, next_bottom, block_top)
    else:
        return "COMPLETED"


if __name__ == '__main__':
    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
    try:
        start_date = parms['start_date']
        contract_address = parms['contract_address']
    except KeyError as e:
        logging.error(f"Parameters start_date and contract_address are required.")
    else:
        end_date = parms.get('end_date')
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
        block_bottom, block_top =  get_block_interval(start_date, end_date=end_date)
        producer = get_kafka_producer()
        res = batch_contract_txs(producer, contract_address, block_bottom, block_top)

