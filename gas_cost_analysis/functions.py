import os
import time,datetime
import pandas as pd
import numpy as np
import logging, requests


methodmap = {'0x1a4d01d2':'remove1','0xecb586a5':'remove3',
             '0x3df02124':'exchange','0x4515cef3':'add',
            '0x6b441a40':'transfer','0x4f12fe97':'apply_new_fee','0x5b5a1467':'commit_new_fee',
            '0x6a1c05ae':'transfer1'}
crv3_token_map = {0:'DAI',1:'USDC',2:'USDT'}
decimal_map = {'DAI':18,'USDC':6,'USDT':6}

def crv3_exchangetx_input(txinput):
    """
    convert Curve 3pool's exchange tx input data to one-dimension from 3-dimensional 
    """
    i_ = float.fromhex(txinput[-256:-192])
    j_ = float.fromhex(txinput[-192:-128])
    i_token_decimal = decimal_map[crv3_token_map[i_]]
    j_token_decimal = decimal_map[crv3_token_map[j_]]
    
    dx_ = float.fromhex(txinput[-128:-64])
    dy_ = float.fromhex(txinput[-64:])
    
    return dx_*10**(-i_token_decimal), dy_*10**(-j_token_decimal)

def crv3_add_input(txinput):
    """
    convert Curve 3pool's add liquidity tx input data to one-dimension 
    """
    i_ = float.fromhex(txinput[-256:-192])
    j_ = float.fromhex(txinput[-192:-128])
    k_ = float.fromhex(txinput[-128:-64])
    
    i_token_decimal = decimal_map[crv3_token_map[0]]
    j_token_decimal = decimal_map[crv3_token_map[1]]
    k_token_decimal = decimal_map[crv3_token_map[2]]
    
    amount_ = i_*10**(-i_token_decimal) + j_*10**(-j_token_decimal) + k_*10**(-k_token_decimal)
    return amount_

def conv_dt_rev(dt_int):
    """
    convert datetime format
    """
    return datetime.datetime(1970,1,1,0,0,0)+datetime.timedelta(seconds=int(dt_int)/1e0)

query_addresses = {'USDC-WETH 500 10':'6d3990ff-faff-4a7c-af1a-85360ccac616',
                  'WBTC-WETH 500 10': 'c62af8d6-f6eb-42a3-96b7-9c4e1760edf0',
                   'DAI-USDC 500 10':'18126de0-bdc5-4b99-9444-094c3e777fbc',
                   'FRAX-USDC 500 10':'782801d3-0733-444a-83f3-9f33f53a2e8f',
                  'USDC-WETH 3000 60': '5c4f1841-4418-46a8-aca4-4987a13f6436',
                  'WBTC-WETH 3000 60': '5ff2b2a2-43cd-4ed7-94db-7b1f58625038',
                  'DAI-USDC 100 1':'6f31682c-847f-422b-8112-c139588ff980',
                  'WBTC-USDC 3000 60':'c6630172-2548-4edb-a062-2a5051c671c7',
                   'WETH-USDT 3000 60':'27c5cc99-dd2a-468c-9999-3b1cd47c9959',
                  'USDC-USDT 100 1':'53c1738e-aead-4aea-91d1-522aec78cc4c',
                  'DAI-USDC 500 10 addliq':'4ffb8299-70e8-4ceb-87b0-7632f3d4e68c'}

def get_df(query):
    """convert json to df"""
    df = pd.DataFrame(query)
    df = df[::-1]
    df = df.set_index('BLOCK_TIMESTAMP')
    df = df.sort_index()
    return df


def get_query(address_key):
    """
    get query from flipside and convert results to df
    """
    query_address_ = query_addresses[address_key]
    query_ = requests.get('https://node-api.flipsidecrypto.com/api/v2/queries/'+query_address_+'/data/latest').json()
    df_ = get_df(query_)
    return df_

def get_curve_exchangetx(txs):
    """
    parse through 3pool's txs and generate df for only exchange txs
    """
    df = pd.DataFrame()
    for tx in txs:
        if tx['txreceipt_status'] == '0':
            continue

        func_ = methodmap[tx['methodId']]
        dt_ = conv_dt_rev(tx['timeStamp'])
        block_ = tx['blockNumber']
        gasprice_ = tx['gasPrice']
        gasused_ = tx['gasUsed']
        timestamp_ = tx['timeStamp']
        
        if func_ == 'exchange':
            amount0_,amount1_ = crv3_exchangetx_input(tx['input'])

            df.loc[dt_,'function'] = func_
            df.loc[dt_,'blocknumber'] = block_
            df.loc[dt_,'gasprice'] = gasprice_
            df.loc[dt_,'gasused'] = gasused_
            df.loc[dt_,'amount'] = amount0_
            df.loc[dt_,'timestamp'] = timestamp_
    return df

def get_curve_addtx(txs):
    """
    parse through 3pool's txs and generate df for only add liquidity txs
    """
    df = pd.DataFrame()
    for tx in txs:
        if tx['txreceipt_status'] == '0':
            continue

        func_ = methodmap[tx['methodId']]
        dt_ = conv_dt_rev(tx['timeStamp'])
        block_ = tx['blockNumber']
        gasprice_ = tx['gasPrice']
        gasused_ = tx['gasUsed']
        timestamp_ = tx['timeStamp']
        
        if func_ == 'add':
            amount0_ = crv3_add_input(tx['input'])

            df.loc[dt_,'function'] = func_
            df.loc[dt_,'blocknumber'] = block_
            df.loc[dt_,'gasprice'] = gasprice_
            df.loc[dt_,'gasused'] = gasused_
            df.loc[dt_,'amount'] = amount0_
            df.loc[dt_,'timestamp'] = timestamp_
    return df