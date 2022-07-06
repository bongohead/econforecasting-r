from sre_constants import SRE_FLAG_DOTALL
from websocket import create_connection
import json
import re
import time
# Copy the web brower header and input as a dictionary
headers = json.dumps({
    'Host': 'data-cme-v2.tradingview.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q:0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-WebSocket-Version': '13',
    'Origin': 'https://s.tradingview.com',
    'Sec-WebSocket-Extensions': 'permessage-deflate',
    'Sec-WebSocket-Key': '26uQw40st31Ru6SVMpNKFA==',
    'DNT': '1',
    'Connection': 'keep-alive, Upgrade',
    'Sec-Fetch-Dest': 'websocket',
    'Sec-Fetch-Mode': 'websocket',
    'Sec-Fetch-Site': 'same-site',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade': 'websocket'
})


# Launch the connection to the server.
ws = create_connection(
    'wss://data-cme-v2.tradingview.com/socket.io/websocket?from=cmewidgetembed%2F&date=2022_07_05-11_35',
    headers = headers
    )

def parse_req(text):
    return json.loads('[' + re.sub('~m~[0-9]+~m~', ',', text).replace(',', '', 1) + ']')

# Perform the handshake.
send_reqs = [
'~m~48~m~{"m":"set_auth_token","p":["widget_user_token"]}',
'~m~55~m~{"m":"chart_create_session","p":["cs_5YEyuGBbTsVx",""]}',
'~m~63~m~{"m":"quote_create_session","p":["qs_snapshoter_vV4SxPR9RNGL"]}',
'~m~133~m~{"m":"quote_set_fields","p":["qs_snapshoter_vV4SxPR9RNGL","logoid","currency-logoid","base-currency-logoid","pro_name","short_name"]}',
'~m~79~m~{"m":"quote_add_symbols","p":["qs_snapshoter_vV4SxPR9RNGL","CBOT_GBX:ZQN2022"]}',
]
for req in send_reqs:
    ws.send(req)
    
# Printing all the result
while True:
    try:
        result = ws.recv()
        print(result)
        
        if len(result) == 0 or bool(re.search('~h~', result)) == True:
            break
        # Else parse
        res = parse_req(result)
        #print(res)
        #print(res[0])
        #if res['m'] == 'qsd': print('Stage 2')
        if len(res) == 2 and res[1]['m'] == 'quote_completed':
            print('Stage 2')
            sst = [
'~m~58~m~{"m":"switch_timezone","p":["cs_5YEyuGBbTsVx","exchange"]}',
'~m~52~m~{"m":"quote_create_session","p":["qs_fy6SNytzRTFy"]}',
'~m~136~m~{"m":"quote_add_symbols","p":["qs_fy6SNytzRTFy","={\"adjustment\":\"splits\",\"session\":\"regular\",\"symbol\":\"CBOT_GBX:ZQN2022\"}"]}',
'~m~145~m~{"m":"resolve_symbol","p":["cs_5YEyuGBbTsVx","sds_sym_1","={\"adjustment\":\"splits\",\"session\":\"regular\",\"symbol\":\"CBOT_GBX:ZQN2022\"}"]}',
'~m~81~m~{"m":"create_series","p":["cs_5YEyuGBbTsVx","sds_1","s1","sds_sym_1","1",300,""]}',
            ]
            for r in sst:
                time.sleep(.1)
                ws.send(r)
           # ws.send('~m~136~m~{"m":"quote_add_symbols","p":["qs_T8Led6KgIM82","={"adjustment":"splits","session":"regular","symbol":"CBOT_GBX:ZQN2022"}"]}')
           # ws.send('~m~145~m~{"m":"resolve_symbol","p":["cs_0W0s1VlnNeUY","sds_sym_1","={\"adjustment\":\"splits\",\"session\":\"regular\",\"symbol\":\"CBOT_GBX:ZQN2022\"}"]}')
           # ws.send('~m~81~m~{"m":"create_series","p":["cs_0W0s1VlnNeUY","sds_1","s1","sds_sym_1","1",300,""]}')
    except Exception as e:
        print(e)
        break