import socketio

sio = socketio.Client()

@sio.event
def connect():
    print('connection established')

@sio.event
def my_message(data):
    print('message received with ', data)
    sio.emit('my response', {'response': 'my response'})

@sio.event
def disconnect():
    print('disconnected from server')

sio.connect('https://data-cme-v2.tradingview.com/socket.io/websocket?from=cmewidgetembed/&date=2022_07_05-11_35')
sio.wait()
print('my sid is', sio)
