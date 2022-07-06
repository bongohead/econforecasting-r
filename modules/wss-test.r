library(websocket)
library(dplyr)

ws <- WebSocket$new(
	url = "wss://data-cme-v2.tradingview.com/socket.io/websocket?from=cmewidgetembed%2F&date=2022_07_05-11_35",
	headers = c(
		'Host' = 'data-cme-v2.tradingview.com',
		'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
		'Accept' = '*/*',
		'Accept-Language' = 'en-US,en;q=0.5',
		'Accept-Encoding' = 'gzip, deflate, br',
		'Sec-WebSocket-Version' = '13',
		'Origin' = 'https://s.tradingview.com',
		'Sec-WebSocket-Extensions' = 'permessage-deflate',
		'Sec-WebSocket-Key' = '4WM8R5vMBKxCCDayxiEX1g==',
		'DNT' = '1',
		'Connection' = 'keep-alive, Upgrade',
		'Sec-Fetch-Dest' = 'websocket',
		'Sec-Fetch-Mode' = 'websocket',
		'Sec-Fetch-Site' = 'same-site',
		'Pragma' = 'no-cache',
		'Cache-Control' = 'no-cache',
		'Upgrade' = 'websocket'
	),
	autoConnect = FALSE
	)
ws$onOpen(function(event) {
	cat("Connection opened\n")
	c(
		'~m~48~m~{"m":"set_auth_token","p":["widget_user_token"]}',
		'~m~55~m~{"m":"chart_create_session","p":["cs_Wii0cSF2zOxe",""]}',
		'~m~63~m~{"m":"quote_create_session","p":["qs_snapshoter_U2On2F67mJLU"]}',
		'~m~133~m~{"m":"quote_set_fields","p":["qs_snapshoter_U2On2F67mJLU","logoid","currency-logoid","base-currency-logoid","pro_name","short_name"]}',
		'~m~79~m~{"m":"quote_add_symbols","p":["qs_snapshoter_U2On2F67mJLU","CBOT_GBX:ZQQ2021"]}'
	) %>%
		purrr::walk(., function(x) ws$send(x))
})
ws$onMessage(function(event) {
	test <<- event
	cat("Client got msg: ", event$data, "\n")
})
ws$onClose(function(event) {
	cat("Client disconnected with code ", event$code,
			" and reason ", event$reason, "\n", sep = "")
})
ws$onError(function(event) {
	cat("Client failed to connect: ", event$message, "\n")
})
ws$connect()




string = '~m~1578989248~m~~m~157~m~{"m":"qsd","p":["qs_snapshoter_lLyG7Zq6XmYQ",{"n":"CBOT_GBX:ZQQ2021","s":"ok","v":{"short_name":"ZQQ2021","pro_name":"CBOT:ZQQ2021","logoid":"country/US"}}]}~m~77~m~{"m":"quote_completed","p":["qs_snapshoter_lLyG7Zq6XmYQ","CBOT_GBX:ZQQ2021"]}'

stringr::str_extract_all(string, '~m~[0-9]+~m~')
stringr::str_detect('~m~4~m~~h~1', 'h~[0-9]+')

