library(websocket)

ws <- WebSocket$new("wss://demo.piesocket.com/v3/channel_1?api_key=VCXCEuvhGcBDP7XhiJJUDvR1e1D3eiVjgZ9VRiaV&notify_self/", autoConnect = FALSE)
ws$onOpen(function(event) {
	cat("Connection opened\n")
})
ws$onMessage(function(event) {
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

