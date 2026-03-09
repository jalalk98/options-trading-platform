from collections import defaultdict

class ConnectionManager:
    def __init__(self):
        self.active_connections = defaultdict(list)

    async def connect(self, symbol, websocket):
        await websocket.accept()
        self.active_connections[symbol].append(websocket)

    def disconnect(self, symbol, websocket):
        if websocket in self.active_connections[symbol]:
            self.active_connections[symbol].remove(websocket)

    async def broadcast(self, symbol, message):
        dead_connections = []

        for connection in self.active_connections[symbol]:
            try:
                await connection.send_json(message)
            except:
                dead_connections.append(connection)

        for conn in dead_connections:
            self.disconnect(symbol, conn)


manager = ConnectionManager()
