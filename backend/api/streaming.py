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

    async def broadcast_all(self, data: dict) -> None:
        """Broadcast to ALL connected WebSocket clients regardless of symbol.
        Used for fill events so any panel receives fills for any symbol."""
        dead = []
        for sym, connections in list(self.active_connections.items()):
            for ws in connections:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.append((sym, ws))
        for sym, ws in dead:
            self.disconnect(sym, ws)


manager = ConnectionManager()
