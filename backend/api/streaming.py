import asyncio
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

    async def _safe_send(self, ws, message):
        try:
            await asyncio.wait_for(ws.send_json(message), timeout=0.5)
            return None
        except Exception:
            return ws

    async def broadcast(self, symbol, message):
        connections = list(self.active_connections[symbol])
        if not connections:
            return
        results = await asyncio.gather(*[self._safe_send(c, message) for c in connections])
        for dead in results:
            if dead is not None:
                self.disconnect(symbol, dead)

    async def broadcast_all(self, data: dict) -> None:
        """Broadcast to ALL connected WebSocket clients regardless of symbol.
        Used for fill events so any panel receives fills for any symbol."""
        for sym, connections in list(self.active_connections.items()):
            results = await asyncio.gather(*[self._safe_send(ws, data) for ws in list(connections)])
            for dead in results:
                if dead is not None:
                    self.disconnect(sym, dead)


manager = ConnectionManager()
