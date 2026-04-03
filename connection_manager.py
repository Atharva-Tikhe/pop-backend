from typing import Dict, Set
from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, pipeline_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.setdefault(pipeline_id, set()).add(websocket)

    def disconnect(self, pipeline_id: str, websocket: WebSocket):
        self.active_connections[pipeline_id].remove(websocket)

    async def send_to_task(self, pipeline_id: str, message: dict):
        print(message, self.active_connections)
        if pipeline_id not in self.active_connections:
            return
        for ws in list(self.active_connections[pipeline_id]):
            print(ws)
            await ws.send_json(message)
            

