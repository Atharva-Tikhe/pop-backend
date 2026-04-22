from typing import Dict, Set
from fastapi import WebSocket
import json

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, pipeline_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.setdefault(pipeline_id, set()).add(websocket)

    def disconnect(self, pipeline_id: str, websocket: WebSocket):
        self.active_connections[pipeline_id].remove(websocket)
    
    async def send_pipeline_updates(self, pipeline_id: str, message: dict, websocket: WebSocket):
        # if pipeline_id in self.active_connections:
        for socket in self.active_connections[pipeline_id]:
            print(f'sending {message} for {pipeline_id} using {websocket}')
            await websocket.send_json(message)
            # await websocket.send_text("Hello")

#     async def send_to_task(self, pipeline_id: str, message: dict):
#         print(message, self.active_connections)
#         if pipeline_id not in self.active_connections:
#             return
#         for ws in list(self.active_connections[pipeline_id]):
#             print('sending json over ws')
#             await ws.send_json(message)
            
# class ConnectionManager:
#     def __init__(self):
#         self.active_connections: list[WebSocket] = []

#     async def connect(self, websocket: WebSocket):
#         await websocket.accept()
#         self.active_connections.append(websocket)

#     def disconnect(self, websocket: WebSocket):
#         self.active_connections.remove(websocket)

#     async def send_personal_message(self, message: str, websocket: WebSocket):
#         await websocket.send_text(message)

#     async def broadcast(self, message: str):
#         for connection in self.active_connections:
#             await connection.send_text(message)
