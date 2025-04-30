from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
from typing import Dict

app = FastAPI()
app.add_middleware(CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active: Dict[WebSocket, str] = {}

    async def connect(self, ws: WebSocket):
        await ws.accept()

    async def register(self, ws: WebSocket, name: str):
        self.active[ws] = name
        await self.broadcast_presence()

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            del self.active[ws]

    async def broadcast_presence(self):
        payload = {"type": "presence", "clients": list(self.active.values())}
        data = json.dumps(payload)
        for client in self.active:
            await client.send_text(data)

    async def send_to(self, recipient_name: str, payload: dict):
        data = json.dumps(payload)
        for ws, name in self.active.items():
            if name == recipient_name:
                await ws.send_text(data)
                return

    async def broadcast_message(self, sender: WebSocket, payload: dict):
        data = json.dumps(payload)
        for client in self.active:
            if client != sender:
                await client.send_text(data)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        join = json.loads(await ws.receive_text())
        if join.get("type") != "join" or "name" not in join:
            await ws.close(code=1003)
            return
        await manager.register(ws, join["name"])
        while True:
            msg = json.loads(await ws.receive_text())
            t = msg.get("type")
            if t == "file_request":
                await manager.send_to(msg["to"], msg)
            elif t == "file_response":
                await manager.send_to(msg["to"], msg)
            elif t == "file_chunk":
                await manager.send_to(msg["to"], msg)
            elif t == "chat":
                await manager.broadcast_message(ws, msg)
    except WebSocketDisconnect:
        manager.disconnect(ws)
        await manager.broadcast_presence()