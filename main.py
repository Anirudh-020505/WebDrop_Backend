from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
from typing import Dict

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
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
        print(f"[Join] {name} connected.")
        await self.broadcast_presence()

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            name = self.active[ws]
            del self.active[ws]
            print(f"[Disconnect] {name} disconnected.")

    async def broadcast_presence(self):
        payload = {"type": "presence", "clients": list(self.active.values())}
        data = json.dumps(payload)
        for client in self.active:
            await client.send_text(data)

    async def send_to(self, sender: WebSocket, recipient_name: str, payload: dict):
        sender_name = self.active.get(sender)

        if sender_name == recipient_name:
            await sender.send_text(json.dumps({
                "type": "error",
                "message": "You cannot send a file to yourself."
            }))
            print(f"[Warning] {sender_name} tried to send to themselves.")
            return

        data = json.dumps(payload)
        found = False
        for ws, name in self.active.items():
            if name == recipient_name:
                await ws.send_text(data)
                found = True
                print(f"[Info] Message sent from {sender_name} to {recipient_name}")
                break

        if not found:
            await sender.send_text(json.dumps({
                "type": "error",
                "message": f"User '{recipient_name}' not found or not connected."
            }))
            print(f"[Error] Recipient '{recipient_name}' not found. Active users: {list(self.active.values())}")

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
            msg_type = msg.get("type")

            if msg_type in {"file_request", "file_response", "file_chunk"}:
                await manager.send_to(ws, msg["to"], msg)

            elif msg_type == "chat":
                await manager.broadcast_message(ws, msg)

    except WebSocketDisconnect:
        manager.disconnect(ws)
        await manager.broadcast_presence()
