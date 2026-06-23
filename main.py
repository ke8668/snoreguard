from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import Dict, Set
import json

app = FastAPI()


# ---------- 提供測試頁面（網址：/snore-test） ----------
@app.get("/snore-test")
async def snore_test_page():
    return FileResponse("static/snore-test.html")


# 額外保留 /static/snore-test.html 這種存取方式，不需要的話可以刪除這行，
# 不影響上面 /snore-test 路由運作
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------- WebSocket 中繼：讓兩支手機互相傳遞偵測資料 ----------
class RoomManager:
    """用房間代碼把兩支手機的 WebSocket 連線配對在一起，純轉發，不處理音訊內容。"""

    def __init__(self):
        self.rooms: Dict[str, Set[WebSocket]] = {}

    async def connect(self, room_code: str, websocket: WebSocket):
        await websocket.accept()
        peers = self.rooms.setdefault(room_code, set())
        peers.add(websocket)
        await self._notify_others(room_code, websocket, {"type": "peer_joined"})

    async def disconnect(self, room_code: str, websocket: WebSocket):
        peers = self.rooms.get(room_code)
        if not peers:
            return
        peers.discard(websocket)
        if not peers:
            del self.rooms[room_code]
        else:
            await self._notify_others(room_code, websocket, {"type": "peer_left"})

    async def relay(self, room_code: str, sender: WebSocket, raw_message: str):
        await self._notify_others(room_code, sender, raw_message, already_json=True)

    async def _notify_others(self, room_code, sender, payload, already_json=False):
        peers = self.rooms.get(room_code, set())
        message = payload if already_json else json.dumps(payload)
        dead = []
        for ws in peers:
            if ws is sender:
                continue
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            peers.discard(ws)


manager = RoomManager()


@app.websocket("/ws/{room_code}")
async def websocket_relay(websocket: WebSocket, room_code: str):
    await manager.connect(room_code, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.relay(room_code, websocket, data)
    except WebSocketDisconnect:
        await manager.disconnect(room_code, websocket)


# ---------- 健康檢查（避免根路徑 404，方便確認服務是否活著） ----------
@app.get("/")
async def root():
    return {"status": "ok", "message": "SnoreGuard backend is running"}
