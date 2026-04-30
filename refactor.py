import re

with open('B:/projects/ai-agent-trading-app/backend/api_server.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update imports
content = content.replace('from fastapi import FastAPI, WebSocket, WebSocketDisconnect', 'from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException')
content = content.replace('from strategy import get_latest_signal', 'from strategy import get_latest_signal\nfrom logger import logger')

# 2. Replace LogBroadcaster
old_broadcaster = '''class LogBroadcaster:
    """Watches the log file and broadcasts new lines to WebSocket clients."""

    def __init__(self):
        self.clients: list[WebSocket] = []
        self._running = False
        self._task: asyncio.Task | None = None

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)

    async def broadcast(self, message: str):
        dead = []
        for ws in self.clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def tail_log(self):
        """Tail the log file and broadcast new lines."""
        self._running = True
        try:
            # Start from end of file
            if os.path.exists(LOG_FILE):
                with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(0, 2)  # Seek to end
                    pos = f.tell()
            else:
                pos = 0

            while self._running:
                if os.path.exists(LOG_FILE):
                    with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(pos)
                        new_lines = f.readlines()
                        pos = f.tell()
                    for line in new_lines:
                        line = line.strip()
                        if line:
                            await self.broadcast(json.dumps({
                                "type": "log",
                                "message": line,
                                "timestamp": datetime.now().isoformat(),
                            }))
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    def start(self, loop: asyncio.AbstractEventLoop):
        self._task = loop.create_task(self.tail_log())

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()'''

new_broadcaster = '''class AsyncQueueHandler(logging.Handler):
    def __init__(self, queue: asyncio.Queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        try:
            msg = self.format(record)
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.queue.put_nowait, msg)
            except RuntimeError:
                pass
        except Exception:
            self.handleError(record)

class LogBroadcaster:
    """Broadcasts new lines to WebSocket clients using an Async logging handler."""

    def __init__(self):
        self.clients: list[WebSocket] = []
        self.queue: asyncio.Queue | None = None
        self._task: asyncio.Task | None = None
        self.handler = None

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)

    async def broadcast(self, message: str):
        dead = []
        for ws in self.clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def process_queue(self):
        try:
            while True:
                msg = await self.queue.get()
                payload = json.dumps({
                    "type": "log",
                    "message": msg,
                    "timestamp": datetime.now().isoformat(),
                })
                await self.broadcast(payload)
        except asyncio.CancelledError:
            pass

    def start(self, loop: asyncio.AbstractEventLoop):
        self.queue = asyncio.Queue()
        self.handler = AsyncQueueHandler(self.queue)
        self.handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(self.handler)
        self._task = loop.create_task(self.process_queue())

    def stop(self):
        if self._task:
            self._task.cancel()
        if self.handler:
            logger.removeHandler(self.handler)'''

content = content.replace(old_broadcaster, new_broadcaster)

# 3. Replace error returns with HTTPException
content = re.sub(
    r'return \{\"status\": \"error\", \"message\": (.+?)\}',
    r'raise HTTPException(status_code=500, detail=\1)',
    content
)

with open('B:/projects/ai-agent-trading-app/backend/api_server.py', 'w', encoding='utf-8') as f:
    f.write(content)
