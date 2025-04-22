from fastapi import FastAPI

from src.database import create_db
from src.api import main_router

app = FastAPI()
app.include_router(main_router)

@app.on_event('startup')
async def startup():
    await create_db()