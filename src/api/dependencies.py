from typing import Annotated

from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
