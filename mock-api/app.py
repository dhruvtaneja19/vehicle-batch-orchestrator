from fastapi import FastAPI

from routers.rc import router as rc_router
from routers.fastag import router as fastag_router
from routers.vrn import router as vrn_router
from routers.valuation import router as valuation_router

app = FastAPI(title="Vehicle Mock APIs")

app.include_router(rc_router)
app.include_router(fastag_router)
app.include_router(vrn_router)
app.include_router(valuation_router)
