from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from app.api.routes.auth import auth_router
from app.api.routes.datasets import datasets_router
from app.api.routes.pipeline import pipeline_router
from app.config import get_settings
from app.db.session import init_db

app = FastAPI()
settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#That app instance is responsible for:
    #Storing all your routes (@app.get, @app.post, etc.)
    #Storing your middleware
    #Storing your custom/overwrite exception handlers
class AuthException(Exception):
    def __init__(self, message: str):
        self.message = message
        
    def __str__(self):
        return self.message
    
class TokenException(Exception):
    def __init__(self, message: str):
        self.message = message
        
    def __str__(self):
        return self.message
#custom exception handler so that error response format(json or in string) can be customised

@app.exception_handler(TokenException)
async def TokenExceptionHandler(request: Request, exc: TokenException):
    return JSONResponse(
        status_code=401,
        content={"message":str(exc)}
    )

@app.exception_handler(AuthException)
async def AuthExceptionHandler(request: Request, exc: AuthException):
    return JSONResponse(
        status_code=403,
        content={"message":str(exc)}
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    errors = exc.errors()
    err_message = errors[0]['msg']
    # messages = [f"{err['loc'][-1]}: {err['msg']}" for err in errors]
    return JSONResponse(
        status_code=402,
        content={"message":err_message}
    )


@app.on_event("startup")
def on_startup() -> None:
    init_db()


app.include_router(auth_router)
app.include_router(datasets_router)
app.include_router(pipeline_router)

def start():
    init_db()
    uvicorn.run('main:app', host='127.0.0.1', port=8000, reload=True)

if __name__ == "__main__":
    start()
    
#main logic of importing 
#when we write import statement it looks for the package and module from the sys.path list (in case of absolute import)