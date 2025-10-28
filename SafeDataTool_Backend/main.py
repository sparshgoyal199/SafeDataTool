from fastapi import FastAPI, Request
from app.api.routes.auth import auth_router
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse,JSONResponse
from fastapi.exceptions import RequestValidationError
#always gives absolute path while importing because python will automatically resolves the path when running through sys.path
#sys.path is just a Python list that tells the interpreter where to look for modules and packages when you use an import statement.
import uvicorn
from app.db.models import User
from app.db.session import init_db
#db.session is acting as the package for init_db
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or specific origin
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
#custom exception handler so that error response format(json or in string) can be customised

class DbException(Exception):
    def __init__(self, message: str):
        self.message = message
        
    def __str__(self):
        return self.message
    
@app.exception_handler(DbException)
async def DbExceptionHandler(request: Request, exc: DbException):
    return JSONResponse(
        status_code=400,
        content={"message":str(exc)}
    )

@app.exception_handler(AuthException)
async def AuthExceptionHandler(request: Request, exc: AuthException):
    return JSONResponse(
        status_code=401,
        content={"message":str(exc)}
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=402,
        content={"message":str(exc)}
    )


app.include_router(auth_router)
def start():
    init_db()
    uvicorn.run('main:app', host='127.0.0.1', port=8000, reload=True)

if __name__ == "__main__":
    start()
    
#main logic of importing 
#when we write import statement it looks for the package and module from the sys.path list (in case of absolute import)