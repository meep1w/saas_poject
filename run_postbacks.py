# run_postbacks.py
import uvicorn
from app.web.postbacks import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
