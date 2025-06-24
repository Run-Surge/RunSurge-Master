import uvicorn
from app.core.config import settings
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'protos'))


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.MASTER_PORT, reload=True, workers=1)