# UA Data Hub

## Backend
cd backend && pip install -r requirements.txt && uvicorn main:app --reload

## Frontend
cd frontend && npm install && npm run dev   # http://localhost:5173 (proxy /api -> :8000)

Mets tes images dans frontend/public/img/ (railways.jpg, blocked.jpg, alerts.jpg, weapons.jpg, maps.jpg, losses.jpg).
