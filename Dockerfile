FROM python:3.12-slim

RUN apt-get update && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core/ ./core/
COPY streamlit_gestion/ ./streamlit_gestion/
COPY viz_app/ ./viz_app/

EXPOSE 4200
# EXPOSE 8501 8050 4200
