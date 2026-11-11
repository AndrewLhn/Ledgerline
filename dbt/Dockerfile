FROM python:3.15-slim

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENTRYPOINT ["dbt"]
CMD ["build"]
