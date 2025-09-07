FROM bitnami/spark:latest

WORKDIR /app

COPY spark_app.py /app/
COPY streamlit_app.py /app/

USER root
RUN pip install --no-cache-dir loguru streamlit

RUN mkdir -p /app/.streamlit

ENV STREAMLIT_HOME=/app/.streamlit

EXPOSE 8501

CMD ["python3", "-m", "streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]