# Gunakan image Python sebagai base
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements.txt dan install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh source code
COPY . .

# Expose port 3000
EXPOSE 3000

# Jalankan aplikasi Flask
CMD ["python", "app.py"]
