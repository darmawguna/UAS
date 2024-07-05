# Gunakan image Node.js yang sesuai
FROM node:18

# Set working directory di dalam container
WORKDIR /usr/src/app

# Salin package.json dan package-lock.json ke dalam container
COPY package*.json ./

# Instal dependencies
RUN npm install

# Salin semua file ke dalam container
COPY . .

# Expose port yang digunakan oleh aplikasi
EXPOSE 3000

# Jalankan aplikasi
CMD ["npm", "start"]
