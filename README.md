# **Live Class Week 6 Data Storage Pacmann**
---

- Merupakan Github Repository untuk Case PacFlight dengan menggunakan dbt
- Lanjutan case dari Live Class Week 3 Data Storage
- Untuk detail dokumentasi dari data, bisa cek di link [berikut](https://docs.google.com/document/d/1cVKuIRFDsnwdXqMB6id-9YUVIhzsA1agR36rqr0w_K0/edit?usp=sharing)
- Untuk Docker database Pacflight terdapat di repository https://github.com/ihdarsyd/pacflight-data
- Terdapat static file di `seeds` untuk generate table `dim_dates` dan `dim_times`. Karena di dbt tidak bisa membuat table baru
- Alasan menggunakan seeds adalah agar menyesuaikan dengan workflow yang sudah dilakukan di Live Class Week 3
- Tetapi karena menggunakan `csv`, maka ada beberapa kolom yang tidak sesuai dengan tipe data nya seperti di `dim_times`. Sehingga harus convert menggunakan `::time`

### **How to Run Docker Database**
---

- Karena file `.sql` dari database memiliki storage yang besar, maka membutuhkan Git LFS
- Oleh karena itu, sebelum clone repo database pastikan sudah menginstall Git LFS terlebih dahulu
- Untuk menginstall Git LFS, bisa merujuk pada dokumentasi Github https://docs.github.com/en/repositories/working-with-files/managing-large-files/installing-git-large-file-storage
- Jika sudah menginstall Git LFS, masukkan command berikut untuk clone repository

    ```bash
    git lfs clone git@github.com:ihdarsyd/pacflight-data.git
    ```

- Jika sudah berhasil clone, jalankan docker dengan command `docker compose up -d`

### **How to Run dbt**
---
1. Install dbt, `pip install dbt-postgres`
2. Config koneksi sesuai dengan data source
3. Run `dbt debug` untuk mengecek koneksi dengan data source
4. Run `dbt deps` untuk menginstall dependensi yang dibutuhkan
5. Run `dbt seeds` untuk generate static table dengan menggunakan csv file 
6. Run `dbt run` untuk menjalankan proses transformasi dengan menggunakan dbt

### **Tools**
---

- dbt
- Postgres
- DBeaver
- Docker