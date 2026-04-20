📊 ETL Pipeline – Pregled
Ovaj dokument opisuje ETL (Extract, Transform, Load) proces korištenjem Apache Sparka i PySparka. Podaci se prikupljaju iz dvije izvore: relacijske baze podataka i CSV datoteke, transformiraju i učitavaju u skladište podataka.

🧩 Faze ETL procesa
1. 📥 Ekstrakcija (Extract)
Cilj: Prikupljanje sirovih podataka iz:

✅ Relacijske baze podataka (npr. PostgreSQL, MySQL)
✅ CSV datoteke (lokalna pohrana ili cloud)
Alati/metode:

spark.read.format("jdbc") – za čitanje iz baze
spark.read.csv() – za učitavanje CSV datoteke
2. 🔧 Transformacija (Transform)
Cilj: Priprema podataka za učitavanje:

Čišćenje (null vrijednosti, duplikati)
Promjena tipova podataka
Filtriranje i odabir relevantnih podataka
Spajanje podataka iz baze i CSV-a (ako je potrebno)
Poslovna logika (npr. agregacije, izračuni)
Preimenovanje ili reorganizacija kolona
Alati/metode:

.withColumn(), .select(), .filter(), .join(), .groupBy() i druge PySpark transformacije
3. 📤 Učitavanje (Load)
Cilj: Učitavanje obrađenih podataka u skladište podataka (data warehouse)

Mogućnosti:

Izvoz u Parquet/CSV na objektno spremište (npr. S3)
Direktno pisanje u bazu putem JDBC konekcije
Korištenje specifičnih konektora (npr. Redshift, Snowflake, BigQuery)
Alati/metode:

write.format("jdbc")
write.mode("overwrite").save()
Skladište-specifični konektori
🧱 Arhitektura Pipelinea
Korak	Zadatak	Tehnologija / metoda
1	Čitanje iz relacijske baze	spark.read.format("jdbc")
2	Čitanje CSV datoteke	spark.read.csv()
3	Čišćenje i priprema podataka	PySpark transformacije
4	Spajanje podataka	.join()
5	Poslovna logika i obrada	.withColumn(), .groupBy(), itd.
6	Učitavanje u skladište podataka	write.format("jdbc") ili konektor
✅ Napomene
Svi koraci se mogu pokretati lokalno ili na Sparku u klasteru
CSV datoteka može biti lokalna ili pohranjena na oblaku
Važno je osigurati da je struktura podataka kompatibilna sa skladištem podataka