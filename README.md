# Proiect Big Data - repo cod

## Notebook proiect - proiect.ipynb

## Sistem IDS

Pas 1 - deschidere terminal nou, `cd live-ids/`

Pas 2 - Build imagini docker - `docker compose build`

Pas 3 - Rulare sistem - `docker compose up`

Pas 4 - Oprire sistem - `docker compose down -v`

## Generare date pentru sistem IDS

Notă - script-ul de simulat atacuri nu a fost publicat pe Git, deoarece folosește nmap și alte tool-uri ce trebuie utilizate doar în medii izolate și strict controlate. Pentru testarea clasificării, se utilizează un container linux ce face port-scan pe container-ul server.

Pas 1 - deschidere terminal nou, `cd docker-network/`

Pas 2 - Build imagini docker - `docker compose build`

Pas 3 - Rulare sistem - `docker compose up`

Pas 4 - Oprire sistem - `docker compose down -v`
