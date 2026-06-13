# Proiect Big Data - repo cod

## Notebook proiect - proiect.ipynb

## Sistem IDS

Pas 1 - deschidere terminal nou, `cd live-ids/`

Pas 2 - Build imagini docker - `docker compose build`

Pas 3 - Rulare sistem - `docker compose up`

Pas 4 - Oprire sistem - `docker compose down -v`

## Generare date pentru sistem IDS

### Disclaimer privind mediul de testare

Toate operațiunile de captură, analiză și simulare de trafic prezentate în acest proiect sunt efectuate exclusiv într-un mediu izolat și strict controlat, folosind o rețea virtuală Docker dedicată. Script-ul care utilizează `tcpdump` rulează doar în această rețea izolată, având scopul de a captura traficul generat între containerele proiectului, fără a interacționa cu rețele externe sau sisteme reale.

Pentru testarea clasificării traficului, sunt generate scenarii controlate, precum port-scan asupra unui container server aflat în aceeași rețea Docker. Script-urile folosite pentru captura traficului și simularea atacurilor nu sunt publicate în repository, deoarece pot utiliza instrumente precum `nmap` sau alte utilitare care trebuie folosite doar în medii izolate și controlate.

Acest proiect are scop strict educațional și experimental. Orice tehnică prezentată trebuie utilizată doar pe sisteme proprii sau pe infrastructuri pentru care există permisiune explicită.

Pas 1 - deschidere terminal nou, `cd docker-network/`

Pas 2 - Build imagini docker - `docker compose build`

Pas 3 - Rulare sistem - `docker compose up`

Pas 4 - Oprire sistem - `docker compose down -v`
