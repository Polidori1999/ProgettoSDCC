Progetto di Service Discovery e Failure Detection basato su Protocollo Gossip
============================================================================
Questo progetto è un'implementazione di un sistema di **service discovery** e **failure detection** decentralizzato per il corso di **Sistemi Distribuiti e Cloud Computing**. Il sistema utilizza un protocollo **Gossip (push–pull)** per permettere a un cluster di nodi di mantenere una visione condivisa e aggiornata dei membri attivi, rilevando guasti e uscite in modo resiliente.

L'implementazione è realizzata in **Go (Golang)**, utilizza **gRPC** per la comunicazione tra i nodi ed è orchestrata tramite **Docker** e **Docker Compose** per una facile gestione e deployment.

Caratteristiche Principali
--------------------------
- **Bootstrap flessibile**: join tramite **registry** (per ottenere il primo peer) e successiva auto-organizzazione via gossip, oppure **bootstrap diretto** con `--peers`.
- **Gossip ibrido**: **rumor spreading** con parametri **B/F/TTL** + **anti-entropy push–pull**; scambio di **digest** per riparare gap e ridurre traffico.
- **Heartbeat con digest smart**: heartbeat periodici con payload compatto (Protobuf) che includono riassunti di stato per accelerare convergenza.
- **Service Discovery a runtime**: aggiunta/rimozione servizi tramite file/pipe di controllo (`--svc-ctrl`, es. `/tmp/services.ctrl`) con propagazione gossip.
- **Failure Detection configurabile**: timeouts/soglie regolabili; rilevazione di **crash** e **leave** con disseminazione rapida dello stato.
- **Resilienza e self-healing**: tolleranza a crash dei nodi (inclusi i seed), reintegro automatico, convergenza in presenza di churn.
- **gRPC + Protobuf**: interfaccia binaria stabile e messaggi compatti; compatibile con più linguaggi.
- **Containerizzazione e repliche esperimenti**: immagini Docker e Compose; scenari riproducibili `experiments/E1.yml` e `experiments/E2.yml`.

Prerequisiti
------------
Per eseguire il progetto servono:
- **Git** per clonare il repository.
- **Docker** (consigliato ≥ 24) e **Docker Compose v2** (`docker compose ...`).
- **Go** (≥ 1.23) **solo** se vuoi lanciare senza Docker.
- 
Come Avviare il Progetto (con Docker Compose)
---------------------------------------------
1. Clona il repository:
   
