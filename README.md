Progetto di Service Discovery e Failure Detection basato su Protocollo Gossip
============================================================================
Questo progetto è un'implementazione di un sistema di **service discovery** e **failure detection** decentralizzato per il corso di **Sistemi Distribuiti e Cloud Computing**. I nodi si organizzano tramite **Gossip (push–pull)**, rilevano crash/leave con **rumor spreading (B/F/TTL)** e riparano gap con **anti-entropy**. Il tutto è containerizzato con **Docker** e orchestrato con **Docker Compose**.

L'implementazione è realizzata in **Go (Golang)** ed è orchestrata tramite **Docker** e **Docker Compose** per una facile gestione e deployment.

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

   ```bash
   git clone https://github.com/Polidori1999/ProgettoSDCC.git

2. Build & up (registry + 5 nodi + un client dimostrativo):  
   ```bash
   docker compose up --build -d

3. Log di un nodo:
   ```bash
   docker compose logs -f node1  
4. Stop di un nodo:
   ```bash
   docker compose stop node2  
5. Crash di un nodo:  
   ```bash
   docker kill --signal SIGKILL node2
6. Riavviare un nodo:
   ```bash
   docker compose start node2
7. Stop e cleanup:
   ```bash
   docker compose down --remove-orphans

   ------------
⚙️ Flag CLI principali (binario gossip-node)

--id (obbligatorio): identificatore host:port del nodo (es. node1:9001)

--port (default 8000): porta UDP locale su cui ascoltare gossip/HB

--registry: host:port del registry (opzionale)

--peers: lista iniziale di peer, CSV (host:port,host:port)

--services: lista servizi esposti localmente, CSV (sum,sub,...)

--svc-ctrl: path file di controllo servizi (es. /tmp/services.ctrl)

--lookup: se valorizzato, il nodo esegue lookup + invocazione del servizio e termina

I servizi integrati sono: sum, sub, mul, div.
-----------------
1. Avviare un client per lookup
   ```bash
    docker compose run --rm --no-deps --name client2 \
   client --id=client2:9010 --port=9010 --registry=registry:9000 --lookup=div

2. Inserimento di un nodo passando per il registry
   ```bash
    docker compose run -d --no-deps --name node6 node1 \
   --id=node6:9006 --port=9006 \
   --registry=registry:9000\
   --services=add \
   --svc-ctrl=/tmp/services.ctrl

Inserimento nodo senza registry
 docker compose run -d --no-deps --name node6 node1 \
   --id=node6:9006 --port=9006 \
   --peers=node2:9002 \
   --services=add \
   --svc-ctrl=/tmp/services.ctrl


Parametri configurabili
=== Rumor-mongering (FD) ===
SDCC_FD_B=3  
SDCC_FD_F=2
SDCC_FD_T=3

=== Heartbeats ===
SDCC_HB_LIGHT_EVERY=3s
SDCC_HB_FULL_EVERY=9s

=== Failure detector timeouts ===
SDCC_SUSPECT_TIMEOUT=20s
SDCC_DEAD_TIMEOUT=30s

=== Repair push–pull ===
SDCC_REPAIR_ENABLED=false
SDCC_REPAIR_EVERY=30s

=== Lookup ===
SDCC_LOOKUP_TTL=3
SDCC_LEARN_FROM_LOOKUP=true
SDCC_LEARN_FROM_HB=true

=== RPC (parametri dei servizi) ===
SDCC_RPC_A=18
SDCC_RPC_B=3

