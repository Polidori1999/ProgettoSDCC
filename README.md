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
- **Containerizzazione e repliche esperimenti**: immagini Docker e Compose; scenari riproducibili `experiments/E1.yml` e `experiments/E2.yml`.

Prerequisiti
------------
Per eseguire il progetto servono:
- **Git** per clonare il repository.
- **Docker** (consigliato ≥ 24) e **Docker Compose v2** (`docker compose ...`).
- **Go** (≥ 1.23) **solo** se vuoi lanciare senza Docker.
  
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

3. Inserimento nodo senza registry
    ```bash
    docker compose run -d --no-deps --name node6 node1 \
   --id=node6:9006 --port=9006 \
   --peers=node2:9002 \
   --services=add \
   --svc-ctrl=/tmp/services.ctrl

--------
Parametri configurabili

=========================================
Rumor-mongering (Failure Detector – B/F/T)

SDCC_FD_B=3 — fanout: quanti peer scelgo a ogni forward del rumor.

SDCC_FD_F=2 — MaxFw: quante volte al massimo un nodo può ri-inoltrare lo stesso rumor.

SDCC_FD_T=3 — TTL: hop budget massimo per il rumor.

Heartbeats

SDCC_HB_LIGHT_EVERY=3s — intervallo tra heartbeat light.

SDCC_HB_FULL_EVERY=9s — intervallo tra heartbeat full.

SDCC_HB_LIGHT_MAX_HINTS=5 — max “peer hints” inseriti negli HB light (piccolo elenco di peer per aiutare il bootstrap/migliorare la connettività).

Failure detector timeouts

SDCC_SUSPECT_TIMEOUT=40s — quanto tempo senza segnali prima di marcare un peer come suspect.

SDCC_DEAD_TIMEOUT=70s — quanto resta in suspect prima di passare a dead (deve essere > SUSPECT).

Anti-entropy (repair push–pull)

SDCC_REPAIR_ENABLED=false — abilita il ciclo periodico di repair.

SDCC_REPAIR_EVERY=30s — frequenza del repair quando abilitato .

Lookup (service discovery)

SDCC_LOOKUP_TTL=3 — hop budget delle richieste lookup .

SDCC_LEARN_FROM_LOOKUP=true — se true, il client aggiorna la registry apprendendo <service → provider> dalla prima risposta che riceve .

SDCC_LEARN_FROM_HB=true — se true, apprende/aggiorna i provider osservando i full HB.

SDCC_CLIENT_DEADLINE=8s — deadline lato client in modalità one-shot (--lookup): tempo massimo d’attesa prima di dire “service not found” .

SDCC_LOOKUP_NEGCACHE_TTL=20s — negative cache TTL: se una lookup “scade” (TTL a 0) senza trovare provider, il nodo memorizza che quel servizio è “assente” e droppa richieste uguali fino a scadenza per evitare traffico inutile.

RPC (servizi demo)

SDCC_RPC_A=18 — parametro A per i servizi aritmetici d’esempio.

SDCC_RPC_B=3 — parametro B per i servizi aritmetici d’esempio.

Logging & bootstrap

SDCC_CLUSTER_LOG_EVERY=10s — ogni quanto stampare il riepilogo cluster (>> Cluster ...). 
SDCC_REGISTRY_MAX_ATTEMPTS=10 — tentativi massimi di bootstrap verso il registry prima di arrendersi.

---------
Esperimenti
1. Per lanciare il primo esperimento per la ricerca di un servizio
    ```bash
    docker compose down -v
   docker compose -f docker-compose.yml -f experiments/E1.yml up -d registry node1 node2 node3 node4 node5
   docker compose -f docker-compose.yml -f experiments/E1.yml run --rm client
2. Per lanciare esperimento No learn from hb
   ```bash
   docker compose down -v   
   docker compose -f docker-compose.yml -f experiments/E2.yml up -d registry node1 node2 node3 node4 node5
   docker compose -f docker-compose.yml -f experiments/E2.yml run --rm client

---------
Comandi utili:
1. Rimozione profili zombie
   ```bash
   sudo aa-remove-unknown        
   sudo systemctl restart docker

