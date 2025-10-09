package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	// --- Parametri del rumor per il Failure Detector ---
	FDB int // fanout B: a quanti peer inoltro ogni rumor
	FDF int // MaxFw F: massimo numero di forward per rumor
	FDT int // TTL T: numero di hop massimi prima di fermare la propagazione

	// --- Heartbeats (gossip periodico) ---
	HBLightEvery time.Duration // intervallo per heartbeat "leggero"
	HBFullEvery  time.Duration // intervallo per heartbeat "full"

	// --- Timeouts del Failure Detector ---
	SuspectTimeout time.Duration // dopo quanto tempo marchio "suspect"
	DeadTimeout    time.Duration // dopo quanto tempo marchio "dead" (deve essere > suspect)

	// --- Anti-entropy (repair push–pull) ---
	RepairEnabled bool          // se true, abilito il ciclo di repair
	RepairEvery   time.Duration // frequenza del repair

	// --- Lookup ---
	LookupTTL         int           // TTL delle richieste di lookup
	LearnFromLookup   bool          // se true, imparo i provider osservando le risposte
	LearnFromHB       bool          // se true, imparo i provider dai full-HB
	ClientDeadline    time.Duration // deadline del client one-shot (lookup)
	LookupNegCacheTTL time.Duration

	// --- Parametri RPC di esempio (servizi aritmetici) ---
	RPCA float64
	RPCB float64

	ClusterLogEvery time.Duration // quanto spesso loggare lo stato cluster

	RegistryMaxAttempts int // tentativi di bootstrap dal registry

	HBLightMaxHints int // <-- NUOVO: max peer hints nei light-HB

}

// leggo un intero da ENV, altrimenti ritorno il default.

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return i
		}
	}
	return def
}

// leggo un booleano da ENV con una mappa "furba" di stringhe.
func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return def
}

// parso la duration
func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return def
}

// leggo un float64 da ENV, altrimenti default.
func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	return def
}

var loadOnce sync.Once

// carico .env (se esiste) la prima volta, poi costruisco il mio Config

func Load() Config {
	loadOnce.Do(func() { _ = loadDotEnv(".env") })

	return Config{
		// --- Default: scelgo numeri prudenti e poi li sovrascrivo da ENV ---
		FDB: envInt("SDCC_FD_B", 3),
		FDF: envInt("SDCC_FD_F", 2),
		FDT: envInt("SDCC_FD_T", 3),

		HBLightEvery: envDuration("SDCC_HB_LIGHT_EVERY", 1*time.Second),
		HBFullEvery:  envDuration("SDCC_HB_FULL_EVERY", 5*time.Second),

		SuspectTimeout: envDuration("SDCC_SUSPECT_TIMEOUT", 15*time.Second),
		DeadTimeout:    envDuration("SDCC_DEAD_TIMEOUT", 22*time.Second),

		RepairEnabled: envBool("SDCC_REPAIR_ENABLED", false),
		RepairEvery:   envDuration("SDCC_REPAIR_EVERY", 30*time.Second),

		LookupTTL:         envInt("SDCC_LOOKUP_TTL", 3),
		LearnFromLookup:   envBool("SDCC_LEARN_FROM_LOOKUP", true),
		LearnFromHB:       envBool("SDCC_LEARN_FROM_HB", true),
		ClientDeadline:    envDuration("SDCC_CLIENT_DEADLINE", 8*time.Second),
		LookupNegCacheTTL: envDuration("SDCC_LOOKUP_NEGCACHE_TTL", 30*time.Second),

		RPCA: envFloat("SDCC_RPC_A", 18),
		RPCB: envFloat("SDCC_RPC_B", 3),

		ClusterLogEvery: envDuration("SDCC_CLUSTER_LOG_EVERY", 10*time.Second),

		RegistryMaxAttempts: envInt("SDCC_REGISTRY_MAX_ATTEMPTS", 10),

		HBLightMaxHints: envInt("SDCC_HB_LIGHT_MAX_HINTS", 5), //
	}
}

// parser minimale di file .env.
// - ignoro righe vuote e commenti (#)
// - supporto opzionale "export KEY=VAL"
// - splitto solo sulla prima '='
// - rimuovo doppi apici o apici singoli esterni
func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {

		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export"))
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.TrimSpace(kv[0])

		v := strings.Trim(strings.TrimSpace(kv[1]), `"'`)
		_ = os.Setenv(k, v)
	}
	return nil
}
