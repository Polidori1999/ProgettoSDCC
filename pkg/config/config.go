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
	// Rumor GOSSIP (Failure Detector)
	FDB int // fanout B
	FDF int // MaxFw F
	FDT int // TTL T (hop)

	// Heartbeats
	HBLightEvery time.Duration
	HBFullEvery  time.Duration

	// Failure detector timeouts
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration

	// Repair push–pull
	RepairEnabled bool
	RepairEvery   time.Duration

	// Lookup
	LookupMode      string // "ttl" | "gossip"
	LookupTTL       int
	LearnFromLookup bool

	// RPC (parametri servizi)
	RPCA float64
	RPCB float64
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}
func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return i
		}
	}
	return def
}
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
func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return def
}
func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	return def
}

// ===== .env loader (senza dipendenze esterne) =====
var loadOnce sync.Once

func Load() Config {
	loadOnce.Do(func() { _ = loadDotEnv(".env") })

	return Config{
		// DEFAULT = quelli che usavi in codice
		FDB: envInt("SDCC_FD_B", 3),
		FDF: envInt("SDCC_FD_F", 2),
		FDT: envInt("SDCC_FD_T", 3),

		HBLightEvery: envDuration("SDCC_HB_LIGHT_EVERY", 1*time.Second),
		HBFullEvery:  envDuration("SDCC_HB_FULL_EVERY", 5*time.Second),

		SuspectTimeout: envDuration("SDCC_SUSPECT_TIMEOUT", 15*time.Second),
		DeadTimeout:    envDuration("SDCC_DEAD_TIMEOUT", 22*time.Second),

		RepairEnabled:   envBool("SDCC_REPAIR_ENABLED", false),
		RepairEvery:     envDuration("SDCC_REPAIR_EVERY", 30*time.Second),
		LookupMode:      envString("SDCC_LOOKUP_MODE", "ttl"),
		LookupTTL:       envInt("SDCC_LOOKUP_TTL", 3),
		LearnFromLookup: envBool("SDCC_LEARN_FROM_LOOKUP", true),

		RPCA: envFloat("SDCC_RPC_A", 18),
		RPCB: envFloat("SDCC_RPC_B", 3),
	}
}

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
