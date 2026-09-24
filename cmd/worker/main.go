package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"stack-atlas-engine/internal/app"
	"stack-atlas-engine/internal/platform/config"
	"stack-atlas-engine/internal/platform/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker configuration: %v\n", err)
		os.Exit(2)
	}

	logger := logging.New(cfg.LogLevel, os.Stdout)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app.Run(ctx, logger)
}
