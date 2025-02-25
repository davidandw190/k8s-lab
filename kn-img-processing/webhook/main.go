package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davidandw190/image-processing-webhook/internal/cloudevents"
	"github.com/davidandw190/image-processing-webhook/internal/config"
	"github.com/davidandw190/image-processing-webhook/internal/handlers"
	"github.com/davidandw190/image-processing-webhook/internal/middleware"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	ceClient, err := cloudevents.NewClient(cfg.SinkURL)
	if err != nil {
		log.Fatalf("Failed to create CloudEvents client: %v", err)
	}

	webhookHandler := handlers.NewWebhookHandler(ceClient, cfg)

	router := http.NewServeMux()

	router.HandleFunc("/", handlers.IndexHandler)
	router.HandleFunc("/health", handlers.HealthCheckHandler)
	router.Handle("/image", middleware.LogRequest(webhookHandler))

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("Starting server on port %s...", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
}
