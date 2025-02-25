package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	cloudeventsClient "github.com/davidandw190/image-processing-webhook/internal/cloudevents"
	"github.com/davidandw190/image-processing-webhook/internal/config"
	"github.com/davidandw190/image-processing-webhook/internal/models"
)

type WebhookHandler struct {
	ceClient *cloudeventsClient.Client
	config   *config.Config
}

func NewWebhookHandler(ceClient *cloudeventsClient.Client, cfg *config.Config) http.Handler {
	return &WebhookHandler{
		ceClient: ceClient,
		config:   cfg,
	}
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendJSONResponse(w, http.StatusMethodNotAllowed, models.Response{
			Success: false,
			Message: "Only POST method is allowed",
		})
		return
	}

	var req models.ImageRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields() // Be strict about the request format

	if err := decoder.Decode(&req); err != nil {
		sendJSONResponse(w, http.StatusBadRequest, models.Response{
			Success: false,
			Message: fmt.Sprintf("Invalid request format: %s", err),
		})
		return
	}

	if req.ImageURL == "" {
		sendJSONResponse(w, http.StatusBadRequest, models.Response{
			Success: false,
			Message: "Image URL is required",
		})
		return
	}

	eventID, err := h.ceClient.SendImageEvent(req.ImageURL, h.config.SourceID, h.config.EventType)
	if err != nil {
		log.Printf("Failed to send event: %v", err)
		sendJSONResponse(w, http.StatusInternalServerError, models.Response{
			Success: false,
			Message: fmt.Sprintf("Failed to process image request: %s", err),
		})
		return
	}

	sendJSONResponse(w, http.StatusOK, models.Response{
		Success: true,
		Message: "Image URL accepted for processing",
		ID:      eventID,
	})
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	fmt.Fprintf(w, "Image Webhook is running. POST to /image to process an image URL")
}

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "OK")
}

func sendJSONResponse(w http.ResponseWriter, statusCode int, resp models.Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(resp)
}
