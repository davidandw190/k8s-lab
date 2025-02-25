package cloudevents

import (
	"context"
	"fmt"
	"log"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
)

type Client struct {
	ceClient client.Client
	sinkURL  string
}

func NewClient(sinkURL string) (*Client, error) {
	var ceClient client.Client
	var err error

	log.Printf("Creating CloudEvents client with sink: %s\n", sinkURL)
	ceClient, err = ce.NewClientHTTP(ce.WithTarget(sinkURL))

	if err != nil {
		return nil, fmt.Errorf("failed to create CloudEvents client: %w", err)
	}

	return &Client{
		ceClient: ceClient,
		sinkURL:  sinkURL,
	}, nil
}

func (c *Client) SendImageEvent(imageURL, source, eventType string) (string, error) {
	event := ce.NewEvent()
	eventID := fmt.Sprintf("%s-%d", eventType, time.Now().Unix())
	event.SetID(eventID)
	event.SetSource(source)
	event.SetType(eventType)
	event.SetTime(time.Now())
	event.SetExtension("category", "storage")

	if err := event.SetData(ce.ApplicationJSON, map[string]string{
		"image_url": imageURL,
	}); err != nil {
		return "", fmt.Errorf("failed to set event data: %w", err)
	}

	if c.sinkURL == "" {
		log.Printf("Would send event: %+v", event)
		return eventID, nil
	}

	ctx := context.Background()
	result := c.ceClient.Send(ctx, event)
	if ce.IsUndelivered(result) {
		return "", fmt.Errorf("failed to deliver event: %w", result)
	}

	log.Printf("Successfully sent event: %s", eventID)
	return eventID, nil
}
