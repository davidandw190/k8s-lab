package models

type ImageRequest struct {
	ImageURL string `json:"image_url"`
}

type Response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	ID      string `json:"id,omitempty"`
}
