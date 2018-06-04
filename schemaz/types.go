package main

type Command struct {
	Group string `json:"group"`
	Key   string `json:"key"`
	Field string `json:"field"`
	Value string `json:"value"`
}

type Tag struct {
	Name string `json:"name"`
}

type Image struct {
	Src string `json:"src"`
}

type Subject struct {
	Id          string    `json:"id"`
	Category    string    `json:"category"`
	Name        string    `json:"name"`
	Uts         int64     `json:"uts"`
	Host        string    `json:"host"`
	FingerPrint string    `json:"fingerprint"`
	Body        string    `json:"body"`
	Url         string    `json:"url"`
	Redis       []Command `json:"redis"`
	Tags        []Tag     `json:"tags"`
	Images      []Image   `json:"images"`
}
