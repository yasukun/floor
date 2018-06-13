package lib

type Command struct {
	Group string `json:"group"`
	Key   string `json:"key"`
	Field string `json:"field"`
	From  string `json:"from"`
	Value string `json:"value"`
}

type Tag struct {
	Name string `json:"name"`
}

type Image struct {
	Src string `json:"src"`
}

type Og struct {
	Url         string `json:"url"`
	Type        string `json:"type"`
	Image       string `json:"image"`
	Description string `json:"description"`
	Determiner  string `json:"determiner"`
	Sitename    string `json:"sitename"`
	Video       string `json:"video"`
}

type Subject struct {
	Id          string    `json:"id"`
	Category    string    `json:"category"`
	Name        string    `json:"name"`
	Uts         int64     `json:"uts"`
	Host        string    `json:"host"`
	FingerPrint string    `json:"fingerprint"`
	Body        string    `json:"body"`
	Opengraph   Og        `json:"opengraph"`
	Redis       []Command `json:"redis"`
	Tags        []Tag     `json:"tags"`
	Images      []Image   `json:"images"`
}

type PostResponse struct {
	Type     string `json:"type"`
	Category string `json:"category"`
	Id       string `json:"id"`
}

type Offset struct {
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
	Offset    int64  `json:"offset"`
}
