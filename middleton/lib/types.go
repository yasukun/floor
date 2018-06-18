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

type ErrResponse struct {
	Message string `json:"message"`
}

type SimpleResponse struct {
	Result string `json:"result"`
}

type IntResponse struct {
	Result int64 `json:"result"`
}

type PostResponse struct {
	Type     string `json:"type"`
	Category string `json:"category"`
	ID       string `json:"id"`
}

type PostRange struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}

type PostID struct {
	ID string `json:"id"`
}

type Offset struct {
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type Activity struct {
	Id          string    `json:"id"`
	Name        string    `json:"name"`
	Uts         int64     `json:"uts"`
	Host        string    `json:"host"`
	FingerPrint string    `json:"fingerprint"`
	Redis       []Command `json:"redis"`
}

type Comment struct {
	Subjectid   string    `json:"subjectid"`
	Id          string    `json:"id"`
	Replyid     string    `json:"replyid"`
	Name        string    `json:"name"`
	Uts         int64     `json:"uts"`
	Host        string    `json:"host"`
	FingerPrint string    `json:"fingerprint"`
	Body        string    `json:"body"`
	Redis       []Command `json:"redis"`
}

type Synonym struct {
	Name   string `json:"name"`
	Locale string `json:"locale"`
}

type Metainfo struct {
	ID       string    `json:"id"`
	Synonyms []Synonym `json:"synonym"`
}

type Rank struct {
	Score  float64     `json:"score"`
	Member interface{} `json:"member"`
}
