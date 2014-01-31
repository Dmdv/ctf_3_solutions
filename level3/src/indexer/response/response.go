package response

type Response struct {
	Success bool     `json:"success"`
	Results []string `json:"results"`
}

func New(results []string) *Response {
	return &Response{
		Success: true,
		Results: results,
	}
}
