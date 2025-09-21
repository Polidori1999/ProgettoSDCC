package proto

type InvokeRequest struct {
	ReqID   string  `json:"req_id"`
	Service string  `json:"service"` // "sum" | "sub" | "mul" | "div"
	A       float64 `json:"a"`
	B       float64 `json:"b"`
}

type InvokeResponse struct {
	ReqID  string  `json:"req_id"`
	OK     bool    `json:"ok"`
	Result float64 `json:"result,omitempty"`
	Error  string  `json:"error,omitempty"`
}
