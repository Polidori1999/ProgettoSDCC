package proto

const (
	ServiceSum = "sum"
	ServiceSub = "sub"
	ServiceMul = "mul"
	ServiceDiv = "div"
)

var BuiltinServices = map[string]struct{}{
	ServiceSum: {},
	ServiceSub: {},
	ServiceMul: {},
	ServiceDiv: {},
}

func IsValidService(name string) bool {
	_, ok := BuiltinServices[name]
	return ok
}
