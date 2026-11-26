package datasportsapi

// Ptr returns a pointer to v, for the optional fields of a Params struct:
//
//	&datasportsapi.TeamsFixturesParams{Type: datasportsapi.Ptr("recent")}
func Ptr[T any](v T) *T { return &v }
