package datazs

import "github.com/golang-jwt/jwt/v5"

type DataImportClaim struct {
	jwt.RegisteredClaims

	// private claims
	KnitId string `json:"knitfab/knitId"`
	RunId  string `json:"knitfab/runId"`
}
