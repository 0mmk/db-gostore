module gostore

go 1.24.0

replace lsm => ./lsm

replace cache => ./cache

require lsm v0.0.0-00010101000000-000000000000

require (
	cache v0.0.0-00010101000000-000000000000 // indirect
	github.com/ortuman/nuke v1.3.0 // indirect
)
