module client

go 1.17

require (
	encode v0.0.0
	server v0.0.0
	balance v0.0.0
	registry v0.0.0
)
replace (
	encode => ../encode
	server => ../server
	balance => ../balance
	registry => ../registry
)