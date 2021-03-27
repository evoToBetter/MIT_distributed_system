# basic check 
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrsequential.go wc.so pg*.txt
more mr-out-0

#manually test
go run mrmaster.go pg-*.txt
go run mrworker.go wc.so

# test 
 sh ./test-mr.sh