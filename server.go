package main

import (
	"io"
	"net/http"
	"net/rpc"

	"CSC569lab4/shared"
)

func main() {
        // create a Membership list
        leader := shared.Node{}
        nodes := shared.NewMembership()
        requests := shared.NewRequests()
		proposals := shared.NewProposal()
        // register nodes with `rpc.DefaultServer`
        rpc.Register(&leader)
        rpc.Register(nodes)
        rpc.Register(requests)
		rpc.Register(proposals)

        // register an HTTP handler for RPC communication
        rpc.HandleHTTP()

        // sample test endpoint
        http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
                io.WriteString(res, "RPC SERVER LIVE!")
        })

        // listen and serve default HTTP server
        http.ListenAndServe("localhost:9005", nil)
}
