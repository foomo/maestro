// Package transport provides typed publish/subscribe bundles for the maestro
// protocol. All message types are carried by goflux Publishers/Subscribers
// constructed over the goflux/transport/nats package — no raw nats.Conn handling
// leaks into Soloist or Player.
//
// Callers wire the bundle once at startup:
//
//	nc, _ := nats.Connect(url)
//	tr := transport.NewTransport(nc)
//	sol, _ := soloist.New(soloist.Options{Transport: tr, ...})
//
// On a NATS cluster shared with other services, scope the deployment's
// subjects with a prefix. Soloist and Player both take their subject
// layout from the Transport, so this is the only place it is set:
//
//	tr, err := transport.NewTransportWithPrefix(nc, "catalogue.maestro")
package transport
