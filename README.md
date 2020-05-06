This provides the API for a worker in the 'ez_arch' framework.

'ez_arch' is a simple framework for defining services over ZMQ.  Workers are intended to be run as a single process / thread.  To scale horizontally just launch more workers.  Internally we open up a DEALER to the ez_arch router's input, and a DEALER on a **pre-determined** designated port for the service.
