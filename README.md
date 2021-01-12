This is an implementation of a publisher-subscriber service with a broker to cache and send the messages to the correct clients.
This project has 3 parts, a Broker, a publisher and a subscriber.

The broker uses threads to not block on sending messages so that one or more clients with a very high ping will not block the sending of messages to the other clients.

To execute this program first build and then perform installDist. the binaries will be installed into ./build/install/Trabalho_1/bin/
First, start the broker server and then start whichever clients you want.

To check the possible arguments for binaries and their purpose use -help.