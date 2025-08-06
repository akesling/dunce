# Dunce: let's just be dumb about it

Dumb stream capture (and replay) for the rest of us.

libpcap is amazing and you should probably use it... but if you want hermetic
tests, already have a stream of bytes (maybe a tokio TCPListener socket?), or
want to capture goldens & write tests for a higher level protocol (Framed?),
then Dunce has your ~~head~~ back covered.
