OtpGo
-----

OtpGo is an OTP (Online Theme Park) server written in [Go](https://go.dev/).

Its goal is to implement the original OTP messages and to maintain accuracy with the original Disney MMO clients: Toontown Online, Pirates of the Caribbean Online,
World of Cars Online, and Pixie Hollow.  It is based on nosyliam's unfinished [AstronGo project](https://github.com/nosyliam/AstronGo/), but heavily modified to support our goals.

It uses cgo and SWIG to compile and run Panda3D's DC parser and packer [from Rocket's otp-with-decompile fork](https://github.com/rocketprogrammer/panda3d/tree/otp-with-decompile).  This is done to maintain compatibility with original game clients, and allows stuff not currently compatible with Astron's parser (e.g. deprecated "uintarray" simple parameter types, used in Toontown; switch statement cases, used in Pirates Online, [see the example here](https://github.com/panda3d/panda3d/blob/master/direct/src/doc/sample.dc#L95-L115)).

This project uses [GopherLua](https://github.com/yuin/gopher-lua) to implement a Lua5.1(+ goto statement in Lua5.2) VM which can be used to extand functionaility of OtpGo by writing your own Client message handler or game-specific custom roles.

[Astron's Readme](https://github.com/Astron/Astron/blob/master/README.md#overview) provides a really good description at how the OTP server works internally.

The entire documentation and unit tests is pretty much a TODO right now, but it'll get it done eventually so please, pardon our dust.

## Projects using OtpGo ##
[Mewtwo](https://gitlab.com/sunrisemmos/Mewtwo) ([Sunrise Games'](https://sunrise.games/) Toontown servers)

[Dialga](https://github.com/WorldOfCarsRE/game-server) ([Sunrise Games'](https://sunrise.games/) World of Cars Online servers)
