MagicFolder
===========

Keep a folder in sync on several hosts using a central server. Heavily
inspired by DropBox.

Usage
-----
Initialize a server repository::

    ssh server.example.com
    mkdir repo.mf; cd repo.mf
    mf init -s

Initialize a client repository::

    mf init server.example.com:repo.mf

Synchronize::

    mf sync

How it works
------------
The server keeps a list of incremental versions (file metadata) and a
hashed blob store (file contents). Clients synchronize by uploading
local changes, the server appends them to its version history, and sends
back a list of changes made by other clients.

Synchronization happens over SSH and is invoked manually. Don't think
about touching any file during a sync because you **will** lose your
data.

Usually files will be smoothly added and removed by clients, but if
there is a conflict, MagicFolder will rename one of the versions. You
should best know to solve the conflict; when you're done, simply remove
the extra file.

Issues and caveats
------------------
While MagicFolder is intended to back up data by replicating on several
hosts, and the author uses it routinely, be warned it's alpha-quality.
In particular there is no provision for detecting changes to files
during synchronization or recovering from errors. The client-server
chatter protocol is also in flux, so be sure to use the same version of
MagicFolder on both client and server.
