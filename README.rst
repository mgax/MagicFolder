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
about touching any file during a sync because you will *lose your data*.

Issues and caveats
------------------
While MagicFolder is intended to back up data by replicating on several
hosts, and the author uses it routinely, be warned it's alpha-quality.
In particular there is no provision for detecting changes to files
during synchronization or recovering from errors.

There is no support yet for merging local and remote changes, and
resolving conflicts - synchronization is simply aborted - but this
essential feature is in the works.
