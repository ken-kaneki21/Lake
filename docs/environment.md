# Environment

Windows amd64 Work session, 12 logical CPUs reported by Python, about 25 GB
free on C: during audit. Host RAM inventory via CIM was denied. Runtime
peak CPU, RAM and storage were not sampled. Git 2.51.0.windows.1 and Python
3.10.0 are available. PyArrow 23.0.1 is installed. The psycopg install probe
found no distribution from this session's configured pip source. Java was
on PATH but its version was not confirmed. Spark, Terraform and gh were
unavailable on PATH.

Docker CLI 29.9.3 and Compose 5.7.0 are available. The desktop-linux named
pipe and default named pipe both denied access. WSL returned
WSL/E_ACCESSDENIED. The native PostgreSQL 18.4 standalone backend executed
the source test without sockets. An earlier authenticated TCP probe
started a server, but psql was denied with Windows socket error 10013.
The probe stopped the server.

PostgreSQL 16.13 is pinned for the source Compose configuration and is
unverified as a container image here. The configuration binds port 55432
to 127.0.0.1 by default and uses the postgres-data named volume. No local
Docker containers were started. No AWS or Azure provisioning occurred.
