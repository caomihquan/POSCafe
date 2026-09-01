FROM mcr.microsoft.com/dotnet/sdk:10.0
WORKDIR /src
COPY . .
RUN dotnet tool install --tool-path /opt/dotnet-tools dotnet-ef --version 10.0.11
ENV PATH="/opt/dotnet-tools:${PATH}"
RUN dotnet restore PosCafe.slnx
RUN dotnet build PosCafe.slnx --no-restore -v:minimal
COPY deploy/migrate.sh /usr/local/bin/poscafe-migrate
RUN chmod +x /usr/local/bin/poscafe-migrate
USER 10001
ENTRYPOINT ["/usr/local/bin/poscafe-migrate"]
