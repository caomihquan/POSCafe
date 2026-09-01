FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /src
COPY . .
RUN dotnet restore PosCafe.slnx
RUN dotnet publish "${PROJECT}" -c Release --no-restore -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
ARG APP_DLL
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENV APP_DLL=${APP_DLL}
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["sh", "-c", "dotnet ${APP_DLL}"]
