# Build stage
FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY KafkaToInfluxDB/*.csproj ./
RUN dotnet restore

# Copy everything else and build
COPY KafkaToInfluxDB/ ./
RUN dotnet publish -c Release -o out

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app
COPY --from=build-env /app/out .

# Set the environment variable for running in production
ENV ASPNETCORE_ENVIRONMENT=Production

# Use a non-root user for better security
RUN adduser --disabled-password --gecos "" appuser
USER appuser

ENTRYPOINT ["dotnet", "KafkaToInfluxDB.dll"]