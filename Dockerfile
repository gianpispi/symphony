FROM elixir:1.19.5-otp-28-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ARG APP_UID=1000
ARG APP_GID=1000

RUN (getent group "$APP_GID" || groupadd --gid "$APP_GID" symphony) \
 && useradd --create-home --uid "$APP_UID" --gid "$APP_GID" symphony

WORKDIR /app
COPY --chown=${APP_UID}:${APP_GID} elixir/ ./

USER symphony
ENV HOME=/home/symphony

RUN mix local.hex --force \
 && mix local.rebar --force \
 && mix deps.get \
 && mix build

ENTRYPOINT ["./bin/symphony", "--i-understand-that-this-will-be-running-without-the-usual-guardrails"]
