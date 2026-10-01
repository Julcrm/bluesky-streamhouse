# =============================================================================
# One-shot Lakekeeper setup (bootstrap + Iceberg warehouse on Garage), idempotent.
# A tiny image rather than a bind-mounted script: the compose works the same locally
# and on Coolify
# =============================================================================

FROM curlimages/curl:8.16.0

COPY docker/lakekeeper/init.sh /init.sh

ENTRYPOINT ["sh", "/init.sh"]
