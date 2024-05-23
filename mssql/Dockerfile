FROM mcr.microsoft.com/mssql/server:2019-latest

ENV ACCEPT_EULA=Y\
    SA_PASSWORD="Password!23"

EXPOSE 1433

COPY db_queries/entrypoint.sh /usr/src/app/
COPY db_queries/Mentoring_Projekt_1_create_v4.sql /usr/src/app/

USER root
RUN chmod +x /usr/src/app/entrypoint.sh
USER mssql

ENTRYPOINT [ "/usr/src/app/entrypoint.sh" ]