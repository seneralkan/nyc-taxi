FROM apache/superset:latest
# Switching to root to install the required packages
USER root

# Example: installing the MySQL driver to connect to the metadata database
RUN pip install psycopg2
RUN pip install pyhive
RUN pip install elasticsearch-dbapi
RUN pip install sqlalchemy-redshift

# Switching back to using the `superset` user
USER superset