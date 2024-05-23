import os
import dataclasses
import pyodbc


@dataclasses.dataclass
class DbConnection:
    """
    Class to connect to the database and execute queries
    """
    _server: str =  os.getenv("SERVER")
    _database: str = os.getenv("DATABASE")
    _user: str = os.getenv("DB_USER")
    _password: str = os.getenv("PASSWORD")
    _port: str = os.getenv("PORT")

    def connect_to_db(self) -> pyodbc.Connection | None:
        """
        Connects to the database
        :return: Connection object if successful, None otherwise
        """
        try:
            connection_string = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                                f"SERVER={self._server},{self._port};"
                                f"DATABASE={self._database};" 
                                f"UID={self._user}; "
                                f"PWD={self._password}") 
            return pyodbc.connect(
                connection_string
            )
        except pyodbc.Error as e:
            print("Please check the connection to the database")
            print(f"Error: {e}")

    def execute_query(self, query: str, is_select: bool = True, *args) -> None | pyodbc.Row:
        """
        Executes a SQL query on the database.
        :param query:  SQL query to be executed
        :param is_select: True if the query is a SELECT statement, False otherwise
        :param args: Additional arguments to be passed to the query
        :return: None for non-SELECT queries, otherwise the result of the query
        """
        try:
            with self.connect_to_db() as conn:
                with conn.cursor() as cursor:
                    if is_select:
                        cursor.execute(query)
                        query_result = cursor.fetchall()
                        cols = [column[0] for column in cursor.description]
                        return query_result, cols
                    else:
                        cursor.execute(query, *args)
                        conn.commit()
        except pyodbc.Error as e:
            raise e
