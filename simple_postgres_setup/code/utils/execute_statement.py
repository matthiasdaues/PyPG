import sqlalchemy
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401


def execute_statement(engine: str, statement: str, success: str, error: str):
    """
    A small utility that accepts an engine and a sql-statement to execute,
    a success message and an error message for logging. 
    A connection using the engine is opened and the statement
    is executed. 
    A success message is returned if the statement was success-
    fully executed. An error message is returned if the execution
    met an error in the database.
    """
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            conn.execute(statement)
            transaction.commit()
            notice = success
            return notice
        except SQLAlchemyError as e:
            transaction.rollback()
            notice = error + ': ' + e
            return notice
