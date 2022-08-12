import io
import datetime
import psycopg2

class LoadData:
    def __init__(self, conn, df, table):
        self.conn = conn
        self.df = df
        self.table = table

    def copy_from_stringio(self):
        """This function copies data from a pandas dataframe to a PostgreSQL table.
        Args:
            conn (object): connection to the database.
            df (pandas.DataFrame): dataframe with the data to be copied.
            table (str): name of the table to be populated.

        Returns:
            None
        """
        buffer = io.StringIO()
        self.df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = self.conn.cursor()

        try:
            # copy_expert method allows to use COPY FROM STDIN
            cursor.copy_expert(f"""COPY {self.table} FROM STDIN WITH (FORMAT CSV)""", buffer)
            # copy_from method cannot handle order_reviews['review_comment_message'] text field with commas
            # cursor.copy_from(buffer, table, null="", sep=",", columns=df.columns)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            self.conn.rollback()
            cursor.close()
            return 1
        print(f">> {datetime.datetime.now()} | [ ETL ] | {table} table populated.")

        cursor.close()
