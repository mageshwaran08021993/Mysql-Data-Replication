import mysql.connector

# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="mypass",
  database="test"
)

# Create a cursor object to interact with the database
mycursor = mydb.cursor()

# Execute a SQL query
mycursor.execute("SELECT * FROM first_t")

# Fetch the results of the query
result = mycursor.fetchall()

# Display the results
for row in result:
  print(row)
